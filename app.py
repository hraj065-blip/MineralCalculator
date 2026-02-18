import os
import re
import json
import time
import pandas as pd
import pdfplumber
import google.generativeai as genai
from flask import Flask, request, send_file, render_template, flash, redirect, url_for
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'goa_dmf_secure_key'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# OPTIONAL: Add your API Key
GEMINI_API_KEY = "YOUR_GEMINI_API_KEY"

# --- 1. FULL FORM MAPPINGS ---
ORE_FULL_FORMS = {
    'L': 'Iron Ore (Lumps)',
    'F': 'Iron Ore (Fines)',
    'C': 'Concentrates',
    'M': 'Manganese',
    'B': 'Bauxite'     
}

GRADE_FULL_FORMS = {
    'A': 'Below 55% Fe',
    'B': '55% to below 58% Fe',
    'C': '58% to below 60% Fe',
    'D': '60% to below 62% Fe',
    'E': '62% to below 65% Fe',
    'F': '65% Fe and above'
}

# --- 2. DATA CLEANING & MATCHING ---

def clean_currency(value):
    if pd.isna(value) or str(value).strip().lower() in ['na', 'nil', '', 'nan']:
        return 0.0
    try:
        return float(str(value).replace(',', '').replace(' ', ''))
    except:
        return 0.0

def get_grade_code(grade_str):
    if pd.isna(grade_str): return 'Unknown'
    s = str(grade_str).strip().upper()
    if s in GRADE_FULL_FORMS: return s
    
    s_lower = s.lower()
    if 'below' in s_lower and '55' in s_lower: return 'A'
    if '65' in s_lower and 'above' in s_lower: return 'F'
    
    nums = re.findall(r'\d+', s)
    if not nums: return 'Unknown'
    val = int(nums[0])
    
    if val < 55: return 'A'
    if 55 <= val < 58: return 'B'
    if 58 <= val < 60: return 'C'
    if 60 <= val < 62: return 'D'
    if 62 <= val < 65: return 'E'
    if val >= 65: return 'F'
    return 'Unknown'

def get_ore_type(ore_str):
    if pd.isna(ore_str): return None
    s = str(ore_str).strip().lower()
    if 'bauxite' in s: return 'B'
    if 'manganese' in s: return 'M'
    if s in ['l', 'f', 'c']: return s.upper()
    if 'lump' in s: return 'L'
    if 'fine' in s: return 'F'
    if 'conc' in s: return 'C'
    return None

# --- 3. TWO-COLUMN PDF ENGINE (THE FIX) ---

def get_pdf_text_single_column(pdf_path):
    """Crops the PDF pages in half to prevent 2-column text mixing."""
    full_text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            w = page.width
            h = page.height
            
            # Extract Left Column
            left_box = (0, 0, w * 0.5, h)
            left_text = page.within_bbox(left_box).extract_text()
            if left_text: full_text += left_text + "\n"
            
            # Extract Right Column
            right_box = (w * 0.5, 0, w, h)
            right_text = page.within_bbox(right_box).extract_text()
            if right_text: full_text += right_text + "\n"
            
    return full_text

def extract_prices_regex(text):
    prices = {"L": {}, "F": {}, "C": {}}
    
    # Strictly find Goa
    match = re.search(r'^\s*Goa\s*$', text, re.MULTILINE | re.IGNORECASE)
    if not match:
        match = re.search(r'\bGoa\b', text, re.IGNORECASE)
    if not match: return prices
    
    start_idx = match.end()
    
    # Find the next state to cap the search block
    states = ['Gujarat', 'Haryana', 'Himachal', 'Jharkhand', 'Karnataka', 'Kerala', 'Madhya', 'Maharashtra', 'Odisha']
    state_pattern = r'\b(' + '|'.join(states) + r')\b'
    end_match = re.search(state_pattern, text[start_idx:], re.IGNORECASE)
    
    end_idx = start_idx + end_match.start() if end_match else start_idx + 1500
    goa_text = text[start_idx:end_idx]
    lines = goa_text.split('\n')
    
    current_ore = None
    
    for i, line in enumerate(lines):
        line_lower = line.lower()
        
        # Track context
        if 'iron ore (lumps)' in line_lower: current_ore = 'L'
        elif 'iron ore (fines)' in line_lower: current_ore = 'F'
        elif 'iron ore conc' in line_lower or 'concentrates' in line_lower: current_ore = 'C'
        elif 'bauxite' in line_lower or 'manganese' in line_lower: current_ore = None 
        
        if current_ore:
            price = None
            
            # Check for NA
            if re.search(r'\bna\b\s*$', line_lower):
                price = 0.0
            else:
                # Look for number at the end of the line
                price_match = re.search(r'(\d{2,5})\s*$', line.strip())
                if price_match:
                    price = float(price_match.group(1))
                # Look for number on the NEXT line (if column was narrow)
                elif i + 1 < len(lines):
                    next_line = lines[i+1].strip()
                    if next_line.lower() == 'na':
                        price = 0.0
                    elif next_line.replace(',', '').isdigit():
                        price = float(next_line.replace(',', ''))
                        
            # Map price to the Grade
            if price is not None:
                if 'below' in line_lower and '55' in line_lower: prices[current_ore]['A'] = price
                elif '55' in line_lower and '58' in line_lower: prices[current_ore]['B'] = price
                elif '58' in line_lower and '60' in line_lower: prices[current_ore]['C'] = price
                elif '60' in line_lower and '62' in line_lower: prices[current_ore]['D'] = price
                elif '62' in line_lower and '65' in line_lower: prices[current_ore]['E'] = price
                elif '65' in line_lower and 'above' in line_lower: prices[current_ore]['F'] = price
                
    return prices

# --- 4. EXCEL PROCESSING ---

def process_data(excel_path, pdf_path):
    # 1. Convert PDF to perfectly clean single-column text
    clean_pdf_text = get_pdf_text_single_column(pdf_path)
    
    # 2. Extract Prices
    prices = extract_prices_regex(clean_pdf_text)
    
    # 3. Parse Excel
    try: df = pd.read_excel(excel_path, header=0)
    except: df = pd.read_excel(excel_path, header=1)
    
    # 4. Process Math
    output_rows = []
    for _, row in df.iterrows():
        qty_col = next((col for col in df.columns if 'quantity' in col.lower()), 'Quantity')
        qty = clean_currency(row.get(qty_col, 0))
        
        ore_raw = row.get('Type of Ore', '')
        grade_raw = row.get('Grade ( Fe%)', '')
        
        ore_code = get_ore_type(ore_raw)
        grade_code = get_grade_code(grade_raw)
        
        ore_desc = ORE_FULL_FORMS.get(ore_code, ore_code)
        grade_desc = GRADE_FULL_FORMS.get(grade_code, grade_code)
        
        rate = 0.0
        status = "Success"
        
        if not ore_code:
            status = "Error: Invalid Ore Type"
        elif grade_code == 'Unknown':
            status = "Error: Invalid Grade"
        elif ore_code in prices and grade_code in prices[ore_code]:
            rate = prices[ore_code][grade_code]
        else:
            status = "Rate Not Found (NA in Gazette)"
            
        base_val = qty * rate
        royalty = base_val * 0.15
        dmf = royalty * 0.30
        
        data = row.to_dict()
        data.update({
            'Standardized Ore': ore_desc,
            'Standardized Grade': grade_desc,
            'IBM Rate (₹)': rate,
            'Base Value (₹)': base_val,
            'Royalty Payable (15%)': royalty,
            'DMF Payable (30%)': dmf,
            'Calculation Status': status
        })
        output_rows.append(data)
        
    # 5. Generate Professional Report
    result_df = pd.DataFrame(output_rows)
    out_path = os.path.join(app.config['UPLOAD_FOLDER'], 'Final_DMF_Report.xlsx')
    
    writer = pd.ExcelWriter(out_path, engine='xlsxwriter')
    result_df.to_excel(writer, index=False, sheet_name='Assessment Report')
    
    workbook = writer.book
    worksheet = writer.sheets['Assessment Report']
    
    header_fmt = workbook.add_format({'bold': True, 'fg_color': '#2C3E50', 'font_color': 'white', 'border': 1})
    currency_fmt = workbook.add_format({'num_format': '₹ #,##0.00', 'border': 1})
    text_fmt = workbook.add_format({'border': 1})
    warn_fmt = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C0006', 'border': 1})
    
    for col_num, value in enumerate(result_df.columns.values):
        worksheet.write(0, col_num, value, header_fmt)
        col_len = max(len(str(value)), 20)
        worksheet.set_column(col_num, col_num, col_len)

    for row_idx, row in result_df.iterrows():
        for col_idx, col_name in enumerate(result_df.columns):
            val = row[col_name]
            cell_fmt = text_fmt
            if '₹' in col_name or '%' in col_name: cell_fmt = currency_fmt
            if col_name == 'Calculation Status' and val != 'Success': cell_fmt = warn_fmt
            worksheet.write(row_idx + 1, col_idx, val, cell_fmt)

    writer.close()
    return out_path

# --- ROUTES ---
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        f1 = request.files.get('mineral_file')
        f2 = request.files.get('price_file')
        if not f1 or not f2:
            flash("Please upload both files.")
            return redirect(url_for('index'))
            
        p1 = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(f1.filename))
        p2 = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(f2.filename))
        f1.save(p1); f2.save(p2)
        
        try:
            report = process_data(p1, p2)
            time.sleep(1)
            return send_file(report, as_attachment=True)
        except Exception as e:
            flash(f"System Error: {str(e)}")
            return redirect(url_for('index'))

    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, port=5001)
