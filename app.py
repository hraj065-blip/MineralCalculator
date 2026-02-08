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

# --- 1. FULL FORM MAPPINGS (The Fix) ---
ORE_FULL_FORMS = {
    'L': 'Iron Ore (Lumps)',
    'F': 'Iron Ore (Fines)',
    'C': 'Concentrates',
    'M': 'Manganese', # Added for safety
    'B': 'Bauxite'      # Added for safety
}

GRADE_FULL_FORMS = {
    'A': 'Below 55% Fe',
    'B': '55% to below 58% Fe',
    'C': '58% to below 60% Fe',
    'D': '60% to below 62% Fe',
    'E': '62% to below 65% Fe',
    'F': '65% Fe and above'
}

# --- 2. PARSING ENGINES ---

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
    
    # Direct code match
    if s in GRADE_FULL_FORMS: return s
    
    # Text analysis
    s_lower = s.lower()
    if 'below' in s_lower and '55' in s_lower: return 'A'
    if '65' in s_lower and 'above' in s_lower: return 'F'
    
    # Numeric analysis
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
    
    # Check full forms first to avoid partial match errors
    if 'bauxite' in s: return 'B'
    if 'manganese' in s: return 'M'
    
    if s in ['l', 'f', 'c']: return s.upper()
    if 'lump' in s: return 'L'
    if 'fine' in s: return 'F'
    if 'conc' in s: return 'C'
    return None

def extract_prices_regex(text):
    """Robust parser for Goa section in IBM PDFs."""
    prices = {"L": {}, "F": {}, "C": {}}
    
    # Find Goa Section
    matches = list(re.finditer(r'(State|Goa)', text, re.IGNORECASE))
    start_idx = -1
    for m in matches:
        if "goa" in m.group(0).lower():
            start_idx = m.start()
            break
            
    if start_idx == -1: return prices
    
    # Limit search scope
    search_text = text[start_idx : start_idx + 4000] 
    lines = search_text.split('\n')
    current_ore = None
    
    grade_keywords = {
        'A': ['below 55', '<55'],
        'B': ['55', '58'],
        'C': ['58', '60'],
        'D': ['60', '62'],
        'E': ['62', '65'],
        'F': ['65', 'above']
    }

    for i, line in enumerate(lines):
        line_lower = line.lower()
        if "lump" in line_lower: current_ore = "L"
        elif "fine" in line_lower: current_ore = "F"
        elif "conc" in line_lower: current_ore = "C"
        
        price_match = re.search(r'(\d{1,3}(?:,\d{3})*|\d+)\s*$', line.strip())
        
        if not price_match and i+1 < len(lines):
             next_line = lines[i+1].strip()
             if next_line.replace(',', '').isdigit():
                 price_match = re.match(r'(\d{1,3}(?:,\d{3})*|\d+)', next_line)

        if current_ore and price_match:
            price = float(price_match.group(1).replace(',', ''))
            for code, keywords in grade_keywords.items():
                match = False
                if code == 'A' and ('below' in line_lower and '55' in line_lower): match = True
                elif code == 'F' and ('65' in line_lower and 'above' in line_lower): match = True
                elif code in ['B','C','D','E'] and all(k in line_lower for k in keywords): match = True
                
                if match:
                    prices[current_ore][code] = price
                    break
    return prices

def process_data(excel_path, pdf_path):
    # 1. Parse PDF
    full_text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages: full_text += page.extract_text() + "\n"
    
    prices = extract_prices_regex(full_text)
    
    # 2. Parse Excel
    try: df = pd.read_excel(excel_path, header=0)
    except: df = pd.read_excel(excel_path, header=1)
    
    # 3. Process
    output_rows = []
    
    for _, row in df.iterrows():
        # Input Data
        qty_col = next((col for col in df.columns if 'quantity' in col.lower()), 'Quantity')
        qty = clean_currency(row.get(qty_col, 0))
        
        ore_raw = row.get('Type of Ore', '')
        grade_raw = row.get('Grade ( Fe%)', '')
        
        # Standardization
        ore_code = get_ore_type(ore_raw)
        grade_code = get_grade_code(grade_raw)
        
        # MAPPING TO FULL FORM (The Update)
        ore_desc = ORE_FULL_FORMS.get(ore_code, ore_code)
        grade_desc = GRADE_FULL_FORMS.get(grade_code, grade_code)
        
        # Pricing
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
        
        # Build Row
        data = row.to_dict()
        data.update({
            'Standardized Ore': ore_desc,    # Full Name Here
            'Standardized Grade': grade_desc, # Full Name Here
            'IBM Rate (₹)': rate,
            'Base Value (₹)': base_val,
            'Royalty Payable (15%)': royalty,
            'DMF Payable (30%)': dmf,
            'Calculation Status': status
        })
        output_rows.append(data)
        
    # 4. Create Professional Excel
    result_df = pd.DataFrame(output_rows)
    out_path = os.path.join(app.config['UPLOAD_FOLDER'], 'Final_DMF_Report.xlsx')
    
    # Formatting Engine
    writer = pd.ExcelWriter(out_path, engine='xlsxwriter')
    result_df.to_excel(writer, index=False, sheet_name='Assessment Report')
    
    workbook = writer.book
    worksheet = writer.sheets['Assessment Report']
    
    # Formats
    header_fmt = workbook.add_format({'bold': True, 'fg_color': '#2C3E50', 'font_color': 'white', 'border': 1})
    currency_fmt = workbook.add_format({'num_format': '₹ #,##0.00', 'border': 1})
    text_fmt = workbook.add_format({'border': 1})
    warn_fmt = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C0006', 'border': 1})
    
    # Apply formats
    for col_num, value in enumerate(result_df.columns.values):
        worksheet.write(0, col_num, value, header_fmt)
        col_len = max(len(str(value)), 20)
        worksheet.set_column(col_num, col_num, col_len)

    # Apply data formatting
    for row_idx, row in result_df.iterrows():
        for col_idx, col_name in enumerate(result_df.columns):
            val = row[col_name]
            cell_fmt = text_fmt
            
            if '₹' in col_name or '%' in col_name:
                cell_fmt = currency_fmt
            
            if col_name == 'Calculation Status' and val != 'Success':
                cell_fmt = warn_fmt
                
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