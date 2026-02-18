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

# --- 3. THE NEW STRICT PDF PARSER ---

def extract_prices_regex(text):
    prices = {"L": {}, "F": {}, "C": {}}
    
    # Step 1: Strictly Isolate Goa Section
    match = re.search(r'\bGoa\b', text, re.IGNORECASE)
    if not match: return prices
    start_idx = match.start()
    
    # Find the precise start of the next state (Gujarat) to prevent bleed-over
    end_match = re.search(r'\bGujarat\b', text[start_idx:], re.IGNORECASE)
    if end_match:
        end_idx = start_idx + end_match.start()
    else:
        end_idx = start_idx + 1500 # Safe cutoff if Gujarat is missing
        
    goa_text = text[start_idx:end_idx]
    
    # Step 2: Flatten text to avoid PDF line-break errors
    flat_text = goa_text.replace('\n', ' ').replace('"', '').replace(',', '')
    
    # Step 3: Find boundaries for Lumps, Fines, and Concentrates
    indices = []
    l_idx = flat_text.lower().find('lump')
    f_idx = flat_text.lower().find('fine')
    c_idx = flat_text.lower().find('conc')
    
    if l_idx != -1: indices.append(('L', l_idx))
    if f_idx != -1: indices.append(('F', f_idx))
    if c_idx != -1: indices.append(('C', c_idx))
    
    indices.sort(key=lambda x: x[1]) # Sort by appearance in text
    
    # Resilient Grade Patterns
    grade_patterns = {
        'A': r'below\s*55',
        'B': r'55.*?58',
        'C': r'58.*?60',
        'D': r'60.*?62',
        'E': r'62.*?65',
        'F': r'65.*?above'
    }
    
    # Step 4: Extract prices within isolated blocks
    for i in range(len(indices)):
        ore_type, start = indices[i]
        # End of this block is the start of the next ore type, or end of Goa text
        end = indices[i+1][1] if i + 1 < len(indices) else len(flat_text)
        block = flat_text[start:end]
        
        for code, pattern in grade_patterns.items():
            m = re.search(pattern, block, re.IGNORECASE)
            if m:
                # Look exactly at the next 40 characters following the grade text
                snippet = block[m.end() : m.end()+40]
                # Pull the first number it sees, or NA
                val_match = re.search(r'(\d{2,5})|NA', snippet, re.IGNORECASE)
                
                if val_match and val_match.group(1): # It is a number, not NA
                    prices[ore_type][code] = float(val_match.group(1))
                    
    return prices

def extract_prices_with_gemini(text_content):
    """If Gemini API is used, pre-filter text so AI doesn't hallucinate other states."""
    match = re.search(r'\bGoa\b', text_content, re.IGNORECASE)
    if match:
        start_idx = match.start()
        end_match = re.search(r'\bGujarat\b', text_content[start_idx:], re.IGNORECASE)
        end_idx = start_idx + end_match.start() if end_match else start_idx + 1500
        safe_text = text_content[start_idx:end_idx]
    else:
        safe_text = text_content

    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
        prompt = f"""Extract Goa Iron Ore Average Sale Price. Return JSON ONLY. Format: {{ "L": {{ "A": 0, "B": 0... }}, "F": {{ "A": 0... }} }} Use 0 for missing/NA. Text: {safe_text}"""
        response = model.generate_content(prompt)
        return json.loads(response.text.strip().replace('```json', '').replace('```', ''))
    except: 
        return None

# --- 4. EXCEL PROCESSING ---

def process_data(excel_path, pdf_path):
    # 1. Parse PDF
    full_text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages: full_text += page.extract_text() + "\n"
    
    prices = {}
    if GEMINI_API_KEY and GEMINI_API_KEY != "YOUR_GEMINI_API_KEY":
        prices = extract_prices_with_gemini(full_text)
    
    if not prices:
        prices = extract_prices_regex(full_text)
    
    # 2. Parse Excel
    try: df = pd.read_excel(excel_path, header=0)
    except: df = pd.read_excel(excel_path, header=1)
    
    # 3. Process Math
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
        
    # 4. Generate Professional Report
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
