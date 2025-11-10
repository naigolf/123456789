from flask import Flask, render_template, request, send_file, url_for
import os
import pdfplumber
from PyPDF2 import PdfWriter, PdfReader
import re
import zipfile
import gc
import tempfile
import time
from threading import Thread

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # จำกัด 50MB

# ใช้ temp folder ของ system
TEMP_DIR = tempfile.gettempdir()
UPLOAD_FOLDER = os.path.join(TEMP_DIR, 'pdf_uploads')
SORTED_FOLDER = os.path.join(TEMP_DIR, 'pdf_sorted')
CONSOLIDATED_FOLDER = os.path.join(TEMP_DIR, 'pdf_consolidated')
ZIPPED_FOLDER = os.path.join(TEMP_DIR, 'pdf_zipped')

# สร้าง folders
for folder in [UPLOAD_FOLDER, SORTED_FOLDER, CONSOLIDATED_FOLDER, ZIPPED_FOLDER]:
    os.makedirs(folder, exist_ok=True)

def clear_folder(folder_path):
    """ลบไฟล์ทั้งหมดในโฟลเดอร์และบังคับ garbage collection"""
    try:
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            if os.path.isfile(item_path):
                try:
                    os.remove(item_path)
                except:
                    pass
        gc.collect()
    except Exception as e:
        print(f"Error clearing folder {folder_path}: {e}")

def sort_pdf_by_order_and_sku(input_pdf_path, output_dir):
    """
    แยก PDF ทีละหน้าเพื่อประหยัด RAM
    """
    writers = {}
    last_order_id = None
    last_sku = None
    
    def extract_order_id(text):
        match = re.search(r"Order ID[: ]+(\d+)", text)
        return match.group(1) if match else None

    def extract_barcode(text):
        match = re.search(r"\b\d{10,18}\b", text)
        return match.group(0) if match else None

    def extract_sku_from_lines(lines):
        for idx, line in enumerate(lines):
            if "Product Name" in line and "Seller SKU" in line:
                if idx + 1 < len(lines):
                    product_line = lines[idx + 1].strip()
                    parts = product_line.split()
                    if len(parts) >= 2:
                        return parts[-2]
        return None

    try:
        with pdfplumber.open(input_pdf_path) as pdf:
            total_pages = len(pdf.pages)
            
            for i in range(total_pages):
                page = pdf.pages[i]
                text = page.extract_text() or ""
                lines = text.splitlines()

                # หา Order ID
                order_id = extract_order_id(text)
                if order_id:
                    last_order_id = order_id
                else:
                    order_id = last_order_id

                # หา Barcode และ SKU
                barcode = extract_barcode(text)
                sku = None

                if barcode is None and last_sku is not None:
                    sku = last_sku
                else:
                    sku = extract_sku_from_lines(lines)

                if not sku:
                    sku = last_sku if last_sku else f"UNKNOWN_{i}"

                sku = sku.replace("/", "_").replace("\\", "_").strip()
                last_sku = sku

                if order_id and sku:
                    group_key = f"{order_id}_{sku}"
                    if group_key not in writers:
                        writers[group_key] = PdfWriter()
                    
                    # อ่านหน้าจาก PyPDF2
                    with open(input_pdf_path, 'rb') as f:
                        reader = PdfReader(f)
                        writers[group_key].add_page(reader.pages[i])
                
                # ล้าง memory ทุก 50 หน้า
                if i > 0 and i % 50 == 0:
                    gc.collect()

        # บันทึกไฟล์ที่แยกแล้ว
        sorted_count = 0
        for group_key, writer in writers.items():
            if len(writer.pages) > 0:
                output_path = os.path.join(output_dir, f"{group_key}.pdf")
                with open(output_path, "wb") as f:
                    writer.write(f)
                sorted_count += 1
        
        writers.clear()
        gc.collect()
        
        return sorted_count

    except Exception as e:
        print(f"Error in sort_pdf_by_order_and_sku: {e}")
        gc.collect()
        return 0

def consolidate_pdfs_by_sku(sorted_dir, consolidated_output_dir):
    """
    รวม PDF ทีละไฟล์เพื่อประหยัด RAM
    """
    order_id_to_primary_sku_map = {}

    # สร้าง map Order ID -> Primary SKU
    for filename in os.listdir(sorted_dir):
        if filename.endswith('.pdf'):
            parts = filename.rsplit('_', 1)
            if len(parts) == 2:
                order_id = parts[0]
                sku = parts[1].replace('.pdf', '')
                if order_id not in order_id_to_primary_sku_map:
                    order_id_to_primary_sku_map[order_id] = sku

    # จัดกลุ่มไฟล์ตาม Primary SKU
    grouped_files = {}
    for filename in os.listdir(sorted_dir):
        if filename.endswith('.pdf'):
            file_path = os.path.join(sorted_dir, filename)
            parts = filename.rsplit('_', 1)
            if len(parts) == 2:
                order_id = parts[0]
                primary_sku = order_id_to_primary_sku_map.get(order_id)
                
                if primary_sku:
                    if primary_sku not in grouped_files:
                        grouped_files[primary_sku] = []
                    grouped_files[primary_sku].append((order_id, file_path))

    # รวมไฟล์ทีละ SKU
    consolidated_count = 0
    for primary_sku, files_list in grouped_files.items():
        files_list.sort(key=lambda x: x[0])
        writer = PdfWriter()

        for order_id, file_path in files_list:
            try:
                with open(file_path, 'rb') as f:
                    reader = PdfReader(f)
                    for page_num in range(len(reader.pages)):
                        writer.add_page(reader.pages[page_num])
                        
                        # ล้าง memory ทุก 30 หน้า
                        if page_num > 0 and page_num % 30 == 0:
                            gc.collect()
                            
            except Exception as e:
                print(f"Error processing {file_path}: {e}")

        if len(writer.pages) > 0:
            output_path = os.path.join(consolidated_output_dir, f"{primary_sku}.pdf")
            with open(output_path, "wb") as f:
                writer.write(f)
            consolidated_count += 1
        
        del writer
        gc.collect()
    
    return consolidated_count

def create_zip_archive(source_dir, output_zip_path):
    """
    สร้าง ZIP แบบ streaming
    """
    try:
        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for filename in os.listdir(source_dir):
                if filename.endswith('.pdf'):
                    file_path = os.path.join(source_dir, filename)
                    zipf.write(file_path, filename)
        return output_zip_path
    except Exception as e:
        print(f"Error creating zip: {e}")
        return None

def cleanup_old_files():
    """ลบไฟล์เก่ากว่า 30 นาที"""
    current_time = time.time()
    max_age = 30 * 60  # 30 minutes
    
    for folder in [UPLOAD_FOLDER, SORTED_FOLDER, CONSOLIDATED_FOLDER, ZIPPED_FOLDER]:
        try:
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                if os.path.isfile(file_path):
                    file_age = current_time - os.path.getmtime(file_path)
                    if file_age > max_age:
                        try:
                            os.remove(file_path)
                        except:
                            pass
            gc.collect()
        except Exception as e:
            print(f"Cleanup error: {e}")

def schedule_cleanup():
    """รัน cleanup ทุก 30 นาที"""
    while True:
        time.sleep(1800)  # 30 minutes
        cleanup_old_files()

# เริ่ม cleanup thread
cleanup_thread = Thread(target=schedule_cleanup, daemon=True)
cleanup_thread.start()

# Routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    if 'pdf_files' not in request.files:
        return '''
        <h1>❌ ไม่พบไฟล์</h1>
        <p><a href="/">กลับไปหน้าแรก</a></p>
        '''

    files = request.files.getlist('pdf_files')
    
    # ล้างโฟลเดอร์เก่า
    for folder in [UPLOAD_FOLDER, SORTED_FOLDER, CONSOLIDATED_FOLDER, ZIPPED_FOLDER]:
        clear_folder(folder)

    uploaded_count = 0
    total_sorted = 0

    try:
        # ประมวลผลทีละไฟล์
        for file in files:
            if file and file.filename:
                filename = file.filename
                file_path = os.path.join(UPLOAD_FOLDER, filename)
                
                file.save(file_path)
                uploaded_count += 1

                # แยกไฟล์
                num_sorted = sort_pdf_by_order_and_sku(file_path, SORTED_FOLDER)
                total_sorted += num_sorted

                # ลบไฟล์ต้นฉบับ
                try:
                    os.remove(file_path)
                except:
                    pass
                gc.collect()

        # รวมไฟล์
        total_consolidated = consolidate_pdfs_by_sku(SORTED_FOLDER, CONSOLIDATED_FOLDER)

        # ลบไฟล์ sorted
        clear_folder(SORTED_FOLDER)

        # สร้าง ZIP
        zip_filename = f"consolidated_{os.urandom(4).hex()}.zip"
        output_zip_path = os.path.join(ZIPPED_FOLDER, zip_filename)
        zip_path = create_zip_archive(CONSOLIDATED_FOLDER, output_zip_path)

        if zip_path and uploaded_count > 0:
            download_url = url_for('download_zip', filename=zip_filename)
            
            # ลบไฟล์ consolidated
            clear_folder(CONSOLIDATED_FOLDER)
            
            return f'''
            <!DOCTYPE html>
            <html lang="th">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>สำเร็จ!</title>
                <style>
                    body {{
                        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        min-height: 100vh;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        padding: 20px;
                    }}
                    .container {{
                        background: white;
                        padding: 40px;
                        border-radius: 20px;
                        box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
                        max-width: 600px;
                        text-align: center;
                    }}
                    h1 {{ color: #28a745; margin-bottom: 20px; }}
                    .stats {{ 
                        background: #f8f9fa;
                        padding: 20px;
                        border-radius: 10px;
                        margin: 20px 0;
                    }}
                    .stats p {{ margin: 10px 0; font-size: 18px; }}
                    .download-btn {{
                        display: inline-block;
                        padding: 15px 30px;
                        background: linear-gradient(135deg, #28a745, #20c997);
                        color: white;
                        text-decoration: none;
                        border-radius: 10px;
                        font-size: 20px;
                        font-weight: 600;
                        margin: 20px 0;
                        transition: transform 0.2s;
                    }}
                    .download-btn:hover {{ transform: translateY(-2px); }}
                    .warning {{
                        color: #dc3545;
                        margin: 20px 0;
                        font-weight: 600;
                    }}
                    .back-btn {{
                        display: inline-block;
                        padding: 10px 20px;
                        background: #6c757d;
                        color: white;
                        text-decoration: none;
                        border-radius: 5px;
                        margin-top: 10px;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>✅ ประมวลผลสำเร็จ!</h1>
                    
                    <div class="stats">
                        <p>📤 อัพโหลด: <strong>{uploaded_count}</strong> ไฟล์</p>
                        <p>📊 แยกไฟล์: <strong>{total_sorted}</strong> PDFs</p>
                        <p>📦 รวมตาม SKU: <strong>{total_consolidated}</strong> PDFs</p>
                    </div>
                    
                    <a href="{download_url}" class="download-btn">
                        ⬇️ ดาวน์โหลดไฟล์ ZIP
                    </a>
                    
                    <p class="warning">
                        ⚠️ กรุณาดาวน์โหลดภายใน 10 นาที<br>
                        ไฟล์จะถูกลบอัตโนมัติเพื่อประหยัดพื้นที่
                    </p>
                    
                    <a href="/" class="back-btn">🔄 อัพโหลดไฟล์ใหม่</a>
                </div>
            </body>
            </html>
            '''
        else:
            return '''
            <h1>❌ เกิดข้อผิดพลาด</h1>
            <p>ไม่สามารถประมวลผลไฟล์ได้</p>
            <p><a href="/">ลองใหม่อีกครั้ง</a></p>
            '''
            
    except Exception as e:
        print(f"Error in upload_files: {e}")
        for folder in [UPLOAD_FOLDER, SORTED_FOLDER, CONSOLIDATED_FOLDER]:
            clear_folder(folder)
        return f'''
        <h1>❌ เกิดข้อผิดพลาด</h1>
        <p>Error: {str(e)}</p>
        <p><a href="/">ลองใหม่อีกครั้ง</a></p>
        '''

@app.route('/download/<filename>')
def download_zip(filename):
    try:
        file_path = os.path.join(ZIPPED_FOLDER, filename)
        
        if not os.path.exists(file_path):
            return '''
            <h1>❌ ไม่พบไฟล์</h1>
            <p>ไฟล์อาจถูกลบไปแล้ว หรือ link หมดอายุ</p>
            <p><a href="/">อัพโหลดไฟล์ใหม่</a></p>
            '''
        
        response = send_file(file_path, as_attachment=True)
        
        # ลบไฟล์หลัง download
        @response.call_on_close
        def cleanup():
            try:
                time.sleep(1)
                if os.path.exists(file_path):
                    os.remove(file_path)
                gc.collect()
            except:
                pass
        
        return response
    except Exception as e:
        print(f"Download error: {e}")
        return '''
        <h1>❌ ไม่สามารถดาวน์โหลดได้</h1>
        <p><a href="/">กลับไปหน้าแรก</a></p>
        '''

@app.errorhandler(413)
def too_large(e):
    return '''
    <h1>❌ ไฟล์ใหญ่เกินไป</h1>
    <p>กรุณาอัพโหลดไฟล์ที่มีขนาดไม่เกิน 50MB</p>
    <p><a href="/">ลองใหม่อีกครั้ง</a></p>
    ''', 413

if __name__ == '__main__':
    # ทำ cleanup ก่อน start
    for folder in [UPLOAD_FOLDER, SORTED_FOLDER, CONSOLIDATED_FOLDER, ZIPPED_FOLDER]:
        clear_folder(folder)
    
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
