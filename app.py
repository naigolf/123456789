from flask import Flask, render_template, request, send_file, url_for
import os
import pdfplumber
from PyPDF2 import PdfWriter, PdfReader
import re
import zipfile
import gc
import tempfile

app = Flask(__name__)

# ใช้ temp folder ของ system แทนการสร้างเอง
TEMP_DIR = tempfile.gettempdir()
UPLOAD_FOLDER = os.path.join(TEMP_DIR, 'pdf_uploads')
SORTED_FOLDER = os.path.join(TEMP_DIR, 'pdf_sorted')
CONSOLIDATED_FOLDER = os.path.join(TEMP_DIR, 'pdf_consolidated')
ZIPPED_FOLDER = os.path.join(TEMP_DIR, 'pdf_zipped')

# สร้าง folders
for folder in [UPLOAD_FOLDER, SORTED_FOLDER, CONSOLIDATED_FOLDER, ZIPPED_FOLDER]:
    os.makedirs(folder, exist_ok=True)

# ฟังก์ชันล้างไฟล์และ memory
def clear_folder(folder_path):
    """ลบไฟล์ทั้งหมดในโฟลเดอร์และบังคับ garbage collection"""
    try:
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)
            if os.path.isfile(item_path):
                os.remove(item_path)
        gc.collect()  # บังคับให้ Python คืน memory
    except Exception as e:
        print(f"Error clearing folder {folder_path}: {e}")

# ฟังก์ชันแยก PDF แบบประหยัด memory
def sort_pdf_by_order_and_sku(input_pdf_path, output_dir):
    """
    ประมวลผล PDF ทีละหน้า เพื่อลดการใช้ RAM
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
        """แยก SKU จากบรรทัดข้อความ"""
        for idx, line in enumerate(lines):
            if "Product Name" in line and "Seller SKU" in line:
                if idx + 1 < len(lines):
                    product_line = lines[idx + 1].strip()
                    parts = product_line.split()
                    if len(parts) >= 2:
                        return parts[-2]
        return None

    try:
        # ใช้ pdfplumber เพื่ออ่านข้อความ
        with pdfplumber.open(input_pdf_path) as pdf:
            total_pages = len(pdf.pages)
            
            # ประมวลผลทีละหน้า
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
                    
                    # อ่านหน้าจาก PyPDF2 Reader
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
        
        # ล้าง writers และ memory
        writers.clear()
        gc.collect()
        
        return sorted_count

    except Exception as e:
        print(f"Error in sort_pdf_by_order_and_sku: {e}")
        gc.collect()
        return 0

# ฟังก์ชันรวม PDF แบบประหยัด memory
def consolidate_pdfs_by_sku(sorted_dir, consolidated_output_dir):
    """
    รวม PDF ทีละไฟล์ เพื่อลดการใช้ RAM
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

        # เพิ่มหน้าทีละไฟล์
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

        # บันทึกไฟล์ที่รวมแล้ว
        if len(writer.pages) > 0:
            output_path = os.path.join(consolidated_output_dir, f"{primary_sku}.pdf")
            with open(output_path, "wb") as f:
                writer.write(f)
            consolidated_count += 1
        
        # ล้าง writer และ memory หลังแต่ละ SKU
        del writer
        gc.collect()
    
    return consolidated_count

# ฟังก์ชันสร้าง ZIP แบบ streaming
def create_zip_archive(source_dir, output_zip_path):
    """
    สร้าง ZIP โดยไม่โหลดไฟล์ทั้งหมดเข้า memory
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

# Routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    if 'pdf_files' not in request.files:
        return '<h1>No files selected</h1><p><a href="/">Try again</a></p>'

    files = request.files.getlist('pdf_files')
    
    # ล้างโฟลเดอร์เก่าทั้งหมด
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
                
                # บันทึกไฟล์
                file.save(file_path)
                uploaded_count += 1

                # แยกไฟล์ทันที
                num_sorted = sort_pdf_by_order_and_sku(file_path, SORTED_FOLDER)
                total_sorted += num_sorted

                # ลบไฟล์ต้นฉบับทันทีเพื่อคืน memory
                os.remove(file_path)
                gc.collect()

        # รวมไฟล์
        total_consolidated = consolidate_pdfs_by_sku(SORTED_FOLDER, CONSOLIDATED_FOLDER)

        # ลบไฟล์ sorted เพื่อคืน disk space
        clear_folder(SORTED_FOLDER)

        # สร้าง ZIP
        zip_filename = f"consolidated_{os.urandom(4).hex()}.zip"
        output_zip_path = os.path.join(ZIPPED_FOLDER, zip_filename)
        zip_path = create_zip_archive(CONSOLIDATED_FOLDER, output_zip_path)

        if zip_path and uploaded_count > 0:
            download_url = url_for('download_zip', filename=zip_filename)
            
            # ลบไฟล์ consolidated เพื่อคืน disk space (เก็บแค่ ZIP)
            clear_folder(CONSOLIDATED_FOLDER)
            
            return f'''
            <h1>✅ Successfully uploaded {uploaded_count} files!</h1>
            <p>📊 Sorted: {total_sorted} PDFs</p>
            <p>📦 Consolidated: {total_consolidated} PDFs by SKU</p>
            <p><a href="{download_url}" style="font-size: 20px; padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px;">⬇️ Download ZIP</a></p>
            <p style="color: red; margin-top: 20px;">⚠️ กรุณาดาวน์โหลดภายใน 10 นาที ไฟล์จะถูกลบอัตโนมัติ</p>
            <p><a href="/">Upload more files</a></p>
            '''
        else:
            return '<h1>Error processing files</h1><p><a href="/">Try again</a></p>'
            
    except Exception as e:
        print(f"Error in upload_files: {e}")
        # ล้างทุกอย่างในกรณีเกิดข้อผิดพลาด
        for folder in [UPLOAD_FOLDER, SORTED_FOLDER, CONSOLIDATED_FOLDER]:
            clear_folder(folder)
        return f'<h1>Error: {str(e)}</h1><p><a href="/">Try again</a></p>'

@app.route('/download/<filename>')
def download_zip(filename):
    try:
        file_path = os.path.join(ZIPPED_FOLDER, filename)
        
        # ส่งไฟล์และลบทันทีหลัง download
        response = send_file(file_path, as_attachment=True)
        
        # ลบไฟล์หลังส่งเสร็จ (ใน background)
        @response.call_on_close
        def cleanup():
            try:
                os.remove(file_path)
                gc.collect()
            except:
                pass
        
        return response
    except Exception as e:
        return f'<h1>File not found</h1><p>กรุณาอัพโหลดไฟล์ใหม่</p><p><a href="/">Go back</a></p>'

# Cleanup task - ลบไฟล์เก่าทุก 30 นาที
def cleanup_old_files():
    """ลบไฟล์ที่เก่ากว่า 30 นาที"""
    import time
    current_time = time.time()
    max_age = 30 * 60  # 30 minutes
    
    for folder in [UPLOAD_FOLDER, SORTED_FOLDER, CONSOLIDATED_FOLDER, ZIPPED_FOLDER]:
        try:
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                if os.path.isfile(file_path):
                    file_age = current_time - os.path.getmtime(file_path)
                    if file_age > max_age:
                        os.remove(file_path)
            gc.collect()
        except Exception as e:
            print(f"Cleanup error: {e}")

if __name__ == '__main__':
    # ทำ cleanup ก่อน start
    for folder in [UPLOAD_FOLDER, SORTED_FOLDER, CONSOLIDATED_FOLDER, ZIPPED_FOLDER]:
        clear_folder(folder)
    
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
