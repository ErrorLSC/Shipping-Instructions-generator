import os
import shutil
from flask import Flask, request, redirect, url_for, render_template, send_from_directory, flash
import pandas as pd
from shipping_bill import pickingcsv_loading, order_num_count, waybill_request, special_note, consolidate_shipment, template_fulfillment, ShipmentDirection

app = Flask(__name__)
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
PROCESSED_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'processed')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROCESSED_FOLDER'] = PROCESSED_FOLDER
app.secret_key = 'supersecretkey'
app.config['MAX_CONTENT_LENGTH'] = 24 * 1024 * 1024

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
if not os.path.exists(PROCESSED_FOLDER):
    os.makedirs(PROCESSED_FOLDER)

def clear_folder(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    files = request.files.getlist("files")
    #print(f"Uploaded files: {files}")
    file_paths = []
    
    # Save uploaded files
    for file in files:
        if file:
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(file_path)
            file_paths.append(file_path)
    
    # Debugging information
    if not file_paths:
        flash('No files uploaded')
        return redirect(url_for('index'))
    
    header_list = ["OSONO","OSHIP","OTYPE","OCUSPO","OCUSNO","OCUSNA","OCUSA1","OCUSA2","ODATE","OSHNA1","OSHNA2","OSHZIP","OSHAD1","OSHAD2","OSHAD3","OSHATN","OTELNO","OSOLNE","OITMN","OSERN","OLOCN","OIDESC","OQTY","ODDATE","ODTIME","OTRNSP","OMEMO1","OMEMO2","OMEMO3","OMEMO4","OSHIPR","OPGC","OPLC"]
    name_block_list = ["戸髙","鳥形","峩朗"]
    address_block_list = ["高知県吾川郡仁淀川町","峩朗"]
    fixed_consolidate_dict = {"ｴﾋﾟﾛｯｸ福岡":"0925580621", "ｴﾋﾟﾛｯｸ大阪":"0727754511","ｴﾋﾟﾛｯｸ仙台": "0223473755", "DRM兵庫":"0795360461","虎乃門千葉": "0436222141"}

    shipment_direction1 = ShipmentDirection()

    try:
        pickdf = pickingcsv_loading(file_paths, header_list)
    except ValueError as e:
        print(f"Error in pickingcsv_loading: {e}")
        flash(f"Error processing files: {e}")
        return redirect(url_for('index'))
    
    if pickdf.empty:
        flash('No data found in the uploaded files')
        return redirect(url_for('index'))
    ordercount = order_num_count(pickdf)
    #print(ordercount)
    
    for picktime in ordercount:
        shipment_direction1.add_shipment(picktime, ordercount[picktime])
    
    waybill_request_dict = waybill_request(pickdf)
    for picktime in waybill_request_dict:
        shipment_direction1.update_tracking_needed_orders(picktime, waybill_request_dict[picktime])
    
    special_note_dict = special_note(pickdf)
    for picktime in special_note_dict:
        shipment_direction1.update_special_instructions(picktime, special_note_dict[picktime])

    consolidate_shipment_dict = consolidate_shipment(pickdf, name_block_list, address_block_list, fixed_consolidate_dict)
    shipment_direction1.add_consolidated_shipment_orders(consolidate_shipment_dict)
    #print(shipment_direction1.get_shipment_by_picktime('1'))
    excel_template = 'static/送り状鑑(更新版_py).xlsx'  # 修改为你的模板路径
    output_file_path = os.path.join(app.config['PROCESSED_FOLDER'], '送り状鑑.xlsx')
    #print(shipment_direction1.get_all_shipments())
    template_fulfillment(excel_template=excel_template, shipment_direction=shipment_direction1, outputpath=output_file_path)
    # Clear the uploads folder after processing
    clear_folder(app.config['UPLOAD_FOLDER'])
    
    return redirect(url_for('download_file', filename='送り状鑑.xlsx'))

@app.route('/download/<filename>')
def download_file(filename):
    file_path = os.path.join(app.config['PROCESSED_FOLDER'], filename)
    if os.path.exists(file_path):
        response = send_from_directory(app.config['PROCESSED_FOLDER'], filename, as_attachment=True)
        # Clear the processed folder after sending the file
        clear_folder(app.config['PROCESSED_FOLDER'])
        return response
    else:
        flash('File not found')
        return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)