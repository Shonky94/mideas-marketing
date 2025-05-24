import os
import json
import tempfile
import subprocess
from pathlib import Path
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, session

app = Flask(__name__)
app.secret_key = "your_secret_key_here"  # For flash messages and session

# Create necessary directories
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_FOLDER = BASE_DIR / "uploads"
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
YOUTUBE_DOC_DIR = DATA_DIR / "youtubedoc"

# Create all required directories
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(INPUT_DIR / "youtube", exist_ok=True)
os.makedirs(INPUT_DIR / "facebook", exist_ok=True)
os.makedirs(INPUT_DIR / "instagram", exist_ok=True)
os.makedirs(YOUTUBE_DOC_DIR, exist_ok=True)

# Configure upload settings
app.config['UPLOAD_FOLDER'] = str(UPLOAD_FOLDER)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload size

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload_youtube_doc', methods=['POST'])
def upload_youtube_doc():
    if 'youtube_doc' not in request.files:
        flash('No file selected')
        return redirect(request.url)
    
    file = request.files['youtube_doc']
    if file.filename == '':
        flash('No file selected')
        return redirect(request.url)
    
    if not file.filename.endswith('.docx'):
        flash('Only .docx files are allowed')
        return redirect(request.url)
    
    # Save the document
    filepath = os.path.join(YOUTUBE_DOC_DIR, file.filename)
    file.save(filepath)
    
    # Store the filepath in session
    session['youtube_doc_path'] = filepath
    
    flash(f'Successfully uploaded: {file.filename}')
    return redirect(url_for('index'))

@app.route('/run_ytword', methods=['POST'])
def run_ytword():
    if 'youtube_doc_path' not in session:
        flash('Upload a YouTube doc file first')
        return redirect(url_for('index'))
    
    # Run the YouTube Word script
    try:
        # Create a temporary file to capture output
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.txt') as tmp:
            tmp_path = tmp.name
        
        # Modify paths in the script
        env = os.environ.copy()
        env['PYTHONPATH'] = os.pathsep.join([env.get('PYTHONPATH', ''), str(BASE_DIR)])
        
        # Run the script and capture output
        process = subprocess.Popen(
            ['python', 'ytword.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            env=env
        )
        stdout, stderr = process.communicate()
        
        # Store the output
        session['ytword_output'] = stdout
        
        if process.returncode == 0:
            flash('YouTube data extraction completed successfully')
            
            # Get the path to the generated excel file
            youtube_excel_path = os.path.join(INPUT_DIR, 'youtube', 'youtube_data.xlsx')
            if os.path.exists(youtube_excel_path):
                # Store the path and get sheet names
                session['youtube_excel_path'] = youtube_excel_path
                import pandas as pd
                excel_file = pd.ExcelFile(youtube_excel_path)
                session['youtube_sheets'] = excel_file.sheet_names
        else:
            flash(f'Error running YouTube data extraction: {stderr}')
        
        return redirect(url_for('index'))
    
    except Exception as e:
        flash(f'Error: {str(e)}')
        return redirect(url_for('index'))

@app.route('/upload_excel', methods=['POST'])
def upload_excel():
    platform = request.form.get('platform')
    if platform not in ['youtube', 'facebook', 'instagram']:
        flash('Invalid platform selected')
        return redirect(url_for('index'))
    
    if 'excel_file' not in request.files:
        flash('No file selected')
        return redirect(url_for('index'))
    
    file = request.files['excel_file']
    if file.filename == '':
        flash('No file selected')
        return redirect(url_for('index'))
    
    if not file.filename.endswith('.xlsx'):
        flash('Only .xlsx files are allowed')
        return redirect(url_for('index'))
    
    # Save the file to the appropriate directory
    save_dir = INPUT_DIR / platform
    filepath = save_dir / file.filename
    file.save(filepath)
    
    # Store the filepath in session
    session[f'{platform}_excel_path'] = str(filepath)
    
    # Get and store the sheet names
    import pandas as pd
    excel_file = pd.ExcelFile(filepath)
    session[f'{platform}_sheets'] = excel_file.sheet_names
    
    flash(f'Successfully uploaded {platform} Excel file: {file.filename}')
    return redirect(url_for('index'))

@app.route('/get_sheets/<platform>', methods=['GET'])
def get_sheets(platform):
    if f'{platform}_sheets' not in session:
        return jsonify({'sheets': []})
    
    return jsonify({'sheets': session[f'{platform}_sheets']})

@app.route('/run_sheet_mapping', methods=['POST'])
def run_sheet_mapping():
    # Get the reference platform (YouTube)
    reference_platform = 'youtube'
    
    # Check if YouTube sheet is selected
    if f'{reference_platform}_selected_sheet' not in request.form or not request.form.get(f'{reference_platform}_selected_sheet'):
        flash('YouTube sheet must be selected as reference')
        return redirect(url_for('index'))
    
    reference_sheet = request.form.get(f'{reference_platform}_selected_sheet')
    
    # Create mapping structure
    mapping = {}
    for platform in ['facebook', 'instagram']:
        if f'{platform}_selected_sheet' in request.form and request.form.get(f'{platform}_selected_sheet'):
            if platform not in mapping:
                mapping[platform] = {}
            
            # Map the selected sheet to the reference sheet name
            selected_sheet = request.form.get(f'{platform}_selected_sheet')
            mapping[platform][selected_sheet] = reference_sheet
    
    # Get the directories for each platform
    input_dirs = {
        'facebook': str(INPUT_DIR / 'facebook'),
        'instagram': str(INPUT_DIR / 'instagram'),
        'youtube': str(INPUT_DIR / 'youtube')
    }
    
    # Run the cleandata.py script directly using its functions
    try:
        # Import the cleandata module from the current directory
        import sys
        sys.path.append(str(BASE_DIR))
        from cleandata import clean_excel_files
        
        # Run the cleaning with the mapping
        results = []
        flash('Starting data cleaning process...')
        
        # Clean the files with the mapping
        clean_excel_files(input_dirs, mapping)
        
        flash('Data cleaning process completed successfully')
        return redirect(url_for('index'))
    
    except Exception as e:
        flash(f'Error during data cleaning: {str(e)}')
        return redirect(url_for('index'))

@app.route('/get_output/<script>', methods=['GET'])
def get_output(script):
    key = f'{script}_output'
    if key in session:
        return jsonify({'output': session[key]})
    return jsonify({'output': 'No output available'})

@app.route('/get_mapping_results', methods=['GET'])
def get_mapping_results():
    if 'mapping_results' in session:
        return jsonify({'results': session['mapping_results']})
    return jsonify({'results': []})

if __name__ == '__main__':
    app.run(debug=True, port=5000)