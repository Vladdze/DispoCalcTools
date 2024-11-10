from flask import Flask, request, render_template, send_file
import pandas as pd
import io

app = Flask(__name__)

# PREPROCESSING FUNCTION
def preprocess_phone_number(phone_number):
    """Strip '+1', keep only the last 10 digits, and ensure numeric."""
    phone_number = ''.join(filter(str.isdigit, str(phone_number)))
    return phone_number[-10:] if len(phone_number) >= 10 else None

# PROCESSING FUNCTION
def process_data(rt_data, sales_data):
    # Apply preprocessing
    rt_data['ProcessedCallerNumber'] = rt_data['Caller'].apply(preprocess_phone_number)
    sales_data['ProcessedCallerNumber'] = sales_data['Number Dialed'].apply(preprocess_phone_number)

    # Remove duplicates from Retreaver Report
    rt_data_unique = rt_data.drop_duplicates(subset=['ProcessedCallerNumber'])

    # Merge data
    merged_data = pd.merge(
        sales_data[['ProcessedCallerNumber']],
        rt_data_unique[['ProcessedCallerNumber', 'CallUUID', 'RecordingURL', 'PubID', 'PublisherName']],
        on='ProcessedCallerNumber',
        how='left'
    )
    return merged_data

@app.route('/')
def upload_files():
    return render_template('upload.html')

@app.route('/process', methods=['POST'])
def process_files():
    # Get uploaded files
    rt_file = request.files.get('retreaver_report')
    sales_file = request.files.get('sales_report')

    if not rt_file or not sales_file:
        return "Please upload both files!", 400

    # Read files into DataFrames
    rt_data = pd.read_csv(rt_file)
    sales_data = pd.read_csv(sales_file)

    # Process data
    merged_data = process_data(rt_data, sales_data)

    # Save the result to an in-memory buffer
    output = io.BytesIO()
    merged_data.to_csv(output, index=False)
    output.seek(0)

    # Render the output in the browser
    return render_template('results.html', table=merged_data.head().to_html(classes='table table-striped'), csv_file=output)

@app.route('/download', methods=['POST'])
def download_file():
    # Retrieve the in-memory file from the POST request
    csv_file = request.form.get('csv_file')

    # Create a new in-memory buffer
    output = io.BytesIO()
    output.write(csv_file.encode())
    output.seek(0)

    # Send the file as a download
    return send_file(output, mimetype='text/csv', as_attachment=True, download_name='Processed_Output.csv')

if __name__ == '__main__':
    app.run(debug=True)