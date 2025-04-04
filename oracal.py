import torch
from transformers import pipeline, AutoTokenizer, AutoModelForSeq2SeqLM
from flask import Flask, request, render_template

# Check if a GPU is available and use it if possible
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# Initialize Flask app
app = Flask(__name__)

def translate_query(oracle_query):
    # Prepare input for the model
    inputs = tokenizer.encode(f"translate oracle to sqlite: {oracle_query}", return_tensors="pt").to(device)
    
    # Generate translation using the model
    outputs = model.generate(inputs, max_length=512, num_beams=4, early_stopping=True)
    
    # Decode the output back into text
    sqlite_query = tokenizer.decode(outputs[0], skip_special_tokens=True)
    
    return sqlite_query

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        oracle_query = request.form['oracle_query']
        sqlite_query = translate_query(oracle_query)
        return render_template('index.html', oracle_query=oracle_query, sqlite_query=sqlite_query)
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
