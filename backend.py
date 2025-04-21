from flask import Flask, request, jsonify, render_template
from pyspark.sql import SparkSession

app = Flask(__name__)
spark = SparkSession.builder.appName("Blockly PySpark").getOrCreate()

@app.route('/', methods=["GET"])
def index():
    return render_template('index.html')

@app.route('/execute_pyspark', methods=['POST'])
def execute_pyspark():
    try:
        code = request.json.get("code")
        
        # Execute the PySpark code dynamically (e.g., using exec or eval)
        exec(code)
        
        return jsonify({"status": "success", "message": "PySpark code executed"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

if __name__ == '__main__':
    app.run(debug=True)