from flask import Flask, request, jsonify, render_template
from pyspark.sql import SparkSession
import csv
import os
import sys

# Declare globals, initialized in the main function after argument checking.
app = Flask(__name__)
spark = None
sc = None

# Directory where data files will be found. Given as a program argument.
DATA_PATH = None

# Serve the main page.
@app.route('/', methods=["GET"])
def index():
    return render_template('index.html')

# Check the existence of a file in the data directory.
@app.route("/check_file", methods=["POST"])
def check_file():
    filename = request.json
    file_path = os.path.join(DATA_PATH, filename)
    exists = os.path.isfile(file_path)
    return jsonify({"status": "success" if exists else "error", "path": file_path})

# Execute the pipeline with the given code.
@app.route('/execute_pyspark', methods=['POST'])
def execute_pyspark():
    try:
        code = request.json
        
        # Create a local namespace where the code will execute
        local_namespace = {
            'sc': sc, 
            'spark': spark,
            'parse_csv': parse_csv
        }
        
        # Execute the PySpark code
        exec(code, globals(), local_namespace)
        
        # Try to extract a result if one is available
        result = None
        if 'result' in local_namespace:
            try:
                result = local_namespace['result']
                if hasattr(result, 'collect'):
                    result = result.collect()
                # Convert to JSON-serializable format
                if isinstance(result, list):
                    result = [str(item) for item in result]
            except Exception as e:
                result = f"Result available but could not be collected: {str(e)}"
        
        return jsonify({
            "status": "success", 
            "message": "PySpark code executed successfully",
            "result": result
        })
    except Exception as e:
        import traceback
        return jsonify({
            "status": "error", 
            "message": str(e),
            "traceback": traceback.format_exc()
        })

def format_result_item(item):
    """Format complex result items for JSON serialization"""
    if isinstance(item, tuple):
        return {
            "key": str(item[0]),
            "value": format_result_item(item[1])
        }
    elif isinstance(item, dict):
        return {k: format_result_item(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [format_result_item(i) for i in item]
    elif isinstance(item, str):
        return item
    elif isinstance(item, (int, float)):
        return item
    else:
        return str(item)    

def parse_csv(path):
    # If path doesn't start with a slash, add the DATA_PATH
    if not path.startswith('/'):
        path = os.path.join(DATA_PATH, path)
    
    # Load the CSV with header handling
    return sc.textFile(path).mapPartitionsWithIndex(
        lambda idx, it: iter(list(it)[1:]) if idx == 0 else it
    ).map(lambda line: next(csv.reader([line])))

def check_args():
    if len(sys.argv) != 2:
        print(f"ERROR: Usage: python {os.path.basename(__file__)} <data-directory-path>", file=sys.stderr)
        sys.exit(1)
    
    if not os.path.isdir(sys.argv[1]):
        print(f"ERROR: Directory {sys.argv[1]} does not exist.", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    # Ensure arguments are valid
    check_args()
    DATA_PATH = sys.argv[1]  # Use the absolute path provided
    
    print(f"Starting Spark with data directory: {DATA_PATH}")
    
    # Initialize Spark with more memory for local processing
    spark = SparkSession.builder \
        .appName("Blockly PySpark") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    sc = spark.sparkContext
    
    # Print Spark configuration for debugging
    print(f"Spark running at: {sc.master}")
    
    # Run app
    app.run(debug=True, host='0.0.0.0', port=5000)