from flask import Flask, request, jsonify, render_template
import os
import sys
import csv
import traceback
import findspark
from flask_cors import CORS
import nbformat

# Use findspark to locate and initialize PySpark
try:
    # Try to find Spark installation
    findspark.init()
    print(f"PySpark found at: {findspark.find()}")
except Exception as e:
    print(f"Error initializing PySpark: {e}")
    print("Please install findspark with: pip install findspark")
    sys.exit(1)

# Now we can import PySpark
from pyspark.sql import SparkSession

# Declare globals, initialized in the main function after argument checking
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes
spark = None
sc = None

# Directory where data files will be found. Given as a program argument.
DATA_PATH = None

# Serve the main page
@app.route('/', methods=["GET"])
def index():
    return render_template('index.html')

# Check the existence of a file in the data directory
@app.route("/check_file", methods=["POST"])
def check_file():
    filename = request.json
    file_path = os.path.join(DATA_PATH, filename)
    exists = os.path.isfile(file_path)
    return jsonify({"status": "success" if exists else "error", "path": file_path})

# Execute the PySpark code received from the frontend
@app.route('/execute_pyspark', methods=['POST'])
def execute_pyspark():
    try:
        # Get the code from the request
        code_data = request.json
        
        # Handle different formats of input
        if isinstance(code_data, dict) and 'code' in code_data:
            code = code_data['code']
        else:
            code = code_data
            
        if not code:
            return jsonify({"status": "error", "message": "No code provided"}), 400
        
        if "from pyspark.sql import SparkSession" in code:
            code = code.replace("from pyspark.sql import SparkSession", "")
        if "spark = SparkSession.builder.appName('PySpark Pipeline').getOrCreate()" in code:
            code = code.replace("spark = SparkSession.builder.appName('PySpark Pipeline').getOrCreate()", "")
        if "sc = spark.sparkContext" in code:
            code = code.replace("sc = spark.sparkContext", "")
        
        # Create a local namespace where the code will execute
        local_namespace = {
            'sc': sc, 
            'spark': spark,
            'parse_csv': parse_csv,
            'DATA_PATH': DATA_PATH  # Make data path available to executed code
        }
        
        # Execute the PySpark code
        exec(code, globals(), local_namespace)
        
        # Try to extract a result if one is available
        result = None
        if 'result' in local_namespace:
            try:
                result = local_namespace['result']
                # If result is a DataFrame, collect it
                if hasattr(result, 'collect'):
                    # Limit collection to prevent memory issues
                    result = result.limit(1000).collect()
                # Convert to JSON-serializable format
                result = format_result_item(result)
            except Exception as e:
                result = f"Result available but could not be collected: {str(e)}"
        
        return jsonify({
            "status": "success", 
            "message": "PySpark code executed successfully",
            "result": result
        })
    except Exception as e:
        return jsonify({
            "status": "error", 
            "message": str(e),
            "traceback": traceback.format_exc()
        })

def format_result_item(item):
    """Format complex result items for JSON serialization"""
    if isinstance(item, tuple):
        return {
            "key": format_result_item(item[0]),
            "value": format_result_item(item[1])
        }
    elif isinstance(item, dict):
        return {k: format_result_item(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [format_result_item(i) for i in item]
    elif hasattr(item, "_asdict"):  # For named tuples and Row objects
        return format_result_item(item._asdict())
    elif isinstance(item, (str, int, float, bool, type(None))):
        return item
    else:
        return str(item)

def parse_csv(path):
    """Parse a CSV file using PySpark"""
    # If path doesn't start with a slash, add the DATA_PATH
    if not path.startswith('/'):
        path = os.path.join(DATA_PATH, path)
    
    # Load the CSV with header handling
    return sc.textFile(path).mapPartitionsWithIndex(
        lambda idx, it: iter(list(it)[1:]) if idx == 0 else it
    ).map(lambda line: next(csv.reader([line])))

def check_args():
    """Check command line arguments"""
    if len(sys.argv) != 2:
        print(f"ERROR: Usage: python {os.path.basename(__file__)} <data-directory-path>", file=sys.stderr)
        sys.exit(1)
    
    data_path = os.path.abspath(sys.argv[1])
    if not os.path.isdir(data_path):
        print(f"ERROR: Directory {data_path} does not exist.", file=sys.stderr)
        sys.exit(1)
    
    return data_path

def initialize_spark():
    """Initialize Spark session with proper configuration"""
    print("Initializing Spark session...")
    
    try:
        # Create a Spark session with more memory for local processing
        session = SparkSession.builder \
            .appName("Blockly PySpark") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.ui.enabled", "false") \
            .master("local[*]") \
            .getOrCreate()
        
        # Set log level to reduce verbosity
        session.sparkContext.setLogLevel("WARN")
        
        print(f"Spark running at: {session.sparkContext.master}")
        return session
    except Exception as e:
        print(f"Error initializing Spark: {e}")
        traceback.print_exc()
        sys.exit(1)

@app.route('/save_project', methods=['POST'])
def save_project():
    """Save the project to a file"""
    try:
        project_data = request.json
        code = project_data.get('code')

        notebook = nbformat.v4.new_notebook()
        notebook.cells.append(nbformat.v4.new_code_cell(code))

        notebook_path = os.path.join(DATA_PATH, 'pyspark.ipynb')

        # Save the notebook to a file
        with open(notebook_path, 'w') as f:
            nbformat.write(notebook, f)

        # if not project_data:
            
        #     return jsonify({"status": "error", "message": "No project data provided"}), 400
        
        # # Save the project data to a file
        # with open(os.path.join(DATA_PATH, 'project.json'), 'w') as f:
        #     f.write(project_data)
        
        return jsonify({"status": "success", "file_url": notebook_path})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

if __name__ == '__main__':
    # Ensure arguments are valid
    DATA_PATH = check_args()
    print(f"Starting Spark with data directory: {DATA_PATH}")
    
    try:
        # Initialize Spark
        spark = initialize_spark()
        sc = spark.sparkContext
        
        # Run Flask app
        print("Starting Flask server...")
        app.run(debug=True, host='0.0.0.0', port=5001)
    except Exception as e:
        print(f"Error starting application: {e}")
        traceback.print_exc()
    finally:
        # Clean up Spark resources
        if sc:
            sc.stop()
        if spark:
            spark.stop()
        print("Spark session stopped.")
