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

# Check the existence of a file in the ./data directory.
@app.route("/check_file", methods=["POST"])
def check_file():
    if os.path.isfile(os.path.join(DATA_PATH, request.json)):
        return jsonify({"status": "success"})
    else:
        return jsonify({"status" : "error"})

# Execute the pipeline with the given code.
@app.route('/execute_pyspark', methods=['POST'])
def execute_pyspark():
    try:
        # Execute the PySpark code dynamically (e.g., using exec or eval)
        exec(request.json)
        return jsonify({"status": "success", "message": "PySpark code executed"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

def parse_csv(path):
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
    DATA_PATH = os.path.join(os.getcwd(), sys.argv[1])

    # Initialize globals after checking arguments
    spark = SparkSession.builder.appName("Blockly PySpark").getOrCreate()
    sc = spark.sparkContext

    # Run app
    app.run(debug=True)