# InteractiveSparkWorkflow

## Set up environment

1. Create a conda environment:

<conda create --name myenv python=3.9>

<conda activate myenv>

2. Install required modules:

With Conda: <conda install pyspark findspark flask flask_cors>

With pip: <pip install requirement.txt>

3. Download Spark 3.4.0 to match your PySpark version:

<wget https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz> (if you have <brew> and <wget>)

OR

<curl -O https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz>

<tar -xvzf spark-3.4.0-bin-hadoop3.tgz>

<mv spark-3.4.0-bin-hadoop3 ~/spark>

## Run the code

1. Open HTML front-end UI:

<open index.html>

2. Run the backend server:

<python backend.py ./data/>

To stop: <Command + C> or <CTRL + C>

