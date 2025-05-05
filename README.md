# InteractiveSparkWorkflow

## Description

This program integrates Google Blockly with PySpark to allow users to visually design and execute Spark data processing pipelines. The frontend uses Blockly’s drag-and-drop interface to create Spark operations like data loading, transformations, and joins. Users can specify custom keys for join operations.

The backend, built with Flask, executes the generated PySpark code on a Spark cluster. The system also supports CSV file processing, enabling large dataset manipulation with PySpark’s distributed capabilities. Real-time feedback and results are provided after execution. This low-code interface simplifies big data processing for users.


## Set up environment

1. Create a conda environment:

```conda create --name myenv python=3.9```

```conda activate myenv```

2. Install required modules:

With Conda: ```conda install pyspark findspark flask flask_cors```

With pip: `pip install requirement.txt`

3. Download Spark 3.4.0 to match your PySpark version:

`wget https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz` (if you have <brew> and <wget>)

OR

`curl -O https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz`

Then:

`tar -xvzf spark-3.4.0-bin-hadoop3.tgz`

`mv spark-3.4.0-bin-hadoop3 ~/spark`

## Run the code

1. Open HTML front-end UI:

`open index.html`

2. Run the backend server:

`python backend.py ./data/`

To stop: `Command + C` or `CTRL + C`


**Note:** You can replace the dataset folder with any directory that you would like to process. Or you can add more data files (`.csv`) to `/data` folder.
