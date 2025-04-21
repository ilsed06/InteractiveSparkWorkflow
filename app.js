// Define PySpark-related blocks
Blockly.defineBlocksWithJsonArray([
    // Existing block for creating a DataFrame
    {
        "type": "create_dataframe",
        "message0": "Create RDD from %1",
        "args0": [
            {
                "type": "field_input",
                "name": "data",
                "text": "data"
            }
        ],
        "output": "String",
        "colour": 0,
        "tooltip": "Creates a DataFrame in PySpark",
        "helpUrl": ""
    },
    // Existing block for selecting columns from a DataFrame
    {
        "type": "select_columns",
        "message0": "Select columns %1 from DataFrame %2",
        "args0": [
            {
                // This input is a field input for entering column names
                "type": "field_input",
                "name": "columns",
                "text": "column1,column2"
            },
            {
                // This input is a reference to another block
                "type": "input_value",
                "name": "df"
            }
        ],
        "output": "String",
        "colour": 60,
        "tooltip": "Select columns from a DataFrame",
        "helpUrl": ""
    },
    // New block for map transformation
    {
        "type": "map",
        "message0": "Map on RDD %1 with function %2",
        "args0": [
            {
                "type": "input_value",
                "name": "df"
            },
            {
                "type": "field_input",
                "name": "func",
                "text": "lambda x: x"
            }
        ],
        "output": "String",
        "colour": 120,
        "tooltip": "Apply a map transformation on DataFrame",
        "helpUrl": ""
    },
    // New block for filter transformation
    {
        "type": "filter",
        "message0": "Filter RDD %1 with condition %2",
        "args0": [
            {
                "type": "input_value",
                "name": "df"
            },
            {
                "type": "field_input",
                "name": "condition",
                "text": "x > 10"
            }
        ],
        "output": "String",
        "colour": 210,
        "tooltip": "Filter rows from a DataFrame based on a condition",
        "helpUrl": ""
    },
    // New block for reduceByKey transformation
    {
        "type": "reduceByKey",
        "message0": "Reduce by key on RDD %1 with function %2",
        "args0": [
            {
                "type": "input_value",
                "name": "df"
            },
            {
                "type": "field_input",
                "name": "func",
                "text": "lambda x, y: x + y"
            }
        ],
        "output": "String",
        "colour": 160,
        "tooltip": "Reduce DataFrame by key",
        "helpUrl": ""
    },
    // New block for groupBy transformation
    {
        "type": "groupBy",
        "message0": "Group RDD %1 by key %2",
        "args0": [
            {
                "type": "input_value",
                "name": "df"
            },
            {
                "type": "field_input",
                "name": "key",
                "text": "column_name"
            }
        ],
        "output": "String",
        "colour": 50,
        "tooltip": "Group DataFrame by a specific key",
        "helpUrl": ""
    },

    // New block for UDF
    {
        "type": "udf",
        "message0": "UDF %1",
        "args0": [
            {
                "type": "field_input",
                "name": "func",
                "text": "lambda x: x"
            }
        ],
        "output": "null",
        "colour": 270,
        "tooltip": "Apply a user-defined function to the DataFrame",
        "helpUrl": ""
    },
    //Block for reading in the data
    {
        "type": "read_csv",
        "message0": "Read CSV %1",
        "args0": [
            {
                "type": "field_input",
                "name": "func",
                "text": "/data/flights.csv"
            }
        ],
        "output": "DataFrame",
        "colour": 270,
        "tooltip": "Apply a user-defined function to the DataFrame",
        "helpUrl": ""
    }
]);

// Initialize Blockly workspace
var workspace = Blockly.inject('blocklyDiv', {
    toolbox: '<xml>' +
                '<block type="create_dataframe"></block>' +
                '<block type="select_columns"></block>' +
                '<block type="map"></block>' +
                '<block type="filter"></block>' +
                '<block type="reduceByKey"></block>' +
                '<block type="groupBy"></block>' +
                '<block type="udf"></block>' +
                '<block type="read_csv"></block>' +
             '</xml>',
    grid: {spacing: 20, length: 3, colour: '#ccc', snap: true}
});

// Function to generate PySpark code from blocks
function generateCode() {
    var code = Blockly.Python.workspaceToCode(workspace);
    console.log(code);
    document.getElementById('generatedCode').textContent = code;
}
