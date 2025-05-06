

// Define PySpark-related blocks with multiple RDD support
Blockly.defineBlocksWithJsonArray([
  // Block for creating a SparkContext (starting point)
  {
    type: "spark_context",
    message0: "PySpark Pipeline",
    nextStatement: "String",
    colour: 230,
    tooltip: "The starting point for a PySpark pipeline",
    helpUrl: "",
  },
  // Block for reading a CSV file with RDD name
  {
    type: "read_csv",
    message0: "Read CSV from %1 as RDD %2",
    args0: [
      {
        type: "field_input",
        name: "filepath",
        text: "flights.csv",
      },
      {
        type: "field_input",
        name: "rdd_name",
        text: "flights_rdd",
      },
    ],
    previousStatement: "String",
    nextStatement: "String",
    colour: 20,
    tooltip: "Read data from a CSV file into a named RDD",
    helpUrl: "",
  },
  // Block for map transformation with source RDD
  // {
  //   type: "map",
  //   message0: "Transform %1 with map %2 as %3",
  //   args0: [
  //     {
  //       type: "field_input",
  //       name: "source_rdd",
  //       text: "flights_rdd",
  //     },
  //     {
  //       type: "field_input",
  //       name: "func",
  //       text: "lambda r: r.split(',')",
  //     },
  //     {
  //       type: "field_input",
  //       name: "target_rdd",
  //       text: "mapped_rdd",
  //     }
  //   ],
  //   previousStatement: "String",
  //   nextStatement: "String",
  //   colour: 120,
  //   tooltip: "Apply a map transformation to a named RDD",
  //   helpUrl: "",
  // },
  // Block for filter transformation with source RDD
  {
    type: "filter",
    message0: "Filter %1 with condition %2 as %3",
    args0: [
      {
        type: "field_input",
        name: "source_rdd",
        text: "flights_rdd",
      },
      {
        type: "field_input",
        name: "condition",
        text: "lambda c: c[0] != 'header'",
      },
      {
        type: "field_input",
        name: "target_rdd",
        text: "filtered_rdd",
      }
    ],
    previousStatement: "String",
    nextStatement: "String",
    colour: 60,
    tooltip: "Filter rows based on a condition from a named RDD",
    helpUrl: "",
  },
  // Block for map with key-value pairs
  {
    type: "map_kv",
    message0: "Map %1 with function %2 as %3",
    args0: [
      {
        type: "field_input",
        name: "source_rdd",
        text: "flights_rdd",
      },
      {
        type: "field_input",
        name: "func",
        text: "lambda x: (x[0], int(x[1]))",
      },
      {
        type: "field_input",
        name: "target_rdd",
        text: "kv_rdd",
      }
    ],
    previousStatement: "String",
    nextStatement: "String",
    colour: 100,
    tooltip: "Map to key-value pairs for reduction operations",
    helpUrl: "",
  },
  // Block for groupByKey
  {
    type: "group_by_key",
    message0: "Group %1 By Key as %2",
    args0: [
      {
        type: "field_input",
        name: "source_rdd",
        text: "kv_rdd",
      },
      {
        type: "field_input",
        name: "target_rdd",
        text: "grouped_rdd",
      }
    ],
    previousStatement: "String",
    nextStatement: "String",
    colour: 160,
    tooltip: "Group data by key",
    helpUrl: "",
  },
  // Block for reduceByKey
  {
    type: "reduce_by_key",
    message0: "Reduce %1 By Key with function %2 as %3",
    args0: [
      {
        type: "field_input",
        name: "source_rdd",
        text: "kv_rdd",
      },
      {
        type: "field_input",
        name: "func",
        text: "lambda a, b: (a[0] + b[0], a[1] + b[1])",
      },
      {
        type: "field_input",
        name: "target_rdd",
        text: "reduced_rdd",
      }
    ],
    previousStatement: "String",
    nextStatement: "String",
    colour: 210,
    tooltip: "Reduce values for each key",
    helpUrl: "",
  },
  // Block for mapValues
  {
    type: "map_values",
    message0: "Map Values of %1 with function %2 as %3",
    args0: [
      {
        type: "field_input",
        name: "source_rdd",
        text: "reduced_rdd",
      },
      {
        type: "field_input",
        name: "func",
        text: "lambda x: x[0] / x[1]",
      },
      {
        type: "field_input",
        name: "target_rdd",
        text: "result_rdd",
      }
    ],
    previousStatement: "String",
    nextStatement: "String",
    colour: 250,
    tooltip: "Apply a function to the values of a key-value RDD",
    helpUrl: "",
  },
  // JOIN BLOCKS
  // Block for joining two RDDs
  {
    type: "join",
    message0: "Join %1 with %2 as %3",
    args0: [
      {
        type: "field_input",
        name: "left_rdd",
        text: "left_rdd",
      },
      {
        type: "field_input",
        name: "right_rdd",
        text: "right_rdd",
      },
      {
        type: "field_input",
        name: "target_rdd",
        text: "joined_rdd",
      }
    ],
    previousStatement: "String",
    nextStatement: "String",
    colour: 280,
    tooltip: "Join two RDDs based on key",
    helpUrl: "",
  },
  // Block for left outer join
  {
    type: "left_outer_join",
    message0: "Left Outer Join %1 with %2 as %3",
    args0: [
      {
        type: "field_input",
        name: "left_rdd",
        text: "left_rdd",
      },
      {
        type: "field_input",
        name: "right_rdd",
        text: "right_rdd",
      },
      {
        type: "field_input",
        name: "target_rdd",
        text: "left_joined_rdd",
      }
    ],
    previousStatement: "String",
    nextStatement: "String",
    colour: 320,
    tooltip: "Left outer join two RDDs based on key",
    helpUrl: "",
  },
  // Block for right outer join
  {
    type: "right_outer_join",
    message0: "Right Outer Join %1 with %2 as %3",
    args0: [
      {
        type: "field_input",
        name: "left_rdd",
        text: "left_rdd",
      },
      {
        type: "field_input",
        name: "right_rdd",
        text: "right_rdd",
      },
      {
        type: "field_input",
        name: "target_rdd",
        text: "right_joined_rdd",
      }
    ],
    previousStatement: "String",
    nextStatement: "String",
    colour: 0,
    tooltip: "Right outer join two RDDs based on key",
    helpUrl: "",
  },
  // Block for union
  {
    type: "union",
    message0: "Union %1 with %2 as %3",
    args0: [
      {
        type: "field_input",
        name: "first_rdd",
        text: "first_rdd",
      },
      {
        type: "field_input",
        name: "second_rdd",
        text: "second_rdd",
      },
      {
        type: "field_input",
        name: "target_rdd",
        text: "union_rdd",
      }
    ],
    previousStatement: "String",
    nextStatement: "String",
    colour: 30,
    tooltip: "Create a union of two RDDs",
    helpUrl: "",
  },
  // Block for intersection
  {
    type: "intersection",
    message0: "Intersection of %1 with %2 as %3",
    args0: [
      {
        type: "field_input",
        name: "first_rdd",
        text: "first_rdd",
      },
      {
        type: "field_input",
        name: "second_rdd",
        text: "second_rdd",
      },
      {
        type: "field_input",
        name: "target_rdd",
        text: "intersection_rdd",
      }
    ],
    previousStatement: "String",
    nextStatement: "String",
    colour: 300,
    tooltip: "Find the intersection of two RDDs",
    helpUrl: "",
  },
  // Block for cogroup
  {
    type: "cogroup",
    message0: "Cogroup %1 with %2 as %3",
    args0: [
      {
        type: "field_input",
        name: "left_rdd",
        text: "left_rdd",
      },
      {
        type: "field_input",
        name: "right_rdd",
        text: "right_rdd",
      },
      {
        type: "field_input",
        name: "target_rdd",
        text: "cogrouped_rdd",
      }
    ],
    previousStatement: "String",
    nextStatement: "String",
    colour: 340,
    tooltip: "Group data from both RDDs that share the same key",
    helpUrl: "",
  },
  // Block for defining a UDF (User Defined Function)
  {
    type: "haversine",
    message0: "Define haversine() function",
    previousStatement: "String",
    nextStatement: "String",
    colour: 340,
    tooltip: "Haversine function for distance calculation",
    helpUrl: "",
  },
  {
    type: 'sequence',
    message0: 'Define sequence() function',
    previousStatement: "String",
    nextStatement: "String",
    colour: 210,
    tooltip: 'A sequence of blocks to be executed in order',
    helpUrl: ''
  },
  {
    type: 'parse_time',
    message0: 'Define parse_time() function',
    previousStatement: "String",
    nextStatement: "String",
    colour: 280,
    tooltip: 'A function to parse time strings into datetime objects',
    helpUrl: ''
  },
  // Block for collecting results
  {
    type: "take",
    message0: "Take %1 from %2",
    args0: [
      {
        type: "field_input",
        name: "num",
        text: "5",
      },
      {
        type: "field_input",
        name: "source_rdd",
        text: "result_rdd",
      }
    ],
    previousStatement: "String",
    colour: 200,
    tooltip: "Take a sample of the results from the RDD",
    helpUrl: "",
  },
  {
    type: "collect",
    message0: "Collect Results from %1",
    args0: [
      {
        type: "field_input",
        name: "source_rdd",
        text: "result_rdd",
      }
    ],
    previousStatement: "String",
    colour: 310,
    tooltip: "Collect and return results to the driver program",
    helpUrl: "",
  },
]);

// Initialize Blockly workspace with a more comprehensive toolbox
var workspace = Blockly.inject("blocklyDiv", {
  toolbox: document.getElementById('toolbox'),
  grid: { spacing: 20, length: 3, colour: "#ccc", snap: true },
  trashcan: true,
  zoom: {
    controls: true,
    wheel: true,
    startScale: 1.0,
    maxScale: 3,
    minScale: 0.3,
    scaleSpeed: 1.2
  }
});

// Initialize with a starting block
function initBlockly() {
  // Clear workspace
  workspace.clear();
  
  // Add the starting block
  var startBlock = workspace.newBlock('spark_context');
  startBlock.initSvg();
  startBlock.render();
  startBlock.moveBy(50, 50);
}

// Call init when page loads
window.addEventListener('load', initBlockly);

// Generate PySpark code
function generateCode() {
  codeBlocks = [];
  imports = [];

  codeBlocks.push("# Staring PySpark Pipeline");
  imports.push("from pyspark.sql import SparkSession");
  codeBlocks.push("spark = SparkSession.builder.appName('PySpark Pipeline').getOrCreate()");
  codeBlocks.push("sc = spark.sparkContext\n");


  // let code = "# Staring PySpark Pipeline\n";
  
  // // Add SparkSession initialization code - this will be used by the backend
  // code += "# The following variables are available:\n";
  // code += "# - spark: SparkSession\n";
  // code += "# - sc: SparkContext\n\n";
  
  // Find the starting block
  let startBlock = null;
  const blocks = workspace.getTopBlocks(true);
  for (const block of blocks) {
    if (block.type === 'spark_context') {
      startBlock = block;
      break;
    }
  }
  
  if (!startBlock) {
    return "# No pipeline defined. Start with a 'PySpark Pipeline' block.";
  }
  
  // Track all RDDs that will be used in the pipeline
  let rddRegistry = new Set();
  let csvFiles = [];
  
  let currentBlock = startBlock.getNextBlock();
  
  while (currentBlock) {
    let code = "";

    switch (currentBlock.type) {
      case 'read_csv':
        const filepath = currentBlock.getFieldValue('filepath');
        const rddName = currentBlock.getFieldValue('rdd_name');

        let parse_csv_code = `def parse_csv(path):\n`;
        parse_csv_code += `    if not path.startswith('/'): path = os.path.join(DATA_PATH, path)\n`;
        parse_csv_code += `    return sc.textFile(path).mapPartitionsWithIndex(\n`;
        parse_csv_code += `        lambda idx, it: iter(list(it)[1:]) if idx == 0 else it\n`;
        parse_csv_code += `    ).map(lambda line: next(csv.reader([line])))\n`;

        if (!codeBlocks.includes(parse_csv_code)) {
          codeBlocks.push(parse_csv_code);
        }
        code = `${rddName} = parse_csv("${filepath}")`;
        rddRegistry.add(rddName);
        csvFiles.push(filepath);
        break;
        
      // case 'map':
      //   const mapSourceRdd = currentBlock.getFieldValue('source_rdd');
      //   const mapFunc = currentBlock.getFieldValue('func');
      //   const mapTargetRdd = currentBlock.getFieldValue('target_rdd');
        
      //   code = `${mapTargetRdd} = ${mapSourceRdd}.map(${mapFunc})`;
      //   rddRegistry.add(mapTargetRdd);
      //   break;
        
      case 'filter':
        const filterSourceRdd = currentBlock.getFieldValue('source_rdd');
        const filterCond = currentBlock.getFieldValue('condition');
        const filterTargetRdd = currentBlock.getFieldValue('target_rdd');
        
        code = `${filterTargetRdd} = ${filterSourceRdd}.filter(${filterCond})`;
        rddRegistry.add(filterTargetRdd);
        break;
        
      case 'map_kv':
        const kvSourceRdd = currentBlock.getFieldValue('source_rdd');
        const kvFunc = currentBlock.getFieldValue('func');
        const kvTargetRdd = currentBlock.getFieldValue('target_rdd');
        
        code = `${kvTargetRdd} = ${kvSourceRdd}.map(${kvFunc})`;
        rddRegistry.add(kvTargetRdd);
        break;
        
      case 'group_by_key':
        const groupSourceRdd = currentBlock.getFieldValue('source_rdd');
        const groupTargetRdd = currentBlock.getFieldValue('target_rdd');
        
        code = `${groupTargetRdd} = ${groupSourceRdd}.groupByKey()`;
        rddRegistry.add(groupTargetRdd);
        break;
        
      case 'reduce_by_key':
        const reduceSourceRdd = currentBlock.getFieldValue('source_rdd');
        const reduceFunc = currentBlock.getFieldValue('func');
        const reduceTargetRdd = currentBlock.getFieldValue('target_rdd');
        
        code = `${reduceTargetRdd} = ${reduceSourceRdd}.reduceByKey(${reduceFunc})`;
        rddRegistry.add(reduceTargetRdd);
        break;
        
      case 'map_values':
        const valuesSourceRdd = currentBlock.getFieldValue('source_rdd');
        const valuesFunc = currentBlock.getFieldValue('func');
        const valuesTargetRdd = currentBlock.getFieldValue('target_rdd');
        
        code = `${valuesTargetRdd} = ${valuesSourceRdd}.mapValues(${valuesFunc})`;
        rddRegistry.add(valuesTargetRdd);
        break;
        
      case 'join':
        const joinLeftRdd = currentBlock.getFieldValue('left_rdd');
        const joinRightRdd = currentBlock.getFieldValue('right_rdd');
        const joinTargetRdd = currentBlock.getFieldValue('target_rdd');
        
        code = `${joinTargetRdd} = ${joinLeftRdd}.join(${joinRightRdd})`;
        rddRegistry.add(joinTargetRdd);
        break;
        
      case 'left_outer_join':
        const leftJoinLeftRdd = currentBlock.getFieldValue('left_rdd');
        const leftJoinRightRdd = currentBlock.getFieldValue('right_rdd');
        const leftJoinTargetRdd = currentBlock.getFieldValue('target_rdd');
        
        code = `${leftJoinTargetRdd} = ${leftJoinLeftRdd}.leftOuterJoin(${leftJoinRightRdd})`;
        rddRegistry.add(leftJoinTargetRdd);
        break;
        
      case 'right_outer_join':
        const rightJoinLeftRdd = currentBlock.getFieldValue('left_rdd');
        const rightJoinRightRdd = currentBlock.getFieldValue('right_rdd');
        const rightJoinTargetRdd = currentBlock.getFieldValue('target_rdd');
        
        code = `${rightJoinTargetRdd} = ${rightJoinLeftRdd}.rightOuterJoin(${rightJoinRightRdd})`;
        rddRegistry.add(rightJoinTargetRdd);
        break;
        
      case 'union':
        const unionFirstRdd = currentBlock.getFieldValue('first_rdd');
        const unionSecondRdd = currentBlock.getFieldValue('second_rdd');
        const unionTargetRdd = currentBlock.getFieldValue('target_rdd');
        
        code = `${unionTargetRdd} = ${unionFirstRdd}.union(${unionSecondRdd})`;
        rddRegistry.add(unionTargetRdd);
        break;
        
      case 'intersection':
        const intersectionFirstRdd = currentBlock.getFieldValue('first_rdd');
        const intersectionSecondRdd = currentBlock.getFieldValue('second_rdd');
        const intersectionTargetRdd = currentBlock.getFieldValue('target_rdd');
        
        code = `${intersectionTargetRdd} = ${intersectionFirstRdd}.intersection(${intersectionSecondRdd})`;
        rddRegistry.add(intersectionTargetRdd);
        break;
        
      case 'cogroup':
        const cogroupLeftRdd = currentBlock.getFieldValue('left_rdd');
        const cogroupRightRdd = currentBlock.getFieldValue('right_rdd');
        const cogroupTargetRdd = currentBlock.getFieldValue('target_rdd');
        
        code = `${cogroupTargetRdd} = ${cogroupLeftRdd}.cogroup(${cogroupRightRdd})`;
        rddRegistry.add(cogroupTargetRdd);
        break;
      
      // case 'udf_definition':
      //   const udfName = currentBlock.getFieldValue('udf_name');
      //   const udfArgs = currentBlock.getFieldValue('udf_args');
      //   const udfBody = currentBlock.getFieldValue('udf_body');
      //   code = `def ${udfName}(${udfArgs}):\n    ${udfBody}`;
      //   break;

      case 'haversine':
        // const udfName = currentBlock.getFieldValue('udf_name');
    
        if (!imports.includes('from math import radians, cos, sin, asin, sqrt')){
          imports.push('from math import radians, cos, sin, asin, sqrt');
        }
        code = `def haversine(lat1, lon1, lat2, lon2):\n`;
        code += `    R = 3956  # radius of Earth in miles\n`;
        code += `    dlat, dlon = radians(lat2 - lat1), radians(lon2 - lon1)\n`;
        code += `    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2\n`;
        code += `    return 2 * R * asin(sqrt(a))\n\n`;
        break;

      case 'sequence':
        code = `def sequence(n):\n`
        code += `    if n == 0: return 1\n`;
        code += `    return sum(comb(n - 1, g - 1) * sequence(n - g) for g in range(1, min(5, n) + 1))\n`;
        break;

      case 'parse_time':
        if (!imports.includes('from datetime import datetime')){
          imports.push('from datetime import datetime')
        }
        code += `def parse_time(ts):\n`;
        code += `    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")\n`;
        break;

      case 'collect':
        const collectSourceRdd = currentBlock.getFieldValue('source_rdd');
        code = `result = ${collectSourceRdd}.collect()`;
        // code = `print("Results from ${collectSourceRdd}:", result)\n`;
        break;
      
      case 'take':
        const takeNum = currentBlock.getFieldValue('num');
        const takeSourceRdd = currentBlock.getFieldValue('source_rdd');
        code = `result = ${takeSourceRdd}.take(${takeNum})`;
        // code = `print("Results from ${takeSourceRdd}:", result)\n`;
        break;
        
      default:
        code += `# Unknown block type: ${currentBlock.type}`;
    }
    codeBlocks.push(code);
    currentBlock = currentBlock.getNextBlock();
  }
  
  // Store the registry and CSV files in globals for verification
  window.rddRegistry = Array.from(rddRegistry);
  window.csvFiles = csvFiles;
  
  return imports.join('\n') + '\n\n' + codeBlocks.join('\n');
}

// Event listener to update code display
workspace.addChangeListener(() => {
  const code = generateCode();
  document.getElementById("generatedCode").textContent = code;
});

// Checks and executes the PySpark code
async function executePyspark() {
  const code = document.getElementById("generatedCode").textContent;
  console.log("Generated PySpark Code:\n", code);
  
  // Check if the code contains at least one CSV read operation
  if (!window.csvFiles || window.csvFiles.length === 0) {
    showMessage("Error", "Your pipeline must include at least one CSV read operation.");
    return;
  }
  
  // Check if all CSV files exist on the backend
  try {
    let allFilesExist = true;
    let missingFiles = [];
    
    // Check each file
    for (const filename of window.csvFiles) {
      const fileCheckResponse = await fetch("http://localhost:5001/check_file", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(filename)
      });
      
      const fileCheckResult = await fileCheckResponse.json();
      
      if (fileCheckResult.status !== "success") {
        allFilesExist = false;
        missingFiles.push({name: filename, path: fileCheckResult.path});
      }
    }
    
    if (!allFilesExist) {
      let errorMsg = "The following files were not found:\n";
      missingFiles.forEach(file => {
        errorMsg += `- "${file.name}" (looked in: ${file.path})\n`;
      });
      showMessage("Files Not Found", errorMsg);
      return;
    }
    
    // Show execution status
    showMessage("Executing", "Running PySpark pipeline...");
    
    // Execute the PySpark code
    const executeResponse = await fetch("http://localhost:5001/execute_pyspark", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        code: code,
        rdds: window.rddRegistry
      })
    });
    
    const executeResult = await executeResponse.json();
    
    if (executeResult.status === "success") {
      // Show success message with results
      // let resultDisplay = "Pipeline executed successfully!";
      
      if (executeResult.result) {
        resultDisplay = JSON.stringify(executeResult.result, null, 2);
      }
      
      showMessage("Success", resultDisplay);
    } else {
      // Show error message
      showMessage("Error", `Execution failed: ${executeResult.message}\n\n${executeResult.traceback || ""}`);
    }
    
  } catch (error) {
    showMessage("Connection Error", `Failed to communicate with the server: ${error.message}`);
  }
}

// Display message to the user
function showMessage(title, message) {
  const resultDiv = document.getElementById("executionResults");
  resultDiv.innerHTML = `<h3>${title}</h3><pre>${message}</pre>`;
  resultDiv.style.display = "block";
}

// Reset the workspace to the initial state
function resetWorkspace() {
  if (confirm("Are you sure you want to clear the workspace?")) {
    initBlockly();
    document.getElementById("executionResults").style.display = "none";
  }
}

// async function saveProject() {
//   const code = document.getElementById("generatedCode").textContent;
  
//   const executeResponse = await fetch("http://localhost:5001/save_project", {
//     method: "POST",
//     headers: { "Content-Type": "application/json" },
//     body: JSON.stringify({
//       code: code
//     })
//   });

//   const executeResult = await executeResponse.json();
//   if (executeResult.status === "success") {
//     const link = document.createElement('a'); 
//     link.href = executeResult.file_url;
//     link.download = "pyspark.ipynb";
//     link.click();
//   } else {
//     alert("Error saving project");
//   }
// }

async function saveProject() {
  const code = document.getElementById("generatedCode").textContent;

  try {
    const executeResponse = await fetch("http://localhost:5001/save_project", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code: code })
    });

    const executeResult = await executeResponse.json();

    if (executeResult.status === "success") {
      const fileUrl = executeResult.file_url;

      // Check if file URL is valid
      if (fileUrl) {
        const link = document.createElement('a'); 
        link.href = fileUrl;  // Make sure the URL is correctly formed, e.g., http://localhost:5001/download/pyspark.ipynb
        link.download = "pyspark.ipynb";
        link.click();
      } else {
        alert("Error: No file URL returned");
      }
    } else {
      alert("Error saving project: " + executeResult.message);
    }
  } catch (error) {
    alert("Error saving project: " + error.message);
  }
}

