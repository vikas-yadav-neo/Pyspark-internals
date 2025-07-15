## Pyspark
This repository documented a comprehensive demonstration of PySpark, the Python API for Apache Spark. It explained Spark’s architecture, major components, execution engine, and data manipulation strategies with both RDDs and DataFrames.

---

## Topics Covered

### Introduction to PySpark
- PySpark combined Python’s simplicity with Apache Spark’s distributed computing capabilities.
- It offered scalability across massive datasets, in-memory execution speed, and versatile APIs for both low- and high-level operations.

### Comparison: Hadoop vs Spark
| Feature          | Apache Hadoop                 | Apache Spark                      |
|------------------|-------------------------------|-----------------------------------|
| Processing Model | MapReduce                     | RDDs with DAG execution engine    |
| Speed            | Slower (disk-based)           | Faster (in-memory)                |
| Fault Tolerance  | Data replication via HDFS     | Lineage-based data recovery       |
| Ease of Use      | Complex, low-level programming| High-level APIs in Python, Scala, R |

---

## Spark Architecture
- **Driver Program**: Started execution by initializing the Spark context.
- **Cluster Manager**: Managed cluster resources via systems like YARN, Mesos, or Standalone.
- **Worker Nodes and Executors**: Hosted tasks, managed in-memory data and reported results.
- **Tasks, Stages, and Jobs**: Represented units of work created upon action execution.

---

## Transformations and Actions

### Transformations
- Included functions such as `map`, `filter`, `reduceByKey`, `groupByKey`, `join`, etc.
- Transformations were lazy and not executed until triggered by an action.

### Actions
- Examples included `collect`, `count`, `take`, and `saveAsTextFile`, which initiated execution and returned results.

### Narrow vs Wide Transformations
| Type      | Description                                          | Examples                   |
|-----------|------------------------------------------------------|----------------------------|
| Narrow    | Single output partition per input partition          | `map`, `filter`            |
| Wide      | Multiple output partitions, involves data shuffling  | `join`, `groupByKey`       |

---

## RDD and DataFrames

### RDD
- Represented immutable, distributed collections of objects.
- Supported low-level transformations and actions, fault tolerance through lineage, and manual partitioning.

### DataFrames
- Provided a structured way to handle data with built-in optimization via the Catalyst engine.
- More suitable for structured and semi-structured data formats like JSON, CSV, etc.

| Aspect        | RDD                        | DataFrame                    |
|---------------|-----------------------------|------------------------------|
| Data Format   | Structured or unstructured  | Structured or semi-structured|
| API Support   | Object-oriented             | Not object-oriented          |
| Source Flex   | Any source                  | Specific structured formats  |

---

## Execution Workflow

### DAG and Scheduler
- Directed Acyclic Graphs (DAGs) represented the logical execution plan of Spark applications.
- The DAG Scheduler built stages and passed tasks to the Task Scheduler, which assigned executors.
- Execution proceeded only after actions were triggered.

### Catalyst and Tungsten
- **Catalyst**: Analyzed queries and created optimized logical and physical plans.
- **Tungsten**: Executed plans with memory and CPU efficiency through whole-stage code generation.

---

## Storage and Caching

### Persistence Techniques
- Datasets were cached using `.persist()` or `.cache()` methods.
- Different storage levels were available, such as `MEMORY_ONLY`, `DISK_ONLY`, `OFF_HEAP`, etc.
- Persisted RDDs allowed fault tolerance and reuse across multiple actions.

---

## Partitioning and Optimization

### Partitioning
- Spark partitioned data based on HDFS block size (typically 128MB).
- `repartition()` evenly redistributed data with shuffle; `coalesce()` minimized shuffle while reducing partitions.

### Salting for Skewed Keys
- Skewed keys were managed by appending salt values to distribute load across tasks and avoid executor overload.

---

## Join Strategies

| Join Strategy            | Description                                |
|--------------------------|--------------------------------------------|
| Broadcast Hash Join      | Used small tables, avoided shuffling       |
| Shuffle Sort Merge Join  | Merged sorted partitions, suited for large datasets |
| Shuffle Hash Join        | Partitioned data into hash buckets         |

---

## Dynamic Resource Allocation
- Spark dynamically added or removed executors during job execution based on demand.
- Spark Submit flags controlled executor counts, idle timeouts, and scheduler behavior.

---

## Memory Management and Best Practices

### Driver Out of Memory
- Occurred when large datasets were collected to the driver.
- Avoided by filtering data before using `collect()` or preferring limited actions like `take()`.

### Executor Out of Memory
- Caused by data skew, misconfiguration, or excessive shuffles.
- Mitigated using salting, memory tuning, and optimized partitioning.

---

## Conclusion
The PySpark demo effectively illustrated how Apache Spark leveraged distributed architecture and in-memory computation to handle large-scale data processing. It provided practical insight into Spark’s core components, transformation strategies, and best practices for efficient application development.
