---
published: false
---
## Writing Custom PySpark UDFs

Apache Spark has become an invaluable tool for processing large amounts of data. The Spark-SQL extension brings the abstraction of a DataFrame to the forefront, allowing for programmatic access to table like objects. This tool facilitates an interesting development workflow for data cleaning and transformation jobs.

Some of the scripts that I work on follow the following pattern:

* Playing with the data to determine the proper transformations
* Writing ETL scripts using Spark as the data processor
* Writing tests that exercise large portions of the transformation
* Deployment

Before 2.2.0, this process required a fair bit of automation to be succesful in a python environment. While the Scala library has the largely integrated toolsets of sbt, scala-test, and spark-testing-base for writing self contained tests, python did not have too many integrated tools. One thing that held this back was the dependencies on the standalone spark environment, which requires installing spark for your particular system.

Spark is now installable via `pip install pyspark`. Before pyspark was installable via pip, the development and testing portion required having a standalone cluster locally in order to run properly. This would need to expose SPARK_HOME. However, the binaries necessary for standalone mode are now packaged directly as long as you have a local jvm instance.

### What is a Spark Package?

The spark sql library is fairly extensive, but doesn't cover every usecase. For those custom use-cases there is an interface to extend the functional of the vanilla functions. For example, you may want to implement your own custom aggregation or data cleaning operation that can be called within a select or group by clause. Fortunately, the spark developers have made it easy to wrap native scala or java code as user defined functions that can be called from within the dataframe environment.

```
TODO:
def writeSomeExamplesHere
	pass
```

At Mozilla, we use hyperloglog, a probabilistic data-structure, to keep track of approximate distinct client counts over a large set of dimensions and time windows.  It provides an interface that fits well into the merge-reduce paradigm, but isn't included by default in the spark library (see algebird's implementation). In addition, our toolchain extends beyond just spark -- we also share the same hyperloglog library in Presto, which enables adhoc querying of the datasets from a comfortable environment.

Developing scala and python packages for spark should be simple and straightforward. This is the process for building a package that can be installed either via maven or pypi. Here we will look into adding simple user defined functions into the DataFrames API.


### Writing a Python Wrapper

The Spark-Package sbt tool provides a mechanism for including python bindings with your project. It will recursively traverse the `python` directory of your project and recursively include any python files as dependencies in the jar. This is fine if you are using the library directly in an interactive environment such as Jupyter or Zeppelin, but does not fare well with local testing of larger scripts.

Consider the following project structure:

```
repo
repo/src/
repo/test/
```

It is impossible to test the transformations that rely on these custom functions without installing the package into the standalone spark instance. In this case, in order to maintain compatibility across toolsets, we would like to create a separate package that tracks this particular hyperloglog implementation.

Roughly this process follows this shape:

* Develop a spark package via sbt plugin
* Write a python wrapper using py4j
* generate the setup script to include the jar
* the jar includes the python data

This will create a package that is both pip installable and available from spark clusters.


* https://github.com/databricks/sbt-spark-package
* https://github.com/curtishoward/sparkudfexamples
* https://github.com/apache/spark/tree/master/python