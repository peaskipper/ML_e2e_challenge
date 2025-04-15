# ML_e2e_challenge


# How to run

### Pre-requisites (including setup for local sparksession)

Python
https://www.python.org/downloads/
Remember to tick set PATH on install.

Java
https://java.com/en/download/help/win...
Set JAVA_HOME system environment variable to be C:\Program Files\Java\{jre version}

Spark
https://spark.apache.org/downloads.html
Unzip the tar file twice and place in C:\Spark
In environment system variables set a SPARK_HOME environment variable to be C:\Spark.
In environment system variables add a new Path to be %SPARK_HOME%\bin.

Hadoop
https://github.com/cdarlint/winutils
Download the winutils.exe.
Add a C:\Hadoop\bin folder.
Add winiutils to this folder.
In environment system variables set a HADOOP_HOME environment variable to be C:\Hadoop.
In environment system variables add a new Path to be %HADOOP_HOME%\bin.

Confirm Spark
Open command prompt with admin privileges
You should just be able to run "spark-shell" from anywhere as you have done environment variables above and it will just work.

Local Spark UI
http://localhost:4040/

Pyspark
https://code.visualstudio.com/

py -3.13 -m venv .pyspar_test_env
.pyspar_test_env\scripts\activate
pip install pyspark
pyspark
.pyspar_test_env\scripts\deactivate

### Resolve relative path issues in the notebook
Use os function to add relative path to current directory
`dir_path = os.getcwd()`
`root_path = os.path.abspath(os.path.join(dir_path, '..'))`


