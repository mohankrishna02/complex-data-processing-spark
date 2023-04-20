## Spark Based Complex JSON and AVRO Data Processing
This project uses Apache Spark to perform operations on JSON data and Avro data. The project reads data from a file in Avro format and data from a URL that returns JSON complex data. It then flattens the JSON complex data, selects specific columns, removes numericals from the username column, performs a left join with the Avro data, and filters the data based on the nationality column being null or not null. The code package adds a date column to the filtered data to indicate whether the customers are available or not.

## Installation 
* Clone the repository to your local machine using
```sh
https://github.com/mohankrishna02/complex-data-processing-spark.git
```
* Install Apache Spark on your machine.
* Import the project into your IDE of choice (such as IntelliJ or Eclipse).
* Build and run the project.

# Usage
* Ensure that you have the necessary dependencies installed, including Apache Spark and Scala.
* Provide the path of the Avro file to the load() function.
* Provide the URL of the JSON data to the fromURL() function.
* Run the program to flatten the JSON data, select specific columns, remove numericals from the username column, perform a left join with the Avro data, and filter the data based on the nationality column being null or not null. The code package adds a date column to the filtered data to indicate whether the customers are available or not.

# Tools/Technologies Used 
* Apache Spark: A fast and general-purpose distributed computing framework.
* Scala: A high-level programming language that runs on the Java Virtual Machine (JVM).
* Maven: A Build tool for Scala projects.
