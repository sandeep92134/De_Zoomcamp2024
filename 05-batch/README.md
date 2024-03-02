# Week 5: Batch Processing

## 5.1 Introduction

* :movie_camera: 5.1.1 [Introduction to Batch Processing](https://youtu.be/dcHe5Fl3MF8?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
* :movie_camera: 5.1.2 [Introduction to Spark](https://youtu.be/FhaqbEOuQ8U?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)


## 5.2 Installation

Follow [these intructions](setup/) to install Spark:

* [Windows](setup/windows.md)
* [Linux](setup/linux.md)
* [MacOS](setup/macos.md)

And follow [this](setup/pyspark.md) to run PySpark in Jupyter

* :movie_camera: 5.2.1 [(Optional) Installing Spark (Linux)](https://youtu.be/hqUbB9c8sKg?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)


## 5.3 Spark SQL and DataFrames

* :movie_camera: 5.3.1 [First Look at Spark/PySpark](https://youtu.be/r_Sf6fCB40c?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb) 
* :movie_camera: 5.3.2 [Spark Dataframes](https://youtu.be/ti3aC1m3rE8?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
* :movie_camera: 5.3.3 [(Optional) Preparing Yellow and Green Taxi Data](https://youtu.be/CI3P4tAtru4?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

Script to prepare the Dataset [download_data.sh](code/download_data.sh)

**Note**: The other way to infer the schema (apart from pandas) for the csv files, is to set the `inferSchema` option to `true` while reading the files in Spark.

* :movie_camera: 5.3.4 [SQL with Spark](https://www.youtube.com/watch?v=uAlp2VuZZPY&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)


## 5.4 Spark Internals

* :movie_camera: 5.4.1 [Anatomy of a Spark Cluster](https://youtu.be/68CipcZt7ZA&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
* :movie_camera: 5.4.2 [GroupBy in Spark](https://youtu.be/9qrDsY_2COo&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
* :movie_camera: 5.4.3 [Joins in Spark](https://youtu.be/lu7TrqAWuH4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

## 5.5 (Optional) Resilient Distributed Datasets

* :movie_camera: 5.5.1 [Operations on Spark RDDs](https://youtu.be/Bdu-xIrF3OM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
* :movie_camera: 5.5.2 [Spark RDD mapPartition](https://youtu.be/k3uB2K99roI&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)


## 5.6 Running Spark in the Cloud

* :movie_camera: 5.6.1 [Connecting to Google Cloud Storage ](https://youtu.be/Yyz293hBVcQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
* :movie_camera: 5.6.2 [Creating a Local Spark Cluster](https://youtu.be/HXBwSlXo5IA&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
* :movie_camera: 5.6.3 [Setting up a Dataproc Cluster](https://youtu.be/osAiAYahvh8&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
* :movie_camera: 5.6.4 [Connecting Spark to Big Query](https://youtu.be/HIm2BOj8C0Q&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)


# Homework

* [2024 Homework](../cohorts/2024)
## Homework_solution 

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the FHV 2019-10 data found here. [FHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz)

### Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?
3.5.0

### Question 2: 

**FHV October 2019**

Read the October 2019 FHV into a Spark Dataframe with a schema as we did in the lessons.</br> 
Repartition the Dataframe to 6 partitions and save it to parquet.</br>
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.</br>

- 1MB
- -> 6MB (6.4MB)
- 25MB
- 87MB



### Question 3: 

**Count records** 

How many taxi trips were there on the 15th of October?</br></br>
Consider only trips that started on the 15th of October.</br>

- 108,164
- 12,856
- 452,470
- -> 62,610

> [!IMPORTANT]
> Be aware of columns order when defining schema

### Question 4: 

**Longest trip for each day** 

What is the length of the longest trip in the dataset in hours?</br>

- -> 631,152.50 Hours
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours



### Question 5: 

**User Interface**

Spark’s User Interface which shows the application's dashboard runs on which local port?</br>

- 80
- 443
- -> 4040
- 8080



### Question 6: 

**Least frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)</br>

Using the zone lookup data and the FHV October 2019 data, what is the name of the LEAST frequent pickup location Zone?</br>

- East Chelsea
- -> Jamaica Bay
- Union Sq
- Crown Heights North


## Submitting the solutions

- Form for submitting: TBA
- Deadline: See the course app


# Community notes

Did you take notes? You can share them here.

* [Notes by Alvaro Navas](https://github.com/ziritrion/dataeng-zoomcamp/blob/main/notes/5_batch_processing.md)
* [Sandy's DE Learning Blog](https://learningdataengineering540969211.wordpress.com/2022/02/24/week-5-de-zoomcamp-5-2-1-installing-spark-on-linux/)
* [Notes by Alain Boisvert](https://github.com/boisalai/de-zoomcamp-2023/blob/main/week5.md)
* [Alternative : Using docker-compose to launch spark by rafik](https://gist.github.com/rafik-rahoui/f98df941c4ccced9c46e9ccbdef63a03) 
* [Marcos Torregrosa's blog (spanish)](https://www.n4gash.com/2023/data-engineering-zoomcamp-semana-5-batch-spark)
* [Notes by Victor Padilha](https://github.com/padilha/de-zoomcamp/tree/master/week5)
* Add your notes here (above this line)
