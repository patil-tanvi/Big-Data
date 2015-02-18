READ ME

The following commands will help to execute the MapReduce jar file in the Hortonworks Sandbox Virtual Machine.
Hadoop version - 2.6.0

____________________________________________________________________________________________________________________________



1. Start the Sandbox Hadoop environment.

2. Login to the hadoop framework from local machine:
ssh <username>@<IPAddress> -p <port number>

ssh hue@127.0.0.1 -p 2222
Password : hadoop

3. Transfer the jar and data files to the virtual box OS from the local machine (through terminal):
scp -P <port no> <filename> <username>@<IPAddress>:

scp -P 2222 movies.dat hue@127.0.0.1:
scp -P 2222 users.dat hue@127.0.0.1:
scp -P 2222 MapReduce.jar hue@127.0.0.1:

4. Transfer the data files from virtual box OS to the HDFS.
hdfs pdfs -put <filename> <folder name>

hdfs dfs -put movies.dat input
hdfs dfs -put movies.dat input2

5. Run the desired classes of the MapReduce.jar on hadoop framework
hadoop jar <jarFile> <class name> <input folder> <output folder>

hadoop jar MapReduce.jar com.mapreduce.MapReduceIMDBFilter input output1
hadoop jar MapReduce.jar com.mapreduce.MapReduceIMDBCount input output2
hadoop jar MapReduce.jar com.mapreduce.MapReduceIMDBGenreFilter input2 output3 Fantasy

6. View the output :
hfs pdfs -cat <output folder>/*		—To view all files in output folder

hdfs dfs -cat output1/*
hdfs dfs -cat output2/*
hdfs dfs -cat output3/*

Note - The output folder must not exist before the hadoop job is run.