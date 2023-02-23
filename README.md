# Spark-Main
Spark project
In this project we have analyse a coronavirus dataset basically

1)First we have make multiple topics on kafka  and schema registry
2)Then write producer program and read diff csv files that are present in the dataset
3)Through producer program , streamed the data to Confluent kafka topic
4)After Streaming the data , we consumed it by making consumer and while consuming it we have dump the data in mongodb

5)Now from mongodb we have consumed the data in pyspark and then perform diff analytical operation on top of it.
