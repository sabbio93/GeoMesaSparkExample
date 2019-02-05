# GeoMesaSparkExample

## To create a kafka producer on the kafka machine run:

```bash
tail -n +3 ~/green_tripdata_2016-01.csv | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sparkstream2
```



## On a machine that can reach the kafka host to run locally(all in one jvm):

* Change if necessary the host at line 25 of SemiStreamRDD.scala with proper ip/name address

```bash
sbt run
```

## To run against a cluster:

* Change if necessary the host at line 25 of SemiStreamRDD.scala with proper ip/name address
* delete `.setMaster("local[*]")` line 20 of SemiStreamRDD.scala



```bash
sbt package
spark-2.4.0-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 org.datasyslab:geospark:1.1.2 ./geospark_2.11-0.1.0-SNAPSHOT.jar

```

