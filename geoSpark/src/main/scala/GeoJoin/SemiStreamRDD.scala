package GeoJoin
import java.lang.Double

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD}

object SemiStreamRDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

     val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka.maas:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "sparkStreamID",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("sparkstream2")
    val lines= KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val polygonRDD =  new PolygonRDD(sc, "../data/2.json", FileDataSplitter.GEOJSON, true)

    val geoFactory = new GeometryFactory()

    // for each RDD perform your Query

    lines.foreachRDD({
      rdd => {

        // create RDD of points out of RDD of strings.
        // here, you could parse whatever format the streaming is.

        val points = rdd.map(record =>
          geoFactory.createPoint(new Coordinate(
            Double.parseDouble(record.value().split(",")(6)),
            Double.parseDouble(record.value().split(",")(7)))))

        // if rdd not empty, perform query
        if(!rdd.isEmpty()) {

          val pointsRDD = new PointRDD(points,StorageLevel.NONE)


          //  set partitioning type, and sample type if required
          pointsRDD.spatialPartitioning(GridType.QUADTREE)
          polygonRDD.spatialPartitioning(pointsRDD.getPartitioner)
          //polygonRDD.spatialPartitioning(pointsRDD.partitionTree)

          // build index
          pointsRDD.buildIndex(IndexType.RTREE, true)

          pointsRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
          polygonRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

          // SpatialJoinQuery returns RDD of Pairs <Polygon, HashSet<Point>>
          val joinResultRDD = JoinQuery.SpatialJoinQuery(pointsRDD, polygonRDD, true, false)

        }
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
