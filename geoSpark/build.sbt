name := "geoSpark"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion= "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.datasyslab" % "geospark" % "1.1.2"
)