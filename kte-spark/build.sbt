name := "kte-spark"

version := "1.0"

scalaVersion := "2.11.7"

ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

libraryDependencies += "joda-time" % "joda-time" % "2.8.1"

libraryDependencies += "com.github.nscala-time" % "nscala-time_2.11" % "2.10.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.1"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.7.4"

libraryDependencies += "org.elasticsearch" % "elasticsearch-spark_2.11" % "2.2.0" intransitive()

libraryDependencies += "com.typesafe" % "config" % "1.3.0"
