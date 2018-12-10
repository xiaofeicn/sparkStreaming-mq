name := "cj-spark-mq"

version := "0.1"

scalaVersion := "2.11.8"



libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "com.alibaba" % "fastjson" % "1.2.38"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.0"
libraryDependencies += "com.rabbitmq" % "amqp-client" % "3.6.6"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.0"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.11"


libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.35"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.5"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.5"