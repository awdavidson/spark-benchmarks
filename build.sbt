name := "spark-benchmark"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.16"

val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
	"org.apache.hadoop" % "hadoop-aws" % "3.3.4",
	"com.amazonaws" % "aws-java-sdk-bundle" % "1.12.317",
	// JSON serialization
	"org.json4s" %% "json4s-native" % "3.6.7",
)

// Exclude scala runtime jars
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
