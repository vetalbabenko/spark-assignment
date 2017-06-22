name := "spark-assignment"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.json4s" %% "json4s-jackson_2.10" % "3.1.0",
  "org.scalatest" % "scalatest_2.12" % "3.0.0" % "test"
)
