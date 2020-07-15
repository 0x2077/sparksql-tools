import sbt._
import Keys._


version := "0.1-SNAPSHOT"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.4"
val scallopVersion = "3.5.0"

lazy val sparksql_tools = (project in file("."))
  .settings(
    mainClass in assembly := Some("my.example.domain.Main"),
    name := "sparksql_tools",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      //"org.apache.spark" %% "spark-core" % sparkVersion,
      //"org.apache.spark" %% "spark-streaming" % sparkVersion,
      //"org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.rogach" %% "scallop" % scallopVersion,
      "org.scalatest" %% "scalatest" % "3.0.8" % Test
    )
  )
