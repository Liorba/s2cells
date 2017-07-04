name := "s2functions"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"  % Provided
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"  % Provided
libraryDependencies += "org.isuper" % "s2-geometry-library-java" % "0.0.1"