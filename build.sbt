name := "SparkLearning"

lazy val commonSettings = Seq(
  organization := "com.paretointel",
  version := "0.1.0",
  scalaVersion := "2.11.1"
)


lazy val root = (project in file(".")).settings(
    commonSettings,
    name := "MSPParser",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
      "org.apache.spark" %% "spark-sql" % "2.4.0"
    ))