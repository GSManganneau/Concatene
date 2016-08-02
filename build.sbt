lazy val root = (project in file(".")).
  settings(
    name := "openValue",
    version := "1.0",
    scalaVersion := "2.11.8",
    libraryDependencies +="org.apache.spark" %% "spark-core" % "2.0.0" 
  )
