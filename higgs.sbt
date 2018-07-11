name := "Higgs Social Graph"

version := "1.0"

scalaVersion := "2.11.8"

scalaSource in Compile := baseDirectory.value / "src"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.1" % "runtime" exclude("org.glassfish.hk2", "hk2-utils") exclude("org.glassfish.hk2", "hk2-locator") exclude("javax.validation", "validation-api")
