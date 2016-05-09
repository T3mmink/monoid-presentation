name := "blacklist-spark"

version := "1.0"

//scalaVersion := "2.11.8"
scalaVersion := "2.10.5"

libraryDependencies += "com.twitter" % "algebird-core_2.10" % "0.12.0"
//libraryDependencies += "com.twitter" %% "util-collection" % "6.34.0"
libraryDependencies += "com.twitter" % "util-collection_2.10" % "6.34.0"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.1"
//libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.1"
//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
//vyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) } [warn] Run 'evicted' to see detailed eviction warnings [warn] Binary version (2.11

//vyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) } [warn] Run 'evicted' to see detailed eviction warnings [warn] Binary version (2.11