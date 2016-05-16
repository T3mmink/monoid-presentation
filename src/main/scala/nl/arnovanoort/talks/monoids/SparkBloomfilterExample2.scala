package nl.arnovanoort.talks.monoids

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import com.twitter.algebird._
import com.twitter.util.{Duration, Stopwatch}
import org.apache.spark.{SparkConf, SparkContext}

  object SparkBloomfilterExample2 {

  def main2(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Simple Application")
    //      conf.setMaster("spark://192.168.0.106:7077")
     conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc.addJar("/home/temmink/data/workspaces/monoid-presentation/target/scala-2.10/blacklist-spark_2.10-1.0.jar")
    val fileStream = sc.textFile("file:///home/temmink/data/workspaces/monoid-presentation/input/blacklists/malware/domains")
//    val fileStream = sc.textFile("file:///home/temmink/data/workspaces/monoid-presentation/input/blacklists/malware/domains")
//    fileStream.saveAsTextFile("hdfs://bangkok:6066/domain")
//    fileStream.saveAsTextFile("hdfs://192.168.0.106:6066/domain")

//    val NUM_SAMPLES = 100000
//    val count = sc.parallelize(1 to NUM_SAMPLES).map{i =>
//      val x = Math.random * 2 - 1
//      val y = Math.random * 2 - 1
//      if (x * x + y * y < 1) 1.0 else 0.0
//    }.reduce(_ + _)
//    println( "count" + count)//count.saveAsTextFile("file:///home/temmink/data/workspaces/monoid-presentation/input/blacklists/count")



//    val file = sc.textFile("hdfs://")
    val bfMonoid: BloomFilterMonoid = createEmptyBloomfilter
//    var bf =  bfMonoid.zero
//    fileStream.flatMap(line => {
//      bfMonoid.create(line)
//
//    }).reduce(_ ++ _)
//    println("contains 1" + bf.contains("zzzpharmx.org"))
//    println("contains 2" + bf.contains("philips.com"))
//    println("contains 3" + bf.contains("zzyxdr.com"))




    // aggregate solution
    val bfaccum :(BF, String) => BF = (bf, line) => bf ++ bfMonoid.create(line)
    val add : (BF, BF)=>BF = _ ++ _
    println("start count")
    val counts = fileStream.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    println("count: " + counts)
    val bf2 = fileStream.aggregate[BF](bfMonoid.zero)(bfaccum,add)

//    val elapsed: () => Duration = Stopwatch.start()


//      val lines = sc.textFile("file:///home/avanoort/data/workspaces/blacklist-spark/input/blacklists/malware/domains")
//    val urlMonoid = lines.foldLeft(bfMonoid.zero){(b,a) => b ++ bfMonoid.create(a)  }

//    val duration: Duration = elapsed()
///      println("duration " + duration)
    println("contains 1" + bf2.contains("dragon128.net"))
    println("contains 2" + bf2.contains("zzz-10.info"))
    println("contains 3" + bf2.contains("zzzpharmx.org"))



// write to file
//    val location = "/home/avanoort/data/workspaces/blacklist-spark/input/blacklists/blacklist.bloomfilter"
//    val outPutstream = new FileOutputStream(location)
//    val out = new ObjectOutputStream(outPutstream)
//    out.writeObject(bf2)
//    out.close()
//    outPutstream.close()

// read bloomfilter
//    val inputStream = new FileInputStream(location)
//    val in = new ObjectInputStream(inputStream)
//    val blacklistMonoid  = in.readObject().asInstanceOf[BF]

//    println("contains beer.com after "  + blacklistMonoid.contains("beer.com"))


  }

    def createEmptyBloomfilter: BloomFilterMonoid = {
        val NUM_HASHES = 4
        val WIDTH = 2500000 //4074369 //3201807
        val SEED = 1
        val bfMonoid = new BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)
        bfMonoid
    }
}
