package nl.arnovanoort.talks.monoids

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import com.twitter.algebird._
import com.twitter.util.{Duration, Stopwatch}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object SparkBloomfilterExample extends App {

  override def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Simple Application")
      conf.setMaster("spark://192.168.0.106:7077")
    val sc = new SparkContext(conf)
    val fileStream = sc.textFile("file:///home/avanoort/data/workspaces/blacklist-spark/input/blacklists/malware/domains")
      val bfMonoid: BloomFilterMonoid = createEmptyBloomfilter

      val bfaccum :(BF, String) => BF = (bf, line) => bf ++ bfMonoid.create(line)
      val add : (BF, BF)=>BF = _ ++ _
println("start count")
    val counts = fileStream.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
println("count: " + counts)
    val bf2 = fileStream.aggregate[BF](bfMonoid.zero)(bfaccum,add)

    val elapsed: () => Duration = Stopwatch.start()


//        val textFile = sc.textFile("hdfs://...")
//        val counts = fileStream
//          .`map(line => (word, 1))
//          .reduceByKey(_ + _)
    //    val textFile = sc.textFile("hdfs://...")
    //    val counts = textFile.flatMap(line => line.split(" "))
    //      .map(word => (word, 1))
    //      .reduceByKey(_ + _)
//    counts.saveAsTextFile("hdfs://...")


//      val lines = sc.textFile("file:///home/avanoort/data/workspaces/blacklist-spark/input/blacklists/malware/domains")
//    val urlMonoid = lines.foldLeft(bfMonoid.zero){(b,a) => b ++ bfMonoid.create(a)  }

    val duration: Duration = elapsed()
      println("duration " + duration)
    println("contains 1" + bf2.contains("dragon128.net"))
    println("contains 2" + bf2.contains("philips.com"))
    println("contains 3" + bf2.contains("thailandpropertyinvestment.net"))


    val location = "/home/avanoort/data/workspaces/blacklist-spark/input/blacklists/blacklist.bloomfilter"
    val outPutstream = new FileOutputStream(location)
    val out = new ObjectOutputStream(outPutstream)
    out.writeObject(bf2)
    out.close()
    outPutstream.close()

    val inputStream = new FileInputStream(location)
    val in = new ObjectInputStream(inputStream)
    val blacklistMonoid  = in.readObject().asInstanceOf[BF]

    println("contains beer.com after "  + blacklistMonoid.contains("beer.com"))


  }

    def createEmptyBloomfilter: BloomFilterMonoid = {
        val NUM_HASHES = 4
        val WIDTH = 2500000 //4074369 //3201807
        val SEED = 1
        val bfMonoid = new BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)
        bfMonoid
    }
}
