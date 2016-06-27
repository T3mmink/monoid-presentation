package nl.arnovanoort.talks.monoids

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import com.twitter.algebird._
import com.twitter.util.{Duration, Stopwatch}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object SparkBloomfilterExample  {

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Simple Application")
//        conf.setMaster("spark://192.168.0.104:7077")
    val sc = new SparkContext(conf)

    sc.addJar("/home/temmink/data/workspaces/monoid-presentation/target/scala-2.10/blacklist-spark-assembly-1.0.jar")

    val fileStream = sc.textFile("file:///home/temmink/data/workspaces/monoid-presentation/input/blacklists/malware/domains",48)
    println("nr partitions " + fileStream.getNumPartitions)
//    fileStream.par
    // aggregate solution
    val bfMonoid: BloomFilterMonoid = createEmptyBloomfilter
    val bfaccum :(BF, String) => BF = (bf, line) => bf ++ bfMonoid.create(line)
    val add : (BF, BF)=>BF = _ ++ _
    println("start count")
    val elapsed: () => Duration = Stopwatch.start()

    val bf2 = fileStream.aggregate[BF](bfMonoid.zero)(bfaccum,add)

    println("time elapsed: " + elapsed())

    println("contains 1" + bf2.contains("dragon128.net"))
    println("contains 2" + bf2.contains("zzz-10.info"))
    println("contains 3" + bf2.contains("zzzpharmx.org"))


    val location = "/home/temmink/data/workspaces/monoid-presentation/input/blacklists/blacklist.bloomfilter"
    val outPutstream = new FileOutputStream(location)
    val out = new ObjectOutputStream(outPutstream)
    out.writeObject(bf2)
    out.close()
    outPutstream.close()

    val inputStream = new FileInputStream(location)
    val in = new ObjectInputStream(inputStream)
    val blacklistMonoid  = in.readObject().asInstanceOf[BF]


  }

    def createEmptyBloomfilter: BloomFilterMonoid = {
        val NUM_HASHES = 4
        val WIDTH = 2500000 //4074369 //3201807
        val SEED = 1
        val bfMonoid = new BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)
        bfMonoid
    }
}
