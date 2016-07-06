package nl.arnovanoort.talks.monoids

import java.io._

import com.twitter.algebird._
import com.twitter.util.{Duration, Stopwatch}
import com.typesafe.scalalogging.Logger
import com.typesafe.scalalogging.slf4j.LazyLogging
import nl.arnovanoort.talks.monoids.Properties._
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object SparkBloomfilterExample extends LazyLogging {


  def main(args: Array[String]) = {
    import nl.arnovanoort.talks.monoids.Properties._
    val conf = new SparkConf().setAppName("Monoid presentation spark example")
    val sc = new SparkContext(conf)

    val fileRDD = sc.textFile(inputFile,48)

    val bfMonoid: BloomFilterMonoid = createEmptyBloomfilter
    // prepare accumulator, this will process BF within partition
    val bfaccum :(BF, String) => BF = (bf, line) => bf ++ bfMonoid.create(line)
    // prepare addition, this will process merging 2 BF's from 2 partitions
    val add : (BF, BF)=>BF = _ ++ _

    // create and start timer
    val elapsed: () => Duration = Stopwatch.start()

    logger.info("start count")
    val bf2 = fileRDD.aggregate[BF](bfMonoid.zero)(bfaccum,add)
    logger.info("time elapsed: {}", elapsed())

    println("contains 1" + bf2.contains("dragon128.net"))
    println("contains 2" + bf2.contains("zzz-10.info"))
    println("contains 3" + bf2.contains("zzzpharmx.org"))

    write2File(bf2)

  }

  def createEmptyBloomfilter: BloomFilterMonoid = {
      val NUM_HASHES = 7
      val WIDTH = 4074369 //2500000 //3201807
      val SEED = 1
      val bfMonoid = new BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)
      bfMonoid
  }

  def write2File(bf2: BF) = {
    import nl.arnovanoort.talks.monoids.Properties._
    val outPutstream = new FileOutputStream(bloomfilterLocation)
    val out = new ObjectOutputStream(outPutstream)
    out.writeObject(bf2)
    out.close()
    outPutstream.close()
  }

  def readFromFile() : BF = {
    val inputStream = new FileInputStream(bloomfilterLocation)
    val in = new ObjectInputStream(inputStream)
    in.readObject().asInstanceOf[BF]
  }

}
