package nl.arnovanoort.talks.monoids

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util

import com.googlecode.javaewah.EWAHCompressedBitmap
import com.twitter.algebird._
import com.twitter.util.{Duration, Stopwatch}
import scala.io.Source

object ScalaBloomfilterExample extends App {

  override def main(args: Array[String]) = {
//    val fileStream = Source.fromFile("/home/avanoort/data/workspaces/blacklist-spark/input/blacklists/blacklist.all")
//  val fileStream = Source.fromFile("/home/avanoort/data/workspaces/blacklist-spark/input/blacklists/malware/domains")
    val fileStream = Source.fromFile("/home/temmink/data/workspaces/monoid-presentation/input/blacklists/malware/domains")

    val lines:Iterator[String] = fileStream.getLines()

    val bfMonoid: BloomFilterMonoid = createEmptyBloomfilter

    val elapsed: () => Duration = Stopwatch.start()

    val urlMonoid = lines.foldLeft(bfMonoid.zero){(b,a) => b ++ bfMonoid.create(a)  }

    val duration: Duration = elapsed()
      println("duration " + duration)
    println("contains 1" + lines )
    println("contains 1" + urlMonoid .contains("dragon128.net"))
    println("contains 2" + urlMonoid .contains("philips.com"))
    println("contains 3" + urlMonoid .contains("thailandpropertyinvestment.net"))

    val location = "/home/temmink/data/workspaces/monoid-presentation/input/blacklists/blacklist.bloomfilter"
//    val location = "/home/avanoort/data/workspaces/blacklist-spark/input/blacklists/blacklist.bloomfilter"
    val outPutstream = new FileOutputStream(location)
    val out = new ObjectOutputStream(outPutstream)
    out.writeObject(urlMonoid)
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
