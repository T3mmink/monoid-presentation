package nl.arnovanoort.talks.monoids

import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source

object ConcatFiles extends App {

  val workdir = "/home/avanoort/data/workspaces/blacklist-spark/input/blacklists/"
  val bw = new BufferedWriter(new FileWriter(workdir + "/merged"))

  val folders = new File(workdir)
  folders.listFiles().foreach(folder =>{
//    println(folder)
    if(folder.isDirectory){
    val files = folder.listFiles().foreach(file => {
      if(file.getName.equals("domains")){
        println("\n" + file)
        val domain = Source.fromFile(file).getLines()
        domain.foreach(line=> bw.write(line + "\n "))

//        domain.getLines().foreach(println(_))
      }
    })
    }

  })
  bw.close()
}
