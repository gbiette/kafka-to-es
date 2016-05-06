/**
  * Created by gregoire on 06/05/16.
  */
object Gaelle {

  def main(args: Array[String]) {

    val possibilities = collection.mutable.HashMap[(Int, Int, Int), Int]()

    for (i <- 1 until 250) {
      for (j <- i until 250) {
        for (k <- j until 250) {
          val produit = i * j * k
          val somme = i + j + k
          if (produit == 2450 && somme % 2 == 0) {
            possibilities.put((i, j, k), somme / 2)
          }
        }
      }
    }

    val counts = collection.mutable.HashMap[Int, Int]()

    possibilities.foreach { case (nb, somme) =>
      val prev = counts.getOrElse(somme, 0)
      counts.put(somme, prev + 1)
    }

    val doublets = counts.filter(_._2 > 1).keySet

    possibilities.filter(s => doublets.contains(s._2)).foreach(println)


  }

}
