import scala.util.Random

object PriorityGenerator {
  def generatePriority(dp:List[Int]): List[Int] = {
    /*
    First generate permutations of all dpId's.
    returns random permutation of Dp's, where priority will be in order:
    first > second > third
     */
    val perm = dp.permutations.toList
    val random = new Random()
    val randomInd = 0 + random.nextInt(5 + 1)
    perm(randomInd)
  }
}
