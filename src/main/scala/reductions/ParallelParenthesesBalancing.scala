package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    chars.foldLeft(0) {
      case (a, b) => if (a < 0) return false
        else b match {
          case '(' => a + 1
          case ')' => a - 1
          case _ => a
        }
    } equals 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      chars.slice(idx, until).foldLeft((arg1, arg2)) {
        case (a, b) => b match {
          case '(' => (a._1 + 1, a._2)
          case ')' => if(a._1>0) (a._1 - 1, a._2) else (a._1, a._2+1)
          case _ => a
        }
      }
    }

    def reduce(from: Int, until: Int) : (Int, Int) = {

      val middle = (until - from) / 2
      if(threshold > middle){
        traverse(from, until, 0, 0)
      } else {
        val (a, b) = parallel(reduce(from, from + middle), reduce(from + middle, until))

       a._1 - b._2 + b._1 -> a._2
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
