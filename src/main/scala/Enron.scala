//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.combinator.syntactical

object Enron extends App {
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("Enron")
  val sc = new SparkContext(conf)

  //val hiveCtx = new HiveContext(sc)

  case class email(from: Array[String], to: Array[String], cc: Array[String], bcc: Array[String])

  def emailSplit(emText: String, listType: String): Array[String] = {
    val em = emText replaceAll("\n", " ") split (" ") filter (x => x.length > 0)
    val headerSeq = List("From:", "To:", "Cc:", "Bcc:", "Subject:") filter (x => !x.equals(listType))
    def emailSplit0(em: Array[String], listType: String, outList: Array[String]): Array[String] = {
      val mCriteria = ((em head) trim, outList length, (em tail) length)
      mCriteria match {
        case (_, _, _) if (mCriteria._2 > 0 && headerSeq.contains(mCriteria._1)) => return outList
        case (_, _, 0) => return outList
        case (_, 0, _) if (mCriteria._1.equals(listType)) => emailSplit0(em tail, listType, outList :+ mCriteria._1)
        case (_, _, _) if (mCriteria._2 > 0) => emailSplit0(em tail, listType, outList :+ mCriteria._1)
        case (_, 0, _) => emailSplit0(em tail, listType, outList)
      }
    }
    emailSplit0(em, listType, new Array[String](0)) filter (x => !x.equals(listType))
  }

  val emails = sc.wholeTextFiles("/home/nnon/dev/enron/maildir/hernandez-j/jrhernandez/*.", 2).cache()
  val emailLines = emails.mapValues(x => emailSplit(x, "To:"))
  
  
  
  val local = emailLines.take(20).foreach(line => println(line._1, line._2.mkString(" ")))
}







//  val headerPattern = "(?!X-)From: [\\S\\s]*Mime-Version:".r
//  flatMapValues(headerPattern findFirstIn _).