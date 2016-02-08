//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.combinator.syntactical

object Enron extends App {
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("Enron")
  val sc = new SparkContext(conf)

  //val hiveCtx = new HiveContext(sc)

  case class Email(date: String, subject: String, from: String, to: Array[String], cc: Array[String], bcc: Array[String])

  def emailSplit(emText: String): Email = {
    val em = emText replaceAll("\n", " ") split (" ") filter (x => x.length > 0)
    val headerSeq = List("Date:", "From:", "To:", "Cc:", "Bcc:", "Subject:", "Mime-Version:")
    def emailSplit0(em: Array[String], listType: String, outList: Array[String]): Array[String] = {
      val mCriteria = ((em head) trim, outList length, (em tail) length)
      val header = headerSeq filter (x => !x.equals(listType))
      mCriteria match {
        case (_, _, _) if (mCriteria._2 > 0 && header.contains(mCriteria._1)) => return outList filter (x => !x.equals(listType))
        case (_, _, 0) => return outList filter (x => !x.equals(listType))
        case (_, 0, _) if (mCriteria._1.equals(listType)) => emailSplit0(em tail, listType, outList :+ mCriteria._1)
        case (_, _, _) if (mCriteria._2 > 0) => emailSplit0(em tail, listType, outList :+ mCriteria._1)
        case (_, 0, _) => emailSplit0(em tail, listType, outList)
      }
    }
    Email((emailSplit0(em, "Date:", new Array[String](0))) mkString(" "),
      (emailSplit0(em, "Subject:", new Array[String](0))) mkString(" "),
      (emailSplit0(em, "From:", new Array[String](0))) mkString(""),
      emailSplit0(em, "To:", new Array[String](0)),
      emailSplit0(em, "Cc:", new Array[String](0)),
      emailSplit0(em, "Bcc:", new Array[String](0)))
  }

  val emails = sc.wholeTextFiles("/home/nnon/dev/enron/maildir/hernandez-j/jrhernandez/*.", 2).cache()
  val emailLines = emails.mapValues(emailSplit).distinct().cache()
  val emailFrom = emailLines.mapValues(em => em.from)
  val emailTo = emailLines.mapValues(em => em.to.mkString(",")).flatMapValues(to => to split(",")).filter(x => !x._2.isEmpty)
  val emailDetails = emailFrom.join(emailTo).values

  val local = emailDetails.take(20).foreach(println(_))//.foreach(line => println(line._1, line._2._1, line._2._2.mkString(" ")))
//  val local = emailLines take(20) //foreach(println _)
}







//  val headerPattern = "(?!X-)From: [\\S\\s]*Mime-Version:".r
//  flatMapValues(headerPattern findFirstIn _).