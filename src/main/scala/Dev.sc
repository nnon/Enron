

class Email(date: String, subject: String, from: String, to: Array[String], cc: Array[String], bcc: Array[String]){
  override def toString(): String = {
    date + "," +
      subject + "," +
      from + "," +
      "To: " + to mkString(",To: ") +
      "Cc: " + cc mkString(",") +
      "Bcc: " + bcc mkString(",")
  }
}

val em = new Email("date","subject", "from", Array("to1", "to2", "to3"), Array("cc"), Array("bcc"))
