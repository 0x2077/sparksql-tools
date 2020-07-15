package my.example.domain

import org.rogach.scallop.{ScallopConf, ScallopOption}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val dir: ScallopOption[String] = opt[String](required = true)
  val inputDfName: ScallopOption[String] = opt[String](default = Some("df"))
  val inputDfArrayColumnName: ScallopOption[String] = opt[String](default = Some("value"))
  val inputFileSuffix: ScallopOption[String] = opt[String](default = Some(".columns"))
  val outputFileSuffix: ScallopOption[String] = opt[String](default = Some(".select_query.sql"))
  verify()
}
