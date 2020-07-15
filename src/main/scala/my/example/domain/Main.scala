package my.example.domain

object Main {
  def main(args: Array[String]): Unit = {
    if (args.head == "--array-splitter") ArraySplitter.main(args.tail)
  }
}
