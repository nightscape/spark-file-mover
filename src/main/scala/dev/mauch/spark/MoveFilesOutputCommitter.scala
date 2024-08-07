package dev.mauch.spark

import dev.mauch.spark.MoveFilesOutputCommitter.MOVE_FILES_OPTION
import org.apache.hadoop.fs._
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}

object MoveFilesOutputCommitter {
  val MOVE_FILES_OPTION = "spark.writer.movefiles"
}
class MoveFilesOutputCommitter(outputPath: Path,
                               context: TaskAttemptContext) extends FileOutputCommitter(outputPath, context) {
  override def commitJob(context: JobContext): Unit = {
    super.commitJob(context)
    val moveFiles = context.getConfiguration.get(MOVE_FILES_OPTION)
    if (moveFiles != null) {
      val fs = FileSystem.get(context.getConfiguration)

      def scalaRemoteIterator[T](iter: RemoteIterator[T]): Iterator[T] = new Iterator[T] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): T = iter.next()
      }
      val targetPathTemplate = moveFiles.split("/")

      val filesInDir = scalaRemoteIterator(fs.listFiles(outputPath, true)).filterNot(_.getPath.getName == "_SUCCESS").toList

      val PartitionRegex = "(\\w+)=([a-zA-Z0-9 -_]*)".r
      val ContainingVariable = "(.*?)\\$([a-zA-Z0-9]+)".r
      val renames = filesInDir.map(_.getPath).map { f =>
        val parents = Seq.iterate(f, f.depth())(_.getParent).reverse
        val partitions = parents.map(_.getName).collect {
          case PartitionRegex(key, value) => key -> value
        }.toMap ++ Map("outputDirectory" -> outputPath.toString)

        val newPath = targetPathTemplate.foldLeft(fs.resolvePath(new Path("/"))) {
          case (path, partitionKey) if partitionKey.contains("$") =>
            val varsReplaced = ContainingVariable.replaceAllIn(partitionKey, mat => {
              val partitionValue = partitions.getOrElse(mat.group(2), throw new NoSuchElementException(s"Key '${mat.group(2)}' not found in partitions $partitions"))
              mat.group(1) + partitionValue
            })
            new Path(path, varsReplaced)
        }
        f -> newPath
      }.toMap
      println(renames)
      val filesPerRenameTarget = renames.groupBy(_._2).mapValues(_.size)
      if (filesPerRenameTarget.count(_._2 > 1) == 0) {
        renames.foreach { case (from, to) => fs.rename(from, to) }
      }
    }
  }
}
