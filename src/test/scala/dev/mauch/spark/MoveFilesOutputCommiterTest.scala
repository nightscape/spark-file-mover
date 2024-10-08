package dev.mauch.spark

import dev.mauch.spark.MoveFilesOutputCommitter.MOVE_FILES_OPTION
import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.test.PathUtils

import java.nio.file.Files
import scala.concurrent.duration.Duration
import scala.reflect.io.Directory

class MoveFilesOutputCommiterTest extends munit.FunSuite {
  override val munitTimeout: Duration = Duration(60, "s")

  private val withHdfs = FunFixture[MiniDFSCluster](
    setup = { testOpts =>
      println("Starting HDFS Cluster...")
      val baseDir = Files.createTempDirectory(s"${getClass.getSimpleName} ${testOpts.name} ".replaceAll("[^a-zA-Z0-9]", "_"))
      val conf = new Configuration()
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toFile.getAbsolutePath)
      conf.setBoolean("dfs.webhdfs.enabled", false)
      val builder = new MiniDFSCluster.Builder(conf)
      val hdfsCluster = builder.nameNodePort(9000).manageNameDfsDirs(true).manageDataDfsDirs(true).format(true).build()
      hdfsCluster.waitClusterUp()
      hdfsCluster
    },
    teardown = _.shutdown(true)
  )

  private val withSpark = FunFixture[SparkSession](
    setup = { _ =>
      SparkSession
        .builder()
        .master("local[*]")
        .config(MoveFilesOutputCommitter.OUTPUT_COMMITTER_CLASS, classOf[MoveFilesOutputCommitter].getName)
        .getOrCreate()
    },
    teardown = { spark =>
      spark.close()
    }
  )
  private val withFixtures = FunFixture.map2(withHdfs, withSpark)
  private val exampleData = Seq(
    ExampleData("data", 1, "foo"),
    ExampleData("data", 1, "fooagain"),
    ExampleData("data", 2, "bar"),
    ExampleData("info", 3, "baz")
  )
  def write(df: DataFrame, outputPath: String, partitionBy: Seq[String] = Seq.empty, targetNamePattern: String = "$outputDirectory"): Unit = {
    val writer = df.write
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .option(MOVE_FILES_OPTION, targetNamePattern)
      .mode(SaveMode.Overwrite)
    val potentiallyPartitioned = if (partitionBy.nonEmpty) writer.partitionBy(partitionBy: _*) else writer
    potentiallyPartitioned.csv(outputPath)
  }
  withFixtures.test("does not move files if there are multiple files in a directory") { case(hdfs, spark) =>
    import spark.implicits._
    val df: DataFrame = exampleData.toDF()
    val outputPath = hdfs.getFileSystem.makeQualified(new Path("/test"))
    write(df.repartition(5), outputPath.toString, targetNamePattern = "$outputDirectory.csv")
    assertDirectoryExists(hdfs, outputPath)
  }

  withFixtures.test("does not move files if the path doesn't have a listed file extension") { case(hdfs, spark) =>
    import spark.implicits._
    val df: DataFrame = exampleData.toDF()
    val outputPath = hdfs.getFileSystem.makeQualified(new Path("/test"))
    write(df.repartition(5), outputPath.toString)
    assertDirectoryExists(hdfs, outputPath)
  }
  withFixtures.test("does move a single file if the path has a listed file extension") { case(hdfs, spark) =>
    import spark.implicits._
    val df: DataFrame = exampleData.toDF()
    val outputPath = hdfs.getFileSystem.makeQualified(new Path("/test"))
    val filePath = hdfs.getFileSystem.makeQualified(new Path("/test.csv"))
    write(df.repartition(1), outputPath.toString, targetNamePattern = "$outputDirectory.csv")
    assertFileExists(hdfs, filePath)
  }

  withFixtures.test("does move a single file in a partition if the path has a listed file extension") {
    case(hdfs, spark) =>
      import spark.implicits._
      val df: DataFrame = exampleData.toDF()
      val outputPath = hdfs.getFileSystem.makeQualified(new Path("/test"))
      write(df.repartition(1), outputPath.toString, partitionBy = Seq("category", "id"), targetNamePattern = "$outputDirectory/cat_$category_id_$id.csv")
      exampleData.foreach { data =>
        val filePath = outputPath.suffix(s"/cat_${data.category}_id_${data.id}.csv")
        assertFileExists(hdfs, filePath)

      }
  }
  withFixtures.test("does move files without a partition") {
    case(hdfs, spark) =>
      import spark.implicits._
      val df: DataFrame = exampleData.toDF()
      val outputPath = hdfs.getFileSystem.makeQualified(new Path("/test"))
      write(df.repartition(1), outputPath.toString, targetNamePattern = "$outputDirectory/fixed_file_name.csv")
      val filePath = outputPath.suffix(s"/fixed_file_name.csv")
      assertFileExists(hdfs, filePath)
  }

  withFixtures.test("handles special characters in partition values correctly") { case(hdfs, spark) =>
    import spark.implicits._
    val df = Seq(
      ("data with space", 1, "foo"),
      ("data_with_underscore", 2, "bar"),
      ("data-with-dash", 3, "baz")
    ).toDF("category", "id", "value")
    val outputPath = hdfs.getFileSystem.makeQualified(new Path("/test"))
    write(df.repartition(1), outputPath.toString, partitionBy = Seq("category", "id"), targetNamePattern = "$outputDirectory/cat_$category_id_$id.csv")

    Seq(
      "cat_data with space_id_1.csv",
      "cat_data_with_underscore_id_2.csv",
      "cat_data-with-dash_id_3.csv"
    ).foreach { fileName =>
      val filePath = outputPath.suffix(s"/$fileName")
      assertFileExists(hdfs, filePath)
    }
  }

  withFixtures.test("handles empty partition values correctly") { case(hdfs, spark) =>
    import spark.implicits._
    val df = Seq(
      ("", 1, "foo"),
      (null, 2, "bar")
    ).toDF("category", "id", "value")
    val outputPath = hdfs.getFileSystem.makeQualified(new Path("/test"))
    write(df.repartition(1), outputPath.toString, partitionBy = Seq("category", "id"), targetNamePattern = "$outputDirectory/cat_$category_id_$id.csv")

    Seq(
      "cat___HIVE_DEFAULT_PARTITION___id_1.csv",
      "cat___HIVE_DEFAULT_PARTITION___id_2.csv"
    ).foreach { fileName =>
      val filePath = outputPath.suffix(s"/$fileName")
      assertFileExists(hdfs, filePath)
    }
  }

  withFixtures.test("handles non-existent partition variables in pattern") { case(hdfs, spark) =>
    import spark.implicits._
    val df = Seq(
      ("data", 1, "foo")
    ).toDF("category", "id", "value")
    val outputPath = hdfs.getFileSystem.makeQualified(new Path("/test"))

    intercept[SparkException] {
      write(df.repartition(1), outputPath.toString, partitionBy = Seq("category", "id"), targetNamePattern = "$outputDirectory/cat_$nonexistent.csv")
    }
  }

  private def assertFileStatusIs(hdfs: MiniDFSCluster, path: Path, check: FileStatus => Boolean): Unit = {
    val fileStatus = hdfs.getFileSystem.getFileStatus(path)
    assert(check(fileStatus), clue = clues(clue(path), clue(fileStatus)))
  }

  private def assertDirectoryExists(hdfs: MiniDFSCluster, path: Path): Unit =
    assertFileStatusIs(hdfs, path, _.isDirectory)

  private def assertFileExists(hdfs: MiniDFSCluster, path: Path): Unit =
    assertFileStatusIs(hdfs, path, _.isFile)

}

case class ExampleData(category: String, id: Int, value: String)
