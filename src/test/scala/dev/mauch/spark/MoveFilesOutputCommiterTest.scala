package dev.mauch.spark

import dev.mauch.spark.MoveFilesOutputCommitter.MOVE_FILES_OPTION
import org.apache.spark.SparkException
import org.apache.spark.sql._

import java.nio.file.{Files, Path}
import scala.concurrent.duration.Duration
import scala.reflect.io.Directory

class MoveFilesOutputCommiterTest extends munit.FunSuite {
  override val munitTimeout: Duration = Duration(60, "s")
  private val withSpark = FunFixture[SparkSession](
    setup = { _ =>
      SparkSession
        .builder()
        .master("local[*]")
        .config("spark.sql.sources.outputCommitterClass", classOf[MoveFilesOutputCommitter].getName)
        .getOrCreate()
    },
    teardown = { spark =>
      spark.close()
    }
  )
  private val withTempDirectory = FunFixture[Path](
    setup = { testOpts =>
      Files.createTempDirectory(s"${getClass.getSimpleName} ${testOpts.name} ".replaceAll("[^a-zA-Z0-9]", "_"))
    },
    teardown = { tempDir =>
      new Directory(tempDir.toFile).deleteRecursively()
    }
  )
  private val withFixtures = FunFixture.map2(withSpark, withTempDirectory)
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
  withFixtures.test("does not move files if there are multiple files in a directory") { case (spark, tempDir) =>
    import spark.implicits._
    val df: DataFrame = exampleData.toDF()
    val outputPath = tempDir.resolve(s"test")
    write(df.repartition(5), outputPath.toString, targetNamePattern = "$outputDirectory.csv")
    assert(Files.exists(outputPath), clue = clue(outputPath))
    assert(Files.isDirectory(outputPath), clue = clue(outputPath))
  }
  withFixtures.test("does not move files if the path doesn't have a listed file extension") { case (spark, tempDir) =>
    import spark.implicits._
    val df: DataFrame = exampleData.toDF()
    val outputPath = tempDir.resolve("test")
    write(df.repartition(5), outputPath.toString)
    assert(Files.exists(outputPath), clue = clue(outputPath))
    assert(Files.isDirectory(outputPath), clue = clue(outputPath))
  }
  withFixtures.test("does move a single file if the path has a listed file extension") { case (spark, tempDir) =>
    import spark.implicits._
    val df: DataFrame = exampleData.toDF()
    val outputPath = tempDir.resolve(s"test")
    val filePath = tempDir.resolve("test.csv")
    write(df.repartition(1), outputPath.toString, targetNamePattern = "$outputDirectory.csv")
    assert(Files.exists(filePath), clue = clue(filePath))
    assert(Files.isRegularFile(filePath), clue = clue(filePath))
  }
  withFixtures.test("does move a single file in a partition if the path has a listed file extension") {
    case (spark, tempDir) =>
      import spark.implicits._
      val df: DataFrame = exampleData.toDF()
      val outputPath = tempDir.resolve(s"test")
      write(df.repartition(1), outputPath.toString, partitionBy = Seq("category", "id"), targetNamePattern = "$outputDirectory/cat_$category_id_$id.csv")
      exampleData.foreach { data =>
        val filePath = outputPath.resolve(s"cat_${data.category}_id_${data.id}.csv")
        assert(Files.exists(filePath), clue = clue(filePath))
        assert(Files.isRegularFile(filePath), clue = clue(filePath))

      }
  }

  withFixtures.test("handles special characters in partition values correctly") { case (spark, tempDir) =>
    import spark.implicits._
    val df = Seq(
      ("data with space", 1, "foo"),
      ("data_with_underscore", 2, "bar"),
      ("data-with-dash", 3, "baz")
    ).toDF("category", "id", "value")
    val outputPath = tempDir.resolve("test")
    write(df.repartition(1), outputPath.toString, partitionBy = Seq("category", "id"), targetNamePattern = "$outputDirectory/cat_$category_id_$id.csv")

    Seq(
      "cat_data with space_id_1.csv",
      "cat_data_with_underscore_id_2.csv",
      "cat_data-with-dash_id_3.csv"
    ).foreach { fileName =>
      val filePath = outputPath.resolve(fileName)
      assert(Files.exists(filePath), clue = clue(filePath))
      assert(Files.isRegularFile(filePath), clue = clue(filePath))
    }
  }

  withFixtures.test("handles empty partition values correctly") { case (spark, tempDir) =>
    import spark.implicits._
    val df = Seq(
      ("", 1, "foo"),
      (null, 2, "bar")
    ).toDF("category", "id", "value")
    val outputPath = tempDir.resolve("test")
    write(df.repartition(1), outputPath.toString, partitionBy = Seq("category", "id"), targetNamePattern = "$outputDirectory/cat_$category_id_$id.csv")

    Seq(
      "cat___HIVE_DEFAULT_PARTITION___id_1.csv",
      "cat___HIVE_DEFAULT_PARTITION___id_2.csv"
    ).foreach { fileName =>
      val filePath = outputPath.resolve(fileName)
      assert(Files.exists(filePath), clue = clue(filePath))
      assert(Files.isRegularFile(filePath), clue = clue(filePath))
    }
  }

  withFixtures.test("handles non-existent partition variables in pattern") { case (spark, tempDir) =>
    import spark.implicits._
    val df = Seq(
      ("data", 1, "foo")
    ).toDF("category", "id", "value")
    val outputPath = tempDir.resolve("test")

    intercept[SparkException] {
      write(df.repartition(1), outputPath.toString, partitionBy = Seq("category", "id"), targetNamePattern = "$outputDirectory/cat_$nonexistent.csv")
    }
  }
}

case class ExampleData(category: String, id: Int, value: String)
