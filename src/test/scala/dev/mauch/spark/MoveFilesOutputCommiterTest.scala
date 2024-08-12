package dev.mauch.spark

import zio._
import zio.test._
import dev.mauch.spark.MoveFilesOutputCommitter.MOVE_FILES_OPTION
import org.apache.spark.sql._

import java.nio.file.{Files, Path}
import scala.reflect.io.Directory

object MoveFilesOutputCommiterTest extends ZIOSpecDefault {
  val sparkLayer: ZLayer[Any, Throwable, SparkSession] =
    ZLayer.scoped(ZIO.acquireRelease(ZIO.attempt {
      SparkSession
        .builder()
        .master("local[*]")
        .config("spark.sql.sources.outputCommitterClass", classOf[MoveFilesOutputCommitter].getName)
        .getOrCreate()
    })(c => ZIO.succeed(c.close())))
  val tempDirLayer: ZLayer[Any, Throwable, Path] = ZLayer.scoped(
    ZIO.acquireRelease(ZIO.attempt(Files.createTempDirectory("test")))(dir =>
      ZIO.attempt(new Directory(dir.toFile).deleteRecursively()).orDie
    )
  )
  private val exampleData = Seq(
    ExampleData("data", 1, "foo"),
    ExampleData("data", 1, "fooagain"),
    ExampleData("data", 2, "bar"),
    ExampleData("info", 3, "baz")
  )
  def write(
    df: DataFrame,
    outputPath: String,
    partitionBy: Seq[String] = Seq.empty,
    targetNamePattern: String = "$outputDirectory"
  ): Task[Unit] = {
    val writer = df.write
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .option(MOVE_FILES_OPTION, targetNamePattern)
      .mode(SaveMode.Overwrite)
    val potentiallyPartitioned = if (partitionBy.nonEmpty) writer.partitionBy(partitionBy: _*) else writer
    ZIO.attempt(potentiallyPartitioned.csv(outputPath))
  }

  def spec: Spec[Any, Throwable] = suite("MoveFilesOutputCommiterTest")(
    test("does not move files if there are multiple files in a directory") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        outputPath = tempDir.resolve("test")
        _ <- write(spark.createDataFrame(exampleData), outputPath.toString)
        fileExists <- ZIO.attempt(Files.exists(outputPath))
        dirExists <- ZIO.attempt(Files.isDirectory(outputPath))
      } yield assertTrue(fileExists, dirExists)
    },
    test("does not move files if the path doesn't have a listed file extension") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        outputPath = tempDir.resolve("test")
        _ <- write(spark.createDataFrame(exampleData).repartition(5), outputPath.toString)
        fileExists <- ZIO.attempt(Files.exists(outputPath))
        dirExists <- ZIO.attempt(Files.isDirectory(outputPath))
      } yield assertTrue(fileExists, dirExists)
    },
    test("does move a single file if the path has a listed file extension") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        outputPath = tempDir.resolve("test")
        _ <- write(
          spark.createDataFrame(exampleData).repartition(1),
          outputPath.toString,
          targetNamePattern = "$outputDirectory.csv"
        )
        filePath = tempDir.resolve("test.csv")
        fileExists <- ZIO.attempt(Files.exists(filePath))
        dirExists <- ZIO.attempt(Files.isRegularFile(filePath))
      } yield assertTrue(fileExists, dirExists)
    },
    test("does move a single file in a partition if the path has a listed file extension") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        outputPath = tempDir.resolve("test")
        _ <- write(
          spark.createDataFrame(exampleData).repartition(1),
          outputPath.toString,
          partitionBy = Seq("category", "id"),
          targetNamePattern = "$outputDirectory/cat_$category_id_$id.csv"
        )
        results <- ZIO.foreach(exampleData) { data =>
          val filePath = outputPath.resolve(s"cat_${data.category}_id_${data.id}.csv")
          for {
            fileExists <- ZIO.attempt(Files.exists(filePath))
            isRegularFile <- ZIO.attempt(Files.isRegularFile(filePath))
          } yield assertTrue(fileExists, isRegularFile)
        }
      } yield results.reduce(_ && _)
    },
    test("handles special characters in partition values correctly") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        df = spark.createDataFrame(Seq(
          ExampleData("data with space", 1, "foo"),
          ExampleData("data_with_underscore", 2, "bar"),
          ExampleData("data-with-dash", 3, "baz")
        ))
        outputPath = tempDir.resolve("test")
        _ <- write(df.repartition(1), outputPath.toString, partitionBy = Seq("category", "id"), targetNamePattern = "$outputDirectory/cat_$category_id_$id.csv")
        results <- ZIO.foreach(Seq(
          "cat_data with space_id_1.csv",
          "cat_data_with_underscore_id_2.csv",
          "cat_data-with-dash_id_3.csv"
        )) { fileName =>
          val filePath = outputPath.resolve(fileName)
          for {
            fileExists <- ZIO.attempt(Files.exists(filePath))
            isRegularFile <- ZIO.attempt(Files.isRegularFile(filePath))
          } yield assertTrue(fileExists, isRegularFile)
        }
      } yield results.reduce(_ && _)
    },
    test("handles empty partition values correctly") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        df = spark.createDataFrame(Seq(
          ExampleData("", 1, "foo"),
          ExampleData(null, 2, "bar")
        ))
        outputPath = tempDir.resolve("test")
        _ <- write(df.repartition(1), outputPath.toString, partitionBy = Seq("category", "id"), targetNamePattern = "$outputDirectory/cat_$category_id_$id.csv")
        results <- ZIO.foreach(Seq(
          "cat___HIVE_DEFAULT_PARTITION___id_1.csv",
          "cat___HIVE_DEFAULT_PARTITION___id_2.csv"
        )) { fileName =>
          val filePath = outputPath.resolve(fileName)
          for {
            fileExists <- ZIO.attempt(Files.exists(filePath))
            isRegularFile <- ZIO.attempt(Files.isRegularFile(filePath))
          } yield assertTrue(fileExists, isRegularFile)
        }
      } yield results.reduce(_ && _)
    },
    test("handles non-existent partition variables in pattern") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        df = spark.createDataFrame(Seq(
          ExampleData("data", 1, "foo")
        ))
        outputPath = tempDir.resolve("test")
        result <- write(df.repartition(1), outputPath.toString, partitionBy = Seq("category", "id"), targetNamePattern = "$outputDirectory/cat_$nonexistent.csv").either
      } yield assertTrue(result.isLeft)
    },
    test("does not move files if there are multiple files in a directory") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        outputPath = tempDir.resolve("test")
        _ <- write(spark.createDataFrame(exampleData), outputPath.toString)
        fileExists <- ZIO.attempt(Files.exists(outputPath))
        dirExists <- ZIO.attempt(Files.isDirectory(outputPath))
      } yield assertTrue(fileExists, dirExists)
    }
  ).provideSome(tempDirLayer).provideLayerShared(sparkLayer)
}

case class ExampleData(category: String, id: Int, value: String)
