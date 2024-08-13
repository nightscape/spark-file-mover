package dev.mauch.spark

import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService, GenericContainer}
import zio._
import zio.test._
import zio.testcontainers._
import dev.mauch.spark.MoveFilesOutputCommitter.MOVE_FILES_OPTION
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.testcontainers.containers.wait.strategy.Wait

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

object MoveFilesOutputCommiterTest extends ZIOSpecDefault {
  val hdfsContainer = GenericContainer(
    "omaraloraini/testcontainers-hdfs:3.0.3-3",
    exposedPorts = Seq(80),
    waitStrategy = Wait.forLogMessage("omaraloraini.testcontainers.hdfs-ready\\n", 1)
  )
  val hdfsConfigLayer: ZLayer[Any, Throwable, Configuration] =
    ZIOTestcontainers.toLayer(hdfsContainer).flatMap { zenv =>
      val container = zenv.get[GenericContainer]
      val conf = for {
        conf <- ZIO.attempt(new Configuration)
        xml <- ZIO.attempt(container.execInContainer("cat", "/config/core-site.xml").getStdout.replace("\n", "").replace("\t", ""))
        _ <- ZIO.attempt(conf.addResource(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))))
      } yield conf
      ZLayer.fromZIO(conf)
    }
  val dfsLayer: ZLayer[Configuration, Throwable, FileSystem] = for {
    conf <- ZLayer.fromZIO(ZIO.service[Configuration])
    dfs <- ZLayer.fromZIO(ZIO.attempt(FileSystem.get(conf.get)))
  } yield dfs
  val sparkLayer: ZLayer[Configuration, Throwable, SparkSession] =
    ZLayer.scoped(ZIO.acquireRelease(for {
      conf <- ZIO.service[Configuration]
      spark <- ZIO.attempt {
        SparkSession
          .builder()
          .master("local[*]")
          .config("spark.hadoop.fs.defaultFS", conf.get("fs.defaultFS"))
          .config("spark.hadoop.fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
          .config("spark.sql.sources.outputCommitterClass", classOf[MoveFilesOutputCommitter].getName)
          .getOrCreate()

      }
    } yield spark)(c => ZIO.succeed(c.close())))
  val tempDirLayer: ZLayer[FileSystem, Throwable, Path] =
    for {
      tempDir <-
        ZLayer.scoped(ZIO.acquireRelease(for {
          dfs <- ZIO.service[FileSystem]
          wd <- ZIO.attempt(dfs.getWorkingDirectory.suffix(s"test-${java.lang.System.currentTimeMillis}"))
          _ <- ZIO.attempt(dfs.mkdirs(wd))
        } yield wd)(dir => ZIO.serviceWith[FileSystem](_.delete(dir, true))))
    } yield tempDir
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
        outputPath = tempDir.suffix("/test")
        _ <- write(spark.createDataFrame(exampleData), outputPath.toString)
        fileExists <- ZIO.serviceWith[FileSystem](_.exists(outputPath))
        dirExists <- ZIO.serviceWith[FileSystem](_.getFileStatus(outputPath).isDirectory)
      } yield assertTrue(fileExists, dirExists)
    },
    test("does not move files if the path doesn't have a listed file extension") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        outputPath = tempDir.suffix("/test")
        _ <- write(spark.createDataFrame(exampleData).repartition(5), outputPath.toString)
        fileExists <- ZIO.serviceWith[FileSystem](_.exists(outputPath))
        dirExists <- ZIO.serviceWith[FileSystem](_.getFileStatus(outputPath).isDirectory)
      } yield assertTrue(fileExists, dirExists)
    },
    test("does move a single file if the path has a listed file extension") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        outputPath = tempDir.suffix("/test")
        _ <- write(
          spark.createDataFrame(exampleData).repartition(1),
          outputPath.toString,
          targetNamePattern = "$outputDirectory.csv"
        )
        filePath = tempDir.suffix("test.csv")
        fileExists <- ZIO.serviceWith[FileSystem](_.exists(filePath))
        dirExists <- ZIO.serviceWith[FileSystem](_.getFileStatus(filePath).isFile)
      } yield assertTrue(fileExists, dirExists)
    },
    test("does move a single file in a partition if the path has a listed file extension") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        outputPath = tempDir.suffix("/test")
        _ <- write(
          spark.createDataFrame(exampleData).repartition(1),
          outputPath.toString,
          partitionBy = Seq("category", "id"),
          targetNamePattern = "$outputDirectory/cat_$category_id_$id.csv"
        )
        results <- ZIO.foreach(exampleData) { data =>
          val filePath = outputPath.suffix(s"cat_${data.category}_id_${data.id}.csv")
          for {
            fileExists <- ZIO.serviceWith[FileSystem](_.exists(filePath))
            isRegularFile <- ZIO.serviceWith[FileSystem](_.getFileStatus(filePath).isFile)
          } yield assertTrue(fileExists, isRegularFile)
        }
      } yield results.reduce(_ && _)
    },
    test("handles special characters in partition values correctly") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        df = spark.createDataFrame(
          Seq(
            ExampleData("data with space", 1, "foo"),
            ExampleData("data_with_underscore", 2, "bar"),
            ExampleData("data-with-dash", 3, "baz")
          )
        )
        outputPath = tempDir.suffix("/test")
        _ <- write(
          df.repartition(1),
          outputPath.toString,
          partitionBy = Seq("category", "id"),
          targetNamePattern = "$outputDirectory/cat_$category_id_$id.csv"
        )
        results <- ZIO.foreach(
          Seq("cat_data with space_id_1.csv", "cat_data_with_underscore_id_2.csv", "cat_data-with-dash_id_3.csv")
        ) { fileName =>
          val filePath = outputPath.suffix(fileName)
          for {
            fileExists <- ZIO.serviceWith[FileSystem](_.exists(filePath))
            isRegularFile <- ZIO.serviceWith[FileSystem](_.getFileStatus(filePath).isFile)
          } yield assertTrue(fileExists, isRegularFile)
        }
      } yield results.reduce(_ && _)
    },
    test("handles empty partition values correctly") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        df = spark.createDataFrame(Seq(ExampleData("", 1, "foo"), ExampleData(null, 2, "bar")))
        outputPath = tempDir.suffix("/test")
        _ <- write(
          df.repartition(1),
          outputPath.toString,
          partitionBy = Seq("category", "id"),
          targetNamePattern = "$outputDirectory/cat_$category_id_$id.csv"
        )
        results <- ZIO.foreach(
          Seq("cat___HIVE_DEFAULT_PARTITION___id_1.csv", "cat___HIVE_DEFAULT_PARTITION___id_2.csv")
        ) { fileName =>
          val filePath = outputPath.suffix(fileName)
          for {
            fileExists <- ZIO.serviceWith[FileSystem](_.exists(filePath))
            isRegularFile <- ZIO.serviceWith[FileSystem](_.getFileStatus(filePath).isFile)
          } yield assertTrue(fileExists, isRegularFile)
        }
      } yield results.reduce(_ && _)
    },
    test("handles non-existent partition variables in pattern") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        df = spark.createDataFrame(Seq(ExampleData("data", 1, "foo")))
        outputPath = tempDir.suffix("/test")
        result <- write(
          df.repartition(1),
          outputPath.toString,
          partitionBy = Seq("category", "id"),
          targetNamePattern = "$outputDirectory/cat_$nonexistent.csv"
        ).either
      } yield assertTrue(result.isLeft)
    },
    test("does not move files if there are multiple files in a directory") {
      for {
        spark <- ZIO.service[SparkSession]
        tempDir <- ZIO.service[Path]
        outputPath = tempDir.suffix("/test")
        _ <- write(spark.createDataFrame(exampleData), outputPath.toString)
        fileExists <- ZIO.serviceWith[FileSystem](_.exists(outputPath))
        dirExists <- ZIO.serviceWith[FileSystem](_.getFileStatus(outputPath).isDirectory)
      } yield assertTrue(fileExists, dirExists)
    }
  )
    .provideSome[SparkSession with FileSystem](tempDirLayer)
    .provideShared(hdfsConfigLayer, dfsLayer, sparkLayer)
}

case class ExampleData(category: String, id: Int, value: String)
