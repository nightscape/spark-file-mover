import $ivy.`io.chris-kipp::mill-ci-release::0.1.10`
import io.kipp.mill.ci.release.CiReleaseModule
import $ivy.`com.lihaoyi::mill-contrib-sonatypecentral:`
import mill.contrib.sonatypecentral.SonatypeCentralPublishModule

import coursier.maven.MavenRepository
import mill._, scalalib._, publish._
import Assembly._

import de.tobiasroeser.mill.vcs.version.VcsVersion

val url = "https://github.com/nightscape/spark-move-files"

object build extends Module {
  def publishVersion = T {
    VcsVersion.vcsState().format(untaggedSuffix = "-SNAPSHOT")
  }
}

trait SparkModule extends Cross.Module2[String, String] with SbtModule with CiReleaseModule with SonatypeCentralPublishModule {
  outer =>
  override def scalaVersion = crossValue
  val sparkVersion = crossValue2
  val Array(sparkMajor, sparkMinor, sparkPatch) = sparkVersion.split("\\.")
  val sparkBinaryVersion = s"$sparkMajor.$sparkMinor"

  override def artifactNameParts =
    Seq("spark-file-mover", sparkBinaryVersion)

  override def millSourcePath = super.millSourcePath / os.up

  override def docSources = T.sources(Seq[PathRef]())

  def pomSettings = PomSettings(
    description = "A Spark extension for writing to single files",
    organization = "dev.mauch",
    url = "https://github.com/nightscape/spark-file-mover",
    licenses = Seq(License.`Apache-2.0`),
    versionControl = VersionControl.github("nightscape", "spark-file-mover"),
    developers = Seq(Developer("nightscape", "Martin Mauch", "https://github.com/nightscape"))
  )

  val hadoopVersion = "3.2.0"
  val sparkDeps = Agg(
    ivy"org.apache.spark::spark-core:$sparkVersion",
    ivy"org.apache.spark::spark-sql:$sparkVersion",
    ivy"org.apache.spark::spark-hive:$sparkVersion",
    ivy"org.apache.hadoop:hadoop-common:$hadoopVersion",
    ivy"org.apache.hadoop:hadoop-auth:$hadoopVersion"
  )

  override def compileIvyDeps = if (sparkVersion < "3.3.0") {
    sparkDeps ++ Agg(ivy"org.slf4j:slf4j-api:1.7.36".excludeOrg("stax"))
  } else {
    sparkDeps
  }

  object test extends SbtTests with TestModule.Munit {

    override def millSourcePath = super.millSourcePath

    override def sources = T.sources {
      Seq(PathRef(millSourcePath / "src" / "test" / "scala"))
    }

    override def resources = T.sources {
      Seq(PathRef(millSourcePath / "src" / "test" / "resources"))
    }

    def scalaVersion = outer.scalaVersion()

    def repositoriesTask = T.task {
      super.repositoriesTask() ++ Seq(MavenRepository("https://jitpack.io"))
    }

    def ivyDeps = sparkDeps ++ Agg(ivy"org.scalameta::munit::1.0.0")
  }
}

val scala213 = "2.13.14"
val scala212 = "2.12.19"
val spark24 = "2.4.8"
val spark30 = "3.0.3"
val spark31 = "3.1.3"
val spark32 = "3.2.4"
val spark33 = "3.3.4"
val spark34 = "3.4.3"
val spark35 = "3.5.1"
val sparkVersions = Seq(spark24, spark30, spark31, spark32, spark33, spark34, spark35)
val crossMatrix =
  sparkVersions.map(spark => (scala212, spark)) ++
    sparkVersions.filter(_ >= "3.2").map(spark => (scala213, spark))
object `spark-file-mover` extends Cross[SparkModule](crossMatrix.reverse) {}
