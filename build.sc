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
    Seq("spark-move-files", sparkBinaryVersion)

  override def millSourcePath = super.millSourcePath / os.up

  object LowerOrEqual {
    def unapply(otherVersion: String): Boolean = otherVersion match {
      case s"${sparkMaj}.${sparkMin}.${sparkPat}" =>
        sparkMaj == sparkMajor && (sparkMin < sparkMinor || (sparkMin == sparkMinor && sparkPat <= sparkPatch))
      case s"${sparkMaj}.${sparkMin}" => sparkMaj == sparkMajor && sparkMin <= sparkMinor
      case sparkMaj => sparkMaj == sparkMajor
    }
  }
  object HigherOrEqual {
    def unapply(otherVersion: String): Boolean = otherVersion match {
      case s"${sparkMaj}.${sparkMin}.${sparkPat}" =>
        sparkMaj == sparkMajor && (sparkMin > sparkMinor || (sparkMin == sparkMinor && sparkPat >= sparkPatch))
      case s"${sparkMaj}.${sparkMin}" => sparkMaj == sparkMajor && sparkMin >= sparkMinor
      case sparkMaj => sparkMaj == sparkMajor
    }
  }

  def sparkVersionSpecificSources = T {
    val versionSpecificDirs = os.list(os.pwd / "src" / "main")
    val Array(sparkMajor, sparkMinor, sparkPatch) = sparkVersion.split("\\.")
    val sparkBinaryVersion = s"$sparkMajor.$sparkMinor"
    versionSpecificDirs.filter(_.last match {
      case "scala" => true
      case `sparkBinaryVersion` => true
      case s"${LowerOrEqual()}_and_up" => true
      case s"${LowerOrEqual()}_to_${HigherOrEqual()}" => true
      case _ => false
    })
  }
  override def sources = T.sources {
    super.sources() ++ sparkVersionSpecificSources().map(PathRef(_))
  }

  override def docSources = T.sources(Seq[PathRef]())

  override def artifactName = "spark-file-mover"

  override def publishVersion = s"${sparkVersion}_${super.publishVersion()}"

  def pomSettings = PomSettings(
    description = "A Spark extension for writing to single files",
    organization = "dev.mauch",
    url = "https://github.com/nightscape/spark-file-mover",
    licenses = Seq(License.`Apache-2.0`),
    versionControl = VersionControl.github("nightscape", "spark-file-mover"),
    developers = Seq(Developer("nightscape", "Martin Mauch", "https://github.com/nightscape"))
  )

  def assemblyRules = Seq(
    Rule.AppendPattern(".*\\.conf"), // all *.conf files will be concatenated into single file
  )

  override def extraPublish = Seq(PublishInfo(assembly(), classifier = None, ivyConfig = "compile"))

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
val spark24 = List("2.4.1", "2.4.7", "2.4.8")
val spark30 = List("3.0.1", "3.0.3")
val spark31 = List("3.1.1", "3.1.2", "3.1.3")
val spark32 = List("3.2.4")
val spark33 = List("3.3.4")
val spark34 = List("3.4.1", "3.4.3")
val spark35 = List("3.5.1")
val sparkVersions = /*spark24 ++ spark30 ++ spark31 ++ spark32 ++*/ spark33 ++ spark34 ++ spark35
val crossMatrix =
  sparkVersions.map(spark => (scala212, spark)) ++
    sparkVersions.filter(_ >= "3.2").map(spark => (scala213, spark))
println(crossMatrix.reverse)
object `spark-file-mover` extends Cross[SparkModule](crossMatrix.reverse) {}
