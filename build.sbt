val http4sVersion = "0.21.2"
val circeVersion = "0.13.0"

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "gm-data-scala-sdk",
    organization := "io.greymatter",
    version:= "1.1.1.0-SNAPSHOT",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    scalacOptions ++= Seq("-target:jvm-1.8", "-Ypartial-unification"),
    scalaVersion := "2.12.12",
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-core" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.http4s" %% "http4s-blaze-client" % http4sVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "io.circe" %% "circe-fs2" % circeVersion
    )
  )
