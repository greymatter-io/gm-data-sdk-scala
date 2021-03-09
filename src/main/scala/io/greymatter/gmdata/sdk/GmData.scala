package io.greymatter.gmdata.sdk

import io.circe.{Decoder, Encoder, Json}
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.{Headers, MediaType, Method, Request, Response, Uri}
import org.http4s.multipart.{Multipart, Part}
import io.circe.generic.auto._
import io.circe.syntax._
import cats.implicits._
import fs2.Stream
import cats.effect.{ConcurrentEffect, Sync}
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.headers.`Content-Type`

import javax.net.ssl.SSLContext
import scala.concurrent.ExecutionContext

trait GmData[F[_]] extends Http4sClientDsl[F] with ResponseHandlingFunctions[F] {

  implicit val securityDecoder = Decoder[Security].either(Decoder[Map[String, String]]).map(_.left.toOption)

  def sendRequest[X](client: Client[F], headers: Headers, existingRequest: F[Request[F]], handleResponseFunction: F[Request[F]] => PartialFunction[Response[F], F[X]])(implicit decoder: Decoder[X], F: Sync[F]): F[X] = {
    val request = existingRequest.map(_.withHeaders(headers))
    client.fetch(request)(handleResponseFunction(request))
  }

  def stream(client: Client[F], headers: Headers, existingRequest: Request[F])(implicit F: Sync[F]): Stream[F, Response[F]] = client.stream(existingRequest.withHeaders(headers))

  def get[X](path: Uri)(client: Client[F], headers: Headers)(implicit F: Sync[F], decoder: Decoder[X]): F[X] = sendRequest[X](client, headers, Method.GET(path), defaultHandleJsonResponseFunction)

  def getSelf(rootUrl: Uri, client: Client[F], headers: Headers)(implicit F: Sync[F]): F[Self] = get(rootUrl / "self")(client, headers)

  def getConfig(rootUrl: Uri, client: Client[F], headers: Headers)(implicit F: Sync[F]): F[Config] = get(rootUrl / "config")(client, headers)

  def getProps(path: Uri.Path, headers: Headers, rootUrl: Uri, client: Client[F])(implicit F: Sync[F]): F[Metadata] = get[Metadata](parseUrl(rootUrl, s"props/$path"))(client, headers)

  def getPropsAndStatus(path: Uri.Path, headers: Headers, rootUrl: Uri, client: Client[F])(implicit F: Sync[F]): F[GmDataResponse[String]] = sendRequest(client, headers, Method.GET(parseUrl(rootUrl, s"props/$path")), handleRawResponse)

  def getList[X](path: Uri.Path, headers: Headers, rootUrl: Uri, client: Client[F])(implicit decoder: Decoder[X], F: Sync[F]): F[List[X]] = get(parseUrl(rootUrl, s"list/$path"))(client, headers)

  def getFileList(path: Uri.Path, headers: Headers, rootUrl: Uri, client: Client[F])(implicit F: Sync[F]) = getList[Metadata](path, headers, rootUrl, client)

  def streamFileList(path: Uri.Path, headers: Headers, rootUrl: Uri, client: Client[F])(implicit F: Sync[F]) = {
    Stream.eval(Method.GET(parseUrl(rootUrl, s"list/$path"))).flatMap{ request =>
      stream(client, headers, request).through(defaultHandleStreamingJsonResponse[Metadata](request))
    }
  }

  def streamFile(path: Uri.Path, headers: Headers, rootUrl: Uri, client: Client[F])(implicit F: Sync[F]) = {
    Stream.eval(Method.GET(parseUrl(rootUrl, s"stream/$path"))).flatMap{ request =>
      stream(client, headers, request).through(defaultHandleStreamingFileResponse(request))
    }
  }

  def createObject(multipart: Multipart[F], rootUrl: Uri, headers: Headers, client: Client[F])(implicit F: Sync[F]) = {
    val request = Method.POST(multipart, rootUrl / "write")
    sendRequest[List[Metadata]](client, multipart.headers ++ headers, request, defaultHandleJsonResponseFunction)
  }

  def createFolder(metadata: List[Metadata], rootUrl: Uri, headers: Headers, client: Client[F])(implicit F: Sync[F]): F[List[Metadata]] = {
    val body = metadata.asJson.deepDropNullValues.noSpaces
    val multipart = Multipart[F](Vector(Part.formData("meta", body)))
    createObject(multipart, rootUrl, headers, client)
  }

  def createFile(metadata: List[Metadata], rootUrl: Uri, headers: Headers, fileStream: Stream[F, Byte], client: Client[F])(implicit F: Sync[F]): F[List[Metadata]] = {
    val body = metadata.asJson.deepDropNullValues.noSpaces
    val multipart = createMultipartWithFile(body, metadata.head.name, fileStream)
    createObject(multipart, rootUrl, headers, client)
  }

  def createMultipartWithFile(metadata: String, fileName: String, fileStream: Stream[F, Byte])(implicit F: Sync[F]): Multipart[F] = {
    val contentType = `Content-Type`(MediaType.application.json)
    Multipart[F](Vector(Part.formData("meta", metadata), Part.fileData("blob", fileName, fileStream, contentType)))
  }

  def getUserFolderName(rootUrl: Uri, headers: Headers, config: Config, client: Client[F])(implicit F: Sync[F]): F[Either[Throwable, String]] =
    getSelf(rootUrl, client, headers).attempt.map(_.flatMap(_.getUserField(config.GMDATA_NAMESPACE_USERFIELD)))

  def getUserFolderPath(rootUrl: Uri, headers: Headers, config: Config, client: Client[F])(implicit F: Sync[F]): F[Either[Throwable, String]] =
    getUserFolderName(rootUrl, headers, config, client).map(_.map(config.GMDATA_NAMESPACE_OID + "/" + _))

  def getUserFolderMetadata(rootUrl: Uri, headers: Headers, client: Client[F], config: Config)(implicit F: Sync[F]): F[Either[Throwable, Metadata]] = for {
    userFolderPath <- getUserFolderPath(rootUrl, headers, config, client)
    metadata <- userFolderPath.flatTraverse(getProps(_, headers, rootUrl, client).attempt)
  } yield metadata

  def createUserFolderMetadata(config: Config, name: String, objectPolicy: Option[Json], security: Option[Security], originalObjectPolicy: Option[String], custom: Option[Json], policy: Option[Policy]) = Metadata(parentoid = config.GMDATA_NAMESPACE_OID,
      name = name,
      objectpolicy = objectPolicy,
      mimetype = None,
      size = None,
      action = "U",
      security = security,
      originalObjectPolicy = originalObjectPolicy,
      custom = custom,
      oid = None,
      tstamp = None,
      policy = policy,
      isdir = Some(true),
      isfile = Some(false))


  def writeUserFolder(rootUrl: Uri, headers: Headers, config: Config, client: Client[F], objectPolicy: Option[Json], security: Option[Security] = None, originalObjectPolicy: Option[String] = None, custom: Option[Json] = None, policy: Option[Policy] = None)(implicit F: Sync[F]) = for {
    metadataEither <- getUserFolderName(rootUrl, headers, config, client).map {
      _.map(createUserFolderMetadata(config, _, objectPolicy, security, originalObjectPolicy, custom, policy))
    }
    write <- metadataEither.flatTraverse(metadata => createFolder(List(metadata), rootUrl, headers, client).attempt)
  } yield write.map(_.head)

  def getOrCreateUserFolder(rootUrl: Uri, headers: Headers, client: Client[F], objectPolicy: Option[Json], security: Option[Security] = None, originalObjectPolicy: Option[String] = None, custom: Option[Json] = None, policy: Option[Policy] = None)(implicit F: Sync[F]) =for {
    configEither <- getConfig(rootUrl, client, headers).attempt
    metadata <- configEither.flatTraverse{ config =>
      getUserFolderMetadata(rootUrl, headers, client, config).flatMap{
        case Right(metadata) => F.delay(metadata).attempt
        case Left(_) => writeUserFolder(rootUrl, headers, config, client, objectPolicy, security, originalObjectPolicy, custom, policy)
      }
    }
  } yield metadata

  def append(metadata: Metadata, file: Stream[F, Byte], rootUrl: Uri, headers: Headers, client: Client[F])(implicit F: Sync[F]): Stream[F, List[Metadata]] = for {
    lastFile <- streamFileList(metadata.parentoid, headers, rootUrl, client).fold("")((last, curr) => List(last, curr.name).max)
    write <- Stream.eval(createFile(List(metadata.copy(name = getNextFileName(lastFile))), rootUrl, headers, file, client))
  } yield write

  def getNextFileName(fileName: String) ={
    if(fileName.isEmpty || !(('a' to 'z') contains (fileName.last + 1))) fileName + "a"
    else fileName.dropRight(1) + fileName.last + 1
  }

  def createAppendableFileFolder(metadata: Metadata, rootUrl: Uri, headers: Headers, client: Client[F])(implicit F: Sync[F]) = {
    val folderMetadata = metadata.copy(isfile = Some(false), isdir = Some(true), mimetype = None)
    createFolder(List(folderMetadata), rootUrl, headers, client)
  }

  def createAppendableFile(metadata: Metadata, file: Stream[F, Byte], rootUrl: Uri, headers: Headers, client: Client[F], chunk: Option[Int] = None)(implicit F: Sync[F]) ={
    for{
      folderOidEither <- Stream.eval(createAppendableFileFolder(metadata, rootUrl, headers, client).attempt.map(_.map(_.head.oid.get)))
      newMetadataEither = folderOidEither.map(folderOid => metadata.copy(parentoid = folderOid))
      writtenMetadata <- newMetadataEither.flatTraverse{ newMetadata =>
        chunk.map(chunkFile(file, _).flatMap(append(newMetadata, _, rootUrl, headers, client))).getOrElse(append(newMetadata, file, rootUrl, headers, client)).attempt
      }
    } yield writtenMetadata
  }

  def chunkFile(file: Stream[F, Byte], chunkBytes: Int)(implicit F: Sync[F]) = file.chunkN(chunkBytes).map(chunk => Stream.emits(chunk.toList).covary[F])


  def parseUrl(rootUrl: Uri, path: String) = {
    val url = s"$rootUrl/${Uri.pathEncode(path)}"
    Uri.fromString(url) match {
      case Right(url) => url
      case Left(err) => throw new Throwable(s"$url is an invalid URL: $err")
    }
  }



  def defaultClient(sslContext: Option[SSLContext] = None)(implicit F: Sync[F], ec: ExecutionContext, ce: ConcurrentEffect[F]) = BlazeClientBuilder[F](ec, sslContext).withCheckEndpointAuthentication(false)
}
