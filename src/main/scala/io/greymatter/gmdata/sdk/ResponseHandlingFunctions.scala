package io.greymatter.gmdata.sdk

import io.circe.Decoder
import io.circe.fs2._
import org.http4s.{Request, Response}
import io.circe.parser.decode
import cats.implicits._
import fs2.Stream
import cats.effect.Sync
import org.http4s.Status.Successful
trait ResponseHandlingFunctions[F[_]] {

  def defaultHandleJsonResponseFunction[X](request: F[Request[F]])(implicit decoder: Decoder[X], F: Sync[F]): PartialFunction[Response[F], F[X]] = {
    case Successful(resp) => resp.as[String].map { response =>
      decode[X](response) match {
        case Right(some) => some
        case Left(err) => throw new Throwable(s"There was a problem decoding $response: $err")
      }
    }
    case errResponse => request.flatMap(handleGmDataError(errResponse, _))
  }

  def handleRawResponse(request: F[Request[F]])(implicit F: Sync[F]): PartialFunction[Response[F], F[GmDataResponse[String]]] = {
    case Successful(resp) => resp.as[String].map(GmDataResponse[String](_, resp.status.code))
    case errResponse => request.flatMap(req => errResponse.as[String].map(err => GmDataResponse(s"There was an error response from ${req.uri}: $err", errResponse.status.code)))
  }

  def defaultHandleStreamingJsonResponse[X](request: Request[F])(stream: Stream[F, Response[F]])(implicit F: Sync[F], d: Decoder[X]) = stream.flatMap {
    case Successful(resp: Response[F]) => resp.body.through(byteArrayParser).through(decoder[F, X])
    case errResponse => Stream.eval(handleGmDataError[X](errResponse, request))
  }

  def defaultHandleStreamingFileResponse(request: Request[F])(stream: Stream[F, Response[F]])(implicit F: Sync[F]) = stream.flatMap {
    case Successful(resp: Response[F]) => resp.body
    case errResponse => Stream.eval(handleGmDataError[Byte](errResponse, request))
  }

  def handleGmDataError[X](response: Response[F], request: Request[F])(implicit F: Sync[F]): F[X] = response.as[String].map(resp => throw gmDataError(resp, request.uri.renderString, response.status.code))

  def gmDataError(error: String, uri: String, code: Int) = new Throwable(s"There was an error response from $uri with response code $code: $error")
}
