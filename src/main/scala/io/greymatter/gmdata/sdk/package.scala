package io.greymatter.gmdata

import io.circe.Json

package object sdk {

  case class Metadata(parentoid: String,
                      name: String,
                      objectpolicy: Option[Json],
                      mimetype: Option[String],
                      size: Option[Long],
                      action: String,
                      security: Option[Security],
                      originalObjectPolicy: Option[String],
                      custom: Option[Json],
                      oid: Option[String],
                      tstamp: Option[String],
                      policy: Option[Policy],
                      isdir: Option[Boolean],
                      isfile: Option[Boolean] = Some(true),
                      allowPartialPermissions: Option[Boolean] = None,
                      sha256plain: Option[String] = None)

  case class Config(GMDATA_NAMESPACE_OID: String, GMDATA_NAMESPACE_USERFIELD: String)

  case class Self(values: Map[String, Option[List[String]]]) {
    val getUserField = (userField: String) => values.mapValues(_.map(_.head))(userField).toRight(new Throwable(s"/self did not return a value for $userField"))
  }

  case class ObjectPolicy(label: String, requirements: Requirements)

  case class Requirements(f: Option[String] = None, a: Option[List[Requirements]] = None, v: Option[String] = None)

  case class Security(label: String, foreground: String, background: String)

  case class Policy(policy: List[String] = List("C", "R", "U", "D", "X", "P"))

  case class GmDataResponse[X](response: X, statusCode: Int)

  case class ObjectPolicyJson(json: Json)

  case class ObjectPolicyString(string: String)


}
