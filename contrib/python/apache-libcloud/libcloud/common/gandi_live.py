# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Gandi Live driver base classes
"""

import json

from libcloud.utils.py3 import httplib
from libcloud.common.base import JsonResponse, ConnectionKey
from libcloud.common.types import ProviderError

__all__ = [
    "API_HOST",
    "GandiLiveBaseError",
    "JsonParseError",
    "ResourceNotFoundError",
    "InvalidRequestError",
    "ResourceConflictError",
    "GandiLiveResponse",
    "GandiLiveConnection",
    "BaseGandiLiveDriver",
]

API_HOST = "dns.api.gandi.net"


class GandiLiveBaseError(ProviderError):
    """
    Exception class for Gandi Live driver
    """

    pass


class JsonParseError(GandiLiveBaseError):
    pass


# Example:
# {
#   "code": 404,
#   "message": "Unknown zone",
#   "object": "LocalizedHTTPNotFound",
#   "cause": "Not Found"
# }
class ResourceNotFoundError(GandiLiveBaseError):
    pass


# Example:
# {
#    "code": 400,
#    "message": "zone or zone_uuid must be set",
#    "object": "HTTPBadRequest",
#    "cause": "No zone set.",
#    "errors": [
#      {
#        "location": "body",
#        "name": "zone_uuid",
#        "description": "\"FAKEUUID\" is not a UUID"
#      }
#   ]
# }
class InvalidRequestError(GandiLiveBaseError):
    pass


# Examples:
# {
#   "code": 409,
#   "message": "Zone Testing already exists",
#   "object": "HTTPConflict",
#   "cause": "Duplicate Entry"
# }
# {
#   "code": 409,
#   "message": "The domain example.org already exists",
#   "object": "HTTPConflict",
#   "cause": "Duplicate Entry"
# }
# {
#   "code": 409,
#   "message": "This zone is still used by 1 domains",
#   "object": "HTTPConflict",
#   "cause": "In use"
# }
class ResourceConflictError(GandiLiveBaseError):
    pass


class GandiLiveResponse(JsonResponse):
    """
    A Base Gandi Live Response class to derive from.
    """

    def success(self):
        """
        Determine if our request was successful.

        For the Gandi Live response class, tag all responses as successful and
        raise appropriate Exceptions from parse_body.

        :return: C{True}
        """

        return True

    def parse_body(self):
        """
        Parse the JSON response body, or raise exceptions as appropriate.

        :return:  JSON dictionary
        :rtype:   ``dict``
        """
        json_error = False
        try:
            body = json.loads(self.body)
        except Exception:
            # If there is both a JSON parsing error and an unsuccessful http
            # response (like a 404), we want to raise the http error and not
            # the JSON one, so don't raise JsonParseError here.
            body = self.body
            json_error = True

        # Service does not appear to return HTTP 202 Accepted for anything.
        valid_http_codes = [
            httplib.OK,
            httplib.CREATED,
        ]
        if self.status in valid_http_codes:
            if json_error:
                raise JsonParseError(body, self.status)
            else:
                return body
        elif self.status == httplib.NO_CONTENT:
            # Parse error for empty body is acceptable, but a non-empty body
            # is not.
            if len(body) > 0:
                msg = '"No Content" response contained content'
                raise GandiLiveBaseError(msg, self.status)
            else:
                return {}
        elif self.status == httplib.NOT_FOUND:
            message = self._get_error(body, json_error)
            raise ResourceNotFoundError(message, self.status)
        elif self.status == httplib.BAD_REQUEST:
            message = self._get_error(body, json_error)
            raise InvalidRequestError(message, self.status)
        elif self.status == httplib.CONFLICT:
            message = self._get_error(body, json_error)
            raise ResourceConflictError(message, self.status)
        else:
            message = self._get_error(body, json_error)
            raise GandiLiveBaseError(message, self.status)

    # Errors are not described at all in Gandi's official documentation.
    # It appears when an error arises, a JSON object is returned along with
    # an HTTP 4xx class code.  The object is structured as:
    # {
    #   code: <code>,
    #   object: <object>,
    #   message: <message>,
    #   cause: <cause>,
    #   errors: [
    #      {
    #         location: <error-location>,
    #         name: <error-name>,
    #         description: <error-description>
    #      }
    #   ]
    # }
    # where
    #  <code> is a number equal to the HTTP response status code
    #  <object> is a string with some internal name for the status code
    #  <message> is a string detailing what the problem is
    #  <cause> is a string that comes from a set of succinct problem summaries
    #  errors is optional; if present:
    #  <error-location> is a string for which part of the request to look in
    #  <error-name> is a string naming the parameter
    #  <error-description> is a string detailing what the problem is
    # Here we ignore object and combine message and cause along with an error
    # if one or more exists.
    def _get_error(self, body, json_error):
        """
        Get the error code and message from a JSON response.

        Incorporate the first error if there are multiple errors.

        :param  body: The body of the JSON response dictionary
        :type   body: ``dict``

        :return:  String containing error message
        :rtype:   ``str``
        """
        if not json_error and "cause" in body:
            message = "{}: {}".format(body["cause"], body["message"])
            if "errors" in body:
                err = body["errors"][0]
                message = "{} ({} in {}: {})".format(
                    message,
                    err.get("location"),
                    err.get("name"),
                    err.get("description"),
                )
        else:
            message = body

        return message


class GandiLiveConnection(ConnectionKey):
    """
    Connection class for the Gandi Live driver
    """

    responseCls = GandiLiveResponse
    host = API_HOST

    def add_default_headers(self, headers):
        """
        Returns default headers as a dictionary.
        """
        headers["Content-Type"] = "application/json"
        headers["X-Api-Key"] = self.key
        return headers

    def encode_data(self, data):
        """Encode data to JSON"""
        return json.dumps(data)


class BaseGandiLiveDriver:
    """
    Gandi Live base driver
    """

    connectionCls = GandiLiveConnection
    name = "GandiLive"
