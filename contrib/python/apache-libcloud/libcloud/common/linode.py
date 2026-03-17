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

from libcloud.utils.py3 import httplib
from libcloud.common.base import JsonResponse, ConnectionKey
from libcloud.common.gandi import BaseObject
from libcloud.common.types import LibcloudError, InvalidCredsError

__all__ = [
    "API_HOST",
    "API_ROOT",
    "DEFAULT_API_VERSION",
    "LinodeException",
    "LinodeResponse",
    "LinodeConnection",
    "LinodeResponseV4",
    "LinodeConnectionV4",
    "LinodeExceptionV4",
    "LinodeDisk",
    "LinodeIPAddress",
]

# Endpoint for the Linode API
API_HOST = "api.linode.com"
API_ROOT = "/"


DEFAULT_API_VERSION = "4.0"

# Constants that map a RAM figure to a PlanID (updated 2014-08-25)
LINODE_PLAN_IDS = {
    1024: "1",
    2048: "2",
    4096: "4",
    8192: "6",
    16384: "7",
    32768: "8",
    49152: "9",
    65536: "10",
    98304: "12",
}

# Available filesystems for disk creation
LINODE_DISK_FILESYSTEMS = ["ext3", "ext4", "swap", "raw"]
LINODE_DISK_FILESYSTEMS_V4 = ["ext3", "ext4", "swap", "raw", "initrd"]


class LinodeException(Exception):
    """Error originating from the Linode API

    This class wraps a Linode API error, a list of which is available in the
    API documentation.  All Linode API errors are a numeric code and a
    human-readable description.
    """

    def __init__(self, code, message):
        self.code = code
        self.message = message
        self.args = (code, message)

    def __str__(self):
        return "(%u) %s" % (self.code, self.message)

    def __repr__(self):
        return "<LinodeException code %u '%s'>" % (self.code, self.message)


class LinodeResponse(JsonResponse):
    """
    Linode API response

    Wraps the HTTP response returned by the Linode API.

    libcloud does not take advantage of batching, so a response will always
    reflect the above format. A few weird quirks are caught here as well.
    """

    objects = None

    def __init__(self, response, connection):
        """Instantiate a LinodeResponse from the HTTP response

        :keyword response: The raw response returned by urllib
        :return: parsed :class:`LinodeResponse`"""
        self.errors = []
        super().__init__(response, connection)

        self.invalid = LinodeException(0xFF, "Invalid JSON received from server")

        # Move parse_body() to here;  we can't be sure of failure until we've
        # parsed the body into JSON.
        self.objects, self.errors = self.parse_body()

        if not self.success():
            # Raise the first error, as there will usually only be one
            raise self.errors[0]

    def parse_body(self):
        """Parse the body of the response into JSON objects

        If the response chokes the parser, action and data will be returned as
        None and errorarray will indicate an invalid JSON exception.

        :return: ``list`` of objects and ``list`` of errors"""
        js = super().parse_body()

        try:
            if isinstance(js, dict):
                # solitary response - promote to list
                js = [js]
            ret = []
            errs = []
            for obj in js:
                if "DATA" not in obj or "ERRORARRAY" not in obj or "ACTION" not in obj:
                    ret.append(None)
                    errs.append(self.invalid)
                    continue
                ret.append(obj["DATA"])
                errs.extend(self._make_excp(e) for e in obj["ERRORARRAY"])
            return (ret, errs)
        except Exception:
            return (None, [self.invalid])

    def success(self):
        """Check the response for success

        The way we determine success is by the presence of an error in
        ERRORARRAY.  If one is there, we assume the whole request failed.

        :return: ``bool`` indicating a successful request"""
        return len(self.errors) == 0

    def _make_excp(self, error):
        """Convert an API error to a LinodeException instance

        :keyword error: JSON object containing ``ERRORCODE`` and
        ``ERRORMESSAGE``
        :type error: dict"""
        if "ERRORCODE" not in error or "ERRORMESSAGE" not in error:
            return None
        if error["ERRORCODE"] == 4:
            return InvalidCredsError(error["ERRORMESSAGE"])
        return LinodeException(error["ERRORCODE"], error["ERRORMESSAGE"])


class LinodeConnection(ConnectionKey):
    """
    A connection to the Linode API

    Wraps SSL connections to the Linode API, automagically injecting the
    parameters that the API needs for each request.
    """

    host = API_HOST
    responseCls = LinodeResponse

    def add_default_params(self, params):
        """
        Add parameters that are necessary for every request

        This method adds ``api_key`` and ``api_responseFormat`` to
        the request.
        """
        params["api_key"] = self.key
        # Be explicit about this in case the default changes.
        params["api_responseFormat"] = "json"
        return params


class LinodeExceptionV4(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return "%s" % self.message

    def __repr__(self):
        return "<LinodeException '%s'>" % self.message


class LinodeResponseV4(JsonResponse):
    valid_response_codes = [
        httplib.OK,
        httplib.NO_CONTENT,
    ]

    def parse_body(self):
        """Parse the body of the response into JSON objects
        :return: ``dict`` of objects"""
        return super().parse_body()

    def parse_error(self):
        """
        Parse the error body and raise the appropriate exception
        """
        status = int(self.status)
        data = self.parse_body()
        # Use only the first error, as there'll be only one most of the time
        error = data["errors"][0]
        reason = error.get("reason")
        # The field in the request that caused this error
        field = error.get("field")

        if field is not None:
            error_msg = "{}-{}".format(reason, field)
        else:
            error_msg = reason

        if status in [httplib.UNAUTHORIZED, httplib.FORBIDDEN]:
            raise InvalidCredsError(value=error_msg)

        raise LibcloudError(
            "%s Status code: %d." % (error_msg, status), driver=self.connection.driver
        )

    def success(self):
        """Check the response for success
        :return: ``bool`` indicating a successful request"""
        return self.status in self.valid_response_codes


class LinodeConnectionV4(ConnectionKey):
    """
    A connection to the Linode API

    Wraps SSL connections to the Linode API
    """

    host = API_HOST
    responseCls = LinodeResponseV4

    def add_default_headers(self, headers):
        """
        Add headers that are necessary for every request

        This method adds ``token`` to the request.
        """
        headers["Authorization"] = "Bearer %s" % (self.key)
        headers["Content-Type"] = "application/json"
        return headers

    def add_default_params(self, params):
        """
        Add parameters that are necessary for every request

        This method adds ``page_size`` to the request to reduce the total
        number of paginated requests to the API.
        """
        # pylint: disable=maybe-no-member
        params["page_size"] = 25
        return params


class LinodeDisk(BaseObject):
    def __init__(self, id, state, name, filesystem, driver, size, extra=None):
        super().__init__(id, state, driver)
        self.name = name
        self.size = size
        self.filesystem = filesystem
        self.extra = extra or {}

    def __repr__(self):
        return (
            "<LinodeDisk: id=%s, name=%s, state=%s, size=%s," " filesystem=%s, driver=%s ...>"
        ) % (
            self.id,
            self.name,
            self.state,
            self.size,
            self.filesystem,
            self.driver.name,
        )


class LinodeIPAddress:
    def __init__(self, inet, public, version, driver, extra=None):
        self.inet = inet
        self.public = public
        self.version = version
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return ("<IPAddress: address=%s, public=%r, version=%s, driver=%s ...>") % (
            self.inet,
            self.public,
            self.version,
            self.driver.name,
        )
