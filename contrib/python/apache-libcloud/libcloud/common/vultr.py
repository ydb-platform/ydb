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

from typing import Any, Dict, Optional

from libcloud.utils.py3 import httplib
from libcloud.common.base import JsonResponse, ConnectionKey
from libcloud.compute.base import VolumeSnapshot

__all__ = [
    "API_HOST",
    "VultrConnection",
    "VultrException",
    "VultrResponse",
    "DEFAULT_API_VERSION",
    "VultrResponseV2",
    "VultrConnectionV2",
    "VultrNetwork",
    "VultrNodeSnapshot",
]

# Endpoint for the Vultr API
API_HOST = "api.vultr.com"

DEFAULT_API_VERSION = "2"


class VultrResponse(JsonResponse):
    objects = None
    error_dict = {}  # type: Dict[str, str]
    errors = None
    ERROR_CODE_MAP = {
        400: "Invalid API location. Check the URL that you are using.",
        403: "Invalid or missing API key. Check that your API key is present"
        + " and matches your assigned key.",
        405: "Invalid HTTP method. Check that the method (POST|GET) matches"
        + " what the documentation indicates.",
        412: "Request failed. Check the response body for a more detailed" + " description.",
        500: "Internal server error. Try again at a later time.",
        503: "Rate limit hit. API requests are limited to an average of 1/s."
        + " Try your request again later.",
    }

    def __init__(self, response, connection):
        self.errors = []
        super().__init__(response=response, connection=connection)
        self.objects, self.errors = self.parse_body_and_errors()
        if not self.success():
            raise self._make_excp(self.errors[0])

    def parse_body_and_errors(self):
        """
        Returns JSON data in a python list.
        """
        json_objects = []
        errors = []

        if self.status in self.ERROR_CODE_MAP:
            self.error_dict["ERRORCODE"] = self.status
            self.error_dict["ERRORMESSAGE"] = self.ERROR_CODE_MAP[self.status]
            errors.append(self.error_dict)

        js = super().parse_body()
        if isinstance(js, dict):
            js = [js]

        json_objects.append(js)

        return (json_objects, errors)

    def _make_excp(self, error):
        """
        Convert API error to a VultrException instance
        """

        return VultrException(error["ERRORCODE"], error["ERRORMESSAGE"])

    def success(self):
        return len(self.errors) == 0


class VultrConnection(ConnectionKey):
    """
    A connection to the Vultr API
    """

    host = API_HOST
    responseCls = VultrResponse

    def add_default_params(self, params):
        """
        Returns default params such as api_key which is
        needed to perform an action.Returns a dictionary.
        Example:/v1/server/upgrade_plan?api_key=self.key
        """
        params["api_key"] = self.key

        return params

    def add_default_headers(self, headers):
        """
        Returns default headers such as content-type.
        Returns a dictionary.
        """
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        headers["Accept"] = "text/plain"

        return headers

    def set_path(self):
        self.path = "/v/"
        return self.path


class VultrResponseV2(JsonResponse):
    valid_response_codes = [
        httplib.OK,
        httplib.CREATED,
        httplib.ACCEPTED,
        httplib.NO_CONTENT,
    ]

    def parse_error(self):
        """
        Parse the error body and raise the appropriate exception
        """
        status = self.status
        data = self.parse_body()
        error_msg = data.get("error", "")

        raise VultrException(code=status, message=error_msg)

    def success(self):
        """Check the response for success

        :return: ``bool`` indicating a successful request
        """
        return self.status in self.valid_response_codes


class VultrConnectionV2(ConnectionKey):
    """
    A connection to the Vultr API v2
    """

    host = API_HOST
    responseCls = VultrResponseV2

    def add_default_headers(self, headers):
        headers["Authorization"] = "Bearer %s" % (self.key)
        headers["Content-Type"] = "application/json"
        return headers

    def add_default_params(self, params):
        params["per_page"] = 500
        return params


class VultrException(Exception):
    """
    Error originating from the Vultr API
    """

    def __init__(self, code, message):
        self.code = code
        self.message = message
        self.args = (code, message)

    def __str__(self):
        return "(%u) %s" % (self.code, self.message)

    def __repr__(self):
        return "VultrException code %u '%s'" % (self.code, self.message)


class VultrNetwork:
    """
    Represents information about a Vultr private network.
    """

    def __init__(
        self,
        id: str,
        cidr_block: str,
        location: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.id = id
        self.cidr_block = cidr_block
        self.location = location
        self.extra = extra or {}

    def __repr__(self):
        return "<Vultrnetwork: id={} cidr_block={} location={}>".format(
            self.id,
            self.cidr_block,
            self.location,
        )


class VultrNodeSnapshot(VolumeSnapshot):
    def __repr__(self):
        return "<VultrNodeSnapshot id={} size={} driver={} state={}>".format(
            self.id,
            self.size,
            self.driver.name,
            self.state,
        )
