# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from libcloud.common.base import XmlResponse, ConnectionKey

__all__ = ["ZonomiException", "ZonomiResponse", "ZonomiConnection"]

# Endpoint for Zonomi API.
API_HOST = "zonomi.com"

SPECIAL_ERRORS = [
    "Not found.",
    "ERROR: This zone is already in your zone list.",
    "Record not deleted.",
]


class ZonomiException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        self.args = (code, message)

    def __str__(self):
        return "{} {}".format(self.code, self.message)

    def __repr__(self):
        return "ZonomiException {} {}".format(self.code, self.message)


class ZonomiResponse(XmlResponse):
    errors = None
    objects = None

    def __init__(self, response, connection):
        self.errors = []
        super().__init__(response=response, connection=connection)
        self.objects, self.errors = self.parse_body_and_errors()
        if self.errors:
            raise self._make_excp(self.errors[0])

    def parse_body_and_errors(self):
        error_dict = {}
        actions = None
        result_counts = None
        action_childrens = None
        data = []
        errors = []
        xml_body = super().parse_body()

        # pylint: disable=no-member
        # Error handling
        if xml_body.text is not None and xml_body.tag == "error":
            error_dict["ERRORCODE"] = self.status
            if xml_body.text.startswith("ERROR: No zone found for"):
                error_dict["ERRORCODE"] = "404"
                error_dict["ERRORMESSAGE"] = "Not found."
            else:
                error_dict["ERRORMESSAGE"] = xml_body.text
            errors.append(error_dict)

        # Data handling
        children = list(xml_body)
        if len(children) == 3:
            result_counts = children[1]
            actions = children[2]

        if actions is not None:
            actions_childrens = list(actions)
            action = actions_childrens[0]
            action_childrens = list(action)

        if action_childrens is not None:
            for child in action_childrens:
                if child.tag == "zone" or child.tag == "record":
                    data.append(child.attrib)

        if result_counts is not None and result_counts.attrib.get("deleted") == "1":
            data.append("DELETED")

        if (
            result_counts is not None
            and result_counts.attrib.get("deleted") == "0"
            and action.get("action") == "DELETE"
        ):
            error_dict["ERRORCODE"] = self.status
            error_dict["ERRORMESSAGE"] = "Record not deleted."
            errors.append(error_dict)

        return (data, errors)

    def success(self):
        return len(self.errors) == 0

    def _make_excp(self, error):
        """
        :param error: contains error code and error message
        :type error: dict
        """
        return ZonomiException(error["ERRORCODE"], error["ERRORMESSAGE"])


class ZonomiConnection(ConnectionKey):
    host = API_HOST
    responseCls = ZonomiResponse

    def add_default_params(self, params):
        """
        Adds default parameters to perform a request,
        such as api_key.
        """
        params["api_key"] = self.key

        return params

    def add_default_headers(self, headers):
        """
        Adds default headers needed to perform a successful
        request such as Content-Type, User-Agent.
        """
        headers["Content-Type"] = "text/xml;charset=UTF-8"

        return headers
