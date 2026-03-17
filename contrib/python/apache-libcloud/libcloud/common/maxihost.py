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
from libcloud.common.types import InvalidCredsError


class MaxihostResponse(JsonResponse):
    valid_response_codes = [
        httplib.OK,
        httplib.ACCEPTED,
        httplib.CREATED,
        httplib.NO_CONTENT,
    ]

    def parse_error(self):
        if self.status == httplib.UNAUTHORIZED:
            body = self.parse_body()
            raise InvalidCredsError(body["message"])
        else:
            body = self.parse_body()
            if "message" in body:
                error = "{} (code: {})".format(body["message"], self.status)
            else:
                error = body
            return error

    def success(self):
        return self.status in self.valid_response_codes


class MaxihostConnection(ConnectionKey):
    """
    Connection class for the Maxihost driver.
    """

    host = "api.maxihost.com"
    responseCls = MaxihostResponse

    def add_default_headers(self, headers):
        """
        Add headers that are necessary for every request

        This method adds apikey to the request.
        """
        headers["Authorization"] = "Bearer %s" % (self.key)
        headers["Content-Type"] = "application/json"
        headers["Accept"] = "application/vnd.maxihost.v1.1+json"
        return headers
