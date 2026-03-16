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

import base64
from typing import Dict, List

from libcloud.utils.py3 import b
from libcloud.common.base import JsonResponse, ConnectionUserAndKey

__all__ = ["API_HOST", "LuadnsException", "LuadnsResponse", "LuadnsConnection"]

# Endpoint for luadns api
API_HOST = "api.luadns.com"


class LuadnsResponse(JsonResponse):
    errors = []  # type: List[Dict]
    objects = []  # type: List[Dict]

    def __init__(self, response, connection):
        super().__init__(response=response, connection=connection)
        self.errors, self.objects = self.parse_body_and_errors()
        if not self.success():
            raise LuadnsException(code=self.status, message=self.errors.pop()["message"])

    def parse_body_and_errors(self):
        js = super().parse_body()
        if "message" in js:
            self.errors.append(js)
        else:
            self.objects.append(js)

        return self.errors, self.objects

    def success(self):
        return len(self.errors) == 0


class LuadnsConnection(ConnectionUserAndKey):
    host = API_HOST
    responseCls = LuadnsResponse

    def add_default_headers(self, headers):
        b64string = b("{}:{}".format(self.user_id, self.key))
        encoded = base64.b64encode(b64string).decode("utf-8")
        authorization = "Basic " + encoded

        headers["Accept"] = "application/json"
        headers["Authorization"] = authorization

        return headers


class LuadnsException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        self.args = (code, message)

    def __str__(self):
        return "{} {}".format(self.code, self.message)

    def __repr__(self):
        return "Luadns {} {}".format(self.code, self.message)
