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

from typing import Dict, List

from libcloud.common.base import JsonResponse, ConnectionKey

__all__ = ["API_HOST", "DNSPodException", "DNSPodResponse", "DNSPodConnection"]

# Endpoint for dnspod api
API_HOST = "api.dnspod.com"


class DNSPodResponse(JsonResponse):
    errors = []  # type: List[Dict]
    objects = []  # type: List[Dict]

    def __init__(self, response, connection):
        super().__init__(response=response, connection=connection)
        self.errors, self.objects = self.parse_body_and_errors()
        if not self.success():
            raise DNSPodException(code=self.status, message=self.errors.pop()["status"]["message"])

    def parse_body_and_errors(self):
        js = super().parse_body()
        if "status" in js and js["status"]["code"] != "1":
            self.errors.append(js)
        else:
            self.objects.append(js)

        return self.errors, self.objects

    def success(self):
        return len(self.errors) == 0


class DNSPodConnection(ConnectionKey):
    host = API_HOST
    responseCls = DNSPodResponse

    def add_default_headers(self, headers):
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        headers["Accept"] = "text/json"

        return headers


class DNSPodException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        self.args = (code, message)

    def __str__(self):
        return "{} {}".format(self.code, self.message)

    def __repr__(self):
        return "DNSPodException {} {}".format(self.code, self.message)
