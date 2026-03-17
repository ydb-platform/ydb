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

__all__ = ["API_HOST", "BuddyNSException", "BuddyNSResponse", "BuddyNSConnection"]

# Endpoint for buddyns api
API_HOST = "www.buddyns.com"


class BuddyNSResponse(JsonResponse):
    errors = []  # type: List[Dict]
    objects = []  # type: List[Dict]

    def __init__(self, response, connection):
        super().__init__(response=response, connection=connection)
        self.errors, self.objects = self.parse_body_and_errors()
        if not self.success():
            raise BuddyNSException(code=self.status, message=self.errors.pop()["detail"])

    def parse_body_and_errors(self):
        js = super().parse_body()
        if "detail" in js:
            self.errors.append(js)
        else:
            self.objects.append(js)

        return self.errors, self.objects

    def success(self):
        return len(self.errors) == 0


class BuddyNSConnection(ConnectionKey):
    host = API_HOST
    responseCls = BuddyNSResponse

    def add_default_headers(self, headers):
        headers["content-type"] = "application/json"
        headers["Authorization"] = "Token" + " " + self.key

        return headers


class BuddyNSException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        self.args = (code, message)

    def __str__(self):
        return "{} {}".format(self.code, self.message)

    def __repr__(self):
        return "BuddyNSException {} {}".format(self.code, self.message)
