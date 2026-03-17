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

import os
import json
import time
import base64

import requests

from libcloud.common.base import JsonResponse, ConnectionKey

# normally we only use itsyou.online but you might want to work
# against staging.itsyou.online for some testing
IYO_URL = os.environ.get("IYO_URL", "https://itsyou.online")


class G8Connection(ConnectionKey):
    """
    Connection class for G8
    """

    responseCls = JsonResponse

    def add_default_headers(self, headers):
        """
        Add headers that are necessary for every request
        """
        self.driver.key = maybe_update_jwt(self.driver.key)
        headers["Authorization"] = "bearer {}".format(self.driver.key)
        headers["Content-Type"] = "application/json"
        return headers


def base64url_decode(input):
    """
    Helper method to base64url_decode a string.

    :param input: Input to decode
    :type  input: str

    :rtype: str
    """
    rem = len(input) % 4

    if rem > 0:
        input += b"=" * (4 - rem)
    return base64.urlsafe_b64decode(input)


def is_jwt_expired(jwt):
    """
    Check if jwt is expired

    :param jwt: jwt token to validate expiration
    :type  jwt: str

    :rtype: bool
    """
    jwt = jwt.encode("utf-8")
    signing_input, _ = jwt.rsplit(b".", 1)
    _, claims_segment = signing_input.split(b".", 1)
    claimsdata = base64url_decode(claims_segment)
    if isinstance(claimsdata, bytes):
        claimsdata = claimsdata.decode("utf-8")
    data = json.loads(claimsdata)
    # check if it's about to expire in the next minute
    return data["exp"] < time.time() + 60


def maybe_update_jwt(jwt):
    """
    Update jwt if it is expired

    :param jwt: jwt token to validate expiration
    :type  jwt: str

    :rtype: str
    """

    if is_jwt_expired(jwt):
        return refresh_jwt(jwt)
    return jwt


def refresh_jwt(jwt):
    """
    Refresh jwt

    :param jwt: jwt token to refresh
    :type  jwt: str

    :rtype: str
    """
    url = IYO_URL + "/v1/oauth/jwt/refresh"
    headers = {"Authorization": "bearer {}".format(jwt)}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text
