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
import time
import random
import string
import hashlib

from libcloud.utils.py3 import httplib, urlencode, basestring
from libcloud.common.base import JsonResponse, ConnectionUserAndKey
from libcloud.common.types import ProviderError, InvalidCredsError

SALT_CHARACTERS = string.ascii_letters + string.digits


class NFSNException(ProviderError):
    def __init__(self, value, http_code, code, driver=None):
        self.code = code
        super().__init__(value, http_code, driver)


class NFSNResponse(JsonResponse):
    def parse_error(self):
        if self.status == httplib.UNAUTHORIZED:
            raise InvalidCredsError("Invalid provider credentials")

        body = self.parse_body()

        if isinstance(body, basestring):
            return body + " (HTTP Code: %d)" % self.status

        error = body.get("error", None)
        debug = body.get("debug", None)
        # If we only have one of "error" or "debug", use the one that we have.
        # If we have both, use both, with a space character in between them.
        value = "No message specified"

        if error is not None:
            value = error

        if debug is not None:
            value = debug

        if error is not None and value is not None:
            value = error + " " + value
        value = value + " (HTTP Code: %d)" % self.status

        return value


class NFSNConnection(ConnectionUserAndKey):
    host = "api.nearlyfreespeech.net"
    responseCls = NFSNResponse
    allow_insecure = False

    def _header(self, action, data):
        """Build the contents of the X-NFSN-Authentication HTTP header. See
        https://members.nearlyfreespeech.net/wiki/API/Introduction for
        more explanation."""
        login = self.user_id
        timestamp = self._timestamp()
        salt = self._salt()
        api_key = self.key
        data = urlencode(data)
        data_hash = hashlib.sha1(data.encode("utf-8")).hexdigest()  # nosec

        string = ";".join((login, timestamp, salt, api_key, action, data_hash))
        string_hash = hashlib.sha1(string.encode("utf-8")).hexdigest()  # nosec

        return ";".join((login, timestamp, salt, string_hash))

    def request(self, action, params=None, data="", headers=None, method="GET"):
        """Add the X-NFSN-Authentication header to an HTTP request."""

        if not headers:
            headers = {}

        if not params:
            params = {}
        header = self._header(action, data)

        headers["X-NFSN-Authentication"] = header

        if method == "POST":
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        return ConnectionUserAndKey.request(self, action, params, data, headers, method)

    def encode_data(self, data):
        """NFSN expects the body to be regular key-value pairs that are not
        JSON-encoded."""

        if data:
            data = urlencode(data)

        return data

    def _salt(self):
        """Return a 16-character alphanumeric string."""
        r = random.SystemRandom()

        return "".join(r.choice(SALT_CHARACTERS) for _ in range(16))

    def _timestamp(self):
        """Return the current number of seconds since the Unix epoch,
        as a string."""

        return str(int(time.time()))
