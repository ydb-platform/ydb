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
import hashlib
from typing import List

from libcloud.http import LibcloudConnection
from libcloud.utils.py3 import httplib
from libcloud.common.base import JsonResponse, ConnectionUserAndKey
from libcloud.common.types import InvalidCredsError
from libcloud.utils.connection import get_response_object

try:
    import simplejson as json
except ImportError:
    import json  # type: ignore


__all__ = ["OvhResponse", "OvhConnection"]

API_HOST = "api.ovh.com"
API_ROOT = "/1.0"
# From https://us.ovhcloud.com/about/company/data-centers
LOCATIONS = {
    "BHS1": {"id": "BHS1", "name": "Beauharnois, Quebec 1", "country": "CA"},
    "BHS2": {"id": "BHS2", "name": "Beauharnois, Quebec 2", "country": "CA"},
    "BHS3": {"id": "BHS3", "name": "Beauharnois, Quebec 3", "country": "CA"},
    "BHS4": {"id": "BHS4", "name": "Beauharnois, Quebec 4", "country": "CA"},
    "BHS5": {"id": "BHS5", "name": "Beauharnois, Quebec 5", "country": "CA"},
    "BHS6": {"id": "BHS6", "name": "Beauharnois, Quebec 6", "country": "CA"},
    "DC1": {"id": "DC1", "name": "Paris DC1", "country": "FR"},
    "FRA1": {"id": "FRA1", "name": "Frankfurt 1", "country": "DE"},
    "GRA1": {"id": "GRA1", "name": "Gravelines 1", "country": "FR"},
    "GRA2": {"id": "GRA2", "name": "Gravelines 2", "country": "FR"},
    "GSW": {"id": "GSW", "name": "Paris GSW", "country": "FR"},
    "HIL1": {"id": "HIL1", "name": "Hillsboro, Oregon 1", "country": "US"},
    "LON1": {"id": "LON1", "name": "London 1", "country": "UK"},
    "P19": {"id": "P19", "name": "Paris P19", "country": "FR"},
    "RBX1": {"id": "RBX1", "name": "Roubaix 1", "country": "FR"},
    "RBX2": {"id": "RBX2", "name": "Roubaix 2", "country": "FR"},
    "RBX3": {"id": "RBX3", "name": "Roubaix 3", "country": "FR"},
    "RBX4": {"id": "RBX4", "name": "Roubaix 4", "country": "FR"},
    "RBX5": {"id": "RBX5", "name": "Roubaix 5", "country": "FR"},
    "RBX6": {"id": "RBX6", "name": "Roubaix 6", "country": "FR"},
    "RBX7": {"id": "RBX7", "name": "Roubaix 7", "country": "FR"},
    "SBG1": {"id": "SBG1", "name": "Strasbourg 1", "country": "FR"},
    "SBG2": {"id": "SBG2", "name": "Strasbourg 2", "country": "FR"},
    "SBG3": {"id": "SBG3", "name": "Strasbourg 3", "country": "FR"},
    "SGP1": {"id": "SGP1", "name": "Singapore 1", "country": "SG"},
    "SYD1": {"id": "SYD1", "name": "Sydney 1", "country": "AU"},
    "VIN1": {"id": "VIN1", "name": "Vint Hill, Virginia 1", "country": "US"},
    "WAW1": {"id": "WAW1", "name": "Warsaw 1", "country": "PL"},
}
DEFAULT_ACCESS_RULES = [
    {"method": "GET", "path": "/*"},
    {"method": "POST", "path": "/*"},
    {"method": "PUT", "path": "/*"},
    {"method": "DELETE", "path": "/*"},
]


class OvhException(Exception):
    pass


class OvhResponse(JsonResponse):
    def parse_error(self):
        response = super().parse_body()
        response = response or {}

        if response.get("errorCode", None) == "INVALID_SIGNATURE":
            raise InvalidCredsError(
                "Signature validation failed, probably " "using invalid credentials"
            )

        return self.body


class OvhConnection(ConnectionUserAndKey):
    """
    A connection to the Ovh API

    Wraps SSL connections to the Ovh API, automagically injecting the
    parameters that the API needs for each request.
    """

    host = API_HOST
    request_path = API_ROOT
    responseCls = OvhResponse
    timestamp = None
    ua = []  # type: List[str]
    LOCATIONS = LOCATIONS
    _timedelta = None

    allow_insecure = True

    def __init__(self, user_id, *args, **kwargs):
        region = kwargs.pop("region", "")

        if region:
            self.host = ("{}.{}".format(region, API_HOST)).lstrip(".")
        else:
            self.host = API_HOST
        self.consumer_key = kwargs.pop("ex_consumer_key", None)

        if self.consumer_key is None:
            consumer_key_json = self.request_consumer_key(user_id)
            msg = (
                "Your consumer key isn't validated, "
                "go to '%(validationUrl)s' for valid it. After instantiate "
                "your driver with \"ex_consumer_key='%(consumerKey)s'\"." % consumer_key_json
            )
            raise OvhException(msg)
        super().__init__(user_id, *args, **kwargs)

    def request_consumer_key(self, user_id):
        action = self.request_path + "/auth/credential"
        data = json.dumps({"accessRules": DEFAULT_ACCESS_RULES, "redirection": "http://ovh.com"})
        headers = {
            "Content-Type": "application/json",
            "X-Ovh-Application": user_id,
        }
        httpcon = LibcloudConnection(host=self.host, port=443)

        try:
            httpcon.request(method="POST", url=action, body=data, headers=headers)
        except Exception as e:
            handle_and_rethrow_user_friendly_invalid_region_error(host=self.host, e=e)

        response = OvhResponse(httpcon.getresponse(), httpcon)

        if response.status == httplib.UNAUTHORIZED:
            raise InvalidCredsError()

        json_response = response.parse_body()
        httpcon.close()

        return json_response

    def get_timestamp(self):
        if not self._timedelta:
            url = "https://{}{}/auth/time".format(self.host, API_ROOT)
            response = get_response_object(url=url, method="GET", headers={})

            if not response or not response.body:
                raise Exception("Failed to get current time from Ovh API")

            timestamp = int(response.body)
            self._timedelta = timestamp - int(time.time())

        return int(time.time()) + self._timedelta

    def make_signature(self, method, action, params, data, timestamp):
        full_url = "https://{}{}".format(self.host, action)

        if params:
            full_url += "?"

            for key, value in params.items():
                full_url += "{}={}&".format(key, value)
            full_url = full_url[:-1]
        sha1 = hashlib.sha1()  # nosec
        base_signature = "+".join(
            [
                self.key,
                self.consumer_key,
                method.upper(),
                full_url,
                data if data else "",
                str(timestamp),
            ]
        )
        sha1.update(base_signature.encode())
        signature = "$1$" + sha1.hexdigest()

        return signature

    def add_default_params(self, params):
        return params

    def add_default_headers(self, headers):
        headers.update(
            {
                "X-Ovh-Application": self.user_id,
                "X-Ovh-Consumer": self.consumer_key,
                "Content-type": "application/json",
            }
        )

        return headers

    def request(self, action, params=None, data=None, headers=None, method="GET", raw=False):
        data = json.dumps(data) if data else None
        timestamp = self.get_timestamp()
        signature = self.make_signature(method, action, params, data, timestamp)
        headers = headers or {}
        headers.update({"X-Ovh-Timestamp": timestamp, "X-Ovh-Signature": signature})

        try:
            return super().request(
                action,
                params=params,
                data=data,
                headers=headers,
                method=method,
                raw=raw,
            )
        except Exception as e:
            handle_and_rethrow_user_friendly_invalid_region_error(host=self.host, e=e)


def handle_and_rethrow_user_friendly_invalid_region_error(host, e):
    """
    Utility method which throws a more user-friendly error in case "name or
    service not known" error is received when sending a request.

    In most cases this error indicates user passed invalid ``region`` argument
    to the driver constructor.
    """
    msg = str(e).lower()

    error_messages_to_throw = [
        "name or service not known",
        "nodename nor servname provided, or not known",
        "getaddrinfo failed",
    ]

    if any([value for value in error_messages_to_throw if value in msg]):
        raise ValueError(
            'Received "name or service not known" error '
            "when sending a request. This likely "
            "indicates invalid region argument was "
            "passed to the driver constructor."
            "Used host: %s. Original error: %s" % (host, str(e))
        )

    raise e
