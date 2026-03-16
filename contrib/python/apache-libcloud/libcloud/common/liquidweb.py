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

from libcloud.utils.py3 import b
from libcloud.common.base import JsonResponse, ConnectionUserAndKey
from libcloud.common.types import ProviderError

__all__ = [
    "API_HOST",
    "LiquidWebException",
    "LiquidWebResponse",
    "LiquidWebConnection",
]


# Endpoint for liquidweb api.
API_HOST = "api.stormondemand.com"


class LiquidWebException(ProviderError):
    """The base class for other Liquidweb exceptions"""

    def __init__(self, value, http_code, extra=None):
        """
        :param value: message contained in error
        :type  value: ``str``

        :param http_code: error code
        :type  http_code: ``int``

        :param extra: extra fields specific to error type
        :type  extra: ``list``
        """
        self.extra = extra
        super().__init__(value, http_code, driver=None)

    def __str__(self):
        return "{}  {}".format(self.http_code, self.value)

    def __repr__(self):
        return "LiquidWebException {} {}".format(self.http_code, self.value)


class APIException(LiquidWebException):
    def __init__(self, error_class, full_msg, http_code, extra=None):
        self.error_class = error_class
        super().__init__(full_msg, http_code, extra=extra)

    def __str__(self):
        return "{}: {}".format(self.error_class, self.value)

    def __repr__(self):
        return "{}: {}".format(self.error_class, self.value)


EXCEPTIONS_FIELDS = {
    "LW::Exception::API::Internal": {"fields": []},
    "LW::Exception::API::InvalidEncoding": {"fields": ["encoding"]},
    "LW::Exception::API::InvalidMethod": {"fields": ["method"]},
    "LW::Exception::API::Maintenance": {"fields": []},
    "LW::Exception::API::RateLimit": {"fields": ["account", "ip", "method"]},
    "LW::Exception::Authorization": {"fields": ["username"]},
    "LW::Exception::DNS::NoResponse": {"fields": ["nameservers"]},
    "LW::Exception::DNS::Servfail": {"fields": ["nameservers"]},
    "LW::Exception::Deserialize": {"fields": ["data", "encoding"]},
    "LW::Exception::DuplicateRecord": {"fields": ["field", "input", "statement"]},
    "LW::Exception::Forbidden": {"fields": []},
    "LW::Exception::Incapable": {"fields": ["capability", "thing"]},
    "LW::Exception::Input": {"fields": ["field"]},
    "LW::Exception::Input::Disallowed": {"fields": ["field"]},
    "LW::Exception::Input::Multiple": {"fields": ["errors", "field", "type"]},
    "LW::Exception::Input::NotInRealm": {"fields": ["field", "valid", "value"]},
    "LW::Exception::Input::OutOfBounds": {"fields": ["field", "max", "min", "value"]},
    "LW::Exception::Input::Required": {"fields": ["field", "position"]},
    "LW::Exception::Input::Unknown": {"fields": ["field", "value"]},
    "LW::Exception::Input::Validation": {"fields": ["field", "type", "value"]},
    "LW::Exception::Permission": {"fields": ["account", "identifier"]},
    "LW::Exception::RecordNotFound": {"fields": ["field", "input"]},
    "LW::Exception::RemoteService::Authorization": {"fields": ["url"]},
    "LW::Exception::Resource": {"fields": ["resource"]},
    "LW::Exception::Resource::Insufficient": {"fields": ["available", "requested", "resource"]},
    "LW::Exception::Resource::Unavailable": {"fields": ["resource"]},
    "LW::Exception::Serialize": {"fields": ["data", "encoding"]},
    "LW::Exception::Workflow::Conflict": {"fields": ["conflict", "workflow"]},
}


class LiquidWebResponse(JsonResponse):
    objects = None
    errors = None

    def __init__(self, response, connection):
        self.errors = []
        super().__init__(response=response, connection=connection)
        self.objects, self.errors = self.parse_body_and_errors()
        if self.errors:
            error = self.errors.pop()
            raise self._make_excp(error, self.status)

    def parse_body_and_errors(self):
        data = []
        errors = []
        js = super().parse_body()
        if "items" in js:
            data.append(js["items"])

        if "name" in js:
            data.append(js)

        if "deleted" in js:
            data.append(js["deleted"])

        if "error_class" in js:
            errors.append(js)

        return (data, errors)

    def success(self):
        """
        Returns ``True`` if our request is successful.
        """
        return len(self.errors) == 0

    def _make_excp(self, error, status):
        """
        Raise LiquidWebException.
        """
        exc_type = error.get("error_class")
        message = error.get("full_message")
        try:
            _type = EXCEPTIONS_FIELDS[exc_type]
            fields = _type.get("fields")
            extra = {}
        except KeyError:
            fields = []

        for field in fields:
            extra[field] = error.get(field)
        return APIException(exc_type, message, status, extra=extra)


class LiquidWebConnection(ConnectionUserAndKey):
    host = API_HOST
    responseCls = LiquidWebResponse

    def add_default_headers(self, headers):
        b64string = b("{}:{}".format(self.user_id, self.key))
        encoded = base64.b64encode(b64string).decode("utf-8")
        authorization = "Basic " + encoded

        headers["Authorization"] = authorization
        headers["Content-Type"] = "application/json"

        return headers
