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
import re

from libcloud.common.base import Response, ConnectionUserAndKey
from libcloud.common.types import ProviderError

OK_CODES = ["200", "211", "212", "213"]
ERROR_CODES = [
    "401",
    "403",
    "405",
    "406",
    "407",
    "408",
    "409",
    "410",
    "411",
    "412",
    "413",
    "414",
    "450",
    "451",
]


class WorldWideDNSException(ProviderError):
    def __init__(self, value, http_code, code, driver=None):
        self.code = code
        super().__init__(value, http_code, driver)


class SuspendedAccount(WorldWideDNSException):
    def __init__(self, http_code, driver=None):
        value = "Login ID you supplied is SUSPENDED, you need to renew" + " your account"
        super().__init__(value, http_code, 401, driver)


class LoginOrPasswordNotMatch(WorldWideDNSException):
    def __init__(self, http_code, driver=None):
        value = "Login ID and/or Password you supplied is not on file or" + " does not match"
        super().__init__(value, http_code, 403, driver)


class NonExistentDomain(WorldWideDNSException):
    def __init__(self, http_code, driver=None):
        value = "Domain name supplied is not in your account"
        super().__init__(value, http_code, 405, driver)


class CouldntRemoveDomain(WorldWideDNSException):
    def __init__(self, http_code, driver=None):
        value = "Error occurred removing domain from name server, try again"
        super().__init__(value, http_code, 406, driver)


class LimitExceeded(WorldWideDNSException):
    def __init__(self, http_code, driver=None):
        value = "Your limit was exceeded, you need to upgrade your account"
        super().__init__(value, http_code, 407, driver)


class ExistentDomain(WorldWideDNSException):
    def __init__(self, http_code, driver=None):
        value = "Domain already exists on our servers"
        super().__init__(value, http_code, 408, driver)


class DomainBanned(WorldWideDNSException):
    def __init__(self, http_code, driver=None):
        value = "Domain is listed in DNSBL and is banned from our servers"
        super().__init__(value, http_code, 409, driver)


class InvalidDomainName(WorldWideDNSException):
    def __init__(self, http_code, driver=None):
        value = "Invalid domain name"
        super().__init__(value, http_code, 410, driver)


class ErrorOnReloadInNameServer(WorldWideDNSException):
    def __init__(self, server, http_code, driver=None):
        value, code = "unknown", "unknown"
        if server == 1:
            value = "Name server #1 kicked an error on reload, contact support"
            code = 411
        elif server == 2:
            value = "Name server #2 kicked an error on reload, contact support"
            code = 412
        elif server == 3:
            value = "Name server #3 kicked an error on reload, contact support"
            code = 413
        super().__init__(value, http_code, code, driver)


class NewUserNotValid(WorldWideDNSException):
    def __init__(self, http_code, driver=None):
        value = "New userid is not valid"
        super().__init__(value, http_code, 414, driver)


class CouldntReachNameServer(WorldWideDNSException):
    def __init__(self, http_code, driver=None):
        value = "Couldn't reach the name server, try again later"
        super().__init__(value, http_code, 450, driver)


class NoZoneFile(WorldWideDNSException):
    def __init__(self, http_code, driver=None):
        value = "No zone file in the name server queried"
        super().__init__(value, http_code, 451, driver)


ERROR_CODE_TO_EXCEPTION_CLS = {
    "401": SuspendedAccount,
    "403": LoginOrPasswordNotMatch,
    "405": NonExistentDomain,
    "406": CouldntRemoveDomain,
    "407": LimitExceeded,
    "408": ExistentDomain,
    "409": DomainBanned,
    "410": InvalidDomainName,
    "411": ErrorOnReloadInNameServer,
    "412": ErrorOnReloadInNameServer,
    "413": ErrorOnReloadInNameServer,
    "414": NewUserNotValid,
    "450": CouldntReachNameServer,
    "451": NoZoneFile,
}


class WorldWideDNSResponse(Response):
    def parse_body(self):
        """
        Parse response body.

        :return: Parsed body.
        :rtype: ``str``
        """
        if self._code_response(self.body):
            codes = re.split("\r?\n", self.body)
            for code in codes:
                if code in OK_CODES:
                    continue
                elif code in ERROR_CODES:
                    exception = ERROR_CODE_TO_EXCEPTION_CLS.get(code)
                    if code in ["411", "412", "413"]:
                        server = int(code[2])
                        raise exception(server, self.status)
                    raise exception(self.status)
        return self.body

    def _code_response(self, body):
        """
        Checks if the response body contains code status.

        :rtype: ``bool``
        """
        available_response_codes = OK_CODES + ERROR_CODES
        codes = re.split("\r?\n", body)
        if codes[0] in available_response_codes:
            return True
        return False


class WorldWideDNSConnection(ConnectionUserAndKey):
    host = "www.worldwidedns.net"
    responseCls = WorldWideDNSResponse

    def add_default_params(self, params):
        """
        Add parameters that are necessary for every request

        This method adds ``NAME`` and ``PASSWORD`` to
        the request.
        """
        params["NAME"] = self.user_id
        params["PASSWORD"] = self.key

        reseller_id = getattr(self, "reseller_id", None)
        if reseller_id:
            params["ID"] = reseller_id

        return params
