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
from libcloud.common.base import JsonResponse, ConnectionUserAndKey


class DNSimpleDNSResponse(JsonResponse):
    def success(self):
        """
        Determine if our request was successful.

        The meaning of this can be arbitrary; did we receive OK status? Did
        the node get created? Were we authenticated?

        :rtype: ``bool``
        :return: ``True`` or ``False``
        """
        # response.success() only checks for 200 and 201 codes. Should we
        # add 204?
        return self.status in [httplib.OK, httplib.CREATED, httplib.NO_CONTENT]


class DNSimpleDNSConnection(ConnectionUserAndKey):
    host = "api.dnsimple.com"
    responseCls = DNSimpleDNSResponse

    def add_default_headers(self, headers):
        """
        Add headers that are necessary for every request

        This method adds ``token`` to the request.
        """
        # TODO: fijarse sobre que info se paso como parametro y en base
        # a esto, fijar el header
        headers["X-DNSimple-Token"] = "{}:{}".format(self.user_id, self.key)
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"
        return headers
