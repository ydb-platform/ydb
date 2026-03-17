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

from base64 import b64encode

from libcloud.utils.py3 import b, httplib
from libcloud.common.base import JsonResponse, ConnectionUserAndKey


class OnAppResponse(JsonResponse):
    """
    OnApp response class
    """

    def success(self):
        """
        Determine if our request was successful.

        The meaning of this can be arbitrary; did we receive OK status? Did
        the node get created? Were we authenticated?

        :rtype: ``bool``
        :return: ``True`` or ``False``
        """
        return self.status in [httplib.OK, httplib.CREATED, httplib.NO_CONTENT]


class OnAppConnection(ConnectionUserAndKey):
    """
    OnApp connection class
    """

    responseCls = OnAppResponse

    def add_default_headers(self, headers):
        """
        Add Basic Authentication header to all the requests.
        It injects the "Authorization: Basic Base64String===" header
        in each request

        :type  headers: ``dict``
        :param headers: Default input headers

        :rtype:         ``dict``
        :return:        Default input headers with the "Authorization" header.
        """
        b64string = b("{}:{}".format(self.user_id, self.key))
        encoded = b64encode(b64string).decode("utf-8")

        headers["Authorization"] = "Basic " + encoded
        return headers
