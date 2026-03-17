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

"""
Common settings and connection objects for gridscale
"""

try:
    import simplejson as json
except Exception:
    import json  # type: ignore

from libcloud.utils.py3 import httplib
from libcloud.common.base import BaseDriver, JsonResponse, PollingConnection, ConnectionUserAndKey
from libcloud.common.types import InvalidCredsError
from libcloud.compute.types import NodeState

NODE_STATE_MAP = {
    "ACTIVE": NodeState.RUNNING,
    "STOPPED": NodeState.STOPPED,
    "UNKNOWN": NodeState.UNKNOWN,
}


class GridscaleResponse(JsonResponse):
    """
    Gridscale API Response
    """

    valid_response_codes = [httplib.OK, httplib.ACCEPTED, httplib.NO_CONTENT]

    def parse_error(self):
        body = self.parse_body()
        if self.status == httplib.UNAUTHORIZED:
            raise InvalidCredsError(body["message"])
        if self.status == httplib.NOT_FOUND:
            raise Exception("The resource you are looking for has not been found.")
        return self.body

    def success(self):
        return self.status in self.valid_response_codes


class GridscaleConnection(ConnectionUserAndKey, PollingConnection):
    """
    gridscale connection class
    Authentication using uuid and api token

    """

    host = "api.gridscale.io"
    responseCls = GridscaleResponse

    def encode_data(self, data):
        return json.dumps(data)

    def add_default_headers(self, headers):
        """
        add parameters that are necessary for each request to be successful

        :param headers: Authentication token
        :type headers: ``str``
        :return: None
        """
        headers["X-Auth-UserId"] = self.user_id
        headers["X-Auth-Token"] = self.key
        headers["Content-Type"] = "application/json"

        return headers

    def async_request(self, *poargs, **kwargs):
        self.async_request_counter = 0
        self.request_method = "_poll_request_initial"
        return super().async_request(*poargs, **kwargs)

    def _poll_request_initial(self, **kwargs):
        if self.async_request_counter == 0:
            self.poll_response_initial = super().request(**kwargs)
            r = self.poll_response_initial
            self.async_request_counter += 1
        else:
            r = self.request(**kwargs)

        return r

    def get_poll_request_kwargs(self, response, context, request_kwargs):
        endpoint_url = "requests/{}".format(response.object["request_uuid"])
        kwargs = {"action": endpoint_url}
        return kwargs

    def has_completed(self, response):
        if response.status == 200:
            request_uuid = self.poll_response_initial.object["request_uuid"]
            request_status = response.object[request_uuid]["status"]
            if request_status == "done":
                return True
        return False


class GridscaleBaseDriver(BaseDriver):
    name = "gridscale"
    website = "https://gridscale.io"
    connectionCls = GridscaleConnection

    def __init__(self, user_id, key, **kwargs):
        super().__init__(user_id, key, **kwargs)

    def _sync_request(self, data=None, endpoint=None, method="GET"):
        raw_result = self.connection.request(endpoint, data=data, method=method)

        return raw_result
