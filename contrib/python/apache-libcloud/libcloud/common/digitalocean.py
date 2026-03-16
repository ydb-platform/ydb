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
Common settings and connection objects for DigitalOcean Cloud
"""

from libcloud.utils.py3 import httplib, parse_qs, urlparse
from libcloud.common.base import BaseDriver, JsonResponse, ConnectionKey
from libcloud.common.types import LibcloudError, InvalidCredsError

__all__ = [
    "DigitalOcean_v2_Response",
    "DigitalOcean_v2_Connection",
    "DigitalOceanBaseDriver",
]


class DigitalOcean_v1_Error(LibcloudError):
    """
    Exception for when attempting to use version 1
    of the DigitalOcean API which is no longer
    supported.
    """

    def __init__(
        self,
        value=(
            "Driver no longer supported: Version 1 of the "
            "DigitalOcean API reached end of life on November 9, "
            "2015. Use the v2 driver. Please visit: "
            "https://developers.digitalocean.com/documentation/changelog/api-v1/sunsetting-api-v1/"
        ),  # noqa: E501
        driver=None,
    ):
        super().__init__(value, driver=driver)


class DigitalOcean_v2_Response(JsonResponse):
    valid_response_codes = [
        httplib.OK,
        httplib.ACCEPTED,
        httplib.CREATED,
        httplib.NO_CONTENT,
    ]

    def parse_error(self):
        if self.status == httplib.UNAUTHORIZED:
            body = self.parse_body()
            raise InvalidCredsError(body["message"])
        else:
            body = self.parse_body()
            if "message" in body:
                error = "{} (code: {})".format(body["message"], self.status)
            else:
                error = body
            return error

    def success(self):
        return self.status in self.valid_response_codes


class DigitalOcean_v2_Connection(ConnectionKey):
    """
    Connection class for the DigitalOcean (v2) driver.
    """

    host = "api.digitalocean.com"
    responseCls = DigitalOcean_v2_Response

    def add_default_headers(self, headers):
        """
        Add headers that are necessary for every request

        This method adds ``token`` to the request.
        """
        headers["Authorization"] = "Bearer %s" % (self.key)
        headers["Content-Type"] = "application/json"
        return headers

    def add_default_params(self, params):
        """
        Add parameters that are necessary for every request

        This method adds ``per_page`` to the request to reduce the total
        number of paginated requests to the API.
        """
        # pylint: disable=maybe-no-member
        params["per_page"] = self.driver.ex_per_page
        return params


class DigitalOceanConnection(DigitalOcean_v2_Connection):
    """
    Connection class for the DigitalOcean driver.
    """

    pass


class DigitalOceanResponse(DigitalOcean_v2_Response):
    pass


class DigitalOceanBaseDriver(BaseDriver):
    """
    DigitalOcean BaseDriver
    """

    name = "DigitalOcean"
    website = "https://www.digitalocean.com"

    def __new__(cls, key, secret=None, api_version="v2", **kwargs):
        if cls is DigitalOceanBaseDriver:
            if api_version == "v1" or secret is not None:
                raise DigitalOcean_v1_Error()
            elif api_version == "v2":
                cls = DigitalOcean_v2_BaseDriver
            else:
                raise NotImplementedError("Unsupported API version: %s" % (api_version))
        return super().__new__(cls, **kwargs)

    def ex_account_info(self):
        raise NotImplementedError("ex_account_info not implemented for this driver")

    def ex_list_events(self):
        raise NotImplementedError("ex_list_events not implemented for this driver")

    def ex_get_event(self, event_id):
        raise NotImplementedError("ex_get_event not implemented for this driver")

    def _paginated_request(self, url, obj):
        raise NotImplementedError("_paginated_requests not implemented for this driver")


class DigitalOcean_v2_BaseDriver(DigitalOceanBaseDriver):
    """
    DigitalOcean BaseDriver using v2 of the API.

    Supports `ex_per_page` ``int`` value keyword parameter to adjust per page
    requests against the API.
    """

    connectionCls = DigitalOcean_v2_Connection

    def __init__(
        self,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        api_version=None,
        region=None,
        ex_per_page=200,
        **kwargs,
    ):
        self.ex_per_page = ex_per_page
        super().__init__(key, **kwargs)

    def ex_account_info(self):
        return self.connection.request("/v2/account").object["account"]

    def ex_list_events(self):
        return self._paginated_request("/v2/actions", "actions")

    def ex_get_event(self, event_id):
        """
        Get an event object

        :param      event_id: Event id (required)
        :type       event_id: ``str``
        """
        params = {}
        return self.connection.request("/v2/actions/%s" % event_id, params=params).object["action"]

    def _paginated_request(self, url, obj):
        """
        Perform multiple calls in order to have a full list of elements when
        the API responses are paginated.

        :param url: API endpoint
        :type url: ``str``

        :param obj: Result object key
        :type obj: ``str``

        :return: ``list`` of API response objects
        :rtype: ``list``
        """
        params = {}
        data = self.connection.request(url)
        try:
            query = urlparse.urlparse(data.object["links"]["pages"]["last"])
            # The query[4] references the query parameters from the url
            pages = parse_qs(query[4])["page"][0]
            values = data.object[obj]
            for page in range(2, int(pages) + 1):
                params.update({"page": page})
                new_data = self.connection.request(url, params=params)

                more_values = new_data.object[obj]
                for value in more_values:
                    values.append(value)
            data = values
        except KeyError:  # No pages.
            data = data.object[obj]
        return data
