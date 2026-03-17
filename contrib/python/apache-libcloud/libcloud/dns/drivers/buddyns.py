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
BuddyNS DNS Driver
"""

try:
    import simplejson as json
except ImportError:
    import json

from libcloud.dns.base import Zone, DNSDriver
from libcloud.dns.types import Provider, ZoneDoesNotExistError, ZoneAlreadyExistsError
from libcloud.common.buddyns import BuddyNSResponse, BuddyNSException, BuddyNSConnection

__all__ = ["BuddyNSDNSDriver"]


class BuddyNSDNSResponse(BuddyNSResponse):
    pass


class BuddyNSDNSConnection(BuddyNSConnection):
    responseCls = BuddyNSDNSResponse


class BuddyNSDNSDriver(DNSDriver):
    name = "BuddyNS DNS"
    website = "https://www.buddyns.com"
    type = Provider.BUDDYNS
    connectionCls = BuddyNSDNSConnection

    def list_zones(self):
        action = "/api/v2/zone/"
        response = self.connection.request(action=action, method="GET")
        zones = self._to_zones(items=response.parse_body())

        return zones

    def get_zone(self, zone_id):
        """
        :param zone_id: Zone domain name (e.g. example.com)
        :return: :class:`Zone`
        """
        action = "/api/v2/zone/%s" % zone_id
        try:
            response = self.connection.request(action=action, method="GET")
        except BuddyNSException as e:
            if e.message == "Not found":
                raise ZoneDoesNotExistError(value=e.message, driver=self, zone_id=zone_id)
            else:
                raise e
        zone = self._to_zone(response.parse_body())

        return zone

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        """
        :param domain: Zone domain name (e.g. example.com)
        :type domain: ``str``

        :param type: Zone type (This is not really used. See API docs for \
          extra parameters)
        :type type: ``str``

        :param ttl: TTL for new records (This is used through the extra param)
        :type ttl: ``int``

        :param extra: Extra attributes that are specific to the driver
        such as ttl.
        :type extra: ``dict``

        :rtype: :class:`Zone`
        Do not forget to pass the master in extra,
        extra = {'master':'65.55.37.62'} for example.
        """
        action = "/api/v2/zone/"
        data = {"name": domain}
        if extra is not None:
            data.update(extra)
        post_data = json.dumps(data)
        try:
            response = self.connection.request(action=action, method="POST", data=post_data)
        except BuddyNSException as e:
            if e.message == "Invalid zone submitted for addition.":
                raise ZoneAlreadyExistsError(value=e.message, driver=self, zone_id=domain)
            else:
                raise e

        zone = self._to_zone(response.parse_body())

        return zone

    def delete_zone(self, zone):
        """
        :param zone: Zone to be deleted.
        :type zone: :class:`Zone`

        :return: Boolean
        """
        action = "/api/v2/zone/%s" % zone.domain
        try:
            self.connection.request(action=action, method="DELETE")
        except BuddyNSException as e:
            if e.message == "Not found":
                raise ZoneDoesNotExistError(value=e.message, driver=self, zone_id=zone.id)
            else:
                raise e

        return True

    def _to_zone(self, item):
        common_keys = [
            "name",
        ]
        extra = {}
        for key in item:
            if key not in common_keys:
                extra[key] = item.get(key)
        zone = Zone(
            domain=item["name"],
            id=item["name"],
            type=None,
            extra=extra,
            ttl=None,
            driver=self,
        )

        return zone

    def _to_zones(self, items):
        zones = []
        for item in items:
            zones.append(self._to_zone(item))

        return zones
