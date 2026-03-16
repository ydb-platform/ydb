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

try:
    import simplejson as json
except ImportError:
    import json

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import (
    Provider,
    RecordType,
    ZoneDoesNotExistError,
    ZoneAlreadyExistsError,
    RecordDoesNotExistError,
)
from libcloud.common.luadns import LuadnsResponse, LuadnsException, LuadnsConnection

__all__ = ["LuadnsDNSDriver"]


class LuadnsDNSResponse(LuadnsResponse):
    pass


class LuadnsDNSConnection(LuadnsConnection):
    responseCls = LuadnsDNSResponse


class LuadnsDNSDriver(DNSDriver):
    type = Provider.LUADNS
    name = "Luadns"
    website = "https://www.luadns.com"
    connectionCls = LuadnsDNSConnection

    RECORD_TYPE_MAP = {
        RecordType.A: "A",
        RecordType.AAAA: "AAAA",
        RecordType.CNAME: "CNAME",
        RecordType.MX: "MX",
        RecordType.NS: "NS",
        RecordType.PTR: "PTR",
        RecordType.SOA: "SOA",
        RecordType.SRV: "SRV",
        RecordType.TXT: "TXT",
    }

    def list_zones(self):
        """
        Return a list of zones.

        :return: ``list`` of :class:`Zone`
        """
        action = "/v1/zones"
        response = self.connection.request(action=action, method="GET")
        zones = self._to_zones(response.parse_body())

        return zones

    def get_zone(self, zone_id):
        """
        Return a Zone instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :rtype: :class:`Zone`
        """
        action = "/v1/zones/%s" % zone_id
        try:
            response = self.connection.request(action=action)
        except LuadnsException as e:
            if e.message in ["Zone not found.", "Resource not found."]:
                raise ZoneDoesNotExistError(zone_id=zone_id, value="", driver=self)
            else:
                raise e

        zone = self._to_zone(response.parse_body())

        return zone

    def delete_zone(self, zone):
        """
        Delete a zone.

        Note: This will delete all the records belonging to this zone.

        :param zone: Zone to delete.
        :type  zone: :class:`Zone`

        :rtype: ``bool``
        """
        action = "/v1/zones/%s" % zone.id
        try:
            response = self.connection.request(action=action, method="DELETE")
        except LuadnsException as e:
            if e.message in ["Resource not found.", "Zone not found."]:
                raise ZoneDoesNotExistError(zone_id=zone.id, value="", driver=self)
            else:
                raise e

        return response.status == 200

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        """
        Create a new zone.

        :param domain: Zone domain name (e.g. example.com)
        :type domain: ``str``

        :param type: Zone type (This is not really used. See API docs for extra
                     parameters).
        :type  type: ``str``

        :param ttl: TTL for new records. (This is not really used)
        :type  ttl: ``int``

        :param extra: Extra attributes (driver specific). ('region_support',
                      'zone_data')
        :type extra: ``dict``

        :rtype: :class:`Zone`
        """
        action = "/v1/zones"
        data = json.dumps({"name": domain})
        try:
            response = self.connection.request(action=action, method="POST", data=data)
        except LuadnsException as e:
            if e.message == "Zone '%s' is taken already." % domain:
                raise ZoneAlreadyExistsError(zone_id=domain, value="", driver=self)
            else:
                raise e
        zone = self._to_zone(response.parse_body())

        return zone

    def list_records(self, zone):
        """
        Return a list of records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        action = "/v1/zones/%s/records" % zone.id
        response = self.connection.request(action=action)
        records = self._to_records(response.parse_body(), zone=zone)

        return records

    def get_record(self, zone_id, record_id):
        """
        Return a Record instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :param record_id: ID of the required record
        :type  record_id: ``str``

        :rtype: :class:`Record`
        """
        zone = self.get_zone(zone_id=zone_id)
        action = "/v1/zones/{}/records/{}".format(zone_id, record_id)
        try:
            response = self.connection.request(action=action)
        except LuadnsException as e:
            if e.message == "Record not found.":
                raise RecordDoesNotExistError(record_id=record_id, driver=self, value="")
            else:
                raise e

        record = self._to_record(response.parse_body(), zone=zone)

        return record

    def delete_record(self, record):
        """
        Delete a record.

        :param record: Record to delete.
        :type  record: :class:`Record`

        :rtype: ``bool``
        """
        action = "/v1/zones/{}/records/{}".format(record.zone.id, record.id)
        try:
            response = self.connection.request(action=action, method="DELETE")
        except LuadnsException as e:
            if e.message == "Record not found.":
                raise RecordDoesNotExistError(record_id=record.id, driver=self, value="")
            else:
                raise e

        return response.status == 200

    def create_record(self, name, zone, type, data, extra=None):
        """
        Create a record.

        :param name: Record name without the domain name (e.g. www).
                     Note: If you want to create a record for a base domain
                     name, you should specify empty string ('') for this
                     argument.
        :type  name: ``str``

        :param zone: Zone which the records will be created for.
        :type zone: :class:`Zone`

        :param type: DNS record type ( 'A', 'AAAA', 'CNAME', 'MX', 'NS',
                     'PTR', 'SOA', 'SRV', 'TXT').
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        :type  data: ``str``

        :param extra: (optional) Extra attributes ('prio', 'ttl').
        :type  extra: ``dict``

        :rtype: :class:`Record`
        """
        action = "/v1/zones/%s/records" % zone.id
        to_post = {"name": name, "content": data, "type": type, "zone_id": int(zone.id)}
        # ttl is required to create a record for luadns
        # pass it through extra like this: extra={'ttl':ttl}
        if extra is not None:
            to_post.update(extra)
        data = json.dumps(to_post)
        try:
            response = self.connection.request(action=action, method="POST", data=data)
        except LuadnsException as e:
            raise e

        record = self._to_record(response.parse_body(), zone=zone)

        return record

    def _to_zone(self, item):
        common_attr = ["id", "name"]
        extra = {}
        for key in item:
            if key not in common_attr:
                extra[key] = item.get(key)
        zone = Zone(
            domain=item["name"],
            id=item["id"],
            type=None,
            ttl=None,
            driver=self,
            extra=extra,
        )

        return zone

    def _to_zones(self, items):
        zones = []
        for item in items:
            zones.append(self._to_zone(item))

        return zones

    def _to_record(self, item, zone):
        common_attr = ["id", "content", "name", "type"]
        extra = {}
        for key in item:
            if key not in common_attr:
                extra[key] = item.get(key)
        record = Record(
            id=item["id"],
            name=item["name"],
            type=item["type"],
            data=item["content"],
            zone=zone,
            driver=self,
            extra=extra,
        )

        return record

    def _to_records(self, items, zone):
        records = []
        for item in items:
            records.append(self._to_record(item, zone))

        return records
