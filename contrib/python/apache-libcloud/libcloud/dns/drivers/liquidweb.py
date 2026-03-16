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
Liquid Web DNS Driver
"""

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
    RecordAlreadyExistsError,
)
from libcloud.common.liquidweb import APIException, LiquidWebResponse, LiquidWebConnection

__all__ = ["LiquidWebDNSDriver"]


class LiquidWebDNSResponse(LiquidWebResponse):
    pass


class LiquidWebDNSConnection(LiquidWebConnection):
    responseCls = LiquidWebDNSResponse


class LiquidWebDNSDriver(DNSDriver):
    type = Provider.LIQUIDWEB
    name = "Liquidweb DNS"
    website = "https://www.liquidweb.com"
    connectionCls = LiquidWebDNSConnection

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
        action = "/v1/Network/DNS/Zone/list"
        response = self.connection.request(action=action, method="POST")

        zones = self._to_zones(response.objects[0])

        return zones

    def list_records(self, zone):
        """
        Return a list of records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        action = "/v1/Network/DNS/Record/list"
        data = json.dumps({"params": {"zone_id": zone.id}})
        response = self.connection.request(action=action, method="POST", data=data)

        records = self._to_records(response.objects[0], zone=zone)

        return records

    def get_zone(self, zone_id):
        """
        Return a Zone instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :rtype: :class:`Zone`
        """
        action = "/v1/Network/DNS/Zone/details"
        data = json.dumps({"params": {"id": zone_id}})
        try:
            response = self.connection.request(action=action, method="POST", data=data)
        except APIException as e:
            if e.error_class == "LW::Exception::RecordNotFound":
                raise ZoneDoesNotExistError(zone_id=zone_id, value=e.value, driver=self)
            else:
                raise e

        zone = self._to_zone(response.objects[0])
        return zone

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
        action = "/v1/Network/DNS/Record/details"
        data = json.dumps({"params": {"id": record_id}})
        try:
            response = self.connection.request(action=action, method="POST", data=data)
        except APIException as e:
            if e.error_class == "LW::Exception::RecordNotFound":
                raise RecordDoesNotExistError(record_id=record_id, driver=self, value=e.value)
            else:
                raise e

        record = self._to_record(response.objects[0], zone=zone)
        return record

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

        For more info, please see:
        https://www.liquidweb.com/storm/api/docs/v1/Network/DNS/Zone.html
        """
        action = "/v1/Network/DNS/Zone/create"
        data = {"params": {"name": domain}}

        if extra is not None:
            data["params"].update(extra)
        try:
            data = json.dumps(data)
            response = self.connection.request(action=action, method="POST", data=data)
        except APIException as e:
            if e.error_class == "LW::Exception::DuplicateRecord":
                raise ZoneAlreadyExistsError(zone_id=domain, value=e.value, driver=self)
            else:
                raise e

        zone = self._to_zone(response.objects[0])

        return zone

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
        action = "/v1/Network/DNS/Record/create"
        to_post = {
            "params": {
                "name": name,
                "rdata": data,
                "type": type,
                "zone": zone.domain,
                "zone_id": zone.id,
            }
        }
        if extra is not None:
            to_post["params"].update(extra)
        data = json.dumps(to_post)
        try:
            response = self.connection.request(action=action, method="POST", data=data)
        except APIException as e:
            if e.error_class == "LW::Exception::DuplicateRecord":
                raise RecordAlreadyExistsError(record_id=name, value=e.value, driver=self)
            else:
                raise e

        record = self._to_record(response.objects[0], zone=zone)
        return record

    def update_record(self, record, name, type, data, extra=None):
        """
        Update an existing record.

        :param record: Record to update.
        :type  record: :class:`Record`

        :param name: Record name without the domain name (e.g. www).
                     Note: If you want to create a record for a base domain
                     name, you should specify empty string ('') for this
                     argument.
        :type  name: ``str``

        :param type: DNS record type ( 'A', 'AAAA', 'CNAME', 'MX', 'NS',
                     'PTR', 'SOA', 'SRV', 'TXT').
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        :type  data: ``str``

        :param extra: (optional) Extra attributes ('name', 'rdata', 'prio',
                      'ttl').
        :type  extra: ``dict``

        :rtype: :class:`Record`
        """
        zone = record.zone
        action = "/v1/Network/DNS/Record/update"
        to_post = {"params": {"id": int(record.id), "name": name, "rdata": data}}
        if extra is not None:
            to_post["params"].update(extra)
        j_data = json.dumps(to_post)
        try:
            response = self.connection.request(action=action, method="PUT", data=j_data)
        except APIException as e:
            if e.error_class == "LW::Exception::RecordNotFound":
                raise RecordDoesNotExistError(record_id=record.id, driver=self, value=e.value)
            else:
                raise e

        record = self._to_record(response.objects[0], zone=zone)
        return record

    def delete_zone(self, zone):
        """
        Delete a zone.

        Note: This will delete all the records belonging to this zone.

        :param zone: Zone to delete.
        :type  zone: :class:`Zone`

        :rtype: ``bool``
        """
        action = "/v1/Network/DNS/Zone/delete"
        data = json.dumps({"params": {"id": zone.id}})
        try:
            response = self.connection.request(action=action, method="POST", data=data)
        except APIException as e:
            if e.error_class == "LW::Exception::RecordNotFound":
                raise ZoneDoesNotExistError(zone_id=zone.id, value=e.value, driver=self)
            else:
                raise e

        return zone.domain in response.objects

    def delete_record(self, record):
        """
        Delete a record.

        :param record: Record to delete.
        :type  record: :class:`Record`

        :rtype: ``bool``
        """
        action = "/v1/Network/DNS/Record/delete"
        data = json.dumps({"params": {"id": record.id}})
        try:
            response = self.connection.request(action=action, method="POST", data=data)
        except APIException as e:
            if e.error_class == "LW::Exception::RecordNotFound":
                raise RecordDoesNotExistError(record_id=record.id, driver=self, value=e.value)
            else:
                raise e

        return record.id in response.objects

    def _to_zone(self, item):
        common_attr = ["id", "name", "type"]
        extra = {}
        for key in item:
            if key not in common_attr:
                extra[key] = item.get(key)
        zone = Zone(
            domain=item["name"],
            id=item["id"],
            type=item["type"],
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
        common_attr = ["id", "rdata", "name", "type"]
        extra = {}
        for key in item:
            if key not in common_attr:
                extra[key] = item.get(key)
        record = Record(
            id=item["id"],
            name=item["name"],
            type=item["type"],
            data=item["rdata"],
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
