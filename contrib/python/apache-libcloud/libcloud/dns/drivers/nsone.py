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

from libcloud.dns.base import Zone, Record, DNSDriver, RecordType
from libcloud.dns.types import (
    Provider,
    ZoneDoesNotExistError,
    ZoneAlreadyExistsError,
    RecordDoesNotExistError,
    RecordAlreadyExistsError,
)
from libcloud.utils.py3 import httplib
from libcloud.common.nsone import NsOneResponse, NsOneException, NsOneConnection

__all__ = ["NsOneDNSDriver"]


class NsOneDNSResponse(NsOneResponse):
    pass


class NsOneDNSConnection(NsOneConnection):
    responseCls = NsOneDNSResponse


class NsOneDNSDriver(DNSDriver):
    name = "NS1 DNS"
    website = "https://ns1.com"
    type = Provider.NSONE
    connectionCls = NsOneDNSConnection

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
        action = "/v1/zones"
        response = self.connection.request(action=action, method="GET")
        zones = self._to_zones(items=response.parse_body())

        return zones

    def get_zone(self, zone_id):
        """
        :param zone_id: Zone domain name (e.g. example.com)
        :return: :class:`Zone`
        """
        action = "/v1/zones/%s" % zone_id
        try:
            response = self.connection.request(action=action, method="GET")
        except NsOneException as e:
            if e.message == "zone not found":
                raise ZoneDoesNotExistError(value=e.message, driver=self, zone_id=zone_id)
            else:
                raise e
        zone = self._to_zone(response.objects[0])

        return zone

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        """
        :param domain: Zone domain name (e.g. example.com)
        :type domain: ``str``

        :param type: Zone type (This is not really used. See API docs for extra
          parameters)
        :type type: ``str``

        :param ttl: TTL for new records (This is used through the extra param)
        :type ttl: ``int``

        :param extra: Extra attributes that are specific to the driver
        such as ttl.
        :type extra: ``dict``

        :rtype: :class:`Zone`
        """
        action = "/v1/zones/%s" % domain
        raw_data = {"zone": domain}
        if extra is not None:
            raw_data.update(extra)
        post_data = json.dumps(raw_data)
        try:
            response = self.connection.request(action=action, method="PUT", data=post_data)
        except NsOneException as e:
            if e.message == "zone already exists":
                raise ZoneAlreadyExistsError(value=e.message, driver=self, zone_id=domain)
            else:
                raise e

        zone = self._to_zone(response.object)

        return zone

    def delete_zone(self, zone):
        """
        :param zone: Zone to be deleted.
        :type zone: :class:`Zone`

        :return: Boolean
        """
        action = "/v1/zones/%s" % zone.domain
        """zones_list = self.list_zones()
        if not self.ex_zone_exists(zone_id=zone.id, zones_list=zones_list):
            raise ZoneDoesNotExistError(value='', driver=self, zone_id=zone.id)
        """
        try:
            response = self.connection.request(action=action, method="DELETE")
        except NsOneException as e:
            if e.message == "zone not found":
                raise ZoneDoesNotExistError(value=e.message, driver=self, zone_id=zone.id)
            else:
                raise e

        return response.status == httplib.OK

    def list_records(self, zone):
        """
        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        action = "/v1/zones/%s" % zone.domain
        try:
            response = self.connection.request(action=action, method="GET")
        except NsOneException as e:
            if e.message == "zone not found":
                raise ZoneDoesNotExistError(value=e.message, driver=self, zone_id=zone.id)
            else:
                raise e
        records = self._to_records(items=response.parse_body()["records"], zone=zone)

        return records

    def get_record(self, zone_id, record_id):
        """
        :param zone_id: The id of the zone where to search for
        the record (e.g. example.com)
        :type zone_id: ``str``
        :param record_id: The type of record to search for
        (e.g. A, AAA, MX etc)

        :return: :class:`Record`
        """
        action = "/v1/zones/{}/{}/{}".format(zone_id, zone_id, record_id)
        try:
            response = self.connection.request(action=action, method="GET")
        except NsOneException as e:
            if e.message == "record not found":
                raise RecordDoesNotExistError(value=e.message, driver=self, record_id=record_id)
            else:
                raise e
        zone = self.get_zone(zone_id=zone_id)
        record = self._to_record(item=response.parse_body(), zone=zone)

        return record

    def delete_record(self, record):
        """
        :param record: Record to delete.
        :type record: :class:`Record`

        :return: Boolean
        """
        action = "/v1/zones/{}/{}/{}".format(record.zone.domain, record.name, record.type)
        try:
            response = self.connection.request(action=action, method="DELETE")
        except NsOneException as e:
            if e.message == "record not found":
                raise RecordDoesNotExistError(value=e.message, driver=self, record_id=record.id)
            else:
                raise e

        return response.status == httplib.OK

    def create_record(self, name, zone, type, data, extra=None):
        """
        :param name: Name of the record to create (e.g. foo).
        :type name: ``str``
        :param zone: Zone where the record should be created.
        :type zone: :class:`Zone`
        :param type: Type of record (e.g. A, MX etc)
        :type type: ``str``
        :param data: Data of the record (e.g. 127.0.0.1 for the A record)
        :type data: ``str``
        :param extra: Extra data needed to create different types of records
        :type extra: ``dict``
        :return: :class:`Record`
        """
        record_name = "{}.{}".format(name, zone.domain) if name != "" else zone.domain  # noqa

        action = "/v1/zones/{}/{}/{}".format(zone.domain, record_name, type)
        if type == RecordType.MX:
            answer = [extra.get("priority", 10), data]
        else:
            answer = [data]

        raw_data = {
            "answers": [{"answer": answer}],
            "type": type,
            "domain": record_name,
            "zone": zone.domain,
        }
        if extra is not None and extra.get("answers"):
            raw_data["answers"] = extra.get("answers")
        post_data = json.dumps(raw_data)
        try:
            response = self.connection.request(action=action, method="PUT", data=post_data)
        except NsOneException as e:
            if e.message == "record already exists":
                raise RecordAlreadyExistsError(value=e.message, driver=self, record_id="")
            else:
                raise e
        record = self._to_record(item=response.parse_body(), zone=zone)

        return record

    def update_record(self, record, name, type, data, extra=None):
        """
        :param record: Record to update
        :type record: :class:`Record`
        :param name: Name of the record to update (e.g. foo).
        :type name: ``str``
        :param type: Type of record (e.g. A, MX etc)
        :type type: ``str``
        :param data: Data of the record (e.g. 127.0.0.1 for the A record)
        :type data: ``str``
        :param extra: Extra data needed to create different types of records
        :type extra: ``dict``
        :return: :class:`Record`
        """
        zone = record.zone
        action = "/v1/zones/{}/{}/{}".format(
            zone.domain,
            "{}.{}".format(name, zone.domain),
            type,
        )
        raw_data = {"answers": [{"answer": [data]}]}
        if extra is not None and extra.get("answers"):
            raw_data["answers"] = extra.get("answers")
        post_data = json.dumps(raw_data)
        try:
            response = self.connection.request(action=action, data=post_data, method="POST")
        except NsOneException as e:
            if e.message == "record does not exist":
                raise RecordDoesNotExistError(value=e.message, driver=self, record_id=record.id)
            else:
                raise e
        record = self._to_record(item=response.parse_body(), zone=zone)

        return record

    def ex_zone_exists(self, zone_id, zones_list):
        """
        Function to check if a `Zone` object exists.
        :param zone_id: ID of the `Zone` object.
        :type zone_id: ``str``

        :param zones_list: A list containing `Zone` objects.
        :type zones_list: ``list``.

        :rtype: Returns `True` or `False`.
        """
        zone_ids = []
        for zone in zones_list:
            zone_ids.append(zone.id)

        return zone_id in zone_ids

    def _to_zone(self, item):
        common_attr = ["zone", "id", "type"]
        extra = {}
        for key in item.keys():
            if key not in common_attr:
                extra[key] = item.get(key)

        zone = Zone(
            domain=item["zone"],
            id=item["id"],
            type=item.get("type"),
            extra=extra,
            ttl=extra.get("ttl"),
            driver=self,
        )

        return zone

    def _to_zones(self, items):
        zones = []
        for item in items:
            zones.append(self._to_zone(item))

        return zones

    def _to_record(self, item, zone):
        common_attr = ["id", "short_answers", "answers", "domain", "type"]
        extra = {}
        for key in item.keys():
            if key not in common_attr:
                extra[key] = item.get(key)
        if item.get("answers") is not None:
            data = item.get("answers")[0]["answer"]
        else:
            data = item.get("short_answers")
        record = Record(
            id=item["id"],
            name=item["domain"],
            type=item["type"],
            data=data,
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
