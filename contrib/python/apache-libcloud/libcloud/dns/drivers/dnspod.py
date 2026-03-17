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

from libcloud.dns.base import Zone, Record, DNSDriver, RecordType
from libcloud.dns.types import (
    Provider,
    ZoneDoesNotExistError,
    ZoneAlreadyExistsError,
    RecordDoesNotExistError,
    RecordAlreadyExistsError,
)
from libcloud.utils.py3 import urlencode
from libcloud.common.dnspod import DNSPodResponse, DNSPodException, DNSPodConnection

__all__ = ["DNSPodDNSDriver"]

ZONE_ALREADY_EXISTS_ERROR_MSGS = [
    "Domain is exists",
    "Domain already exists as " "an alias of another domain",
]
ZONE_DOES_NOT_EXIST_ERROR_MSGS = [
    "Domain not under you or your user",
    "Domain id invalid",
]

RECORD_DOES_NOT_EXIST_ERRORS_MSGS = ["Record id invalid"]


class DNSPodDNSResponse(DNSPodResponse):
    pass


class DNSPodDNSConnection(DNSPodConnection):
    responseCls = DNSPodDNSResponse


class DNSPodDNSDriver(DNSDriver):
    name = "DNSPod"
    website = "https://dnspod.com"
    type = Provider.DNSPOD
    connectionCls = DNSPodDNSConnection

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

    def _make_request(self, action, method, data=None):
        data = data or {}
        if not data.get("user_token"):
            data["user_token"] = self.key
        if not data.get("format"):
            data["format"] = "json"
        data = urlencode(data)
        r = self.connection.request(action=action, method=method, data=data)
        return r

    def list_zones(self):
        action = "/Domain.List"
        try:
            response = self._make_request(action=action, method="POST")
        except DNSPodException as e:
            if e.message == "No domains":
                return []
        zones = self._to_zones(items=response.object["domains"])

        return zones

    def delete_zone(self, zone):
        """
        :param zone: Zone to be deleted.
        :type zone: :class:`Zone`

        :return: Boolean
        """
        action = "/Domain.Remove"
        data = {"domain_id": zone.id}
        try:
            self._make_request(action=action, method="POST", data=data)
        except DNSPodException as e:
            if e.message in ZONE_DOES_NOT_EXIST_ERROR_MSGS:
                raise ZoneDoesNotExistError(value=e.message, driver=self, zone_id=zone.id)
            else:
                raise e

        return True

    def get_zone(self, zone_id):
        """
        :param zone_id: Zone domain name (e.g. example.com)
        :return: :class:`Zone`
        """
        action = "/Domain.Info"
        data = {"domain_id": zone_id}
        try:
            response = self._make_request(action=action, method="POST", data=data)
        except DNSPodException as e:
            if e.message in ZONE_DOES_NOT_EXIST_ERROR_MSGS:
                raise ZoneDoesNotExistError(value=e.message, driver=self, zone_id=zone_id)
            else:
                raise e
        zone = self._to_zone(response.object["domain"])

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
        action = "/Domain.Create"
        data = {"domain": domain}
        if extra is not None:
            data.update(extra)
        try:
            response = self._make_request(action=action, method="POST", data=data)
        except DNSPodException as e:
            if e.message in ZONE_ALREADY_EXISTS_ERROR_MSGS:
                raise ZoneAlreadyExistsError(value=e.message, driver=self, zone_id=domain)
            else:
                raise e

        zone = self._to_zone(response.object["domain"])

        return zone

    def list_records(self, zone):
        """
        Return a list of records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        action = "/Record.List"
        data = {"domain_id": zone.id}
        try:
            response = self._make_request(action=action, data=data, method="POST")
        except DNSPodException as e:
            if e.message in ZONE_DOES_NOT_EXIST_ERROR_MSGS:
                raise ZoneDoesNotExistError(value="", driver=self, zone_id=zone.id)
            else:
                raise e
        records = self._to_records(response.object["records"], zone=zone)

        return records

    def delete_record(self, record):
        """
        Delete a record.

        :param record: Record to delete.
        :type  record: :class:`Record`

        :rtype: ``bool``
        """
        action = "/Record.Remove"
        data = {"domain_id": record.zone.id, "record_id": record.id}
        try:
            self._make_request(action=action, method="POST", data=data)
        except DNSPodException as e:
            if e.message in RECORD_DOES_NOT_EXIST_ERRORS_MSGS:
                raise RecordDoesNotExistError(record_id=record.id, driver=self, value="")
            elif e.message in ZONE_DOES_NOT_EXIST_ERROR_MSGS:
                raise ZoneDoesNotExistError(zone_id=record.zone.id, driver=self, value="")
            else:
                raise e

        return True

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
        action = "/Record.Info"
        data = {"domain_id": zone_id, "record_id": record_id}
        try:
            response = self._make_request(action=action, method="POST", data=data)
        except DNSPodException as e:
            if e.message in RECORD_DOES_NOT_EXIST_ERRORS_MSGS:
                raise RecordDoesNotExistError(record_id=record_id, driver=self, value="")
            elif e.message in ZONE_DOES_NOT_EXIST_ERROR_MSGS:
                raise ZoneDoesNotExistError(zone_id=zone_id, driver=self, value="")
            else:
                raise e

        record = self._to_record(response.object["record"], zone=zone)

        return record

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
        action = "/Record.Create"
        data = {
            "sub_domain": name,
            "value": data,
            "record_type": type,
            "domain_id": zone.id,
        }
        # ttl is optional
        # pass it through extra like this: extra={'ttl':ttl}
        # record_line is a required parameter
        # pass it through extra like this: extra={'record_line':'default'}
        # when creating MX records you need to pass mx through extra
        # mx ranges from 1 to 20
        # extra = {'ttl': '13', 'record_line': default, 'mx': 1}
        if extra is not None:
            data.update(extra)
        try:
            response = self._make_request(action=action, method="POST", data=data)
        except DNSPodException as e:
            if e.message == ("Record impacted, same record exists " "or CNAME/URL impacted"):
                raise RecordAlreadyExistsError(record_id="", driver=self, value=name)
            raise e

        record_id = response.object["record"].get("id")
        record = self.get_record(zone_id=zone.id, record_id=record_id)

        return record

    def _to_zone(self, item):
        common_attr = ["name", "id", "ttl"]
        extra = {}
        for key in item.keys():
            if key not in common_attr:
                extra[key] = item.get(key)

        zone = Zone(
            domain=item.get("name") or item.get("domain"),
            id=item.get("id"),
            type=None,
            extra=extra,
            ttl=item.get("ttl"),
            driver=self,
        )

        return zone

    def _to_zones(self, items):
        zones = []
        for item in items:
            zones.append(self._to_zone(item))

        return zones

    def _to_record(self, item, zone):
        common_attr = ["id", "value", "name", "type"]
        extra = {}
        for key in item:
            if key not in common_attr:
                extra[key] = item.get(key)
        record = Record(
            id=item.get("id"),
            name=item.get("name") or item.get("sub_domain"),
            type=item.get("type") or item.get("record_type"),
            data=item.get("value"),
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
