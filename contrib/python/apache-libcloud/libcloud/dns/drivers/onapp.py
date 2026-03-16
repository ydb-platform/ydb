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
OnApp DNS Driver
"""

__all__ = ["OnAppDNSDriver"]

import json

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import Provider, RecordType
from libcloud.common.onapp import OnAppConnection

DEFAULT_ZONE_TTL = 1200


class OnAppDNSDriver(DNSDriver):
    type = Provider.ONAPP
    name = "OnApp"
    website = "http://onapp.com/"
    connectionCls = OnAppConnection

    RECORD_TYPE_MAP = {
        RecordType.SOA: "SOA",
        RecordType.NS: "NS",
        RecordType.A: "A",
        RecordType.AAAA: "AAAA",
        RecordType.CNAME: "CNAME",
        RecordType.MX: "MX",
        RecordType.TXT: "TXT",
        RecordType.SRV: "SRV",
    }

    def list_zones(self):
        """
        Return a list of zones.

        :return: ``list`` of :class:`Zone`
        """
        response = self.connection.request("/dns_zones.json")

        zones = self._to_zones(response.object)

        return zones

    def get_zone(self, zone_id):
        """
        Return a Zone instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :rtype: :class:`Zone`
        """
        response = self.connection.request("/dns_zones/%s.json" % zone_id)
        zone = self._to_zone(response.object)

        return zone

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        """
        Create a new zone.

        :param domain: Zone domain name (e.g. example.com)
        :type domain: ``str``

        :param type: Zone type (All zones are master by design).
        :type  type: ``str``

        :param ttl: TTL for new records. (This is not really used)
        :type  ttl: ``int``

        :param extra: Extra attributes (set auto_populate: 0 if you
        don't want to auto populate with existing DNS records). (optional)
        :type extra: ``dict``

        :rtype: :class:`Zone`

        For more info, please see:
        https://docs.onapp.com/display/52API/Add+DNS+Zone
        """
        dns_zone = {"name": domain}

        if extra is not None:
            dns_zone.update(extra)
        dns_zone_data = json.dumps({"dns_zone": dns_zone})
        response = self.connection.request(
            "/dns_zones.json",
            method="POST",
            headers={"Content-type": "application/json"},
            data=dns_zone_data,
        )
        zone = self._to_zone(response.object)

        return zone

    def delete_zone(self, zone):
        """
        Delete a zone.

        Note: This will also delete all the records belonging to this zone.

        :param zone: Zone to delete.
        :type  zone: :class:`Zone`

        :rtype: ``bool``
        """
        self.connection.request("/dns_zones/%s.json" % zone.id, method="DELETE")

        return True

    def list_records(self, zone):
        """
        Return a list of records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        response = self.connection.request("/dns_zones/%s/records.json" % zone.id)
        dns_records = response.object["dns_zone"]["records"]
        records = self._to_records(dns_records, zone)

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
        response = self.connection.request(
            "/dns_zones/{}/records/{}.json".format(zone_id, record_id)
        )
        record = self._to_record(response.object, zone_id=zone_id)

        return record

    def create_record(self, name, zone, type, data, extra=None):
        """
        Create a new record.

        :param name: Record name without the domain name (e.g. www).
                     Note: If you want to create a record for a base domain
                     name, you should specify empty string ('') for this
                     argument.
        :type  name: ``str``

        :param zone: Zone where the requested record is created.
        :type  zone: :class:`Zone`

        :param type: DNS record type (A, AAAA, ...).
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        Used only for A and AAAA record types.
        :type  data: ``str``

        :param extra: Extra attributes (driver specific). (optional)
        :type extra: ``dict``

        :rtype: :class:`Record`

        For more info, please see:
        https://docs.onapp.com/display/52API/Add+DNS+Record
        """
        dns_record = self._format_record(name, type, data, extra)
        dns_record_data = json.dumps({"dns_record": dns_record})
        response = self.connection.request(
            "/dns_zones/%s/records.json" % zone.id,
            method="POST",
            headers={"Content-type": "application/json"},
            data=dns_record_data,
        )
        record = self._to_record(response.object, zone=zone)

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

        :param type: DNS record type (A, AAAA, ...).
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        Used only for A and AAAA record types.
        :type  data: ``str``

        :param extra: (optional) Extra attributes (driver specific).
        :type  extra: ``dict``

        :rtype: :class:`Record`

        For more info, please see:
        https://docs.onapp.com/display/52API/Edit+DNS+Records
        """
        zone = record.zone
        dns_record = self._format_record(name, type, data, extra)
        dns_record_data = json.dumps({"dns_record": dns_record})
        self.connection.request(
            "/dns_zones/{}/records/{}.json".format(zone.id, record.id),
            method="PUT",
            headers={"Content-type": "application/json"},
            data=dns_record_data,
        )
        record = self.get_record(zone.id, record.id)

        return record

    def delete_record(self, record):
        """
        Delete a record.

        :param record: Record to delete.
        :type  record: :class:`Record`

        :rtype: ``bool``

        For more info, please see:
        https://docs.onapp.com/display/52API/Delete+DNS+Record
        """
        zone_id = record.zone.id
        self.connection.request(
            "/dns_zones/{}/records/{}.json".format(zone_id, record.id), method="DELETE"
        )

        return True

    #
    # Helper methods
    #

    def _format_record(self, name, type, data, extra):
        if name == "":
            name = "@"

        if extra is None:
            extra = {}
        record_type = self.RECORD_TYPE_MAP[type]
        new_record = {
            "name": name,
            "ttl": extra.get("ttl", DEFAULT_ZONE_TTL),
            "type": record_type,
        }

        if type == RecordType.MX:
            additions = {
                "priority": extra.get("priority", 1),
                "hostname": extra.get("hostname"),
            }
        elif type == RecordType.SRV:
            additions = {
                "port": extra.get("port"),
                "weight": extra.get("weight", 1),
                "priority": extra.get("priority", 1),
                "hostname": extra.get("hostname"),
            }
        elif type == RecordType.A:
            additions = {"ip": data}
        elif type == RecordType.CNAME:
            additions = {"hostname": extra.get("hostname")}
        elif type == RecordType.AAAA:
            additions = {"ip": data}
        elif type == RecordType.TXT:
            additions = {"txt": extra.get("txt")}
        elif type == RecordType.NS:
            additions = {"hostname": extra.get("hostname")}
        else:
            additions = {}

        new_record.update(additions)

        return new_record

    def _to_zones(self, data):
        zones = []

        for zone in data:
            _zone = self._to_zone(zone)
            zones.append(_zone)

        return zones

    def _to_zone(self, data):
        dns_zone = data.get("dns_zone")
        id = dns_zone.get("id")
        name = dns_zone.get("name")
        extra = {
            "user_id": dns_zone.get("user_id"),
            "cdn_reference": dns_zone.get("cdn_reference"),
            "created_at": dns_zone.get("created_at"),
            "updated_at": dns_zone.get("updated_at"),
        }

        type = "master"

        return Zone(
            id=id,
            domain=name,
            type=type,
            ttl=DEFAULT_ZONE_TTL,
            driver=self,
            extra=extra,
        )

    def _to_records(self, data, zone):
        records = []
        data = data.values()

        for data_type in data:
            for item in data_type:
                record = self._to_record(item, zone=zone)
                records.append(record)
        records.sort(key=lambda x: x.id, reverse=False)

        return records

    def _to_record(self, data, zone_id=None, zone=None):
        if not zone:  # We need zone_id or zone
            zone = self.get_zone(zone_id)
        record = data.get("dns_record")
        id = record.get("id")
        name = record.get("name")
        type = record.get("type")
        ttl = record.get("ttl", None)

        return Record(
            id=id,
            name=name,
            type=type,
            data=record,
            zone=zone,
            driver=self,
            ttl=ttl,
            extra={},
        )
