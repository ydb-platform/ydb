# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Zonomi DNS Driver
"""

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import (
    Provider,
    RecordType,
    ZoneDoesNotExistError,
    ZoneAlreadyExistsError,
    RecordDoesNotExistError,
    RecordAlreadyExistsError,
)
from libcloud.common.zonomi import ZonomiResponse, ZonomiException, ZonomiConnection

__all__ = [
    "ZonomiDNSDriver",
]


class ZonomiDNSResponse(ZonomiResponse):
    pass


class ZonomiDNSConnection(ZonomiConnection):
    responseCls = ZonomiDNSResponse


class ZonomiDNSDriver(DNSDriver):
    type = Provider.ZONOMI
    name = "Zonomi DNS"
    website = "https://zonomi.com"
    connectionCls = ZonomiDNSConnection

    RECORD_TYPE_MAP = {RecordType.A: "A", RecordType.MX: "MX", RecordType.TXT: "TXT"}

    def list_zones(self):
        """
        Return a list of zones.

        :return: ``list`` of :class:`Zone`
        """
        action = "/app/dns/dyndns.jsp?"
        params = {"action": "QUERYZONES", "api_key": self.key}

        response = self.connection.request(action=action, params=params)
        zones = self._to_zones(response.objects)

        return zones

    def list_records(self, zone):
        """
        Return a list of records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        action = "/app/dns/dyndns.jsp?"
        params = {"action": "QUERY", "name": "**." + zone.id}
        try:
            response = self.connection.request(action=action, params=params)
        except ZonomiException as e:
            if e.code == "404":
                raise ZoneDoesNotExistError(zone_id=zone.id, driver=self, value=e.message)
            raise e

        records = self._to_records(response.objects, zone)

        return records

    def get_zone(self, zone_id):
        """
        Return a Zone instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :rtype: :class:`Zone`
        """
        zone = None
        zones = self.list_zones()
        for z in zones:
            if z.id == zone_id:
                zone = z

        if zone is None:
            raise ZoneDoesNotExistError(zone_id=zone_id, driver=self, value="")

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
        record = None
        zone = self.get_zone(zone_id=zone_id)
        records = self.list_records(zone=zone)

        for r in records:
            if r.id == record_id:
                record = r

        if record is None:
            raise RecordDoesNotExistError(record_id=record_id, driver=self, value="")

        return record

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        """
        Create a new zone.

        :param zone_id: Zone domain name (e.g. example.com)
        :type zone_id: ``str``

        :rtype: :class:`Zone`
        """
        action = "/app/dns/addzone.jsp?"
        params = {"name": domain}
        try:
            self.connection.request(action=action, params=params)
        except ZonomiException as e:
            if e.message == "ERROR: This zone is already in your zone list.":
                raise ZoneAlreadyExistsError(zone_id=domain, driver=self, value=e.message)
            raise e

        zone = Zone(id=domain, domain=domain, type="master", ttl=ttl, driver=self, extra=extra)
        return zone

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

        :param type: DNS record type (A, MX, TXT).
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        :type  data: ``str``

        :param extra: Extra attributes (driver specific, e.g. 'prio' or 'ttl').
                      (optional)
        :type extra: ``dict``

        :rtype: :class:`Record`
        """
        action = "/app/dns/dyndns.jsp?"
        if name:
            record_name = name + "." + zone.domain
        else:
            record_name = zone.domain
        params = {"action": "SET", "name": record_name, "value": data, "type": type}

        if type == "MX" and extra is not None:
            params["prio"] = extra.get("prio")
        try:
            response = self.connection.request(action=action, params=params)
        except ZonomiException as e:
            if ("ERROR: No zone found for %s" % record_name) in e.message:
                raise ZoneDoesNotExistError(zone_id=zone.id, driver=self, value=e.message)
            raise e

        # we determine if an A or MX record already exists
        # by looking at the response.If the key 'skipped' is present in the
        # response, it means record already exists. If this is True,
        # then raise RecordAlreadyExistsError
        if len(response.objects) != 0 and response.objects[0].get("skipped") == "unchanged":
            raise RecordAlreadyExistsError(record_id=name, driver=self, value="")

        if "DELETED" in response.objects:
            for el in response.objects[:2]:
                if el.get("content") == data:
                    response.objects = [el]
        records = self._to_records(response.objects, zone=zone)
        return records[0]

    def delete_zone(self, zone):
        """
        Delete a zone.

        Note: This will delete all the records belonging to this zone.

        :param zone: Zone to delete.
        :type  zone: :class:`Zone`

        :rtype: ``bool``
        """
        action = "/app/dns/dyndns.jsp?"
        params = {"action": "DELETEZONE", "name": zone.id}
        try:
            response = self.connection.request(action=action, params=params)
        except ZonomiException as e:
            if e.code == "404":
                raise ZoneDoesNotExistError(zone_id=zone.id, driver=self, value=e.message)
            raise e

        return "DELETED" in response.objects

    def delete_record(self, record):
        """
        Use this method to delete a record.

        :param record: record to delete
        :type record: `Record`

        :rtype: Bool
        """
        action = "/app/dns/dyndns.jsp?"
        params = {"action": "DELETE", "name": record.name, "type": record.type}
        try:
            response = self.connection.request(action=action, params=params)
        except ZonomiException as e:
            if e.message == "Record not deleted.":
                raise RecordDoesNotExistError(record_id=record.id, driver=self, value=e.message)
            raise e

        return "DELETED" in response.objects

    def ex_convert_to_secondary(self, zone, master):
        """
        Convert existent zone to slave.

        :param zone: Zone to convert.
        :type  zone: :class:`Zone`

        :param master: the specified master name server IP address.
        :type  master: ``str``

        :rtype: Bool
        """
        action = "/app/dns/converttosecondary.jsp?"
        params = {"name": zone.domain, "master": master}
        try:
            self.connection.request(action=action, params=params)
        except ZonomiException as e:
            if "ERROR: Could not find" in e.message:
                raise ZoneDoesNotExistError(zone_id=zone.id, driver=self, value=e.message)
        return True

    def ex_convert_to_master(self, zone):
        """
        Convert existent zone to master.

        :param zone: Zone to convert.
        :type  zone: :class:`Zone`

        :rtype: Bool
        """
        action = "/app/dns/converttomaster.jsp?"
        params = {"name": zone.domain}
        try:
            self.connection.request(action=action, params=params)
        except ZonomiException as e:
            if "ERROR: Could not find" in e.message:
                raise ZoneDoesNotExistError(zone_id=zone.id, driver=self, value=e.message)
        return True

    def _to_zone(self, item):
        if item["type"] == "NATIVE":
            type = "master"
        elif item["type"] == "SLAVE":
            type = "slave"
        zone = Zone(
            id=item["name"],
            domain=item["name"],
            type=type,
            driver=self,
            extra={},
            ttl=None,
        )

        return zone

    def _to_zones(self, items):
        zones = []
        for item in items:
            zones.append(self._to_zone(item))

        return zones

    def _to_record(self, item, zone):
        if len(item.get("ttl")) > 0:
            ttl = item.get("ttl").split(" ")[0]
        else:
            ttl = None
        extra = {"ttl": ttl, "prio": item.get("prio")}
        if len(item["name"]) > len(zone.domain):
            full_domain = item["name"]
            index = full_domain.index("." + zone.domain)
            record_name = full_domain[:index]
        else:
            record_name = zone.domain
        record = Record(
            id=record_name,
            name=record_name,
            data=item["content"],
            type=item["type"],
            zone=zone,
            driver=self,
            ttl=ttl,
            extra=extra,
        )

        return record

    def _to_records(self, items, zone):
        records = []
        for item in items:
            records.append(self._to_record(item, zone))

        return records
