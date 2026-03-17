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
Digital Ocean DNS Driver
"""

__all__ = ["DigitalOceanDNSDriver"]

import json

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import Provider, RecordType
from libcloud.utils.py3 import httplib
from libcloud.common.digitalocean import DigitalOcean_v2_BaseDriver, DigitalOcean_v2_Connection


class DigitalOceanDNSDriver(DigitalOcean_v2_BaseDriver, DNSDriver):
    connectionCls = DigitalOcean_v2_Connection
    type = Provider.DIGITAL_OCEAN
    name = "DigitalOcean"
    website = "https://www.digitalocean.com"

    RECORD_TYPE_MAP = {
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
        data = self._paginated_request("/v2/domains", "domains")
        return list(map(self._to_zone, data))

    def list_records(self, zone):
        """
        Return a list of records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        data = self._paginated_request("/v2/domains/%s/records" % (zone.id), "domain_records")
        # TODO: Not use list comprehension to add zone to record for proper data map
        #       functionality? This passes a reference to zone for each data currently
        #       to _to_record which returns a Record. map() does not take keywords
        return list(map(self._to_record, data, [zone for z in data]))

    def get_zone(self, zone_id):
        """
        Return a Zone instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :rtype: :class:`Zone`
        """
        data = self.connection.request("/v2/domains/%s" % (zone_id)).object["domain"]

        return self._to_zone(data)

    def get_record(self, zone_id, record_id):
        """
        Return a Record instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :param record_id: ID of the required record
        :type  record_id: ``str``

        :rtype: :class:`Record`
        """
        data = self.connection.request(
            "/v2/domains/{}/records/{}".format(zone_id, record_id)
        ).object["domain_record"]

        # TODO: Any way of not using get_zone which polls the API again
        #       without breaking the DNSDriver.get_record parameters?
        return self._to_record(data, self.get_zone(zone_id))

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        """
        Create a new zone.

        :param domain: Zone domain name (e.g. example.com)
        :type domain: ``str``

        :param type: Zone type (master / slave) (does nothing).
        :type  type: ``str``

        :param ttl: TTL for new records. (does nothing)
        :type  ttl: ``int``

        :param extra: Extra attributes (to set ip). (optional)
                      Note: This can be used to set the default A record with
                      {"ip" : "IP.AD.DR.ESS"} otherwise 127.0.0.1 is used
        :type extra: ``dict``

        :rtype: :class:`Zone`
        """
        params = {"name": domain}
        try:
            params["ip_address"] = extra["ip"]
        except Exception:
            params["ip_address"] = "127.0.0.1"

        res = self.connection.request("/v2/domains", data=json.dumps(params), method="POST")

        return Zone(
            id=res.object["domain"]["name"],
            domain=res.object["domain"]["name"],
            type="master",
            ttl=1800,
            driver=self,
            extra={},
        )

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
        :type  data: ``str``

        :param extra: Extra attributes for MX and SRV. (Depends on record)
                      {"priority" : 0, "port" : 443, "weight" : 100}
        :type extra: ``dict``

        :rtype: :class:`Record`
        """
        params = {"type": self.RECORD_TYPE_MAP[type], "name": name, "data": data}
        if extra:
            try:
                params["priority"] = extra["priority"]
            except KeyError:
                params["priority"] = None
            try:
                params["port"] = extra["port"]
            except KeyError:
                params["port"] = None
            try:
                params["weight"] = extra["weight"]
            except KeyError:
                params["weight"] = None

            if "ttl" in extra:
                params["ttl"] = extra["ttl"]

        res = self.connection.request(
            "/v2/domains/%s/records" % zone.id, data=json.dumps(params), method="POST"
        )

        return Record(
            id=res.object["domain_record"]["id"],
            name=res.object["domain_record"]["name"],
            type=type,
            data=data,
            zone=zone,
            ttl=res.object["domain_record"].get("ttl", None),
            driver=self,
            extra=extra,
        )

    def update_record(self, record, name=None, type=None, data=None, extra=None):
        """
        Update an existing record.

        :param record: Record to update.
        :type  record: :class:`Record`

        :param name: Record name without the domain name (e.g. www). (Ignored)
                     Note: The value is pulled from the record being updated
        :type  name: ``str``

        :param type: DNS record type (A, AAAA, ...). (Ignored)
                     Note: Updating records does not support changing type
                     so this value is ignored
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        :type  data: ``str``

        :param extra: (optional) Extra attributes (driver specific).
        :type  extra: ``dict``

        :rtype: :class:`Record`
        """
        params = {"type": record.type, "name": record.name, "data": data}
        if data is None:
            params["data"] = record.data
        if extra:
            try:
                params["priority"] = extra["priority"]
            except KeyError:
                params["priority"] = None
            try:
                params["port"] = extra["port"]
            except KeyError:
                params["port"] = None
            try:
                params["weight"] = extra["weight"]
            except KeyError:
                params["weight"] = None

            if "ttl" in extra:
                params["ttl"] = extra["ttl"]

        res = self.connection.request(
            "/v2/domains/{}/records/{}".format(record.zone.id, record.id),
            data=json.dumps(params),
            method="PUT",
        )

        return Record(
            id=res.object["domain_record"]["id"],
            name=res.object["domain_record"]["name"],
            type=record.type,
            data=data,
            zone=record.zone,
            ttl=res.object["domain_record"].get("ttl", None),
            driver=self,
            extra=extra,
        )

    def delete_zone(self, zone):
        """
        Delete a zone.

        Note: This will delete all the records belonging to this zone.

        :param zone: Zone to delete.
        :type  zone: :class:`Zone`

        :rtype: ``bool``
        """
        params = {}

        res = self.connection.request("/v2/domains/%s" % zone.id, params=params, method="DELETE")

        return res.status == httplib.NO_CONTENT

    def delete_record(self, record):
        """
        Delete a record.

        :param record: Record to delete.
        :type  record: :class:`Record`

        :rtype: ``bool``
        """
        params = {}

        res = self.connection.request(
            "/v2/domains/{}/records/{}".format(record.zone.id, record.id),
            params=params,
            method="DELETE",
        )
        return res.status == httplib.NO_CONTENT

    def _to_record(self, data, zone=None):
        extra = {
            "port": data["port"],
            "priority": data["priority"],
            "weight": data["weight"],
        }
        return Record(
            id=data["id"],
            name=data["name"],
            type=self._string_to_record_type(data["type"]),
            data=data["data"],
            zone=zone,
            ttl=data.get("ttl", None),
            driver=self,
            extra=extra,
        )

    def _to_zone(self, data):
        extra = {"zone_file": data["zone_file"]}
        return Zone(
            id=data["name"],
            domain=data["name"],
            type="master",
            ttl=data["ttl"],
            driver=self,
            extra=extra,
        )
