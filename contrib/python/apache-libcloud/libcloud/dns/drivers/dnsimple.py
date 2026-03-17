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
DNSimple DNS Driver
"""

__all__ = ["DNSimpleDNSDriver"]

try:
    import simplejson as json
except ImportError:
    import json

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import Provider, RecordType
from libcloud.common.dnsimple import DNSimpleDNSConnection

DEFAULT_ZONE_TTL = 3600


class DNSimpleDNSDriver(DNSDriver):
    type = Provider.DNSIMPLE
    name = "DNSimple"
    website = "https://dnsimple.com/"
    connectionCls = DNSimpleDNSConnection

    RECORD_TYPE_MAP = {
        RecordType.A: "A",
        RecordType.AAAA: "AAAA",
        RecordType.ALIAS: "ALIAS",
        RecordType.CNAME: "CNAME",
        RecordType.HINFO: "HINFO",
        RecordType.MX: "MX",
        RecordType.NAPTR: "NAPTR",
        RecordType.NS: "NS",
        "POOL": "POOL",
        RecordType.SOA: "SOA",
        RecordType.SPF: "SPF",
        RecordType.SRV: "SRV",
        RecordType.SSHFP: "SSHFP",
        RecordType.TXT: "TXT",
        RecordType.URL: "URL",
    }

    def list_zones(self):
        """
        Return a list of zones.

        :return: ``list`` of :class:`Zone`
        """
        response = self.connection.request("/v1/domains")

        zones = self._to_zones(response.object)
        return zones

    def list_records(self, zone):
        """
        Return a list of records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        response = self.connection.request("/v1/domains/%s/records" % zone.id)
        records = self._to_records(response.object, zone)
        return records

    def get_zone(self, zone_id):
        """
        Return a Zone instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :rtype: :class:`Zone`
        """
        response = self.connection.request("/v1/domains/%s" % zone_id)
        zone = self._to_zone(response.object)
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
        response = self.connection.request("/v1/domains/{}/records/{}".format(zone_id, record_id))
        record = self._to_record(response.object, zone_id=zone_id)
        return record

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        """
        Create a new zone.

        :param domain: Zone domain name (e.g. example.com)
        :type domain: ``str``

        :param type: Zone type (All zones are master by design).
        :type  type: ``str``

        :param ttl: TTL for new records. (This is not really used)
        :type  ttl: ``int``

        :param extra: Extra attributes (driver specific). (optional)
        :type extra: ``dict``

        :rtype: :class:`Zone`

        For more info, please see:
        http://developer.dnsimple.com/v1/domains/
        """
        r_json = {"name": domain}
        if extra is not None:
            r_json.update(extra)
        r_data = json.dumps({"domain": r_json})
        response = self.connection.request("/v1/domains", method="POST", data=r_data)
        zone = self._to_zone(response.object)
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

        :param type: DNS record type (A, AAAA, ...).
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        :type  data: ``str``

        :param extra: Extra attributes (driver specific). (optional)
        :type extra: ``dict``

        :rtype: :class:`Record`
        """
        r_json = {"name": name, "record_type": type, "content": data}
        if extra is not None:
            r_json.update(extra)
        r_data = json.dumps({"record": r_json})
        response = self.connection.request(
            "/v1/domains/%s/records" % zone.id, method="POST", data=r_data
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
        :type  data: ``str``

        :param extra: (optional) Extra attributes (driver specific).
        :type  extra: ``dict``

        :rtype: :class:`Record`
        """
        zone = record.zone
        r_json = {"name": name, "content": data}
        if extra is not None:
            r_json.update(extra)
        r_data = json.dumps({"record": r_json})
        response = self.connection.request(
            "/v1/domains/{}/records/{}".format(zone.id, record.id),
            method="PUT",
            data=r_data,
        )
        record = self._to_record(response.object, zone=zone)
        return record

    def delete_zone(self, zone):
        """
        Delete a zone.

        Note: This will delete all the records belonging to this zone.

        :param zone: Zone to delete.
        :type  zone: :class:`Zone`

        :rtype: ``bool``
        """
        self.connection.request("/v1/domains/%s" % zone.id, method="DELETE")
        return True

    def delete_record(self, record):
        """
        Delete a record.

        :param record: Record to delete.
        :type  record: :class:`Record`

        :rtype: ``bool``
        """
        zone_id = record.zone.id
        self.connection.request(
            "/v1/domains/{}/records/{}".format(zone_id, record.id), method="DELETE"
        )
        return True

    def _to_zones(self, data):
        zones = []
        for zone in data:
            _zone = self._to_zone(zone)
            zones.append(_zone)

        return zones

    def _to_zone(self, data):
        domain = data.get("domain")
        id = domain.get("id")
        name = domain.get("name")
        extra = {
            "registrant_id": domain.get("registrant_id"),
            "user_id": domain.get("user_id"),
            "unicode_name": domain.get("unicode_name"),
            "token": domain.get("token"),
            "state": domain.get("state"),
            "language": domain.get("language"),
            "lockable": domain.get("lockable"),
            "auto_renew": domain.get("auto_renew"),
            "whois_protected": domain.get("whois_protected"),
            "record_count": domain.get("record_count"),
            "service_count": domain.get("service_count"),
            "expires_on": domain.get("expires_on"),
            "created_at": domain.get("created_at"),
            "updated_at": domain.get("updated_at"),
        }

        # All zones are primary by design
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
        for item in data:
            record = self._to_record(item, zone=zone)
            records.append(record)
        return records

    def _to_record(self, data, zone_id=None, zone=None):
        if not zone:  # We need zone_id or zone
            zone = self.get_zone(zone_id)
        record = data.get("record")
        id = record.get("id")
        name = record.get("name")
        type = record.get("record_type")
        data = record.get("content")
        extra = {
            "ttl": record.get("ttl"),
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
            "domain_id": record.get("domain_id"),
            "priority": record.get("prio"),
        }
        return Record(
            id=id,
            name=name,
            type=type,
            data=data,
            zone=zone,
            driver=self,
            ttl=record.get("ttl", None),
            extra=extra,
        )
