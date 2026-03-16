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
NFSN DNS Driver
"""
import re

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import (
    Provider,
    RecordType,
    ZoneDoesNotExistError,
    RecordDoesNotExistError,
    RecordAlreadyExistsError,
)
from libcloud.utils.py3 import httplib
from libcloud.common.nfsn import NFSNConnection
from libcloud.common.exceptions import BaseHTTPError

__all__ = [
    "NFSNDNSDriver",
]

# The NFSN API does not return any internal "ID" strings for any DNS records.
# This means that we must set all returned Record objects' id properties to
# None. It also means that we cannot implement libcloud APIs that rely on
# record_id, such as get_record(). Instead, the NFSN-specific
# ex_get_records_by() method will return the desired Record objects.
#
# Additionally, the NFSN API does not provide ways to create, delete, or list
# all zones, so create_zone(), delete_zone(), and list_zones() are not
# implemented.


class NFSNDNSDriver(DNSDriver):
    type = Provider.NFSN
    name = "NFSN DNS"
    website = "https://www.nearlyfreespeech.net"
    connectionCls = NFSNConnection

    RECORD_TYPE_MAP = {
        RecordType.A: "A",
        RecordType.AAAA: "AAAA",
        RecordType.CNAME: "CNAME",
        RecordType.MX: "MX",
        RecordType.NS: "NS",
        RecordType.SRV: "SRV",
        RecordType.TXT: "TXT",
        RecordType.PTR: "PTR",
    }

    def list_records(self, zone):
        """
        Return a list of all records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        # Just use ex_get_records_by() with no name or type filters.
        return self.ex_get_records_by(zone)

    def get_zone(self, zone_id):
        """
        Return a Zone instance.

        :param zone_id: name of the required zone, for example "example.com".
        :type  zone_id: ``str``

        :rtype: :class:`Zone`
        :raises: ZoneDoesNotExistError: If no zone could be found.
        """
        # We will check if there is a serial property for this zone. If so,
        # then the zone exists.
        try:
            self.connection.request(action="/dns/%s/serial" % zone_id)
        except BaseHTTPError as e:
            if e.code == httplib.NOT_FOUND:
                raise ZoneDoesNotExistError(zone_id=None, driver=self, value=e.message)
            raise e
        return Zone(id=None, domain=zone_id, type="master", ttl=3600, driver=self)

    def ex_get_records_by(self, zone, name=None, type=None):
        """
        Return a list of records for the provided zone, filtered by name and/or
        type.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :param zone: Zone where the requested records are found.
        :type  zone: :class:`Zone`

        :param name: name of the records, for example "www". (optional)
        :type  name: ``str``

        :param type: DNS record type (A, MX, TXT). (optional)
        :type  type: :class:`RecordType`

        :return: ``list`` of :class:`Record`
        """
        payload = {}
        if name is not None:
            payload["name"] = name
        if type is not None:
            payload["type"] = type

        action = "/dns/%s/listRRs" % zone.domain
        response = self.connection.request(action=action, data=payload, method="POST")
        return self._to_records(response, zone)

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

        :param extra: Extra attributes (driver specific, e.g. 'ttl').
                      (optional)
        :type extra: ``dict``

        :rtype: :class:`Record`
        """
        action = "/dns/%s/addRR" % zone.domain
        payload = {"name": name, "data": data, "type": type}
        if extra is not None and extra.get("ttl", None) is not None:
            payload["ttl"] = extra["ttl"]
        try:
            self.connection.request(action=action, data=payload, method="POST")
        except BaseHTTPError as e:
            exists_re = re.compile(r"That RR already exists on the domain")
            if e.code == httplib.BAD_REQUEST and re.search(exists_re, e.message) is not None:
                value = '"{}" already exists in {}'.format(name, zone.domain)
                raise RecordAlreadyExistsError(value=value, driver=self, record_id=None)
            raise e
        return self.ex_get_records_by(zone=zone, name=name, type=type)[0]

    def delete_record(self, record):
        """
        Use this method to delete a record.

        :param record: record to delete
        :type record: `Record`

        :rtype: Bool
        """
        action = "/dns/%s/removeRR" % record.zone.domain
        payload = {"name": record.name, "data": record.data, "type": record.type}
        try:
            self.connection.request(action=action, data=payload, method="POST")
        except BaseHTTPError as e:
            if e.code == httplib.NOT_FOUND:
                raise RecordDoesNotExistError(value=e.message, driver=self, record_id=None)
            raise e
        return True

    def _to_record(self, item, zone):
        ttl = int(item["ttl"])
        return Record(
            id=None,
            name=item["name"],
            data=item["data"],
            type=item["type"],
            zone=zone,
            driver=self,
            ttl=ttl,
        )

    def _to_records(self, items, zone):
        records = []
        for item in items.object:
            records.append(self._to_record(item, zone))
        return records
