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
PowerDNS Driver
"""
import json

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import Provider, RecordType, ZoneDoesNotExistError, ZoneAlreadyExistsError
from libcloud.utils.py3 import httplib
from libcloud.common.base import JsonResponse, ConnectionKey
from libcloud.common.types import InvalidCredsError, MalformedResponseError
from libcloud.common.exceptions import BaseHTTPError

__all__ = [
    "PowerDNSDriver",
]


class PowerDNSResponse(JsonResponse):
    def success(self):
        i = int(self.status)
        return 200 <= i <= 299

    def parse_error(self):
        if self.status == httplib.UNAUTHORIZED:
            raise InvalidCredsError("Invalid provider credentials")

        try:
            body = self.parse_body()
        except MalformedResponseError as e:
            body = "{}: {}".format(e.value, e.body)
        try:
            errors = [body["error"]]
        except TypeError:
            # parse_body() gave us a simple string, not a dict.
            return "%s (HTTP Code: %d)" % (body, self.status)
        try:
            errors.append(body["errors"])
        except KeyError:
            # The PowerDNS API does not return the "errors" list all the time.
            pass

        return "%s (HTTP Code: %d)" % (" ".join(errors), self.status)


class PowerDNSConnection(ConnectionKey):
    responseCls = PowerDNSResponse

    def add_default_headers(self, headers):
        headers["X-API-Key"] = self.key
        return headers


class PowerDNSDriver(DNSDriver):
    type = Provider.POWERDNS
    name = "PowerDNS"
    website = "https://www.powerdns.com/"
    connectionCls = PowerDNSConnection

    RECORD_TYPE_MAP = {
        RecordType.A: "A",
        RecordType.AAAA: "AAAA",
        RecordType.AFSDB: "AFSDB",
        RecordType.CERT: "CERT",
        RecordType.CNAME: "CNAME",
        RecordType.DNSKEY: "DNSKEY",
        RecordType.DS: "DS",
        RecordType.HINFO: "HINFO",
        RecordType.KEY: "KEY",
        RecordType.LOC: "LOC",
        RecordType.MX: "MX",
        RecordType.NAPTR: "NAPTR",
        RecordType.NS: "NS",
        RecordType.NSEC: "NSEC",
        RecordType.OPENPGPKEY: "OPENPGPKEY",
        RecordType.PTR: "PTR",
        RecordType.RP: "RP",
        RecordType.RRSIG: "RRSIG",
        RecordType.SOA: "SOA",
        RecordType.SPF: "SPF",
        RecordType.SSHFP: "SSHFP",
        RecordType.SRV: "SRV",
        RecordType.TLSA: "TLSA",
        RecordType.TXT: "TXT",
    }

    def __init__(
        self,
        key,
        secret=None,
        secure=False,
        host=None,
        port=None,
        api_version="experimental",
        **kwargs,
    ):
        """
        PowerDNS Driver defaulting to using PowerDNS 3.x API (ie
        "experimental").

        :param    key: API key or username to used (required)
        :type     key: ``str``

        :param    secure: Whether to use HTTPS or HTTP. Note: Off by default
                          for PowerDNS.
        :type     secure: ``bool``

        :param    host: Hostname used for connections.
        :type     host: ``str``

        :param    port: Port used for connections.
        :type     port: ``int``

        :param    api_version: Specifies the API version to use.
                               ``experimental`` and ``v1`` are the only valid
                               options. Defaults to using ``experimental``
                               (optional)
        :type     api_version: ``str``

        :return: ``None``
        """
        # libcloud doesn't really have a concept of "servers". We'll just use
        # localhost for now.
        self.ex_server = "localhost"

        if api_version == "experimental":
            # PowerDNS 3.x has no API root prefix.
            self.api_root = ""
        elif api_version == "v1":
            # PowerDNS 4.x has an '/api/v1' root prefix.
            self.api_root = "/api/v1"
        else:
            raise NotImplementedError("Unsupported API version: %s" % api_version)

        super().__init__(key=key, secure=secure, host=host, port=port, **kwargs)

    def create_record(self, name, zone, type, data, extra=None):
        """
        Create a new record.

        There are two PowerDNS-specific quirks here. Firstly, this method will
        silently clobber any pre-existing records that might already exist. For
        example, if PowerDNS already contains a "test.example.com" A record,
        and you create that record using this function, then the old A record
        will be replaced with your new one.

        Secondly, PowerDNS requires that you provide a ttl for all new records.
        In other words, the "extra" parameter must be ``{'ttl':
        <some-integer>}`` at a minimum.

        :param name: FQDN of the new record, for example "www.example.com".
        :type  name: ``str``

        :param zone: Zone where the requested record is created.
        :type  zone: :class:`Zone`

        :param type: DNS record type (A, AAAA, ...).
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        :type  data: ``str``

        :param extra: Extra attributes (driver specific, e.g. 'ttl').
                      Note that PowerDNS *requires* a ttl value for every
                      record.
        :type extra: ``dict``

        :rtype: :class:`Record`
        """
        extra = extra or {}

        action = "{}/servers/{}/zones/{}".format(self.api_root, self.ex_server, zone.id)
        if extra is None or extra.get("ttl", None) is None:
            raise ValueError("PowerDNS requires a ttl value for every record")

        if self._pdns_version() == 3:
            record = {
                "content": data,
                "disabled": False,
                "name": name,
                "ttl": extra["ttl"],
                "type": type,
            }
            payload = {
                "rrsets": [
                    {
                        "name": name,
                        "type": type,
                        "changetype": "REPLACE",
                        "records": [record],
                    }
                ]
            }
        elif self._pdns_version() == 4:
            record = {
                "content": data,
                "disabled": extra.get("disabled", False),
                "set-ptr": False,
            }
            payload = {
                "rrsets": [
                    {
                        "name": name,
                        "type": type,
                        "changetype": "REPLACE",
                        "ttl": extra["ttl"],
                        "records": [record],
                    }
                ]
            }

            if "comment" in extra:
                payload["rrsets"][0]["comments"] = extra["comment"]

        try:
            self.connection.request(action=action, data=json.dumps(payload), method="PATCH")
        except BaseHTTPError as e:
            if e.code == httplib.UNPROCESSABLE_ENTITY and e.message.startswith(
                "Could not find domain"
            ):
                raise ZoneDoesNotExistError(zone_id=zone.id, driver=self, value=e.message)
            raise e
        return Record(
            id=None,
            name=name,
            data=data,
            type=type,
            zone=zone,
            driver=self,
            ttl=extra["ttl"],
        )

    def create_zone(self, domain, type=None, ttl=None, extra={}):
        """
        Create a new zone.

        There are two PowerDNS-specific quirks here. Firstly, the "type" and
        "ttl" parameters are ignored (no-ops). The "type" parameter is simply
        not implemented, and PowerDNS does not have an ability to set a
        zone-wide default TTL. (TTLs must be set per-record.)

        Secondly, PowerDNS requires that you provide a list of nameservers for
        the zone upon creation.  In other words, the "extra" parameter must be
        ``{'nameservers': ['ns1.example.org']}`` at a minimum.

        :param name: Zone domain name (e.g. example.com)
        :type  name: ``str``

        :param domain: Zone type (master / slave). (optional).  Note that the
                       PowerDNS driver does nothing with this parameter.
        :type  domain: :class:`Zone`

        :param ttl: TTL for new records. (optional). Note that the PowerDNS
                    driver does nothing with this parameter.
        :type  ttl: ``int``

        :param extra: Extra attributes (driver specific).
                      For example, specify
                      ``extra={'nameservers': ['ns1.example.org']}`` to set
                      a list of nameservers for this new zone.
        :type extra: ``dict``

        :rtype: :class:`Zone`
        """
        action = "{}/servers/{}/zones".format(self.api_root, self.ex_server)
        if extra is None or extra.get("nameservers", None) is None:
            msg = "PowerDNS requires a list of nameservers for every new zone"
            raise ValueError(msg)
        payload = {"name": domain, "kind": "Native"}
        payload.update(extra)
        zone_id = domain + "."
        try:
            self.connection.request(action=action, data=json.dumps(payload), method="POST")
        except BaseHTTPError as e:
            if e.code == httplib.UNPROCESSABLE_ENTITY and e.message.startswith(
                "Domain '%s' already exists" % domain
            ):
                raise ZoneAlreadyExistsError(zone_id=zone_id, driver=self, value=e.message)
            raise e
        return Zone(id=zone_id, domain=domain, type=None, ttl=None, driver=self, extra=extra)

    def delete_record(self, record):
        """
        Use this method to delete a record.

        :param record: record to delete
        :type record: `Record`

        :rtype: ``bool``
        """
        action = "{}/servers/{}/zones/{}".format(
            self.api_root,
            self.ex_server,
            record.zone.id,
        )
        payload = {"rrsets": [{"name": record.name, "type": record.type, "changetype": "DELETE"}]}
        try:
            self.connection.request(action=action, data=json.dumps(payload), method="PATCH")
        except BaseHTTPError:
            # I'm not sure if we should raise a ZoneDoesNotExistError here. The
            # base DNS API only specifies that we should return a bool. So,
            # let's ignore this code for now.
            # if e.code == httplib.UNPROCESSABLE_ENTITY and \
            #     e.message.startswith('Could not find domain'):
            #     raise ZoneDoesNotExistError(zone_id=zone.id, driver=self,
            #                                 value=e.message)
            # raise e
            return False
        return True

    def delete_zone(self, zone):
        """
        Use this method to delete a zone.

        :param zone: zone to delete
        :type zone: `Zone`

        :rtype: ``bool``
        """
        action = "{}/servers/{}/zones/{}".format(self.api_root, self.ex_server, zone.id)
        try:
            self.connection.request(action=action, method="DELETE")
        except BaseHTTPError:
            # I'm not sure if we should raise a ZoneDoesNotExistError here. The
            # base DNS API only specifies that we should return a bool. So,
            # let's ignore this code for now.
            # if e.code == httplib.UNPROCESSABLE_ENTITY and \
            #     e.message.startswith('Could not find domain'):
            #     raise ZoneDoesNotExistError(zone_id=zone.id, driver=self,
            #                                 value=e.message)
            # raise e
            return False
        return True

    def get_zone(self, zone_id):
        """
        Return a Zone instance.

        (Note that PowerDNS does not support per-zone TTL defaults, so all Zone
        objects will have ``ttl=None``.)

        :param zone_id: name of the required zone with the trailing period, for
                        example "example.com.".
        :type  zone_id: ``str``

        :rtype: :class:`Zone`
        :raises: ZoneDoesNotExistError: If no zone could be found.
        """
        action = "{}/servers/{}/zones/{}".format(self.api_root, self.ex_server, zone_id)
        try:
            response = self.connection.request(action=action, method="GET")
        except BaseHTTPError as e:
            if e.code == httplib.UNPROCESSABLE_ENTITY:
                raise ZoneDoesNotExistError(zone_id=zone_id, driver=self, value=e.message)
            raise e
        return self._to_zone(response.object)

    def list_records(self, zone):
        """
        Return a list of all records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        action = "{}/servers/{}/zones/{}".format(self.api_root, self.ex_server, zone.id)
        try:
            response = self.connection.request(action=action, method="GET")
        except BaseHTTPError as e:
            if e.code == httplib.UNPROCESSABLE_ENTITY and e.message.startswith(
                "Could not find domain"
            ):
                raise ZoneDoesNotExistError(zone_id=zone.id, driver=self, value=e.message)
            raise e
        return self._to_records(response, zone)

    def list_zones(self):
        """
        Return a list of zones.

        :return: ``list`` of :class:`Zone`
        """
        action = "{}/servers/{}/zones".format(self.api_root, self.ex_server)
        response = self.connection.request(action=action, method="GET")
        return self._to_zones(response)

    def update_record(self, record, name, type, data, extra=None):
        """
        Update an existing record.

        :param record: Record to update.
        :type  record: :class:`Record`

        :param name: FQDN of the new record, for example "www.example.com".
        :type  name: ``str``

        :param type: DNS record type (A, AAAA, ...).
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        :type  data: ``str``

        :param extra: (optional) Extra attributes (driver specific).
        :type  extra: ``dict``

        :rtype: :class:`Record`
        """
        action = "{}/servers/{}/zones/{}".format(
            self.api_root,
            self.ex_server,
            record.zone.id,
        )
        if extra is None or extra.get("ttl", None) is None:
            raise ValueError("PowerDNS requires a ttl value for every record")

        if self._pdns_version() == 3:
            updated_record = {
                "content": data,
                "disabled": False,
                "name": name,
                "ttl": extra["ttl"],
                "type": type,
            }
            payload = {
                "rrsets": [
                    {"name": record.name, "type": record.type, "changetype": "DELETE"},
                    {
                        "name": name,
                        "type": type,
                        "changetype": "REPLACE",
                        "records": [updated_record],
                    },
                ]
            }
        elif self._pdns_version() == 4:
            disabled = False
            if "disabled" in extra:
                disabled = extra["disabled"]
            updated_record = {
                "content": data,
                "disabled": disabled,
                "set-ptr": False,
            }
            payload = {
                "rrsets": [
                    {
                        "name": name,
                        "type": type,
                        "changetype": "REPLACE",
                        "ttl": extra["ttl"],
                        "records": [updated_record],
                    }
                ]
            }

            if "comment" in extra:
                payload["rrsets"][0]["comments"] = extra["comment"]

        try:
            self.connection.request(action=action, data=json.dumps(payload), method="PATCH")
        except BaseHTTPError as e:
            if e.code == httplib.UNPROCESSABLE_ENTITY and e.message.startswith(
                "Could not find domain"
            ):
                raise ZoneDoesNotExistError(zone_id=record.zone.id, driver=self, value=e.message)
            raise e
        return Record(
            id=None,
            name=name,
            data=data,
            type=type,
            zone=record.zone,
            driver=self,
            ttl=extra["ttl"],
        )

    def _to_zone(self, item):
        extra = {}
        for e in [
            "kind",
            "dnssec",
            "account",
            "masters",
            "serial",
            "notified_serial",
            "last_check",
        ]:
            extra[e] = item[e]
        # XXX: we have to hard-code "ttl" to "None" here because PowerDNS does
        # not support per-zone ttl defaults. However, I don't know what "type"
        # should be; probably not None.
        return Zone(
            id=item["id"],
            domain=item["name"],
            type=None,
            ttl=None,
            driver=self,
            extra=extra,
        )

    def _to_zones(self, items):
        zones = []
        for item in items.object:
            zones.append(self._to_zone(item))
        return zones

    def _to_record(self, item, zone, record=None):
        if record is None:
            data = item["content"]
        else:
            data = record["content"]
        return Record(
            id=None,
            name=item["name"],
            data=data,
            type=item["type"],
            zone=zone,
            driver=self,
            ttl=item["ttl"],
        )

    def _to_records(self, items, zone):
        records = []
        if self._pdns_version() == 3:
            for item in items.object["records"]:
                records.append(self._to_record(item, zone))
        elif self._pdns_version() == 4:
            for item in items.object["rrsets"]:
                for record in item["records"]:
                    records.append(self._to_record(item, zone, record))
        return records

    def _pdns_version(self):
        if self.api_root == "":
            return 3
        elif self.api_root == "/api/v1":
            return 4

        raise ValueError("PowerDNS version has not been declared")
