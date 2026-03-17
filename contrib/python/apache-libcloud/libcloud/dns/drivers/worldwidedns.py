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
World Wide DNS Driver
"""

__all__ = ["WorldWideDNSDriver"]

import re

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import (
    Provider,
    RecordType,
    RecordError,
    ZoneDoesNotExistError,
    RecordDoesNotExistError,
)
from libcloud.common.types import LibcloudError
from libcloud.common.worldwidedns import WorldWideDNSConnection

MAX_RECORD_ENTRIES = 40  # Maximum record entries for zone


class WorldWideDNSError(LibcloudError):
    def __repr__(self):
        return "<WorldWideDNSError in " + repr(self.driver) + " " + repr(self.value) + ">"


class WorldWideDNSDriver(DNSDriver):
    type = Provider.WORLDWIDEDNS
    name = "World Wide DNS"
    website = "https://www.worldwidedns.net/"
    connectionCls = WorldWideDNSConnection

    RECORD_TYPE_MAP = {
        RecordType.MX: "MX",
        RecordType.CNAME: "CNAME",
        RecordType.A: "A",
        RecordType.NS: "NS",
        RecordType.SRV: "SRV",
        RecordType.TXT: "TXT",
    }

    def __init__(
        self,
        key,
        secret=None,
        reseller_id=None,
        secure=True,
        host=None,
        port=None,
        **kwargs,
    ):
        """
        :param    key: API key or username to used (required)
        :type     key: ``str``

        :param    secret: Secret password to be used (required)
        :type     secret: ``str``

        :param    reseller_id: Reseller ID for reseller accounts
        :type     reseller_id: ``str``

        :param    secure: Whether to use HTTPS or HTTP. Note: Some providers
                          only support HTTPS, and it is on by default.
        :type     secure: ``bool``

        :param    host: Override hostname used for connections.
        :type     host: ``str``

        :param    port: Override port used for connections.
        :type     port: ``int``

        :return: ``None``
        """
        super().__init__(key=key, secret=secret, secure=secure, host=host, port=port, **kwargs)
        self.reseller_id = reseller_id

    def list_zones(self):
        """
        Return a list of zones.

        :return: ``list`` of :class:`Zone`

        For more info, please see:
        https://www.worldwidedns.net/dns_api_protocol_list.asp
        or
        https://www.worldwidedns.net/dns_api_protocol_list_reseller.asp
        """
        action = "/api_dns_list.asp"

        if self.reseller_id is not None:
            action = "/api_dns_list_reseller.asp"
        zones = self.connection.request(action)

        if len(zones.body) == 0:
            return []
        else:
            return self._to_zones(zones.body)

    def iterate_records(self, zone):
        """
        Return a generator to iterate over records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :rtype: ``generator`` of :class:`Record`
        """
        records = self._to_records(zone)
        yield from records

    def get_zone(self, zone_id):
        """
        Return a Zone instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :rtype: :class:`Zone`
        """
        zones = self.list_zones()
        zone = [zone for zone in zones if zone.id == zone_id]

        if len(zone) == 0:
            raise ZoneDoesNotExistError(
                driver=self, value="The zone doesn't exists", zone_id=zone_id
            )

        return zone[0]

    def get_record(self, zone_id, record_id):
        """
        Return a Record instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :param record_id: ID number of the required record.
        :type  record_id: ``str``

        :rtype: :class:`Record`
        """
        zone = self.get_zone(zone_id)
        try:
            if int(record_id) not in range(1, MAX_RECORD_ENTRIES + 1):
                raise RecordDoesNotExistError(
                    value="Record doesn't exists",
                    driver=zone.driver,
                    record_id=record_id,
                )
        except ValueError:
            raise WorldWideDNSError(value="Record id should be a string number", driver=self)
        subdomain = zone.extra.get("S%s" % record_id)
        type = zone.extra.get("T%s" % record_id)
        data = zone.extra.get("D%s" % record_id)
        record = self._to_record(record_id, subdomain, type, data, zone)

        return record

    def update_zone(self, zone, domain, type="master", ttl=None, extra=None, ex_raw=False):
        """
        Update an existing zone.

        :param zone: Zone to update.
        :type  zone: :class:`Zone`

        :param domain: Zone domain name (e.g. example.com)
        :type  domain: ``str``

        :param type: Zone type (master / slave).
        :type  type: ``str``

        :param ttl: TTL for new records. (optional)
        :type  ttl: ``int``

        :param extra: Extra attributes (driver specific) (optional). Values not
                      specified such as *SECURE*, *IP*, *FOLDER*, *HOSTMASTER*,
                      *REFRESH*, *RETRY* and *EXPIRE* will be kept as already
                      is. The same will be for *S(1 to 40)*, *T(1 to 40)* and
                      *D(1 to 40)* if not in raw mode and for *ZONENS* and
                      *ZONEDATA* if it is.
        :type  extra: ``dict``

        :param ex_raw: Mode we use to do the update using zone file or not.
        :type  ex_raw: ``bool``

        :rtype: :class:`Zone`

        For more info, please see
        https://www.worldwidedns.net/dns_api_protocol_list_domain.asp
        or
        https://www.worldwidedns.net/dns_api_protocol_list_domain_raw.asp
        or
        https://www.worldwidedns.net/dns_api_protocol_list_domain_reseller.asp
        or
        https://www.worldwidedns.net/dns_api_protocol_list_domain_raw_reseller.asp
        """

        if extra is not None:
            not_specified = [key for key in zone.extra.keys() if key not in extra.keys()]
        else:
            not_specified = zone.extra.keys()

        if ttl is None:
            ttl = zone.ttl

        params = {"DOMAIN": domain, "TTL": ttl}

        for key in not_specified:
            params[key] = zone.extra[key]

        if extra is not None:
            params.update(extra)

        if ex_raw:
            action = "/api_dns_modify_raw.asp"

            if self.reseller_id is not None:
                action = "/api_dns_modify_raw_reseller.asp"
            method = "POST"
        else:
            action = "/api_dns_modify.asp"

            if self.reseller_id is not None:
                action = "/api_dns_modify_reseller.asp"
            method = "GET"
        response = self.connection.request(action, params=params, method=method)  # noqa
        zone = self.get_zone(zone.id)

        return zone

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

        :param type: DNS record type (MX, CNAME, A, NS, SRV, TXT).
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        :type  data: ``str``

        :param extra: Contains 'entry' Entry position (1 thru 40)
        :type extra: ``dict``

        :rtype: :class:`Record`
        """

        if (extra is None) or ("entry" not in extra):
            raise WorldWideDNSError(value="You must enter 'entry' parameter", driver=self)
        record_id = extra.get("entry")

        if name == "":
            name = "@"

        if type not in self.RECORD_TYPE_MAP:
            raise RecordError(
                value="Record type is not allowed",
                driver=record.zone.driver,
                record_id=name,
            )
        zone = record.zone
        extra = {
            "S%s" % record_id: name,
            "T%s" % record_id: type,
            "D%s" % record_id: data,
        }
        zone = self.update_zone(zone, zone.domain, extra=extra)
        record = self.get_record(zone.id, record_id)

        return record

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        """
        Create a new zone.

        :param domain: Zone domain name (e.g. example.com)
        :type domain: ``str``

        :param type: Zone type (master / slave).
        :type  type: ``str``

        :param ttl: TTL for new records. (optional)
        :type  ttl: ``int``

        :param extra: Extra attributes (driver specific). (optional). Possible
                      parameter in here should be *DYN* which values should be
                      1 for standard and 2 for dynamic. Default is 1.
        :type extra: ``dict``

        :rtype: :class:`Zone`

        For more info, please see
        https://www.worldwidedns.net/dns_api_protocol_new_domain.asp
        or
        https://www.worldwidedns.net/dns_api_protocol_new_domain_reseller.asp
        """

        if type == "master":
            _type = 0
        elif type == "slave":
            _type = 1
        else:
            raise ValueError(f"Unsupported type: {type}")

        if extra:
            dyn = extra.get("DYN") or 1
        else:
            dyn = 1
        params = {"DOMAIN": domain, "TYPE": _type}
        action = "/api_dns_new_domain.asp"

        if self.reseller_id is not None:
            params["DYN"] = dyn
            action = "/api_dns_new_domain_reseller.asp"
        self.connection.request(action, params=params)
        zone = self.get_zone(domain)

        if ttl is not None:
            zone = self.update_zone(zone, zone.domain, ttl=ttl)

        return zone

    def create_record(self, name, zone, type, data, extra=None):
        """
        Create a new record.

        We can create 40 record per domain. If all slots are full, we can
        replace one of them by choosing a specific entry in ``extra`` argument.

        :param name: Record name without the domain name (e.g. www).
                     Note: If you want to create a record for a base domain
                     name, you should specify empty string ('') for this
                     argument.
        :type  name: ``str``

        :param zone: Zone where the requested record is created.
        :type  zone: :class:`Zone`

        :param type: DNS record type (MX, CNAME, A, NS, SRV, TXT).
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        :type  data: ``str``

        :param extra: Contains 'entry' Entry position (1 thru 40)
        :type extra: ``dict``

        :rtype: :class:`Record`
        """

        if (extra is None) or ("entry" not in extra):
            # If no entry is specified, we look for an available one. If all
            # are full, raise error.
            record_id = self._get_available_record_entry(zone)

            if not record_id:
                raise WorldWideDNSError(value="All record entries are full", driver=zone.driver)
        else:
            record_id = extra.get("entry")

        if name == "":
            name = "@"

        if type not in self.RECORD_TYPE_MAP:
            raise RecordError(
                value="Record type is not allowed",
                driver=zone.driver,
                record_id=record_id,
            )
        extra = {
            "S%s" % record_id: name,
            "T%s" % record_id: type,
            "D%s" % record_id: data,
        }
        zone = self.update_zone(zone, zone.domain, extra=extra)
        record = self.get_record(zone.id, record_id)

        return record

    def delete_zone(self, zone):
        """
        Delete a zone.

        Note: This will delete all the records belonging to this zone.

        :param zone: Zone to delete.
        :type  zone: :class:`Zone`

        :rtype: ``bool``

        For more information, please see
        https://www.worldwidedns.net/dns_api_protocol_delete_domain.asp
        or
        https://www.worldwidedns.net/dns_api_protocol_delete_domain_reseller.asp
        """
        params = {"DOMAIN": zone.domain}
        action = "/api_dns_delete_domain.asp"

        if self.reseller_id is not None:
            action = "/api_dns_delete_domain_reseller.asp"
        response = self.connection.request(action, params=params)

        return response.success()

    def delete_record(self, record):
        """
        Delete a record.

        :param record: Record to delete.
        :type  record: :class:`Record`

        :rtype: ``bool``
        """
        zone = record.zone

        for index in range(MAX_RECORD_ENTRIES):
            if record.name == zone.extra["S%s" % (index + 1)]:
                entry = index + 1

                break
        extra = {"S%s" % entry: "", "T%s" % entry: "NONE", "D%s" % entry: ""}
        self.update_zone(zone, zone.domain, extra=extra)

        return True

    def ex_view_zone(self, domain, name_server):
        """
        View zone file from a name server

        :param domain: Domain name.
        :type  domain: ``str``

        :param name_server: Name server to check. (1, 2 or 3)
        :type  name_server: ``int``

        :rtype: ``str``

        For more info, please see:
        https://www.worldwidedns.net/dns_api_protocol_viewzone.asp
        or
        https://www.worldwidedns.net/dns_api_protocol_viewzone_reseller.asp
        """
        params = {"DOMAIN": domain, "NS": name_server}
        action = "/api_dns_viewzone.asp"

        if self.reseller_id is not None:
            action = "/api_dns_viewzone_reseller.asp"
        response = self.connection.request(action, params=params)

        return response.object

    def ex_transfer_domain(self, domain, user_id):
        """
        This command will allow you, if you are a reseller, to change the
        userid on a domain name to another userid in your account ONLY if that
        new userid is already created.

        :param domain: Domain name.
        :type  domain: ``str``

        :param user_id: The new userid to connect to the domain name.
        :type  user_id: ``str``

        :rtype: ``bool``

        For more info, please see:
        https://www.worldwidedns.net/dns_api_protocol_transfer.asp
        """

        if self.reseller_id is None:
            raise WorldWideDNSError("This is not a reseller account", driver=self)
        params = {"DOMAIN": domain, "NEW_ID": user_id}
        response = self.connection.request("/api_dns_transfer.asp", params=params)

        return response.success()

    def _get_available_record_entry(self, zone):
        """Return an available entry to store a record."""
        entries = zone.extra

        for entry in range(1, MAX_RECORD_ENTRIES + 1):
            subdomain = entries.get("S%s" % entry)
            _type = entries.get("T%s" % entry)
            data = entries.get("D%s" % entry)

            if not any([subdomain, _type, data]):
                return entry

        return None

    def _to_zones(self, data):
        domain_list = re.split("\r?\n", data)
        zones = []

        for line in domain_list:
            zone = self._to_zone(line)
            zones.append(zone)

        return zones

    def _to_zone(self, line):
        data = line.split("\x1f")
        name = data[0]

        if data[1] == "P":
            type = "master"
            domain_data = self._get_domain_data(name)
            resp_lines = re.split("\r?\n", domain_data.body)
            soa_block = resp_lines[:6]
            zone_data = resp_lines[6:]
            extra = {
                "HOSTMASTER": soa_block[0],
                "REFRESH": soa_block[1],
                "RETRY": soa_block[2],
                "EXPIRE": soa_block[3],
                "SECURE": soa_block[5],
            }
            ttl = soa_block[4]

            for line in range(MAX_RECORD_ENTRIES):
                line_data = zone_data[line].split("\x1f")
                extra["S%s" % (line + 1)] = line_data[0]
                _type = line_data[1]
                extra["T%s" % (line + 1)] = _type if _type != "NONE" else ""
                try:
                    extra["D%s" % (line + 1)] = line_data[2]
                except IndexError:
                    extra["D%s" % (line + 1)] = ""
        elif data[1] == "S":
            type = "slave"
            extra = {}
            ttl = 0
        else:
            ttl = 0

        return Zone(id=name, domain=name, type=type, ttl=ttl, driver=self, extra=extra)

    def _get_domain_data(self, name):
        params = {"DOMAIN": name}
        data = self.connection.request("/api_dns_list_domain.asp", params=params)

        return data

    def _to_records(self, zone):
        records = []

        for record_id in range(1, MAX_RECORD_ENTRIES + 1):
            subdomain = zone.extra["S%s" % (record_id)]
            type = zone.extra["T%s" % (record_id)]
            data = zone.extra["D%s" % (record_id)]

            if subdomain and type and data:
                record = self._to_record(record_id, subdomain, type, data, zone)
                records.append(record)

        return records

    def _to_record(self, _id, subdomain, type, data, zone):
        return Record(id=_id, name=subdomain, type=type, data=data, zone=zone, driver=zone.driver)
