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


import datetime
from typing import Any, Dict, List, Type, Union, Iterator, Optional

from libcloud import __version__
from libcloud.dns.types import RecordType
from libcloud.common.base import BaseDriver, Connection, ConnectionUserAndKey

__all__ = ["Zone", "Record", "DNSDriver"]


class Zone:
    """
    DNS zone.
    """

    def __init__(
        self,
        id,  # type: str
        domain,  # type: str
        type,  # type: str
        ttl,  # type: int
        driver,  # type: DNSDriver
        extra=None,  # type: Optional[dict]
    ):
        """
        :param id: Zone id.
        :type id: ``str``

        :param domain: The name of the domain.
        :type domain: ``str``

        :param type: Zone type (master, slave).
        :type type: ``str``

        :param ttl: Default TTL for records in this zone (in seconds).
        :type ttl: ``int``

        :param driver: DNSDriver instance.
        :type driver: :class:`DNSDriver`

        :param extra: (optional) Extra attributes (driver specific).
        :type extra: ``dict``
        """
        self.id = str(id) if id else None
        self.domain = domain
        self.type = type
        self.ttl = ttl or None
        self.driver = driver
        self.extra = extra or {}

    def list_records(self):
        # type: () -> List[Record]
        return self.driver.list_records(zone=self)

    def create_record(self, name, type, data, extra=None):
        # type: (str, RecordType, str, Optional[dict]) -> Record
        return self.driver.create_record(name=name, zone=self, type=type, data=data, extra=extra)

    def update(
        self,
        domain=None,  # type: Optional[str]
        type=None,  # type: Optional[str]
        ttl=None,  # type: Optional[int]
        extra=None,  # type: Optional[dict]
    ):
        # type: (...) -> Zone
        return self.driver.update_zone(zone=self, domain=domain, type=type, ttl=ttl, extra=extra)

    def delete(self):
        # type: () -> bool
        return self.driver.delete_zone(zone=self)

    def export_to_bind_format(self):
        # type: () -> str
        return self.driver.export_zone_to_bind_format(zone=self)

    def export_to_bind_zone_file(self, file_path):
        # type: (str) -> None
        self.driver.export_zone_to_bind_zone_file(zone=self, file_path=file_path)

    def __repr__(self):
        # type: () -> str
        return "<Zone: domain={}, ttl={}, provider={} ...>".format(
            self.domain,
            self.ttl,
            self.driver.name,
        )


class Record:
    """
    Zone record / resource.
    """

    def __init__(
        self,
        id,  # type: str
        name,  # type: str
        type,  # type: RecordType
        data,  # type: str
        zone,  # type: Zone
        driver,  # type: DNSDriver
        ttl=None,  # type: Optional[int]
        extra=None,  # type: Optional[dict]
    ):
        """
        :param id: Record id
        :type id: ``str``

        :param name: Hostname or FQDN.
        :type name: ``str``

        :param type: DNS record type (A, AAAA, ...).
        :type type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        :type data: ``str``

        :param zone: Zone instance.
        :type zone: :class:`Zone`

        :param driver: DNSDriver instance.
        :type driver: :class:`DNSDriver`

        :param ttl: Record TTL.
        :type ttl: ``int``

        :param extra: (optional) Extra attributes (driver specific).
        :type extra: ``dict``
        """
        self.id = str(id) if id else None
        self.name = name
        self.type = type
        self.data = data
        self.zone = zone
        self.driver = driver
        self.ttl = ttl
        self.extra = extra or {}

    def update(
        self,
        name=None,  # type: Optional[str]
        type=None,  # type: Optional[RecordType]
        data=None,  # type: Optional[str]
        extra=None,  # type: Optional[dict]
    ):
        # type: (...) -> Record
        return self.driver.update_record(record=self, name=name, type=type, data=data, extra=extra)

    def delete(self):
        # type: () -> bool
        return self.driver.delete_record(record=self)

    def _get_numeric_id(self):
        # type: () -> Union[int, str]
        """
        Return numeric ID for the provided record if the ID is a digit.

        This method is used for sorting the values when exporting Zone to a
        BIND format.
        """
        record_id = self.id

        if record_id is None:
            return ""

        if record_id.isdigit():
            record_id_int = int(record_id)
            return record_id_int

        return record_id

    def __repr__(self):
        # type: () -> str
        zone = self.zone.domain if self.zone.domain else self.zone.id
        return "<Record: zone=%s, name=%s, type=%s, data=%s, provider=%s, " "ttl=%s ...>" % (
            zone,
            self.name,
            self.type,
            self.data,
            self.driver.name,
            self.ttl,
        )


class DNSDriver(BaseDriver):
    """
    A base DNSDriver class to derive from

    This class is always subclassed by a specific driver.
    """

    connectionCls = ConnectionUserAndKey  # type: Type[Connection]
    name = None  # type: str
    website = None  # type: str

    # Map libcloud record type enum to provider record type name
    RECORD_TYPE_MAP = {}  # type: Dict[RecordType, str]

    def __init__(
        self,
        key,  # type: str
        secret=None,  # type: Optional[str]
        secure=True,  # type: bool
        host=None,  # type: Optional[str]
        port=None,  # type: Optional[int]
        **kwargs,  # type: Optional[Any]
    ):
        # type: (...) -> None
        """
        :param    key: API key or username to used (required)
        :type     key: ``str``

        :param    secret: Secret password to be used (required)
        :type     secret: ``str``

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

    def list_record_types(self):
        # type: () -> List[RecordType]
        """
        Return a list of RecordType objects supported by the provider.

        :return: ``list`` of :class:`RecordType`
        """
        return list(self.RECORD_TYPE_MAP.keys())

    def iterate_zones(self):
        # type: () -> Iterator[Zone]
        """
        Return a generator to iterate over available zones.

        :rtype: ``generator`` of :class:`Zone`
        """
        raise NotImplementedError("iterate_zones not implemented for this driver")

    def list_zones(self):
        # type: () -> List[Zone]
        """
        Return a list of zones.

        :return: ``list`` of :class:`Zone`
        """
        return list(self.iterate_zones())

    def iterate_records(self, zone):
        # type: (Zone) -> Iterator[Record]
        """
        Return a generator to iterate over records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :rtype: ``generator`` of :class:`Record`
        """
        raise NotImplementedError("iterate_records not implemented for this driver")

    def list_records(self, zone):
        # type: (Zone) -> List[Record]
        """
        Return a list of records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        return list(self.iterate_records(zone))

    def get_zone(self, zone_id):
        # type: (str) -> Zone
        """
        Return a Zone instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :rtype: :class:`Zone`
        """
        raise NotImplementedError("get_zone not implemented for this driver")

    def get_record(self, zone_id, record_id):
        # type: (str, str) -> Record
        """
        Return a Record instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :param record_id: ID of the required record
        :type  record_id: ``str``

        :rtype: :class:`Record`
        """
        raise NotImplementedError("get_record not implemented for this driver")

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        # type: (str, str, Optional[int], Optional[dict]) -> Zone
        """
        Create a new zone.

        :param domain: Zone domain name (e.g. example.com)
        :type domain: ``str``

        :param type: Zone type (master / slave).
        :type  type: ``str``

        :param ttl: TTL for new records. (optional)
        :type  ttl: ``int``

        :param extra: Extra attributes (driver specific). (optional)
        :type extra: ``dict``

        :rtype: :class:`Zone`
        """
        raise NotImplementedError("create_zone not implemented for this driver")

    def update_zone(
        self,
        zone,  # type: Zone
        domain,  # type: Optional[str]
        type="master",  # type: Optional[str]
        ttl=None,  # type: Optional[int]
        extra=None,  # type: Optional[dict]
    ):
        # type: (...) -> Zone
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

        :param extra: Extra attributes (driver specific). (optional)
        :type  extra: ``dict``

        :rtype: :class:`Zone`
        """
        raise NotImplementedError("update_zone not implemented for this driver")

    def create_record(self, name, zone, type, data, extra=None):
        # type: (str, Zone, RecordType, str, Optional[dict]) -> Record
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
        raise NotImplementedError("create_record not implemented for this driver")

    def update_record(
        self,
        record,  # type: Record
        name,  # type: Optional[str]
        type,  # type: Optional[RecordType]
        data,  # type: Optional[str]
        extra=None,  # type: Optional[dict]
    ):
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
        raise NotImplementedError("update_record not implemented for this driver")

    def delete_zone(self, zone):
        # type: (Zone) -> bool
        """
        Delete a zone.

        Note: This will delete all the records belonging to this zone.

        :param zone: Zone to delete.
        :type  zone: :class:`Zone`

        :rtype: ``bool``
        """
        raise NotImplementedError("delete_zone not implemented for this driver")

    def delete_record(self, record):
        # type: (Record) -> bool
        """
        Delete a record.

        :param record: Record to delete.
        :type  record: :class:`Record`

        :rtype: ``bool``
        """
        raise NotImplementedError("delete_record not implemented for this driver")

    def export_zone_to_bind_format(self, zone):
        # type: (Zone) -> str
        """
        Export Zone object to the BIND compatible format.

        :param zone: Zone to export.
        :type  zone: :class:`Zone`

        :return: Zone data in BIND compatible format.
        :rtype: ``str``
        """
        if zone.type != "master":
            raise ValueError("You can only generate BIND out for master zones")

        lines = []

        # For consistent output, records are sorted based on the id
        records = zone.list_records()
        records = sorted(records, key=Record._get_numeric_id)

        date = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        values = {"version": __version__, "date": date}

        lines.append("; Generated by Libcloud v%(version)s on %(date)s UTC" % values)
        lines.append("$ORIGIN {domain}.".format(domain=zone.domain))
        lines.append("$TTL {domain_ttl}\n".format(domain_ttl=zone.ttl))

        for record in records:
            line = self._get_bind_record_line(record=record)
            lines.append(line)

        output = "\n".join(lines)
        return output

    def export_zone_to_bind_zone_file(self, zone, file_path):
        # type: (Zone, str) -> None
        """
        Export Zone object to the BIND compatible format and write result to a
        file.

        :param zone: Zone to export.
        :type  zone: :class:`Zone`

        :param file_path: File path where the output will be saved.
        :type  file_path: ``str``
        """
        result = self.export_zone_to_bind_format(zone=zone)

        with open(file_path, "w") as fp:
            fp.write(result)

    def _get_bind_record_line(self, record):
        # type: (Record) -> str
        """
        Generate BIND record line for the provided record.

        :param record: Record to generate the line for.
        :type  record: :class:`Record`

        :return: Bind compatible record line.
        :rtype: ``str``
        """
        parts = []  # type: List[Any]

        if record.name:
            name = "{name}.{domain}".format(
                name=record.name,
                domain=record.zone.domain,
            )
        else:
            name = record.zone.domain

        name += "."

        ttl = record.extra["ttl"] if "ttl" in record.extra else record.zone.ttl
        ttl = str(ttl)
        data = record.data

        if record.type in [
            RecordType.CNAME,
            RecordType.DNAME,
            RecordType.MX,
            RecordType.PTR,
            RecordType.SRV,
        ]:
            # Make sure trailing dot is present
            if data[len(data) - 1] != ".":
                data += "."

        if record.type in [RecordType.TXT, RecordType.SPF] and " " in data:
            # Escape the quotes
            data = data.replace('"', '\\"')

            # Quote the string
            data = '"%s"' % (data)

        if record.type in [RecordType.MX, RecordType.SRV]:
            priority = str(record.extra["priority"])
            parts = [name, ttl, "IN", str(record.type), priority, data]
        else:
            parts = [name, ttl, "IN", str(record.type), data]

        line = "\t".join(parts)
        return line

    def _string_to_record_type(self, string):
        # type: (str) -> RecordType
        """
        Return a string representation of a DNS record type to a
        libcloud RecordType ENUM.

        :rtype: ``str``
        """
        string = string.upper()
        record_type = getattr(RecordType, string)
        return record_type
