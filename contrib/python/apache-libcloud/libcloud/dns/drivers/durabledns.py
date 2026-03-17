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
DurableDNS Driver
"""
from xml.etree.ElementTree import tostring

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import (
    Provider,
    RecordType,
    ZoneDoesNotExistError,
    ZoneAlreadyExistsError,
    RecordDoesNotExistError,
)
from libcloud.utils.py3 import httplib, ensure_string
from libcloud.common.durabledns import (
    SCHEMA_BUILDER_MAP,
    DurableResponse,
    DurableConnection,
    DurableDNSException,
)
from libcloud.common.durabledns import _schema_builder as api_schema_builder

__all__ = [
    "ZONE_EXTRA_PARAMS_DEFAULT_VALUES",
    "RECORD_EXTRA_PARAMS_DEFAULT_VALUES",
    "DEFAULT_TTL",
    "DurableDNSResponse",
    "DurableDNSConnection",
    "DurableDNSDriver",
]

# This will be the default values for each extra attributes when are not
# specified in the 'extra' parameter
ZONE_EXTRA_PARAMS_DEFAULT_VALUES = {
    "ns": "ns1.durabledns.com.",
    "mbox": "support.durabledns.com",
    "refresh": "28800",
    "retry": 7200,
    "expire": 604800,
    "minimum": 82000,
    "xfer": "",
    "update_acl": "",
}

RECORD_EXTRA_PARAMS_DEFAULT_VALUES = {"aux": 0, "ttl": 3600}

DEFAULT_TTL = 3600


class DurableDNSResponse(DurableResponse):
    pass


class DurableDNSConnection(DurableConnection):
    responseCls = DurableDNSResponse


class DurableDNSDriver(DNSDriver):
    type = Provider.DURABLEDNS
    name = "DurableDNS"
    website = "https://durabledns.com"
    connectionCls = DurableDNSConnection

    RECORD_TYPE_MAP = {
        RecordType.A: "A",
        RecordType.AAAA: "AAAA",
        RecordType.CNAME: "CNAME",
        RecordType.HINFO: "HINFO",
        RecordType.MX: "MX",
        RecordType.NS: "NS",
        RecordType.PTR: "PTR",
        RecordType.RP: "RP",
        RecordType.SRV: "SRV",
        RecordType.TXT: "TXT",
    }

    def list_zones(self):
        """
        Return a list of zones.

        :return: ``list`` of :class:`Zone`
        """
        schema_params = SCHEMA_BUILDER_MAP.get("list_zones")
        attributes = schema_params.get("attributes")
        schema = api_schema_builder(
            schema_params.get("urn_nid"), schema_params.get("method"), attributes
        )
        params = {"apiuser": self.key, "apikey": self.secret}
        urn = list(schema)[0]
        for child in urn:
            key = child.tag.split(":")[2]
            if key in attributes:
                child.text = str(params.get(key))
        req_data = tostring(schema)
        action = "/services/dns/listZones.php"
        params = {}
        headers = {"SOAPAction": "urn:listZoneswsdl#listZones"}
        response = self.connection.request(
            action=action, params=params, data=req_data, method="POST", headers=headers
        )
        # listZones method doesn't return full data in zones as getZone
        # method does.
        zones = []
        for data in response.objects:
            zone = self.get_zone(data.get("id"))
            zones.append(zone)
        return zones

    def list_records(self, zone):
        """
        Return a list of records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        schema_params = SCHEMA_BUILDER_MAP.get("list_records")
        attributes = schema_params.get("attributes")
        schema = api_schema_builder(
            schema_params.get("urn_nid"), schema_params.get("method"), attributes
        )
        params = {"apiuser": self.key, "apikey": self.secret, "zonename": zone.id}
        urn = list(schema)[0]
        for child in urn:
            key = child.tag.split(":")[2]
            if key in attributes:
                child.text = str(params.get(key))
        req_data = tostring(schema)
        action = "/services/dns/listRecords.php?"
        params = {}
        headers = {"SOAPAction": "urn:listRecordswsdl#listRecords"}
        try:
            response = self.connection.request(
                action=action,
                params=params,
                data=req_data,
                method="POST",
                headers=headers,
            )
        except DurableDNSException as e:
            if "Zone does not exist" in e.message:
                raise ZoneDoesNotExistError(zone_id=zone.id, driver=self, value=e.message)
            raise e

        # listRecords method doesn't return full data in records as getRecord
        # method does.
        records = []
        for data in response.objects:
            record = self.get_record(zone.id, data.get("id"))
            records.append(record)

        return records

    def get_zone(self, zone_id):
        """
        Return a Zone instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :rtype: :class:`Zone`
        """
        schema_params = SCHEMA_BUILDER_MAP.get("get_zone")
        attributes = schema_params.get("attributes")
        schema = api_schema_builder(
            schema_params.get("urn_nid"), schema_params.get("method"), attributes
        )
        params = {"apiuser": self.key, "apikey": self.secret, "zonename": zone_id}
        urn = list(schema)[0]
        for child in urn:
            key = child.tag.split(":")[2]
            if key in attributes:
                child.text = str(params.get(key))
        req_data = tostring(schema)
        action = "/services/dns/getZone.php?"
        params = {}
        headers = {"SOAPAction": "urn:getZonewsdl#getZone"}
        try:
            response = self.connection.request(
                action=action,
                params=params,
                data=req_data,
                method="POST",
                headers=headers,
            )
        except DurableDNSException as e:
            if "Zone does not exist" in e.message:
                raise ZoneDoesNotExistError(zone_id=zone_id, driver=self, value=e.message)
            raise e

        zones = self._to_zones(response.objects)

        return zones[0]

    def get_record(self, zone_id, record_id):
        """
        Return a Record instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :param record_id: ID of the required record
        :type  record_id: ``str``

        :rtype: :class:`Record`
        """
        schema_params = SCHEMA_BUILDER_MAP.get("get_record")
        attributes = schema_params.get("attributes")
        schema = api_schema_builder(
            schema_params.get("urn_nid"), schema_params.get("method"), attributes
        )
        params = {
            "apiuser": self.key,
            "apikey": self.secret,
            "zonename": zone_id,
            "recordid": record_id,
        }
        urn = list(schema)[0]
        for child in urn:
            key = child.tag.split(":")[2]
            if key in attributes:
                child.text = str(params.get(key))
        req_data = tostring(schema)
        action = "/services/dns/getRecord.php?"
        params = {}
        headers = {"SOAPAction": "urn:getRecordwsdl#getRecord"}
        try:
            response = self.connection.request(
                action=action,
                params=params,
                data=req_data,
                method="POST",
                headers=headers,
            )
        except DurableDNSException as e:
            if "Zone does not exist" in e.message:
                raise ZoneDoesNotExistError(zone_id=zone_id, driver=self, value=e.message)
            if "Record does not exist" in e.message:
                raise RecordDoesNotExistError(record_id=record_id, driver=self, value=e.message)
            raise e

        zone = self.get_zone(zone_id)
        record = self._to_record(response.objects[0], zone)
        return record

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        """
        Create a new zone.

        :param domain: Name of zone, followed by a dot (.) (e.g. example.com.)
        :type  domain: ``str``

        :param type: Zone type (Only master available). (optional)
        :type  type: ``str``

        :param ttl: TTL for new records. (optional)
        :type  ttl: ``int``

        :param extra: Extra attributes ('mbox', 'ns', 'minimum', 'refresh',
                                        'expire', 'update_acl', 'xfer').
                      (optional)
        :type extra: ``dict``

        :rtype: :class:`Zone`
        """
        if extra is None:
            extra = ZONE_EXTRA_PARAMS_DEFAULT_VALUES
        else:
            extra_fields = ZONE_EXTRA_PARAMS_DEFAULT_VALUES.keys()
            missing = set(extra_fields).difference(set(extra.keys()))
            for field in missing:
                extra[field] = ZONE_EXTRA_PARAMS_DEFAULT_VALUES.get(field)
        schema_params = SCHEMA_BUILDER_MAP.get("create_zone")
        attributes = schema_params.get("attributes")
        schema = api_schema_builder(
            schema_params.get("urn_nid"), schema_params.get("method"), attributes
        )
        params = {
            "apiuser": self.key,
            "apikey": self.secret,
            "zonename": domain,
            "ttl": ttl or DEFAULT_TTL,
        }
        params.update(extra)
        urn = list(schema)[0]
        for child in urn:
            key = child.tag.split(":")[2]
            if key in attributes:
                if isinstance(params.get(key), int):
                    child.text = "%d"
                else:
                    child.text = "%s"
        # We can't insert values directly in child.text because API raises
        # and exception for values that need to be integers. And tostring
        # method from ElementTree can't handle int values.
        skel = ensure_string(tostring(schema))  # Deal with PY3
        req_data = skel % (
            self.key,
            self.secret,
            domain,
            extra.get("ns"),
            extra.get("mbox"),
            extra.get("refresh"),
            extra.get("retry"),
            extra.get("expire"),
            extra.get("minimum"),
            ttl or DEFAULT_TTL,
            extra.get("xfer"),
            extra.get("update_acl"),
        )
        action = "/services/dns/createZone.php?"
        params = {}
        headers = {"SOAPAction": "urn:createZonewsdl#createZone"}
        try:
            self.connection.request(
                action=action,
                params=params,
                data=req_data,
                method="POST",
                headers=headers,
            )
        except DurableDNSException as e:
            if "Zone Already Exist" in e.message:
                raise ZoneAlreadyExistsError(zone_id=domain, driver=self, value=e.message)
            raise e

        zone = self.get_zone(domain)
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

        :param extra: Extra attributes (e.g. 'aux', 'ttl'). (optional)
        :type extra: ``dict``

        :rtype: :class:`Record`
        """
        if extra is None:
            extra = RECORD_EXTRA_PARAMS_DEFAULT_VALUES
        else:
            if "aux" not in extra:
                extra["aux"] = RECORD_EXTRA_PARAMS_DEFAULT_VALUES.get("aux")
            if "ttl" not in extra:
                extra["ttl"] = RECORD_EXTRA_PARAMS_DEFAULT_VALUES.get("ttl")
        extra["ddns_enabled"] = "N"
        schema_params = SCHEMA_BUILDER_MAP.get("create_record")
        attributes = schema_params.get("attributes")
        schema = api_schema_builder(
            schema_params.get("urn_nid"), schema_params.get("method"), attributes
        )
        params = {
            "apiuser": self.key,
            "apikey": self.secret,
            "zonename": zone.id,
            "name": name,
            "type": type,
            "data": data,
        }
        params.update(extra)
        urn = list(schema)[0]
        for child in urn:
            key = child.tag.split(":")[2]
            if key in attributes:
                if isinstance(params.get(key), int):
                    child.text = "%d"
                else:
                    child.text = "%s"
        # We can't insert values directly in child.text because API raises
        # and exception for values that need to be integers. And tostring
        # method from ElementTree can't handle int values.
        skel = ensure_string(tostring(schema))  # Deal with PY3
        req_data = skel % (
            self.key,
            self.secret,
            zone.id,
            name,
            type,
            data,
            extra.get("aux"),
            extra.get("ttl"),
            extra.get("ddns_enabled"),
        )
        action = "/services/dns/createRecord.php?"
        headers = {"SOAPAction": "urn:createRecordwsdl#createRecord"}
        try:
            response = self.connection.request(
                action=action, data=req_data, method="POST", headers=headers
            )
            objects = response.objects
        except DurableDNSException as e:
            # In DurableDNS is possible to create records with same data.
            # Their ID's will be different but the API does not implement
            # the RecordAlreadyExist exception. Only ZoneDoesNotExist will
            # be handled.
            if "Zone does not exist" in e.message:
                raise ZoneDoesNotExistError(zone_id=zone.id, driver=self, value=e.message)
            raise e

        record_item = objects[0]
        record_item["name"] = name
        record_item["type"] = type
        record_item["data"] = data
        record_item["ttl"] = extra.get("ttl")
        record_item["aux"] = extra.get("aux")

        record = self._to_record(record_item, zone)
        return record

    def update_zone(self, zone, domain, type="master", ttl=None, extra=None):
        """
        Update an existing zone.

        :param zone: Zone to update.
        :type  zone: :class:`Zone`

        :param domain: Name of zone, followed by a dot (.) (e.g. example.com.)
        :type  domain: ``str``

        :param type: Zone type (master / slave).
        :type  type: ``str``

        :param ttl: TTL for new records. (optional)
        :type  ttl: ``int``

        :param extra: Extra attributes ('ns', 'mbox', 'refresh', 'retry',
                      'expire', 'minimum', 'xfer', 'update_acl'). (optional)
        :type  extra: ``dict``

        :rtype: :class:`Zone`
        """
        if ttl is None:
            ttl = zone.ttl
        if extra is None:
            extra = zone.extra
        else:
            extra_fields = ZONE_EXTRA_PARAMS_DEFAULT_VALUES.keys()
            missing = set(extra_fields).difference(set(extra.keys()))
            for field in missing:
                extra[field] = zone.extra.get(field)
        schema_params = SCHEMA_BUILDER_MAP.get("update_zone")
        attributes = schema_params.get("attributes")
        schema = api_schema_builder(
            schema_params.get("urn_nid"), schema_params.get("method"), attributes
        )
        params = {
            "apiuser": self.key,
            "apikey": self.secret,
            "zonename": domain,
            "ttl": ttl,
        }
        params.update(extra)
        urn = list(schema)[0]
        for child in urn:
            key = child.tag.split(":")[2]
            if key in attributes:
                if isinstance(params.get(key), int):
                    child.text = "%d"
                else:
                    child.text = "%s"
        # We can't insert values directly in child.text because API raises
        # and exception for values that need to be integers. And tostring
        # method from ElementTree can't handle int values.
        skel = ensure_string(tostring(schema))  # Deal with PY3
        req_data = skel % (
            self.key,
            self.secret,
            domain,
            extra["ns"],
            extra["mbox"],
            extra["refresh"],
            extra["retry"],
            extra["expire"],
            extra["minimum"],
            ttl,
            extra["xfer"],
            extra["update_acl"],
        )
        action = "/services/dns/updateZone.php?"
        headers = {"SOAPAction": "urn:updateZonewsdl#updateZone"}
        try:
            self.connection.request(action=action, data=req_data, method="POST", headers=headers)
        except DurableDNSException as e:
            if "Zone does not exist" in e.message:
                raise ZoneDoesNotExistError(zone_id=zone.id, driver=self, value=e.message)
            raise e

        # After update the zone, serial number change. In order to have it
        # updated, we need to get again the zone data.
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

        :param type: DNS record type (A, AAAA, ...).
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        :type  data: ``str``

        :param extra: (optional) Extra attributes (driver specific).
        :type  extra: ``dict``

        :rtype: :class:`Record`
        """
        zone = record.zone
        if extra is None:
            extra = record.extra
        else:
            extra_fields = ["aux", "ttl"]
            missing = set(extra_fields).difference(set(extra.keys()))
            for field in missing:
                extra[field] = record.extra.get(field)
        extra["ddns_enabled"] = "N"
        schema_params = SCHEMA_BUILDER_MAP.get("update_record")
        attributes = schema_params.get("attributes")
        schema = api_schema_builder(
            schema_params.get("urn_nid"), schema_params.get("method"), attributes
        )
        params = {
            "apiuser": self.key,
            "apikey": self.secret,
            "zonename": zone.id,
            "id": record.id,
            "name": name,
            "data": data,
        }
        params.update(extra)
        urn = list(schema)[0]
        for child in urn:
            key = child.tag.split(":")[2]
            if key in attributes:
                if isinstance(params.get(key), int):
                    child.text = "%d"
                else:
                    child.text = "%s"
        # We can't insert values directly in child.text because API raises
        # and exception for values that need to be integers. And tostring
        # method from ElementTree can't handle int values.
        skel = ensure_string(tostring(schema))  # Deal with PY3
        req_data = skel % (
            self.key,
            self.secret,
            zone.id,
            record.id,
            name,
            extra.get("aux"),
            data,
            extra.get("ttl"),
            extra.get("ddns_enabled"),
        )
        action = "/services/dns/updateRecord.php?"
        headers = {"SOAPAction": "urn:updateRecordwsdl#updateRecord"}
        try:
            self.connection.request(action=action, data=req_data, method="POST", headers=headers)
        except DurableDNSException as e:
            if "Zone does not exist" in e.message:
                raise ZoneDoesNotExistError(zone_id=zone.id, driver=self, value=e.message)
            raise e

        record_item = {}
        record_item["id"] = record.id
        record_item["name"] = name
        record_item["type"] = type
        record_item["data"] = data
        record_item["ttl"] = extra.get("ttl")
        record_item["aux"] = extra.get("aux")
        record = self._to_record(record_item, zone)
        return record

    def delete_zone(self, zone):
        """
        Delete a zone.

        Note: This will delete all the records belonging to this zone.

        :param zone: Zone to delete.
        :type  zone: :class:`Zone`

        :rtype: ``bool``
        """
        schema_params = SCHEMA_BUILDER_MAP.get("delete_zone")
        attributes = schema_params.get("attributes")
        schema = api_schema_builder(
            schema_params.get("urn_nid"), schema_params.get("method"), attributes
        )
        params = {"apiuser": self.key, "apikey": self.secret, "zonename": zone.id}
        urn = list(schema)[0]
        for child in urn:
            key = child.tag.split(":")[2]
            if key in attributes:
                child.text = str(params.get(key))
        req_data = tostring(schema)
        action = "/services/dns/deleteZone.php?"
        headers = {"SOAPAction": "urn:deleteZonewsdl#deleteZone"}
        try:
            response = self.connection.request(
                action=action, data=req_data, method="POST", headers=headers
            )
        except DurableDNSException as e:
            if "Zone does not exist" in e.message:
                raise ZoneDoesNotExistError(zone_id=zone.id, driver=self, value=e.message)
            raise e

        return response.status in [httplib.OK]

    def delete_record(self, record):
        """
        Delete a record.

        :param record: Record to delete.
        :type  record: :class:`Record`

        :rtype: ``bool``
        """
        schema_params = SCHEMA_BUILDER_MAP.get("delete_record")
        attributes = schema_params.get("attributes")
        schema = api_schema_builder(
            schema_params.get("urn_nid"), schema_params.get("method"), attributes
        )
        params = {
            "apiuser": self.key,
            "apikey": self.secret,
            "zonename": record.zone.id,
            "id": record.id,
        }
        urn = list(schema)[0]
        for child in urn:
            key = child.tag.split(":")[2]
            if key in attributes:
                child.text = str(params.get(key))
        req_data = tostring(schema)
        action = "/services/dns/deleteRecord.php?"
        headers = {"SOAPAction": "urn:deleteRecordwsdl#deleteRecord"}
        try:
            response = self.connection.request(
                action=action, data=req_data, headers=headers, method="POST"
            )
        except DurableDNSException as e:
            if "Record does not exists" in e.message:
                raise RecordDoesNotExistError(record_id=record.id, driver=self, value=e.message)
            if "Zone does not exist" in e.message:
                raise ZoneDoesNotExistError(zone_id=record.zone.id, driver=self, value=e.message)
            raise e

        return response.status in [httplib.OK]

    def _to_zone(self, item):
        extra = item.get("extra")
        # DurableDNS does not return information about zone type. This will be
        # set as master by default.
        zone = Zone(
            id=item.get("id"),
            type="master",
            domain=item.get("id"),
            ttl=item.get("ttl"),
            driver=self,
            extra=extra,
        )

        return zone

    def _to_zones(self, items):
        zones = []
        for item in items:
            zones.append(self._to_zone(item))

        return zones

    def _to_record(self, item, zone=None):
        extra = {"aux": int(item.get("aux")), "ttl": int(item.get("ttl"))}
        record = Record(
            id=item.get("id"),
            type=item.get("type"),
            zone=zone,
            name=item.get("name"),
            data=item.get("data"),
            driver=self,
            ttl=item.get("ttl", None),
            extra=extra,
        )

        return record

    def _to_records(self, items, zone=None):
        records = []
        for item in items:
            records.append(self._to_record(item, zone))

        return records
