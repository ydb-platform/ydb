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

__all__ = ["LinodeDNSDriver"]

from datetime import datetime

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import Provider, RecordType, ZoneDoesNotExistError, RecordDoesNotExistError
from libcloud.utils.py3 import httplib
from libcloud.utils.misc import get_new_obj, merge_valid_keys
from libcloud.common.linode import (
    API_ROOT,
    DEFAULT_API_VERSION,
    LinodeResponse,
    LinodeException,
    LinodeConnection,
    LinodeResponseV4,
    LinodeExceptionV4,
    LinodeConnectionV4,
)

try:
    import simplejson as json
except ImportError:
    import json


VALID_ZONE_EXTRA_PARAMS = [
    "SOA_Email",
    "Refresh_sec",
    "Retry_sec",
    "Expire_sec",
    "status",
    "master_ips",
]

VALID_RECORD_EXTRA_PARAMS = ["Priority", "Weight", "Port", "Protocol", "TTL_sec"]

VALID_ZONE_EXTRA_PARAMS_V4 = [
    "description",
    "expire_sec",
    "master_ips",
    "refresh_sec",
    "retry_sec",
    "soa_email",
    "status",
    "tags",
]

VALID_RECORD_EXTRA_PARAMS_V4 = [
    "port",
    "priority",
    "protocol",
    "service",
    "tag",
    "ttl_sec",
    "target",
    "weight",
]


class LinodeDNSDriver(DNSDriver):
    type = Provider.LINODE
    name = "Linode DNS"
    website = "http://www.linode.com/"

    def __new__(
        cls,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        api_version=DEFAULT_API_VERSION,
        **kwargs,
    ):
        if cls is LinodeDNSDriver:
            if api_version == "3.0":
                cls = LinodeDNSDriverV3
            elif api_version == "4.0":
                cls = LinodeDNSDriverV4
            else:
                raise NotImplementedError(
                    "No Linode driver found for API version: %s" % (api_version)
                )
        return super().__new__(cls)


class LinodeDNSResponse(LinodeResponse):
    def _make_excp(self, error):
        result = super()._make_excp(error)
        if isinstance(result, LinodeException) and result.code == 5:
            context = self.connection.context

            if context["resource"] == "zone":
                result = ZoneDoesNotExistError(
                    value="", driver=self.connection.driver, zone_id=context["id"]
                )

            elif context["resource"] == "record":
                result = RecordDoesNotExistError(
                    value="", driver=self.connection.driver, record_id=context["id"]
                )
        return result


class LinodeDNSConnection(LinodeConnection):
    responseCls = LinodeDNSResponse


class LinodeDNSDriverV3(LinodeDNSDriver):
    connectionCls = LinodeDNSConnection

    RECORD_TYPE_MAP = {
        RecordType.NS: "NS",
        RecordType.MX: "MX",
        RecordType.A: "A",
        RecordType.AAAA: "AAAA",
        RecordType.CNAME: "CNAME",
        RecordType.TXT: "TXT",
        RecordType.SRV: "SRV",
    }

    def list_zones(self):
        params = {"api_action": "domain.list"}
        data = self.connection.request(API_ROOT, params=params).objects[0]
        zones = self._to_zones(data)
        return zones

    def list_records(self, zone):
        params = {"api_action": "domain.resource.list", "DOMAINID": zone.id}

        self.connection.set_context(context={"resource": "zone", "id": zone.id})
        data = self.connection.request(API_ROOT, params=params).objects[0]
        records = self._to_records(items=data, zone=zone)
        return records

    def get_zone(self, zone_id):
        params = {"api_action": "domain.list", "DomainID": zone_id}
        self.connection.set_context(context={"resource": "zone", "id": zone_id})
        data = self.connection.request(API_ROOT, params=params).objects[0]
        zones = self._to_zones(data)

        if len(zones) != 1:
            raise ZoneDoesNotExistError(value="", driver=self, zone_id=zone_id)

        return zones[0]

    def get_record(self, zone_id, record_id):
        zone = self.get_zone(zone_id=zone_id)
        params = {
            "api_action": "domain.resource.list",
            "DomainID": zone_id,
            "ResourceID": record_id,
        }
        self.connection.set_context(context={"resource": "record", "id": record_id})
        data = self.connection.request(API_ROOT, params=params).objects[0]
        records = self._to_records(items=data, zone=zone)

        if len(records) != 1:
            raise RecordDoesNotExistError(value="", driver=self, record_id=record_id)

        return records[0]

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        """
        Create a new zone.

        API docs: http://www.linode.com/api/dns/domain.create
        """
        params = {"api_action": "domain.create", "Type": type, "Domain": domain}

        if ttl:
            params["TTL_sec"] = ttl

        merged = merge_valid_keys(params=params, valid_keys=VALID_ZONE_EXTRA_PARAMS, extra=extra)
        data = self.connection.request(API_ROOT, params=params).objects[0]
        zone = Zone(
            id=data["DomainID"],
            domain=domain,
            type=type,
            ttl=ttl,
            extra=merged,
            driver=self,
        )
        return zone

    def update_zone(self, zone, domain=None, type=None, ttl=None, extra=None):
        """
        Update an existing zone.

        API docs: http://www.linode.com/api/dns/domain.update
        """
        params = {"api_action": "domain.update", "DomainID": zone.id}

        if type:
            params["Type"] = type

        if domain:
            params["Domain"] = domain

        if ttl:
            params["TTL_sec"] = ttl

        merged = merge_valid_keys(params=params, valid_keys=VALID_ZONE_EXTRA_PARAMS, extra=extra)
        self.connection.request(API_ROOT, params=params).objects[0]
        updated_zone = get_new_obj(
            obj=zone,
            klass=Zone,
            attributes={"domain": domain, "type": type, "ttl": ttl, "extra": merged},
        )
        return updated_zone

    def create_record(self, name, zone, type, data, extra=None):
        """
        Create a new record.

        API docs: http://www.linode.com/api/dns/domain.resource.create
        """
        params = {
            "api_action": "domain.resource.create",
            "DomainID": zone.id,
            "Name": name,
            "Target": data,
            "Type": self.RECORD_TYPE_MAP[type],
        }
        merged = merge_valid_keys(params=params, valid_keys=VALID_RECORD_EXTRA_PARAMS, extra=extra)

        result = self.connection.request(API_ROOT, params=params).objects[0]
        record = Record(
            id=result["ResourceID"],
            name=name,
            type=type,
            data=data,
            extra=merged,
            zone=zone,
            driver=self,
            ttl=merged.get("TTL_sec", None),
        )
        return record

    def update_record(self, record, name=None, type=None, data=None, extra=None):
        """
        Update an existing record.

        API docs: http://www.linode.com/api/dns/domain.resource.update
        """
        params = {
            "api_action": "domain.resource.update",
            "ResourceID": record.id,
            "DomainID": record.zone.id,
        }

        if name:
            params["Name"] = name

        if data:
            params["Target"] = data

        if type is not None:
            params["Type"] = self.RECORD_TYPE_MAP[type]

        merged = merge_valid_keys(params=params, valid_keys=VALID_RECORD_EXTRA_PARAMS, extra=extra)

        self.connection.request(API_ROOT, params=params).objects[0]
        updated_record = get_new_obj(
            obj=record,
            klass=Record,
            attributes={"name": name, "data": data, "type": type, "extra": merged},
        )
        return updated_record

    def delete_zone(self, zone):
        params = {"api_action": "domain.delete", "DomainID": zone.id}

        self.connection.set_context(context={"resource": "zone", "id": zone.id})
        data = self.connection.request(API_ROOT, params=params).objects[0]

        return "DomainID" in data

    def delete_record(self, record):
        params = {
            "api_action": "domain.resource.delete",
            "DomainID": record.zone.id,
            "ResourceID": record.id,
        }

        self.connection.set_context(context={"resource": "record", "id": record.id})
        data = self.connection.request(API_ROOT, params=params).objects[0]

        return "ResourceID" in data

    def _to_zones(self, items):
        """
        Convert a list of items to the Zone objects.
        """
        zones = []

        for item in items:
            zones.append(self._to_zone(item))

        return zones

    def _to_zone(self, item):
        """
        Build an Zone object from the item dictionary.
        """
        extra = {
            "SOA_Email": item["SOA_EMAIL"],
            "status": item["STATUS"],
            "description": item["DESCRIPTION"],
        }
        zone = Zone(
            id=item["DOMAINID"],
            domain=item["DOMAIN"],
            type=item["TYPE"],
            ttl=item["TTL_SEC"],
            driver=self,
            extra=extra,
        )
        return zone

    def _to_records(self, items, zone=None):
        """
        Convert a list of items to the Record objects.
        """
        records = []

        for item in items:
            records.append(self._to_record(item=item, zone=zone))

        return records

    def _to_record(self, item, zone=None):
        """
        Build a Record object from the item dictionary.
        """
        extra = {
            "protocol": item["PROTOCOL"],
            "ttl_sec": item["TTL_SEC"],
            "port": item["PORT"],
            "weight": item["WEIGHT"],
            "priority": item["PRIORITY"],
        }
        type = self._string_to_record_type(item["TYPE"])
        record = Record(
            id=item["RESOURCEID"],
            name=item["NAME"],
            type=type,
            data=item["TARGET"],
            zone=zone,
            driver=self,
            ttl=item["TTL_SEC"],
            extra=extra,
        )
        return record


class LinodeDNSResponseV4(LinodeResponseV4):
    pass


class LinodeDNSConnectionV4(LinodeConnectionV4):
    responseCls = LinodeDNSResponseV4


class LinodeDNSDriverV4(LinodeDNSDriver):
    connectionCls = LinodeDNSConnectionV4

    RECORD_TYPE_MAP = {
        RecordType.SOA: "SOA",
        RecordType.NS: "NS",
        RecordType.MX: "MX",
        RecordType.A: "A",
        RecordType.AAAA: "AAAA",
        RecordType.CNAME: "CNAME",
        RecordType.TXT: "TXT",
        RecordType.SRV: "SRV",
        RecordType.CAA: "CAA",
    }

    def list_zones(self):
        """
        Return a list of zones.

        :return: ``list`` of :class:`Zone`
        """
        data = self._paginated_request("/v4/domains", "data")
        return [self._to_zone(zone) for zone in data]

    def list_records(self, zone):
        """
        Return a list of records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        if not isinstance(zone, Zone):
            raise LinodeExceptionV4("Invalid zone instance")

        data = self._paginated_request("/v4/domains/%s/records" % zone.id, "data")

        return [self._to_record(record, zone=zone) for record in data]

    def get_zone(self, zone_id):
        """
        Return a Zone instance.

        :param zone_id: ID of the zone to get
        :type  zone_id: ``str``

        :rtype: :class:`Zone`
        """
        data = self.connection.request("/v4/domains/%s" % zone_id).object

        return self._to_zone(data)

    def get_record(self, zone_id, record_id):
        """
        Return a record instance.

        :param zone_id: ID of the record's zone
        :type  zone_id: ``str``

        :param record_id: ID of the record to get
        :type  record_id: ``str``

        :rtype: :class:`Record`
        """
        data = self.connection.request(
            "/v4/domains/{}/records/{}".format(zone_id, record_id)
        ).object

        return self._to_record(data, self.get_zone(zone_id))

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        """
        Create a new zone.

        :param domain: Zone domain name (e.g. example.com)
        :type domain: ``str``

        :keyword type: Zone type (master / slave).
        :type  type: ``str``

        :keyword ttl: TTL for new records. (optional)
        :type  ttl: ``int``

        :keyword extra: Extra attributes.('description', 'expire_sec', \
        'master_ips','refresh_sec', 'retry_sec', 'soa_email',\
        'status', 'tags'). 'soa_email' required for master zones
        :type extra: ``dict``

        :rtype: :class:`Zone`
        """

        attr = {
            "domain": domain,
            "type": type,
        }
        if ttl is not None:
            attr["ttl_sec"] = ttl

        merge_valid_keys(params=attr, valid_keys=VALID_ZONE_EXTRA_PARAMS_V4, extra=extra)
        data = self.connection.request("/v4/domains", data=json.dumps(attr), method="POST").object

        return self._to_zone(data)

    def create_record(self, name, zone, type, data, extra=None):
        """
        Create a record.

        :param name: The name of this Record.For A records\
        this is the subdomain being associated with an IP address.
        :type  name: ``str``

        :param zone: Zone which the records will be created for.
        :type zone: :class:`Zone`

        :param type: DNS record type.
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).\
         For A records, this is the address the domain should resolve to.
        :type  data: ``str``

        :param extra: Extra attributes.('port', 'priority', \
        'protocol', 'service', 'tag', 'ttl_sec', 'target', 'weight')
        :type  extra: ``dict``

        :rtype: :class:`Record`
        """
        if not isinstance(zone, Zone):
            raise LinodeExceptionV4("Invalid zone instance")

        attr = {
            "type": self.RECORD_TYPE_MAP[type],
            "name": name,
            "target": data,
        }

        merge_valid_keys(params=attr, valid_keys=VALID_RECORD_EXTRA_PARAMS_V4, extra=extra)
        data = self.connection.request(
            "/v4/domains/%s/records" % zone.id, data=json.dumps(attr), method="POST"
        ).object

        return self._to_record(data, zone=zone)

    def update_zone(self, zone, domain, type="master", ttl=None, extra=None):
        """
        Update an existing zone.

        :param zone: Zone to update.
        :type  zone: :class:`Zone`

        :param domain: Name of zone
        :type  domain: ``str``

        :param type: Zone type (master / slave).
        :type  type: ``str``

        :param ttl: TTL for new records. (optional)
        :type  ttl: ``int``

        :param extra: Extra attributes ('description', 'expire_sec', \
        'master_ips','refresh_sec', 'retry_sec', 'soa_email','status', 'tags')

        :type  extra: ``dict``

        :rtype: :class:`Zone`
        """
        if not isinstance(zone, Zone):
            raise LinodeExceptionV4("Invalid zone instance")

        attr = {
            "domain": domain,
            "type": type,
        }

        if ttl is not None:
            attr["ttl_sec"] = ttl

        merge_valid_keys(params=attr, valid_keys=VALID_ZONE_EXTRA_PARAMS_V4, extra=extra)

        data = self.connection.request(
            "/v4/domains/%s" % zone.id, data=json.dumps(attr), method="PUT"
        ).object

        return self._to_zone(data)

    def update_record(self, record, name=None, type=None, data=None, extra=None):
        """
        Update an existing record.

        :param record: Record to update.
        :type  record: :class:`Record`

        :param name: Record name.This field's actual usage\
         depends on the type of record this represents.
        :type  name: ``str``

        :param type: DNS record type
        :type  type: :class:`RecordType`

        :param data: Data for the record (depends on the record type).
        :type  data: ``str``

        :param extra: Extra attributes.('port', 'priority', \
        'protocol', 'service', 'tag', 'ttl_sec', 'target', 'weight')
        :type  extra: ``dict``

        :rtype: :class:`Record`
        """
        if not isinstance(record, Record):
            raise LinodeExceptionV4("Invalid zone instance")

        zone = record.zone
        attr = {}

        if name is not None:
            attr["name"] = name

        if type is not None:
            attr["type"] = self.RECORD_TYPE_MAP[type]

        if data is not None:
            attr["target"] = data

        merge_valid_keys(params=attr, valid_keys=VALID_RECORD_EXTRA_PARAMS_V4, extra=extra)

        data = self.connection.request(
            "/v4/domains/{}/records/{}".format(zone.id, record.id),
            data=json.dumps(attr),
            method="PUT",
        ).object

        return self._to_record(data, zone=zone)

    def delete_zone(self, zone):
        """
        Delete a zone.

        This will delete all the records belonging to this zone.

        :param zone: Zone to delete.
        :type  zone: :class:`Zone`

        :rtype: ``bool``
        """
        if not isinstance(zone, Zone):
            raise LinodeExceptionV4("Invalid zone instance")

        response = self.connection.request("/v4/domains/%s" % zone.id, method="DELETE")
        return response.status == httplib.OK

    def delete_record(self, record):
        """
        Delete a record.

        :param record: Record to delete.
        :type  record: :class:`Record`

        :rtype: ``bool``
        """
        if not isinstance(record, Record):
            raise LinodeExceptionV4("Invalid record instance")

        zone = record.zone

        response = self.connection.request(
            "/v4/domains/{}/records/{}".format(zone.id, record.id), method="DELETE"
        )
        return response.status == httplib.OK

    def _to_record(self, item, zone=None):
        extra = {
            "port": item["port"],
            "weight": item["weight"],
            "priority": item["priority"],
            "service": item["service"],
            "protocol": item["protocol"],
            "created": self._to_datetime(item["created"]),
            "updated": self._to_datetime(item["updated"]),
        }
        type = self._string_to_record_type(item["type"])
        record = Record(
            id=item["id"],
            name=item["name"],
            type=type,
            data=item["target"],
            zone=zone,
            driver=self,
            ttl=item["ttl_sec"],
            extra=extra,
        )
        return record

    def _to_zone(self, item):
        """
        Build an Zone object from the item dictionary.
        """
        extra = {
            "soa_email": item["soa_email"],
            "status": item["status"],
            "description": item["description"],
            "tags": item["tags"],
            "retry_sec": item["retry_sec"],
            "master_ips": item["master_ips"],
            "axfr_ips": item["axfr_ips"],
            "expire_sec": item["expire_sec"],
            "refresh_sec": item["refresh_sec"],
            "created": self._to_datetime(item["created"]),
            "updated": self._to_datetime(item["updated"]),
        }
        zone = Zone(
            id=item["id"],
            domain=item["domain"],
            type=item["type"],
            ttl=item["ttl_sec"],
            driver=self,
            extra=extra,
        )
        return zone

    def _to_datetime(self, strtime):
        return datetime.strptime(strtime, "%Y-%m-%dT%H:%M:%S")

    def _paginated_request(self, url, obj, params=None):
        """
        Perform multiple calls in order to have a full list of elements when
        the API responses are paginated.

        :param url: API endpoint
        :type url: ``str``

        :param obj: Result object key
        :type obj: ``str``

        :param params: Request parameters
        :type params: ``dict``

        :return: ``list`` of API response objects
        :rtype: ``list``
        """
        objects = []
        params = params if params is not None else {}

        ret = self.connection.request(url, params=params).object

        data = list(ret.get(obj, []))
        current_page = int(ret.get("page", 1))
        num_of_pages = int(ret.get("pages", 1))
        objects.extend(data)
        for page in range(current_page + 1, num_of_pages + 1):
            # add param to request next page
            params["page"] = page
            ret = self.connection.request(url, params=params).object
            data = list(ret.get(obj, []))
            objects.extend(data)
        return objects
