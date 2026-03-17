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
import copy

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import Provider, RecordType, ZoneDoesNotExistError, RecordDoesNotExistError
from libcloud.utils.py3 import httplib
from libcloud.utils.misc import get_new_obj, merge_valid_keys
from libcloud.common.base import PollingConnection
from libcloud.common.types import LibcloudError
from libcloud.common.openstack import OpenStackDriverMixin
from libcloud.common.rackspace import AUTH_URL
from libcloud.common.exceptions import BaseHTTPError
from libcloud.compute.drivers.openstack import OpenStack_1_1_Response, OpenStack_1_1_Connection

__all__ = ["RackspaceDNSResponse", "RackspaceDNSConnection"]

VALID_ZONE_EXTRA_PARAMS = ["email", "comment", "ns1"]
VALID_RECORD_EXTRA_PARAMS = ["ttl", "comment", "priority", "created", "updated"]


class RackspaceDNSResponse(OpenStack_1_1_Response):
    """
    Rackspace DNS Response class.
    """

    def parse_error(self):
        status = int(self.status)
        context = self.connection.context
        body = self.parse_body()

        if status == httplib.NOT_FOUND:
            if context["resource"] == "zone":
                raise ZoneDoesNotExistError(value="", driver=self, zone_id=context["id"])
            elif context["resource"] == "record":
                raise RecordDoesNotExistError(value="", driver=self, record_id=context["id"])
        if body:
            if "code" and "message" in body:
                err = "{} - {} ({})".format(body["code"], body["message"], body["details"])
                return err
            elif "validationErrors" in body:
                errors = [m for m in body["validationErrors"]["messages"]]
                err = "Validation errors: %s" % ", ".join(errors)
                return err

        raise LibcloudError("Unexpected status code: %s" % (status))


class RackspaceDNSConnection(OpenStack_1_1_Connection, PollingConnection):
    """
    Rackspace DNS Connection class.
    """

    responseCls = RackspaceDNSResponse
    XML_NAMESPACE = None
    poll_interval = 2.5
    timeout = 30

    auth_url = AUTH_URL
    _auth_version = "2.0"

    def __init__(self, *args, **kwargs):
        self.region = kwargs.pop("region", None)
        super().__init__(*args, **kwargs)

    def get_poll_request_kwargs(self, response, context, request_kwargs):
        job_id = response.object["jobId"]
        kwargs = {"action": "/status/%s" % (job_id), "params": {"showDetails": True}}
        return kwargs

    def has_completed(self, response):
        status = response.object["status"]
        if status == "ERROR":
            data = response.object["error"]

            if "code" and "message" in data:
                message = "{} - {} ({})".format(
                    data["code"],
                    data["message"],
                    data["details"],
                )
            else:
                message = data["message"]

            raise LibcloudError(message, driver=self.driver)

        return status == "COMPLETED"

    def get_endpoint(self):
        if "2.0" in self._auth_version:
            ep = self.service_catalog.get_endpoint(
                name="cloudDNS", service_type="rax:dns", region=None
            )
        else:
            raise LibcloudError("Auth version %s not supported" % (self._auth_version))

        public_url = ep.url

        # This is a nasty hack, but because of how global auth and old accounts
        # work, there is no way around it.
        if self.region == "us":
            # Old UK account, which only has us endpoint in the catalog
            public_url = public_url.replace("https://lon.dns.api", "https://dns.api")
        if self.region == "uk":
            # Old US account, which only has uk endpoint in the catalog
            public_url = public_url.replace("https://dns.api", "https://lon.dns.api")

        return public_url


class RackspacePTRRecord:
    def __init__(self, id, ip, domain, driver, extra=None):
        self.id = str(id) if id else None
        self.ip = ip
        self.type = RecordType.PTR
        self.domain = domain
        self.driver = driver
        self.extra = extra or {}

    def update(self, domain, extra=None):
        return self.driver.ex_update_ptr_record(record=self, domain=domain, extra=extra)

    def delete(self):
        return self.driver.ex_delete_ptr_record(record=self)

    def __repr__(self):
        return "<{}: ip={}, domain={}, provider={} ...>".format(
            self.__class__.__name__,
            self.ip,
            self.domain,
            self.driver.name,
        )


class RackspaceDNSDriver(DNSDriver, OpenStackDriverMixin):
    name = "Rackspace DNS"
    website = "http://www.rackspace.com/"
    type = Provider.RACKSPACE
    connectionCls = RackspaceDNSConnection

    def __init__(self, key, secret=None, secure=True, host=None, port=None, region="us", **kwargs):
        valid_regions = self.list_regions()
        if region not in valid_regions:
            raise ValueError("Invalid region: %s" % (region))

        OpenStackDriverMixin.__init__(self, **kwargs)
        super().__init__(key=key, secret=secret, host=host, port=port, region=region)

    RECORD_TYPE_MAP = {
        RecordType.A: "A",
        RecordType.AAAA: "AAAA",
        RecordType.CNAME: "CNAME",
        RecordType.MX: "MX",
        RecordType.NS: "NS",
        RecordType.PTR: "PTR",
        RecordType.SRV: "SRV",
        RecordType.TXT: "TXT",
    }

    @classmethod
    def list_regions(cls):
        return ["us", "uk"]

    def iterate_zones(self):
        offset = 0
        limit = 100
        while True:
            params = {
                "limit": limit,
                "offset": offset,
            }
            response = self.connection.request(action="/domains", params=params).object
            zones_list = response["domains"]
            for item in zones_list:
                yield self._to_zone(item)

            if _rackspace_result_has_more(response, len(zones_list), limit):
                offset += limit
            else:
                break

    def iterate_records(self, zone):
        self.connection.set_context({"resource": "zone", "id": zone.id})
        offset = 0
        limit = 100
        while True:
            params = {
                "showRecord": True,
                "limit": limit,
                "offset": offset,
            }
            response = self.connection.request(
                action="/domains/%s" % (zone.id), params=params
            ).object
            records_list = response["recordsList"]
            records = records_list["records"]
            for item in records:
                record = self._to_record(data=item, zone=zone)
                yield record

            if _rackspace_result_has_more(records_list, len(records), limit):
                offset += limit
            else:
                break

    def get_zone(self, zone_id):
        self.connection.set_context({"resource": "zone", "id": zone_id})
        response = self.connection.request(action="/domains/%s" % (zone_id))
        zone = self._to_zone(data=response.object)
        return zone

    def get_record(self, zone_id, record_id):
        zone = self.get_zone(zone_id=zone_id)
        self.connection.set_context({"resource": "record", "id": record_id})
        response = self.connection.request(
            action="/domains/{}/records/{}".format(zone_id, record_id)
        ).object
        record = self._to_record(data=response, zone=zone)
        return record

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        extra = extra if extra else {}

        # Email address is required
        if "email" not in extra:
            raise ValueError('"email" key must be present in extra dictionary')

        payload = {
            "name": domain,
            "emailAddress": extra["email"],
            "recordsList": {"records": []},
        }

        if ttl:
            payload["ttl"] = ttl

        if "comment" in extra:
            payload["comment"] = extra["comment"]

        data = {"domains": [payload]}
        response = self.connection.async_request(action="/domains", method="POST", data=data)
        zone = self._to_zone(data=response.object["response"]["domains"][0])
        return zone

    def update_zone(self, zone, domain=None, type=None, ttl=None, extra=None):
        # Only ttl, comment and email address can be changed
        extra = extra if extra else {}

        if domain:
            raise LibcloudError("Domain cannot be changed", driver=self)

        data = {}

        if ttl:
            data["ttl"] = int(ttl)

        if "email" in extra:
            data["emailAddress"] = extra["email"]

        if "comment" in extra:
            data["comment"] = extra["comment"]

        type = type if type else zone.type
        ttl = ttl if ttl else zone.ttl

        self.connection.set_context({"resource": "zone", "id": zone.id})
        self.connection.async_request(action="/domains/%s" % (zone.id), method="PUT", data=data)
        merged = merge_valid_keys(
            params=copy.deepcopy(zone.extra),
            valid_keys=VALID_ZONE_EXTRA_PARAMS,
            extra=extra,
        )
        updated_zone = get_new_obj(
            obj=zone, klass=Zone, attributes={"type": type, "ttl": ttl, "extra": merged}
        )
        return updated_zone

    def create_record(self, name, zone, type, data, extra=None):
        # Name must be a FQDN - e.g. if domain is "foo.com" then a record
        # name is "bar.foo.com"
        extra = extra if extra else {}

        name = self._to_full_record_name(domain=zone.domain, name=name)
        data = {"name": name, "type": self.RECORD_TYPE_MAP[type], "data": data}

        if "ttl" in extra:
            data["ttl"] = int(extra["ttl"])

        if "priority" in extra:
            data["priority"] = int(extra["priority"])

        payload = {"records": [data]}
        self.connection.set_context({"resource": "zone", "id": zone.id})
        response = self.connection.async_request(
            action="/domains/%s/records" % (zone.id), data=payload, method="POST"
        ).object
        record = self._to_record(data=response["response"]["records"][0], zone=zone)
        return record

    def update_record(self, record, name=None, type=None, data=None, extra=None):
        # Only data, ttl, and comment attributes can be modified, but name
        # attribute must always be present.
        extra = extra if extra else {}

        name = self._to_full_record_name(domain=record.zone.domain, name=record.name)
        payload = {"name": name}

        if data:
            payload["data"] = data

        if "ttl" in extra:
            payload["ttl"] = extra["ttl"]

        if "comment" in extra:
            payload["comment"] = extra["comment"]

        type = type if type is not None else record.type
        data = data if data else record.data

        self.connection.set_context({"resource": "record", "id": record.id})
        self.connection.async_request(
            action="/domains/{}/records/{}".format(record.zone.id, record.id),
            method="PUT",
            data=payload,
        )

        merged = merge_valid_keys(
            params=copy.deepcopy(record.extra),
            valid_keys=VALID_RECORD_EXTRA_PARAMS,
            extra=extra,
        )
        updated_record = get_new_obj(
            obj=record,
            klass=Record,
            attributes={"type": type, "data": data, "driver": self, "extra": merged},
        )
        return updated_record

    def delete_zone(self, zone):
        self.connection.set_context({"resource": "zone", "id": zone.id})
        self.connection.async_request(action="/domains/%s" % (zone.id), method="DELETE")
        return True

    def delete_record(self, record):
        self.connection.set_context({"resource": "record", "id": record.id})
        self.connection.async_request(
            action="/domains/{}/records/{}".format(record.zone.id, record.id),
            method="DELETE",
        )
        return True

    def ex_iterate_ptr_records(self, device):
        """
        Return a generator to iterate over existing PTR Records.

        The ``device`` should be an instance of one of these:
            :class:`libcloud.compute.base.Node`
            :class:`libcloud.loadbalancer.base.LoadBalancer`

        And it needs to have the following ``extra`` fields set:
            service_name - the service catalog name for the device
            uri - the URI pointing to the GET endpoint for the device

        Those are automatically set for you if you got the device from
        the Rackspace driver for that service.

        For example:
            server = rs_compute.ex_get_node_details(id)
            ptr_iter = rs_dns.ex_list_ptr_records(server)

            loadbalancer = rs_lbs.get_balancer(id)
            ptr_iter = rs_dns.ex_list_ptr_records(loadbalancer)

        Note: the Rackspace DNS API docs indicate that the device 'href' is
        optional, but testing does not bear this out. It throws a
        400 Bad Request error if you do not pass in the 'href' from
        the server or loadbalancer.  So ``device`` is required.

        :param device: the device that owns the IP
        :rtype: ``generator`` of :class:`RackspacePTRRecord`
        """
        _check_ptr_extra_fields(device)
        params = {"href": device.extra["uri"]}

        service_name = device.extra["service_name"]

        # without a valid context, the 404 on empty list will blow up
        # in the error-handling code
        self.connection.set_context({"resource": "ptr_records"})
        try:
            response = self.connection.request(
                action="/rdns/%s" % (service_name), params=params
            ).object
            records = response["records"]
            link = dict(rel=service_name, **params)
            for item in records:
                record = self._to_ptr_record(data=item, link=link)
                yield record
        except BaseHTTPError as exc:
            # 404 just means empty list
            if exc.code == 404:
                return
            raise

    def ex_get_ptr_record(self, service_name, record_id):
        """
        Get a specific PTR record by id.

        :param service_name: the service catalog name of the linked device(s)
                             i.e. cloudLoadBalancers or cloudServersOpenStack
        :param record_id: the id (i.e. PTR-12345) of the PTR record
        :rtype: instance of :class:`RackspacePTRRecord`
        """
        self.connection.set_context({"resource": "record", "id": record_id})
        response = self.connection.request(
            action="/rdns/{}/{}".format(service_name, record_id)
        ).object
        item = next(iter(response["recordsList"]["records"]))
        return self._to_ptr_record(data=item, link=response["link"])

    def ex_create_ptr_record(self, device, ip, domain, extra=None):
        """
        Create a PTR record for a specific IP on a specific device.

        The ``device`` should be an instance of one of these:
            :class:`libcloud.compute.base.Node`
            :class:`libcloud.loadbalancer.base.LoadBalancer`

        And it needs to have the following ``extra`` fields set:
            service_name - the service catalog name for the device
            uri - the URI pointing to the GET endpoint for the device

        Those are automatically set for you if you got the device from
        the Rackspace driver for that service.

        For example:
            server = rs_compute.ex_get_node_details(id)
            rs_dns.create_ptr_record(server, ip, domain)

            loadbalancer = rs_lbs.get_balancer(id)
            rs_dns.create_ptr_record(loadbalancer, ip, domain)

        :param device: the device that owns the IP
        :param ip: the IP for which you want to set reverse DNS
        :param domain: the fqdn you want that IP to represent
        :param extra: a ``dict`` with optional extra values:
            ttl - the time-to-live of the PTR record
        :rtype: instance of :class:`RackspacePTRRecord`
        """
        _check_ptr_extra_fields(device)

        if extra is None:
            extra = {}

        # the RDNS API reverse the name and data fields for PTRs
        # the record name *should* be the ip and the data the fqdn
        data = {"name": domain, "type": RecordType.PTR, "data": ip}

        if "ttl" in extra:
            data["ttl"] = extra["ttl"]

        payload = {
            "recordsList": {"records": [data]},
            "link": {
                "content": "",
                "href": device.extra["uri"],
                "rel": device.extra["service_name"],
            },
        }
        response = self.connection.async_request(action="/rdns", method="POST", data=payload).object
        item = next(iter(response["response"]["records"]))
        return self._to_ptr_record(data=item, link=payload["link"])

    def ex_update_ptr_record(self, record, domain=None, extra=None):
        """
        Update a PTR record for a specific IP on a specific device.

        If you need to change the domain or ttl, use this API to
        update the record by deleting the old one and creating a new one.

        :param record: the original :class:`RackspacePTRRecord`
        :param domain: the fqdn you want that IP to represent
        :param extra: a ``dict`` with optional extra values:
            ttl - the time-to-live of the PTR record
        :rtype: instance of :class:`RackspacePTRRecord`
        """
        if domain is not None and domain == record.domain:
            domain = None

        if extra is not None:
            extra = dict(extra)
            for key in extra:
                if key in record.extra and record.extra[key] == extra[key]:
                    del extra[key]

        if domain is None and not extra:
            # nothing to do, it already matches
            return record

        _check_ptr_extra_fields(record)
        ip = record.ip

        self.ex_delete_ptr_record(record)
        # records have the same metadata in 'extra' as the original device
        # so you can pass the original record object in instead
        return self.ex_create_ptr_record(record, ip, domain, extra=extra)

    def ex_delete_ptr_record(self, record):
        """
        Delete an existing PTR Record

        :param record: the original :class:`RackspacePTRRecord`
        :rtype: ``bool``
        """
        _check_ptr_extra_fields(record)
        self.connection.set_context({"resource": "record", "id": record.id})
        self.connection.async_request(
            action="/rdns/%s" % (record.extra["service_name"]),
            method="DELETE",
            params={"href": record.extra["uri"], "ip": record.ip},
        )
        return True

    def _to_zone(self, data):
        id = data["id"]
        domain = data["name"]
        type = "master"
        ttl = data.get("ttl", 0)
        extra = {}

        if "emailAddress" in data:
            extra["email"] = data["emailAddress"]

        if "comment" in data:
            extra["comment"] = data["comment"]

        zone = Zone(id=str(id), domain=domain, type=type, ttl=int(ttl), driver=self, extra=extra)
        return zone

    def _to_record(self, data, zone):
        id = data["id"]
        fqdn = data["name"]
        name = self._to_partial_record_name(domain=zone.domain, name=fqdn)
        type = self._string_to_record_type(data["type"])
        record_data = data["data"]
        extra = {"fqdn": fqdn}

        for key in VALID_RECORD_EXTRA_PARAMS:
            if key in data:
                extra[key] = data[key]

        record = Record(
            id=str(id),
            name=name,
            type=type,
            data=record_data,
            zone=zone,
            driver=self,
            ttl=extra.get("ttl", None),
            extra=extra,
        )
        return record

    def _to_ptr_record(self, data, link):
        id = data["id"]
        ip = data["data"]
        domain = data["name"]
        extra = {"uri": link["href"], "service_name": link["rel"]}

        for key in VALID_RECORD_EXTRA_PARAMS:
            if key in data:
                extra[key] = data[key]

        record = RackspacePTRRecord(id=str(id), ip=ip, domain=domain, driver=self, extra=extra)
        return record

    def _to_full_record_name(self, domain, name):
        """
        Build a FQDN from a domain and record name.

        :param domain: Domain name.
        :type domain: ``str``

        :param name: Record name.
        :type name: ``str``
        """
        if name:
            name = "{}.{}".format(name, domain)
        else:
            name = domain

        return name

    def _to_partial_record_name(self, domain, name):
        """
        Remove domain portion from the record name.

        :param domain: Domain name.
        :type domain: ``str``

        :param name: Full record name (fqdn).
        :type name: ``str``
        """
        if name == domain:
            # Map "root" record names to None to be consistent with other
            # drivers
            return None

        # Strip domain portion
        name = name.replace(".%s" % (domain), "")
        return name

    def _ex_connection_class_kwargs(self):
        kwargs = self.openstack_connection_kwargs()
        kwargs["region"] = self.region
        return kwargs


def _rackspace_result_has_more(response, result_length, limit):
    # If rackspace returns less than the limit, then we've reached the end of
    # the result set.
    if result_length < limit:
        return False

    # Paginated results return links to the previous and next sets of data, but
    # 'next' only exists when there is more to get.
    for item in response.get("links", ()):
        if item["rel"] == "next":
            return True
    return False


def _check_ptr_extra_fields(device_or_record):
    if not (
        hasattr(device_or_record, "extra")
        and isinstance(device_or_record.extra, dict)
        and device_or_record.extra.get("uri") is not None
        and device_or_record.extra.get("service_name") is not None
    ):
        raise LibcloudError(
            "Can't create PTR Record for %s because it "
            "doesn't have a 'uri' and 'service_name' in "
            "'extra'" % device_or_record
        )
