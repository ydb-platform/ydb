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
Point DNS Driver
"""

__all__ = ["PointDNSException", "Redirect", "MailRedirect", "PointDNSDriver"]

try:
    import simplejson as json
except ImportError:
    import json

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import Provider, RecordType, ZoneDoesNotExistError, RecordDoesNotExistError
from libcloud.utils.py3 import httplib
from libcloud.common.types import ProviderError, MalformedResponseError
from libcloud.common.pointdns import PointDNSConnection
from libcloud.common.exceptions import BaseHTTPError


class PointDNSException(ProviderError):
    def __init__(self, value, http_code, driver=None):
        super().__init__(value=value, http_code=http_code, driver=driver)
        self.args = (http_code, value)


class Redirect:
    """
    Point DNS redirect.
    """

    def __init__(self, id, name, data, type, driver, zone, iframe=None, query=False):
        """
        :param id: Redirect id.
        :type id: ``str``

        :param name: The FQDN for the record.
        :type name: ``str``

        :param data: The data field. (redirect_to)
        :type data: ``str``

        :param type: The type of redirects 301, 302 or 0 for iframes.
        :type type: ``str``

        :param driver: DNSDriver instance.
        :type driver: :class:`DNSDriver`

        :param zone: Zone where redirect belongs.
        :type  zone: :class:`Zone`

        :param iframe: Title of iframe (optional).
        :type iframe: ``str``

        :param query: boolean Information about including query string when
                      redirecting. (optional).
        :type query: ``bool``
        """
        self.id = str(id) if id else None
        self.name = name
        self.data = data
        self.type = str(type) if type else None
        self.driver = driver
        self.zone = zone
        self.iframe = iframe
        self.query = query

    def update(self, data, name=None, type=None, iframe=None, query=None):
        return self.driver.ex_update_redirect(
            redirect=self, name=name, data=data, type=type, iframe=iframe, query=query
        )

    def delete(self):
        return self.driver.ex_delete_redirect(redirect=self)

    def __repr__(self):
        return "<PointDNSRedirect: name={}, data={}, type={} ...>".format(
            self.name,
            self.data,
            self.type,
        )


class MailRedirect:
    """
    Point DNS mail redirect.
    """

    def __init__(self, id, source, destination, zone, driver):
        """
        :param id: MailRedirect id.
        :type id: ``str``

        :param source: The source address of mail redirect.
        :type source: ``str``

        :param destination: The destination address of mail redirect.
        :type destination: ``str``

        :param zone: Zone where mail redirect belongs.
        :type  zone: :class:`Zone`

        :param driver: DNSDriver instance.
        :type driver: :class:`DNSDriver`
        """
        self.id = str(id) if id else None
        self.source = source
        self.destination = destination
        self.zone = zone
        self.driver = driver

    def update(self, destination, source=None):
        return self.driver.ex_update_mail_redirect(
            mail_r=self, destination=destination, source=None
        )

    def delete(self):
        return self.driver.ex_delete_mail_redirect(mail_r=self)

    def __repr__(self):
        return "<PointDNSMailRedirect: source={}, destination={},zone={} ...>".format(
            self.source,
            self.destination,
            self.zone.id,
        )


class PointDNSDriver(DNSDriver):
    type = Provider.POINTDNS
    name = "Point DNS"
    website = "https://pointhq.com/"
    connectionCls = PointDNSConnection

    RECORD_TYPE_MAP = {
        RecordType.A: "A",
        RecordType.AAAA: "AAAA",
        RecordType.ALIAS: "ALIAS",
        RecordType.CNAME: "CNAME",
        RecordType.MX: "MX",
        RecordType.NS: "NS",
        RecordType.PTR: "PTR",
        RecordType.SRV: "SRV",
        RecordType.SSHFP: "SSHFP",
        RecordType.TXT: "TXT",
    }

    def list_zones(self):
        """
        Return a list of zones.

        :return: ``list`` of :class:`Zone`
        """
        response = self.connection.request("/zones")
        zones = self._to_zones(response.object)
        return zones

    def list_records(self, zone):
        """
        Return a list of records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        response = self.connection.request("/zones/%s/records" % zone.id)
        records = self._to_records(response.object, zone)
        return records

    def get_zone(self, zone_id):
        """
        Return a Zone instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :rtype: :class:`Zone`
        """
        try:
            response = self.connection.request("/zones/%s" % zone_id)
        except MalformedResponseError as e:
            if e.body == "Not found":
                raise ZoneDoesNotExistError(
                    driver=self, value="The zone doesn't exists", zone_id=zone_id
                )
            raise e

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
        try:
            response = self.connection.request("/zones/{}/records/{}".format(zone_id, record_id))
        except MalformedResponseError as e:
            if e.body == "Not found":
                raise RecordDoesNotExistError(
                    value="Record doesn't exists", driver=self, record_id=record_id
                )
            raise e

        record = self._to_record(response.object, zone_id=zone_id)
        return record

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        """
        Create a new zone.

        :param domain: Zone domain name (e.g. example.com)
        :type domain: ``str``

        :param type: Zone type (All zones are master by design).
        :type  type: ``str``

        :param ttl: TTL for new records. (optional)
        :type  ttl: ``int``

        :param extra: Extra attributes (driver specific). (optional)
        :type extra: ``dict``

        :rtype: :class:`Zone`
        """
        r_json = {"name": domain}
        if ttl is not None:
            r_json["ttl"] = ttl
        if extra is not None:
            r_json.update(extra)
        r_data = json.dumps({"zone": r_json})
        try:
            response = self.connection.request("/zones", method="POST", data=r_data)
        except BaseHTTPError as e:
            raise PointDNSException(value=e.message, http_code=e.code, driver=self)
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
        r_json = {"name": name, "data": data, "record_type": type}
        if extra is not None:
            r_json.update(extra)
        r_data = json.dumps({"zone_record": r_json})
        try:
            response = self.connection.request(
                "/zones/%s/records" % zone.id, method="POST", data=r_data
            )
        except BaseHTTPError as e:
            raise PointDNSException(value=e.message, http_code=e.code, driver=self)
        record = self._to_record(response.object, zone=zone)
        return record

    def update_zone(self, zone, domain, type="master", ttl=None, extra=None):
        """
        Update an existing zone.

        :param zone: Zone to update.
        :type  zone: :class:`Zone`

        :param domain: Zone domain name (e.g. example.com)
        :type  domain: ``str``

        :param type: Zone type (All zones are master by design).
        :type  type: ``str``

        :param ttl: TTL for new records. (optional)
        :type  ttl: ``int``

        :param extra: Extra attributes (group, user-id). (optional)
        :type  extra: ``dict``

        :rtype: :class:`Zone`
        """
        r_json = {"name": domain}
        if extra is not None:
            r_json.update(extra)
        r_data = json.dumps({"zone": r_json})
        try:
            response = self.connection.request("/zones/%s" % zone.id, method="PUT", data=r_data)
        except (BaseHTTPError, MalformedResponseError) as e:
            if isinstance(e, MalformedResponseError) and e.body == "Not found":
                raise ZoneDoesNotExistError(
                    value="Zone doesn't exists", driver=self, zone_id=zone.id
                )
            raise PointDNSException(value=e.message, http_code=e.code, driver=self)
        zone = self._to_zone(response.object)
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
        r_json = {"name": name, "data": data, "record_type": type}
        if extra is not None:
            r_json.update(extra)
        r_data = json.dumps({"zone_record": r_json})
        try:
            response = self.connection.request(
                "/zones/{}/records/{}".format(zone.id, record.id), method="PUT", data=r_data
            )
        except (BaseHTTPError, MalformedResponseError) as e:
            if isinstance(e, MalformedResponseError) and e.body == "Not found":
                raise RecordDoesNotExistError(
                    value="Record doesn't exists", driver=self, record_id=record.id
                )
            raise PointDNSException(value=e.message, http_code=e.code, driver=self)
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
        try:
            self.connection.request("/zones/%s" % zone.id, method="DELETE")
        except MalformedResponseError as e:
            if e.body == "Not found":
                raise ZoneDoesNotExistError(
                    driver=self, value="The zone doesn't exists", zone_id=zone.id
                )
            raise e
        return True

    def delete_record(self, record):
        """
        Delete a record.

        :param record: Record to delete.
        :type  record: :class:`Record`

        :rtype: ``bool``
        """
        zone_id = record.zone.id
        record_id = record.id
        try:
            self.connection.request(
                "/zones/{}/records/{}".format(zone_id, record_id), method="DELETE"
            )
        except MalformedResponseError as e:
            if e.body == "Not found":
                raise RecordDoesNotExistError(
                    value="Record doesn't exists", driver=self, record_id=record_id
                )
            raise e
        return True

    def ex_list_redirects(self, zone):
        """
        :param zone: Zone to list redirects for.
        :type zone: :class:`Zone`

        :rtype: ``list`` of :class:`Record`
        """
        response = self.connection.request("/zones/%s/redirects" % zone.id)
        redirects = self._to_redirects(response.object, zone)
        return redirects

    def ex_list_mail_redirects(self, zone):
        """
        :param zone: Zone to list redirects for.
        :type zone: :class:`Zone`

        :rtype: ``list`` of :class:`MailRedirect`
        """
        response = self.connection.request("/zones/%s/mail_redirects" % zone.id)
        mail_redirects = self._to_mail_redirects(response.object, zone)
        return mail_redirects

    def ex_create_redirect(self, redirect_to, name, type, zone, iframe=None, query=None):
        """
        :param redirect_to: The data field. (redirect_to)
        :type redirect_to: ``str``

        :param name: The FQDN for the record.
        :type name: ``str``

        :param type: The type of redirects 301, 302 or 0 for iframes.
        :type type: ``str``

        :param zone: Zone to list redirects for.
        :type zone: :class:`Zone`

        :param iframe: Title of iframe (optional).
        :type iframe: ``str``

        :param query: boolean Information about including query string when
                      redirecting. (optional).
        :type query: ``bool``

        :rtype: :class:`Record`
        """
        r_json = {"name": name, "redirect_to": redirect_to}
        if type is not None:
            r_json["redirect_type"] = type
        if iframe is not None:
            r_json["iframe_title"] = iframe
        if query is not None:
            r_json["redirect_query_string"] = query
        r_data = json.dumps({"zone_redirect": r_json})
        try:
            response = self.connection.request(
                "/zones/%s/redirects" % zone.id, method="POST", data=r_data
            )
        except (BaseHTTPError, MalformedResponseError) as e:
            raise PointDNSException(value=e.message, http_code=e.code, driver=self)
        redirect = self._to_redirect(response.object, zone=zone)
        return redirect

    def ex_create_mail_redirect(self, destination, source, zone):
        """
        :param destination: The destination address of mail redirect.
        :type destination: ``str``

        :param source: The source address of mail redirect.
        :type source: ``str``

        :param zone: Zone to list redirects for.
        :type zone: :class:`Zone`

        :rtype: ``list`` of :class:`MailRedirect`
        """
        r_json = {"destination_address": destination, "source_address": source}
        r_data = json.dumps({"zone_mail_redirect": r_json})
        try:
            response = self.connection.request(
                "/zones/%s/mail_redirects" % zone.id, method="POST", data=r_data
            )
        except (BaseHTTPError, MalformedResponseError) as e:
            raise PointDNSException(value=e.message, http_code=e.code, driver=self)
        mail_redirect = self._to_mail_redirect(response.object, zone=zone)
        return mail_redirect

    def ex_get_redirect(self, zone_id, redirect_id):
        """
        :param zone: Zone to list redirects for.
        :type zone: :class:`Zone`

        :param redirect_id: Redirect id.
        :type redirect_id: ``str``

        :rtype: ``list`` of :class:`Redirect`
        """
        try:
            response = self.connection.request(
                "/zones/{}/redirects/{}".format(zone_id, redirect_id)
            )
        except (BaseHTTPError, MalformedResponseError) as e:
            if isinstance(e, MalformedResponseError) and e.body == "Not found":
                raise PointDNSException(
                    value="Couldn't found redirect",
                    http_code=httplib.NOT_FOUND,
                    driver=self,
                )
            raise PointDNSException(value=e.message, http_code=e.code, driver=self)
        redirect = self._to_redirect(response.object, zone_id=zone_id)
        return redirect

    def ex_get_mail_redirects(self, zone_id, mail_r_id):
        """
        :param zone: Zone to list redirects for.
        :type zone: :class:`Zone`

        :param mail_r_id: Mail redirect id.
        :type mail_r_id: ``str``

        :rtype: ``list`` of :class:`MailRedirect`
        """
        try:
            response = self.connection.request(
                "/zones/{}/mail_redirects/{}".format(zone_id, mail_r_id)
            )
        except (BaseHTTPError, MalformedResponseError) as e:
            if isinstance(e, MalformedResponseError) and e.body == "Not found":
                raise PointDNSException(
                    value="Couldn't found mail redirect",
                    http_code=httplib.NOT_FOUND,
                    driver=self,
                )
            raise PointDNSException(value=e.message, http_code=e.code, driver=self)
        mail_redirect = self._to_mail_redirect(response.object, zone_id=zone_id)
        return mail_redirect

    def ex_update_redirect(
        self, redirect, redirect_to=None, name=None, type=None, iframe=None, query=None
    ):
        """
        :param redirect: Record to update
        :type id: :class:`Redirect`

        :param redirect_to: The data field. (optional).
        :type redirect_to: ``str``

        :param name: The FQDN for the record.
        :type name: ``str``

        :param type: The type of redirects 301, 302 or 0 for iframes.
                     (optional).
        :type type: ``str``

        :param iframe: Title of iframe (optional).
        :type iframe: ``str``

        :param query: boolean Information about including query string when
                      redirecting. (optional).
        :type query: ``bool``

        :rtype: ``list`` of :class:`Redirect`
        """
        zone_id = redirect.zone.id
        r_json = {}
        if redirect_to is not None:
            r_json["redirect_to"] = redirect_to
        if name is not None:
            r_json["name"] = name
        if type is not None:
            r_json["record_type"] = type
        if iframe is not None:
            r_json["iframe_title"] = iframe
        if query is not None:
            r_json["redirect_query_string"] = query
        r_data = json.dumps({"zone_redirect": r_json})
        try:
            response = self.connection.request(
                "/zones/{}/redirects/{}".format(zone_id, redirect.id),
                method="PUT",
                data=r_data,
            )
        except (BaseHTTPError, MalformedResponseError) as e:
            if isinstance(e, MalformedResponseError) and e.body == "Not found":
                raise PointDNSException(
                    value="Couldn't found redirect",
                    http_code=httplib.NOT_FOUND,
                    driver=self,
                )
            raise PointDNSException(value=e.message, http_code=e.code, driver=self)
        redirect = self._to_redirect(response.object, zone=redirect.zone)
        return redirect

    def ex_update_mail_redirect(self, mail_r, destination, source=None):
        """
        :param mail_r: Mail redirect to update
        :type mail_r: :class:`MailRedirect`

        :param destination: The destination address of mail redirect.
        :type destination: ``str``

        :param source: The source address of mail redirect. (optional)
        :type source: ``str``

        :rtype: ``list`` of :class:`MailRedirect`
        """
        zone_id = mail_r.zone.id
        r_json = {"destination_address": destination}
        if source is not None:
            r_json["source_address"] = source
        r_data = json.dumps({"zone_redirect": r_json})
        try:
            response = self.connection.request(
                "/zones/{}/mail_redirects/{}".format(zone_id, mail_r.id),
                method="PUT",
                data=r_data,
            )
        except (BaseHTTPError, MalformedResponseError) as e:
            if isinstance(e, MalformedResponseError) and e.body == "Not found":
                raise PointDNSException(
                    value="Couldn't found mail redirect",
                    http_code=httplib.NOT_FOUND,
                    driver=self,
                )
            raise PointDNSException(value=e.message, http_code=e.code, driver=self)
        mail_redirect = self._to_mail_redirect(response.object, zone=mail_r.zone)
        return mail_redirect

    def ex_delete_redirect(self, redirect):
        """
        :param mail_r: Redirect to delete
        :type mail_r: :class:`Redirect`

        :rtype: ``bool``
        """
        zone_id = redirect.zone.id
        redirect_id = redirect.id
        try:
            self.connection.request(
                "/zones/{}/redirects/{}".format(zone_id, redirect_id), method="DELETE"
            )
        except (BaseHTTPError, MalformedResponseError) as e:
            if isinstance(e, MalformedResponseError) and e.body == "Not found":
                raise PointDNSException(
                    value="Couldn't found redirect",
                    http_code=httplib.NOT_FOUND,
                    driver=self,
                )
            raise PointDNSException(value=e.message, http_code=e.code, driver=self)
        return True

    def ex_delete_mail_redirect(self, mail_r):
        """
        :param mail_r: Mail redirect to update
        :type mail_r: :class:`MailRedirect`

        :rtype: ``bool``
        """
        zone_id = mail_r.zone.id
        mail_r_id = mail_r.id
        try:
            self.connection.request(
                "/zones/{}/mail_redirects/{}".format(zone_id, mail_r_id), method="DELETE"
            )
        except (BaseHTTPError, MalformedResponseError) as e:
            if isinstance(e, MalformedResponseError) and e.body == "Not found":
                raise PointDNSException(
                    value="Couldn't found mail redirect",
                    http_code=httplib.NOT_FOUND,
                    driver=self,
                )
            raise PointDNSException(value=e.message, http_code=e.code, driver=self)
        return True

    def _to_zones(self, data):
        zones = []
        for zone in data:
            _zone = self._to_zone(zone)
            zones.append(_zone)

        return zones

    def _to_zone(self, data):
        zone = data.get("zone")
        id = zone.get("id")
        name = zone.get("name")
        ttl = zone.get("ttl")
        extra = {"group": zone.get("group"), "user-id": zone.get("user-id")}

        # All zones are a primary ones by design, so they
        # assume that are the master source of info about the
        # zone, which is the case when domain DNS records
        # points to PointDNS nameservers.
        type = "master"

        return Zone(id=id, domain=name, type=type, ttl=ttl, driver=self, extra=extra)

    def _to_records(self, data, zone):
        records = []
        for item in data:
            record = self._to_record(item, zone=zone)
            records.append(record)
        return records

    def _to_record(self, data, zone_id=None, zone=None):
        if not zone:  # We need zone_id or zone
            zone = self.get_zone(zone_id)
        record = data.get("zone_record")
        id = record.get("id")
        name = record.get("name")
        type = record.get("record_type")
        data = record.get("data")
        extra = {
            "ttl": record.get("ttl"),
            "zone_id": record.get("zone_id"),
            "aux": record.get("aux"),
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

    def _to_redirects(self, data, zone):
        redirects = []
        for item in data:
            redirect = self._to_redirect(item, zone=zone)
            redirects.append(redirect)
        return redirects

    def _to_redirect(self, data, zone_id=None, zone=None):
        if not zone:  # We need zone_id or zone
            zone = self.get_zone(zone_id)
        record = data.get("zone_redirect")
        id = record.get("id")
        name = record.get("name")
        redirect_to = record.get("redirect_to")
        type = record.get("redirect_type")
        iframe = record.get("iframe_title")
        query = record.get("redirect_query_string")
        return Redirect(id, name, redirect_to, type, self, zone, iframe=iframe, query=query)

    def _to_mail_redirects(self, data, zone):
        mail_redirects = []
        for item in data:
            mail_redirect = self._to_mail_redirect(item, zone=zone)
            mail_redirects.append(mail_redirect)
        return mail_redirects

    def _to_mail_redirect(self, data, zone_id=None, zone=None):
        if not zone:  # We need zone_id or zone
            zone = self.get_zone(zone_id)
        record = data.get("zone_mail_redirect")
        id = record.get("id")
        destination = record.get("destination_address")
        source = record.get("source_address")
        return MailRedirect(id, source, destination, zone, self)
