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

__all__ = ["GoDaddyDNSDriver"]

try:
    import simplejson as json
except Exception:
    import json

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import Provider, RecordType, RecordDoesNotExistError
from libcloud.utils.py3 import httplib
from libcloud.common.base import JsonResponse, ConnectionKey
from libcloud.common.types import LibcloudError

API_HOST = "api.godaddy.com"
VALID_RECORD_EXTRA_PARAMS = ["prio", "ttl"]


class GoDaddyDNSException(LibcloudError):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        self.args = (code, message)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "<GoDaddyDNSException in {}: {}>".format(self.code, self.message)


class GoDaddyDNSResponse(JsonResponse):
    valid_response_codes = [
        httplib.OK,
        httplib.ACCEPTED,
        httplib.CREATED,
        httplib.NO_CONTENT,
    ]

    def parse_body(self):
        if not self.body:
            return None
        # json.loads doesn't like the regex expressions used in godaddy schema
        self.body = self.body.replace("\\.", "\\\\.")

        data = json.loads(self.body)
        return data

    def parse_error(self):
        data = self.parse_body()
        raise GoDaddyDNSException(code=data["code"], message=data["message"])

    def success(self):
        return self.status in self.valid_response_codes


class GoDaddyDNSConnection(ConnectionKey):
    responseCls = GoDaddyDNSResponse
    host = API_HOST

    allow_insecure = False

    def __init__(
        self,
        key,
        secret,
        secure=True,
        shopper_id=None,
        host=None,
        port=None,
        url=None,
        timeout=None,
        proxy_url=None,
        backoff=None,
        retry_delay=None,
    ):
        super().__init__(
            key,
            secure=secure,
            host=host,
            port=port,
            url=url,
            timeout=timeout,
            proxy_url=proxy_url,
            backoff=backoff,
            retry_delay=retry_delay,
        )
        self.key = key
        self.secret = secret
        self.shopper_id = shopper_id

    def add_default_headers(self, headers):
        if self.shopper_id is not None:
            headers["X-Shopper-Id"] = self.shopper_id
        headers["Content-type"] = "application/json"
        headers["Authorization"] = "sso-key {}:{}".format(self.key, self.secret)
        return headers


class GoDaddyDNSDriver(DNSDriver):
    """
    A driver for GoDaddy DNS.

    This is for customers of GoDaddy
    who wish to purchase, update existing domains
    and manage records for DNS zones owned by GoDaddy NS servers.
    """

    type = Provider.GODADDY
    name = "GoDaddy DNS"
    website = "https://www.godaddy.com/"
    connectionCls = GoDaddyDNSConnection

    RECORD_TYPE_MAP = {
        RecordType.A: "A",
        RecordType.AAAA: "AAAA",
        RecordType.CNAME: "CNAME",
        RecordType.MX: "MX",
        RecordType.NS: "SPF",
        RecordType.SRV: "SRV",
        RecordType.TXT: "TXT",
    }

    def __init__(self, shopper_id, key, secret, secure=True, host=None, port=None):
        """
        Instantiate a new `GoDaddyDNSDriver`

        :param  shopper_id: Your customer ID or shopper ID with GoDaddy
        :type   shopper_id: ``str``

        :param  key: Your access key from developer.godaddy.com
        :type   key: ``str``

        :param  secret: Your access key secret
        :type   secret: ``str``
        """
        self.shopper_id = shopper_id
        super().__init__(
            key=key,
            secret=secret,
            secure=secure,
            host=host,
            port=port,
            shopper_id=str(shopper_id),
        )

    def list_zones(self):
        """
        Return a list of zones (purchased domains)

        :return: ``list`` of :class:`Zone`
        """
        result = self.connection.request("/v1/domains/").object
        zones = self._to_zones(result)
        return zones

    def list_records(self, zone):
        """
        Return a list of records for the provided zone.

        :param zone: Zone to list records for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`Record`
        """
        result = self.connection.request("/v1/domains/%s/records" % (zone.domain)).object
        records = self._to_records(items=result, zone=zone)
        return records

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
        new_record = self._format_record(name, type, data, extra)
        self.connection.request(
            "/v1/domains/%s/records" % (zone.domain),
            method="PATCH",
            data=json.dumps([new_record]),
        )
        id = self._get_id_of_record(name, type)
        return Record(
            id=id,
            name=name,
            type=type,
            data=data,
            zone=zone,
            driver=self,
            ttl=new_record["ttl"],
            extra=extra,
        )

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
        new_record = self._format_record(name, type, data, extra)
        self.connection.request(
            "/v1/domains/{}/records/{}/{}".format(record.zone.domain, record.type, record.name),
            method="PUT",
            data=json.dumps([new_record]),
        )
        id = self._get_id_of_record(name, type)
        return Record(
            id=id,
            name=name,
            type=type,
            data=data,
            zone=record.zone,
            driver=self,
            ttl=new_record["ttl"],
            extra=extra,
        )

    def get_record(self, zone_id, record_id):
        """
        Return a Record instance.

        :param zone_id: ID of the required zone
        :type  zone_id: ``str``

        :param record_id: ID of the required record
        :type  record_id: ``str``

        :rtype: :class:`Record`
        """
        parts = record_id.split(":")
        result = self.connection.request(
            "/v1/domains/{}/records/{}/{}".format(zone_id, parts[1], parts[0])
        ).object
        if len(result) == 0:
            raise RecordDoesNotExistError(record_id, driver=self, record_id=record_id)
        return self._to_record(result[0], self.get_zone(zone_id))

    def get_zone(self, zone_id):
        """
        Get a zone (by domain)

        :param  zone_id: The domain, not the ID
        :type   zone_id: ``str``

        :rtype:  :class:`Zone`
        """
        result = self.connection.request("/v1/domains/%s/" % zone_id).object
        zone = self._to_zone(result)
        return zone

    def delete_zone(self, zone):
        """
        Delete a zone.

        Note: This will CANCEL a purchased domain

        :param zone: Zone to delete.
        :type  zone: :class:`Zone`

        :rtype: ``bool``
        """
        self.connection.request("/v1/domains/%s" % (zone.domain), method="DELETE")
        # no error means ok
        return True

    def ex_check_availability(self, domain, for_transfer=False):
        """
        Check the availability of the domain

        :param   domain: the domain name e.g. wazzlewobbleflooble.com
        :type    domain: ``str``

        :param   for_transfer: Check if domain is available for transfer
        :type    for_transfer: ``bool``

        :rtype: `list` of :class:`GoDaddyAvailability`
        """
        result = self.connection.request(
            "/v1/domains/available",
            method="GET",
            params={"domain": domain, "forTransfer": str(for_transfer)},
        ).object
        return GoDaddyAvailability(
            domain=result["domain"],
            available=result["available"],
            price=result["price"],
            currency=result["currency"],
            period=result["period"],
        )

    def ex_list_tlds(self):
        """
        List available TLDs for sale

        :rtype: ``list`` of :class:`GoDaddyTLD`
        """
        result = self.connection.request("/v1/domains/tlds", method="GET").object
        return self._to_tlds(result)

    def ex_get_purchase_schema(self, tld):
        """
        Get the schema that needs completing to purchase a new domain
        Use this in conjunction with ex_purchase_domain

        :param   tld: The top level domain e.g com, eu, uk
        :type    tld: ``str``

        :rtype: `dict` the JSON Schema
        """
        result = self.connection.request(
            "/v1/domains/purchase/schema/%s" % tld, method="GET"
        ).object
        return result

    def ex_get_agreements(self, tld, privacy=True):
        """
        Get the legal agreements for a tld
        Use this in conjunction with ex_purchase_domain

        :param   tld: The top level domain e.g com, eu, uk
        :type    tld: ``str``

        :rtype: `dict` the JSON Schema
        """
        result = self.connection.request(
            "/v1/domains/agreements",
            params={"tlds": tld, "privacy": str(privacy)},
            method="GET",
        ).object
        agreements = []
        for item in result:
            agreements.append(
                GoDaddyLegalAgreement(
                    agreement_key=item["agreementKey"],
                    title=item["title"],
                    url=item["url"],
                    content=item["content"],
                )
            )
        return agreements

    def ex_purchase_domain(self, purchase_request):
        """
        Purchase a domain with GoDaddy

        :param  purchase_request: The completed document
            from ex_get_purchase_schema
        :type   purchase_request: ``dict``

        :rtype: :class:`GoDaddyDomainPurchaseResponse` Your order
        """
        result = self.connection.request(
            "/v1/domains/purchase", data=purchase_request, method="POST"
        ).object
        return GoDaddyDomainPurchaseResponse(
            order_id=result["orderId"],
            item_count=result["itemCount"],
            total=result["total"],
            currency=result["currency"],
        )

    def _format_record(self, name, type, data, extra):
        if extra is None:
            extra = {}
        new_record = {}
        if type == RecordType.SRV:
            new_record = {
                "type": type,
                "name": name,
                "data": data,
                "priority": 1,
                "ttl": extra.get("ttl", 5),
                "service": extra.get("service", ""),
                "protocol": extra.get("protocol", ""),
                "port": extra.get("port", ""),
                "weight": extra.get("weight", "1"),
            }
        else:
            new_record = {
                "type": type,
                "name": name,
                "data": data,
                "ttl": extra.get("ttl", 5),
            }
        if type == RecordType.MX:
            new_record["priority"] = 1
        return new_record

    def _to_zones(self, items):
        zones = []
        for item in items:
            zones.append(self._to_zone(item))
        return zones

    def _to_zone(self, item):
        extra = {"expires": item.get("expires", None)}
        zone = Zone(
            id=item["domainId"],
            domain=item["domain"],
            type="master",
            ttl=None,
            driver=self,
            extra=extra,
        )
        return zone

    def _to_records(self, items, zone=None):
        records = []

        for item in items:
            records.append(self._to_record(item=item, zone=zone))
        return records

    def _to_record(self, item, zone=None):
        ttl = item["ttl"]
        type = self._string_to_record_type(item["type"])
        name = item["name"]
        id = self._get_id_of_record(name, type)
        record = Record(
            id=id,
            name=name,
            type=type,
            data=item["data"],
            zone=zone,
            driver=self,
            ttl=ttl,
        )
        return record

    def _to_tlds(self, items):
        tlds = []
        for item in items:
            tlds.append(self._to_tld(item))
        return tlds

    def _to_tld(self, item):
        return GoDaddyTLD(name=item["name"], tld_type=item["type"])

    def _get_id_of_record(self, name, type):
        return "{}:{}".format(name, type)

    def _ex_connection_class_kwargs(self):
        return {"shopper_id": self.shopper_id}


class GoDaddyAvailability:
    def __init__(self, domain, available, price, currency, period):
        self.domain = domain
        self.available = bool(available)
        # currency comes in micro-units, convert to dollars.
        self.price = float(price) / 1000000
        self.currency = currency
        self.period = int(period)


class GoDaddyTLD:
    def __init__(self, name, tld_type):
        self.name = name
        self.type = tld_type


class GoDaddyDomainPurchaseResponse:
    def __init__(self, order_id, item_count, total, currency):
        self.order_id = order_id
        self.item_count = item_count
        self.total = total
        self.current = currency


class GoDaddyLegalAgreement:
    def __init__(self, agreement_key, title, url, content):
        self.agreement_key = agreement_key
        self.title = title
        self.url = url
        self.content = content
