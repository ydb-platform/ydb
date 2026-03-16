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
AuroraDNS DNS Driver
"""

import hmac
import json
import base64
import datetime
from hashlib import sha256

from libcloud.dns.base import Zone, Record, DNSDriver
from libcloud.dns.types import (
    RecordType,
    ZoneDoesNotExistError,
    ZoneAlreadyExistsError,
    RecordDoesNotExistError,
)
from libcloud.utils.py3 import b, httplib
from libcloud.common.base import JsonResponse, ConnectionUserAndKey
from libcloud.common.types import LibcloudError, ProviderError, InvalidCredsError

API_HOST = "api.auroradns.eu"

# Default TTL required by libcloud, but doesn't do anything in AuroraDNS
DEFAULT_ZONE_TTL = 3600
DEFAULT_ZONE_TYPE = "master"

VALID_RECORD_PARAMS_EXTRA = ["ttl", "prio", "health_check_id", "disabled"]


class AuroraDNSHealthCheckType:
    """
    Healthcheck type.
    """

    HTTP = "HTTP"
    HTTPS = "HTTPS"
    TCP = "TCP"


class HealthCheckError(LibcloudError):
    error_type = "HealthCheckError"

    def __init__(self, value, driver, health_check_id):
        self.health_check_id = health_check_id
        super().__init__(value=value, driver=driver)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "<{} in {}, health_check_id={}, value={}>".format(
            self.error_type,
            repr(self.driver),
            self.health_check_id,
            self.value,
        )


class HealthCheckDoesNotExistError(HealthCheckError):
    error_type = "HealthCheckDoesNotExistError"


class AuroraDNSHealthCheck:
    """
    AuroraDNS Healthcheck resource.
    """

    def __init__(
        self,
        id,
        type,
        hostname,
        ipaddress,
        port,
        interval,
        path,
        threshold,
        health,
        enabled,
        zone,
        driver,
        extra=None,
    ):
        """
        :param id: Healthcheck id
        :type id: ``str``

        :param hostname: Hostname or FQDN of the target
        :type hostname: ``str``

        :param ipaddress: IPv4 or IPv6 address of the target
        :type ipaddress: ``str``

        :param port: The port on the target to monitor
        :type port: ``int``

        :param interval: The interval of the health check
        :type interval: ``int``

        :param path: The path to monitor on the target
        :type path: ``str``

        :param threshold: The threshold of before marking a check as failed
        :type threshold: ``int``

        :param health: The current health of the health check
        :type health: ``bool``

        :param enabled: If the health check is currently enabled
        :type enabled: ``bool``

        :param zone: Zone instance.
        :type zone: :class:`Zone`

        :param driver: DNSDriver instance.
        :type driver: :class:`DNSDriver`

        :param extra: (optional) Extra attributes (driver specific).
        :type extra: ``dict``
        """
        self.id = str(id) if id else None
        self.type = type
        self.hostname = hostname
        self.ipaddress = ipaddress
        self.port = int(port) if port else None
        self.interval = int(interval)
        self.path = path
        self.threshold = int(threshold)
        self.health = bool(health)
        self.enabled = bool(enabled)
        self.zone = zone
        self.driver = driver
        self.extra = extra or {}

    def update(
        self,
        type=None,
        hostname=None,
        ipaddress=None,
        port=None,
        interval=None,
        path=None,
        threshold=None,
        enabled=None,
        extra=None,
    ):
        return self.driver.ex_update_healthcheck(
            healthcheck=self,
            type=type,
            hostname=hostname,
            ipaddress=ipaddress,
            port=port,
            path=path,
            interval=interval,
            threshold=threshold,
            enabled=enabled,
            extra=extra,
        )

    def delete(self):
        return self.driver.ex_delete_healthcheck(healthcheck=self)

    def __repr__(self):
        return (
            "<AuroraDNSHealthCheck: zone=%s, id=%s, type=%s, hostname=%s, "
            "ipaddress=%s, port=%d, interval=%d, health=%s, provider=%s"
            "...>"
            % (
                self.zone.id,
                self.id,
                self.type,
                self.hostname,
                self.ipaddress,
                self.port,
                self.interval,
                self.health,
                self.driver.name,
            )
        )


class AuroraDNSResponse(JsonResponse):
    def success(self):
        return self.status in [httplib.OK, httplib.CREATED, httplib.ACCEPTED]

    def parse_error(self):
        status = int(self.status)
        error = {"driver": self, "value": ""}

        if status == httplib.UNAUTHORIZED:
            error["value"] = "Authentication failed"
            raise InvalidCredsError(**error)
        elif status == httplib.FORBIDDEN:
            error["value"] = "Authorization failed"
            error["http_code"] = status
            raise ProviderError(**error)
        elif status == httplib.NOT_FOUND:
            context = self.connection.context
            if context["resource"] == "zone":
                error["zone_id"] = context["id"]
                raise ZoneDoesNotExistError(**error)
            elif context["resource"] == "record":
                error["record_id"] = context["id"]
                raise RecordDoesNotExistError(**error)
            elif context["resource"] == "healthcheck":
                error["health_check_id"] = context["id"]
                raise HealthCheckDoesNotExistError(**error)
        elif status == httplib.CONFLICT:
            context = self.connection.context
            if context["resource"] == "zone":
                error["zone_id"] = context["id"]
                raise ZoneAlreadyExistsError(**error)
        elif status == httplib.BAD_REQUEST:
            context = self.connection.context
            body = self.parse_body()
            raise ProviderError(value=body["errormsg"], http_code=status, driver=self)


class AuroraDNSConnection(ConnectionUserAndKey):
    host = API_HOST
    responseCls = AuroraDNSResponse

    def calculate_auth_signature(self, secret_key, method, url, timestamp):
        b64_hmac = base64.b64encode(
            hmac.new(b(secret_key), b(method) + b(url) + b(timestamp), digestmod=sha256).digest()
        )

        return b64_hmac.decode("utf-8")

    def gen_auth_header(self, api_key, secret_key, method, url, timestamp):
        signature = self.calculate_auth_signature(secret_key, method, url, timestamp)

        auth_b64 = base64.b64encode(b("{}:{}".format(api_key, signature)))
        return "AuroraDNSv1 %s" % (auth_b64.decode("utf-8"))

    def request(self, action, params=None, data="", headers=None, method="GET"):
        if not headers:
            headers = {}
        if not params:
            params = {}

        if method in ("POST", "PUT"):
            headers = {"Content-Type": "application/json; charset=UTF-8"}

        t = datetime.datetime.utcnow()
        timestamp = t.strftime("%Y%m%dT%H%M%SZ")

        headers["X-AuroraDNS-Date"] = timestamp
        headers["Authorization"] = self.gen_auth_header(
            self.user_id, self.key, method, action, timestamp
        )

        return super().request(
            action=action, params=params, data=data, method=method, headers=headers
        )


class AuroraDNSDriver(DNSDriver):
    name = "AuroraDNS"
    website = "https://www.pcextreme.nl/en/aurora/dns"
    connectionCls = AuroraDNSConnection

    RECORD_TYPE_MAP = {
        RecordType.A: "A",
        RecordType.AAAA: "AAAA",
        RecordType.CNAME: "CNAME",
        RecordType.MX: "MX",
        RecordType.NS: "NS",
        RecordType.SOA: "SOA",
        RecordType.SRV: "SRV",
        RecordType.TXT: "TXT",
        RecordType.DS: "DS",
        RecordType.PTR: "PTR",
        RecordType.SSHFP: "SSHFP",
        RecordType.TLSA: "TLSA",
    }

    HEALTHCHECK_TYPE_MAP = {
        AuroraDNSHealthCheckType.HTTP: "HTTP",
        AuroraDNSHealthCheckType.HTTPS: "HTTPS",
        AuroraDNSHealthCheckType.TCP: "TCP",
    }

    def iterate_zones(self):
        res = self.connection.request("/zones")
        for zone in res.parse_body():
            yield self.__res_to_zone(zone)

    def iterate_records(self, zone):
        self.connection.set_context({"resource": "zone", "id": zone.id})
        res = self.connection.request("/zones/%s/records" % zone.id)

        for record in res.parse_body():
            yield self.__res_to_record(zone, record)

    def get_zone(self, zone_id):
        self.connection.set_context({"resource": "zone", "id": zone_id})
        res = self.connection.request("/zones/%s" % zone_id)
        zone = res.parse_body()
        return self.__res_to_zone(zone)

    def get_record(self, zone_id, record_id):
        self.connection.set_context({"resource": "record", "id": record_id})
        res = self.connection.request("/zones/{}/records/{}".format(zone_id, record_id))
        record = res.parse_body()

        zone = self.get_zone(zone_id)

        return self.__res_to_record(zone, record)

    def create_zone(self, domain, type="master", ttl=None, extra=None):
        self.connection.set_context({"resource": "zone", "id": domain})
        res = self.connection.request("/zones", method="POST", data=json.dumps({"name": domain}))
        zone = res.parse_body()
        return self.__res_to_zone(zone)

    def create_record(self, name, zone, type, data, extra=None):
        if name is None:
            name = ""

        rdata = {"name": name, "type": self.RECORD_TYPE_MAP[type], "content": data}

        rdata = self.__merge_extra_data(rdata, extra)

        if "ttl" not in rdata:
            rdata["ttl"] = DEFAULT_ZONE_TTL

        self.connection.set_context({"resource": "zone", "id": zone.id})
        res = self.connection.request(
            "/zones/%s/records" % zone.id, method="POST", data=json.dumps(rdata)
        )

        record = res.parse_body()
        return self.__res_to_record(zone, record)

    def delete_zone(self, zone):
        self.connection.set_context({"resource": "zone", "id": zone.id})
        self.connection.request("/zones/%s" % zone.id, method="DELETE")
        return True

    def delete_record(self, record):
        self.connection.set_context({"resource": "record", "id": record.id})
        self.connection.request(
            "/zones/{}/records/{}".format(record.zone.id, record.id), method="DELETE"
        )
        return True

    def list_record_types(self):
        types = []
        for record_type in self.RECORD_TYPE_MAP.keys():
            types.append(record_type)

        return types

    def update_record(self, record, name, type, data, extra=None):
        rdata = {}

        if name is not None:
            rdata["name"] = name

        if type is not None:
            rdata["type"] = self.RECORD_TYPE_MAP[type]

        if data is not None:
            rdata["content"] = data

        rdata = self.__merge_extra_data(rdata, extra)

        self.connection.set_context({"resource": "record", "id": record.id})
        self.connection.request(
            "/zones/{}/records/{}".format(record.zone.id, record.id),
            method="PUT",
            data=json.dumps(rdata),
        )

        return self.get_record(record.zone.id, record.id)

    def ex_list_healthchecks(self, zone):
        """
        List all Health Checks in a zone.

        :param zone: Zone to list health checks for.
        :type zone: :class:`Zone`

        :return: ``list`` of :class:`AuroraDNSHealthCheck`
        """
        healthchecks = []
        self.connection.set_context({"resource": "zone", "id": zone.id})
        res = self.connection.request("/zones/%s/health_checks" % zone.id)

        for healthcheck in res.parse_body():
            healthchecks.append(self.__res_to_healthcheck(zone, healthcheck))

        return healthchecks

    def ex_get_healthcheck(self, zone, health_check_id):
        """
        Get a single Health Check from a zone

        :param zone: Zone in which the health check is
        :type zone: :class:`Zone`

        :param health_check_id: ID of the required health check
        :type  health_check_id: ``str``

        :return: :class:`AuroraDNSHealthCheck`
        """
        self.connection.set_context({"resource": "healthcheck", "id": health_check_id})
        res = self.connection.request("/zones/{}/health_checks/{}".format(zone.id, health_check_id))
        check = res.parse_body()

        return self.__res_to_healthcheck(zone, check)

    def ex_create_healthcheck(
        self,
        zone,
        type,
        hostname,
        port,
        path,
        interval,
        threshold,
        ipaddress=None,
        enabled=True,
        extra=None,
    ):
        """
        Create a new Health Check in a zone

        :param zone: Zone in which the health check should be created
        :type zone: :class:`Zone`

        :param type: The type of health check to be created
        :type  type: :class:`AuroraDNSHealthCheckType`

        :param hostname: The hostname of the target to monitor
        :type  hostname: ``str``

        :param port: The port of the target to monitor. E.g. 80 for HTTP
        :type  port: ``int``

        :param path: The path of the target to monitor. Only used by HTTP
                     at this moment. Usually this is simple /.
        :type  path: ``str``

        :param interval: The interval of checks. 10, 30 or 60 seconds.
        :type  interval: ``int``

        :param threshold: The threshold of failures before the healthcheck is
                          marked as failed.
        :type  threshold: ``int``

        :param ipaddress: (optional) The IP Address of the target to monitor.
                          You can pass a empty string if this is not required.
        :type  ipaddress: ``str``

        :param enabled: (optional) If this healthcheck is enabled to run
        :type  enabled: ``bool``

        :param extra: (optional) Extra attributes (driver specific).
        :type  extra: ``dict``

        :return: :class:`AuroraDNSHealthCheck`
        """
        cdata = {
            "type": self.HEALTHCHECK_TYPE_MAP[type],
            "hostname": hostname,
            "ipaddress": ipaddress,
            "port": int(port),
            "interval": int(interval),
            "path": path,
            "threshold": int(threshold),
            "enabled": enabled,
        }

        self.connection.set_context({"resource": "zone", "id": zone.id})
        res = self.connection.request(
            "/zones/%s/health_checks" % zone.id, method="POST", data=json.dumps(cdata)
        )

        healthcheck = res.parse_body()
        return self.__res_to_healthcheck(zone, healthcheck)

    def ex_update_healthcheck(
        self,
        healthcheck,
        type=None,
        hostname=None,
        ipaddress=None,
        port=None,
        path=None,
        interval=None,
        threshold=None,
        enabled=None,
        extra=None,
    ):
        """
        Update an existing Health Check

        :param zone: The healthcheck which has to be updated
        :type zone: :class:`AuroraDNSHealthCheck`

        :param type: (optional) The type of health check to be created
        :type  type: :class:`AuroraDNSHealthCheckType`

        :param hostname: (optional) The hostname of the target to monitor
        :type  hostname: ``str``

        :param ipaddress: (optional) The IP Address of the target to monitor.
                          You can pass a empty string if this is not required.
        :type  ipaddress: ``str``

        :param port: (optional) The port of the target to monitor. E.g. 80
                     for HTTP
        :type  port: ``int``

        :param path: (optional) The path of the target to monitor.
                     Only used by HTTP at this moment. Usually just '/'.
        :type  path: ``str``

        :param interval: (optional) The interval of checks.
                         10, 30 or 60 seconds.
        :type  interval: ``int``

        :param threshold: (optional) The threshold of failures before the
                          healthcheck is marked as failed.
        :type  threshold: ``int``

        :param enabled: (optional) If this healthcheck is enabled to run
        :type  enabled: ``bool``

        :param extra: (optional) Extra attributes (driver specific).
        :type  extra: ``dict``

        :return: :class:`AuroraDNSHealthCheck`
        """
        cdata = {}

        if type is not None:
            cdata["type"] = self.HEALTHCHECK_TYPE_MAP[type]

        if hostname is not None:
            cdata["hostname"] = hostname

        if ipaddress is not None:
            if len(ipaddress) == 0:
                cdata["ipaddress"] = None
            else:
                cdata["ipaddress"] = ipaddress

        if port is not None:
            cdata["port"] = int(port)

        if path is not None:
            cdata["path"] = path

        if interval is not None:
            cdata["interval"] = int(interval)

        if threshold is not None:
            cdata["threshold"] = threshold

        if enabled is not None:
            cdata["enabled"] = bool(enabled)

        self.connection.set_context({"resource": "healthcheck", "id": healthcheck.id})

        self.connection.request(
            "/zones/{}/health_checks/{}".format(healthcheck.zone.id, healthcheck.id),
            method="PUT",
            data=json.dumps(cdata),
        )

        return self.ex_get_healthcheck(healthcheck.zone, healthcheck.id)

    def ex_delete_healthcheck(self, healthcheck):
        """
        Remove an existing Health Check

        :param zone: The healthcheck which has to be removed
        :type zone: :class:`AuroraDNSHealthCheck`
        """
        self.connection.set_context({"resource": "healthcheck", "id": healthcheck.id})

        self.connection.request(
            "/zones/{}/health_checks/{}".format(healthcheck.zone.id, healthcheck.id),
            method="DELETE",
        )
        return True

    def __res_to_record(self, zone, record):
        if len(record["name"]) == 0:
            name = None
        else:
            name = record["name"]

        extra = {}
        extra["created"] = record["created"]
        extra["modified"] = record["modified"]
        extra["disabled"] = record["disabled"]
        extra["ttl"] = record["ttl"]
        extra["priority"] = record["prio"]

        return Record(
            id=record["id"],
            name=name,
            type=record["type"],
            data=record["content"],
            zone=zone,
            driver=self.connection.driver,
            ttl=record["ttl"],
            extra=extra,
        )

    def __res_to_zone(self, zone):
        return Zone(
            id=zone["id"],
            domain=zone["name"],
            type=DEFAULT_ZONE_TYPE,
            ttl=DEFAULT_ZONE_TTL,
            driver=self.connection.driver,
            extra={
                "created": zone["created"],
                "servers": zone["servers"],
                "account_id": zone["account_id"],
                "cluster_id": zone["cluster_id"],
            },
        )

    def __res_to_healthcheck(self, zone, healthcheck):
        return AuroraDNSHealthCheck(
            id=healthcheck["id"],
            type=healthcheck["type"],
            hostname=healthcheck["hostname"],
            ipaddress=healthcheck["ipaddress"],
            health=healthcheck["health"],
            threshold=healthcheck["threshold"],
            path=healthcheck["path"],
            interval=healthcheck["interval"],
            port=healthcheck["port"],
            enabled=healthcheck["enabled"],
            zone=zone,
            driver=self.connection.driver,
        )

    def __merge_extra_data(self, rdata, extra):
        if extra is not None:
            for param in VALID_RECORD_PARAMS_EXTRA:
                if param in extra:
                    rdata[param] = extra[param]

        return rdata
