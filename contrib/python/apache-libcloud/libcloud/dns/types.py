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

from libcloud.common.types import LibcloudError

__all__ = [
    "Provider",
    "RecordType",
    "ZoneError",
    "ZoneDoesNotExistError",
    "ZoneAlreadyExistsError",
    "RecordError",
    "RecordDoesNotExistError",
    "RecordAlreadyExistsError",
    "OLD_CONSTANT_TO_NEW_MAPPING",
]


class Provider:
    """
    Defines for each of the supported providers

    Non-Dummy drivers are sorted in alphabetical order. Please preserve this
    ordering when adding new drivers.
    """

    DUMMY = "dummy"
    AURORADNS = "auroradns"
    BUDDYNS = "buddyns"
    CLOUDFLARE = "cloudflare"
    DIGITAL_OCEAN = "digitalocean"
    DNSIMPLE = "dnsimple"
    DURABLEDNS = "durabledns"
    GANDI = "gandi"
    GANDI_LIVE = "gandi_live"
    GODADDY = "godaddy"
    GOOGLE = "google"
    LINODE = "linode"
    LIQUIDWEB = "liquidweb"
    LUADNS = "luadns"
    NFSN = "nfsn"
    NSONE = "nsone"
    ONAPP = "onapp"
    POINTDNS = "pointdns"
    POWERDNS = "powerdns"
    RACKSPACE = "rackspace"
    RCODEZERO = "rcodezero"
    ROUTE53 = "route53"
    VULTR = "vultr"
    WORLDWIDEDNS = "worldwidedns"
    ZERIGO = "zerigo"
    ZONOMI = "zonomi"
    DNSPOD = "dnspod"
    # Deprecated
    RACKSPACE_US = "rackspace_us"
    RACKSPACE_UK = "rackspace_uk"


OLD_CONSTANT_TO_NEW_MAPPING = {
    Provider.RACKSPACE_US: Provider.RACKSPACE,
    Provider.RACKSPACE_UK: Provider.RACKSPACE,
}


class RecordType:
    # TODO: Fix all the affected code and tests and use base Type class here
    """
    DNS record type.
    """
    A = "A"
    AAAA = "AAAA"
    AFSDB = "A"
    ALIAS = "ALIAS"
    CERT = "CERT"
    CNAME = "CNAME"
    DNAME = "DNAME"
    DNSKEY = "DNSKEY"
    DS = "DS"
    GEO = "GEO"
    HINFO = "HINFO"
    KEY = "KEY"
    LOC = "LOC"
    MX = "MX"
    NAPTR = "NAPTR"
    NS = "NS"
    NSEC = "NSEC"
    OPENPGPKEY = "OPENPGPKEY"
    PTR = "PTR"
    REDIRECT = "REDIRECT"
    RP = "RP"
    RRSIG = "RRSIG"
    SOA = "SOA"
    SPF = "SPF"
    SRV = "SRV"
    SSHFP = "SSHFP"
    TLSA = "TLSA"
    TXT = "TXT"
    URL = "URL"
    WKS = "WKS"
    CAA = "CAA"


class ZoneError(LibcloudError):
    error_type = "ZoneError"
    kwargs = ("zone_id",)

    def __init__(self, value, driver, zone_id):
        self.zone_id = zone_id
        super().__init__(value=value, driver=driver)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "<{} in {}, zone_id={}, value={}>".format(
            self.error_type,
            repr(self.driver),
            self.zone_id,
            self.value,
        )


class ZoneDoesNotExistError(ZoneError):
    error_type = "ZoneDoesNotExistError"


class ZoneAlreadyExistsError(ZoneError):
    error_type = "ZoneAlreadyExistsError"


class RecordError(LibcloudError):
    error_type = "RecordError"

    def __init__(self, value, driver, record_id):
        self.record_id = record_id
        super().__init__(value=value, driver=driver)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "<{} in {}, record_id={}, value={}>".format(
            self.error_type,
            repr(self.driver),
            self.record_id,
            self.value,
        )


class RecordDoesNotExistError(RecordError):
    error_type = "RecordDoesNotExistError"


class RecordAlreadyExistsError(RecordError):
    error_type = "RecordAlreadyExistsError"
