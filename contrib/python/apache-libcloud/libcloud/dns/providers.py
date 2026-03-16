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

from types import ModuleType
from typing import TYPE_CHECKING, Type, Union

from libcloud.dns.types import OLD_CONSTANT_TO_NEW_MAPPING, Provider
from libcloud.common.providers import get_driver as _get_provider_driver
from libcloud.common.providers import set_driver as _set_provider_driver

if TYPE_CHECKING:
    # NOTE: This is needed to avoid having setup.py depend on requests
    from libcloud.dns.base import DNSDriver


__all__ = ["DRIVERS", "get_driver", "set_driver"]

DRIVERS = {
    Provider.DUMMY: ("libcloud.dns.drivers.dummy", "DummyDNSDriver"),
    Provider.LINODE: ("libcloud.dns.drivers.linode", "LinodeDNSDriver"),
    Provider.ZERIGO: ("libcloud.dns.drivers.zerigo", "ZerigoDNSDriver"),
    Provider.RACKSPACE: ("libcloud.dns.drivers.rackspace", "RackspaceDNSDriver"),
    Provider.ROUTE53: ("libcloud.dns.drivers.route53", "Route53DNSDriver"),
    Provider.GANDI: ("libcloud.dns.drivers.gandi", "GandiDNSDriver"),
    Provider.GANDI_LIVE: ("libcloud.dns.drivers.gandi_live", "GandiLiveDNSDriver"),
    Provider.GOOGLE: ("libcloud.dns.drivers.google", "GoogleDNSDriver"),
    Provider.DIGITAL_OCEAN: (
        "libcloud.dns.drivers.digitalocean",
        "DigitalOceanDNSDriver",
    ),
    Provider.WORLDWIDEDNS: ("libcloud.dns.drivers.worldwidedns", "WorldWideDNSDriver"),
    Provider.DNSIMPLE: ("libcloud.dns.drivers.dnsimple", "DNSimpleDNSDriver"),
    Provider.POINTDNS: ("libcloud.dns.drivers.pointdns", "PointDNSDriver"),
    Provider.VULTR: ("libcloud.dns.drivers.vultr", "VultrDNSDriver"),
    Provider.LIQUIDWEB: ("libcloud.dns.drivers.liquidweb", "LiquidWebDNSDriver"),
    Provider.ZONOMI: ("libcloud.dns.drivers.zonomi", "ZonomiDNSDriver"),
    Provider.DURABLEDNS: ("libcloud.dns.drivers.durabledns", "DurableDNSDriver"),
    Provider.AURORADNS: ("libcloud.dns.drivers.auroradns", "AuroraDNSDriver"),
    Provider.GODADDY: ("libcloud.dns.drivers.godaddy", "GoDaddyDNSDriver"),
    Provider.CLOUDFLARE: ("libcloud.dns.drivers.cloudflare", "CloudFlareDNSDriver"),
    Provider.NFSN: ("libcloud.dns.drivers.nfsn", "NFSNDNSDriver"),
    Provider.NSONE: ("libcloud.dns.drivers.nsone", "NsOneDNSDriver"),
    Provider.LUADNS: ("libcloud.dns.drivers.luadns", "LuadnsDNSDriver"),
    Provider.BUDDYNS: ("libcloud.dns.drivers.buddyns", "BuddyNSDNSDriver"),
    Provider.POWERDNS: ("libcloud.dns.drivers.powerdns", "PowerDNSDriver"),
    Provider.ONAPP: ("libcloud.dns.drivers.onapp", "OnAppDNSDriver"),
    Provider.RCODEZERO: ("libcloud.dns.drivers.rcodezero", "RcodeZeroDNSDriver"),
    # Deprecated
    Provider.RACKSPACE_US: ("libcloud.dns.drivers.rackspace", "RackspaceUSDNSDriver"),
    Provider.RACKSPACE_UK: ("libcloud.dns.drivers.rackspace", "RackspaceUKDNSDriver"),
}


def get_driver(provider):
    # type: (Union[Provider, str]) -> Type[DNSDriver]
    deprecated_constants = OLD_CONSTANT_TO_NEW_MAPPING
    return _get_provider_driver(
        drivers=DRIVERS, provider=provider, deprecated_constants=deprecated_constants
    )


def set_driver(provider, module, klass):
    # type: (Union[Provider, str], ModuleType, type) -> Type[DNSDriver]
    return _set_provider_driver(drivers=DRIVERS, provider=provider, module=module, klass=klass)
