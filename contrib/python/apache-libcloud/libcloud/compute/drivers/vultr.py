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
Vultr Driver
"""
import json
import time
import base64
from typing import Any, Dict, List, Union, Optional
from functools import update_wrapper

from libcloud.utils.py3 import httplib, urlencode
from libcloud.common.base import JsonResponse, ConnectionKey
from libcloud.common.types import LibcloudError, InvalidCredsError, ServiceUnavailableError
from libcloud.common.vultr import (
    DEFAULT_API_VERSION,
    VultrNetwork,
    VultrConnectionV2,
    VultrNodeSnapshot,
)
from libcloud.compute.base import (
    Node,
    KeyPair,
    NodeSize,
    NodeImage,
    NodeDriver,
    NodeLocation,
    StorageVolume,
)
from libcloud.compute.types import Provider, NodeState, StorageVolumeState, VolumeSnapshotState
from libcloud.utils.iso8601 import parse_date
from libcloud.utils.publickey import get_pubkey_openssh_fingerprint

# For matching region by id
VULTR_COMPUTE_INSTANCE_LOCATIONS = {
    "1": {
        "DCID": "1",
        "name": "New Jersey",
        "country": "US",
        "continent": "North America",
        "state": "NJ",
        "regioncode": "EWR",
    },
    "2": {
        "DCID": "2",
        "name": "Chicago",
        "country": "US",
        "continent": "North America",
        "state": "IL",
        "regioncode": "ORD",
    },
    "3": {
        "DCID": "3",
        "name": "Dallas",
        "country": "US",
        "continent": "North America",
        "state": "TX",
        "regioncode": "DFW",
    },
    "4": {
        "DCID": "4",
        "name": "Seattle",
        "country": "US",
        "continent": "North America",
        "state": "WA",
        "regioncode": "SEA",
    },
    "5": {
        "DCID": "5",
        "name": "Los Angeles",
        "country": "US",
        "continent": "North America",
        "state": "CA",
        "regioncode": "LAX",
    },
    "6": {
        "DCID": "6",
        "name": "Atlanta",
        "country": "US",
        "continent": "North America",
        "state": "GA",
        "regioncode": "ATL",
    },
    "7": {
        "DCID": "7",
        "name": "Amsterdam",
        "country": "NL",
        "continent": "Europe",
        "state": "",
        "regioncode": "AMS",
    },
    "8": {
        "DCID": "8",
        "name": "London",
        "country": "GB",
        "continent": "Europe",
        "state": "",
        "regioncode": "LHR",
    },
    "9": {
        "DCID": "9",
        "name": "Frankfurt",
        "country": "DE",
        "continent": "Europe",
        "state": "",
        "regioncode": "FRA",
    },
    "12": {
        "DCID": "12",
        "name": "Silicon Valley",
        "country": "US",
        "continent": "North America",
        "state": "CA",
        "regioncode": "SJC",
    },
    "19": {
        "DCID": "19",
        "name": "Sydney",
        "country": "AU",
        "continent": "Australia",
        "state": "",
        "regioncode": "SYD",
    },
    "22": {
        "DCID": "22",
        "name": "Toronto",
        "country": "CA",
        "continent": "North America",
        "state": "",
        "regioncode": "YTO",
    },
    "24": {
        "DCID": "24",
        "name": "Paris",
        "country": "FR",
        "continent": "Europe",
        "state": "",
        "regioncode": "CDG",
    },
    "25": {
        "DCID": "25",
        "name": "Tokyo",
        "country": "JP",
        "continent": "Asia",
        "state": "",
        "regioncode": "NRT",
    },
    "34": {
        "DCID": "34",
        "name": "Seoul",
        "country": "KR",
        "continent": "Asia",
        "state": "",
        "regioncode": "ICN",
    },
    "39": {
        "DCID": "39",
        "name": "Miami",
        "country": "US",
        "continent": "North America",
        "state": "FL",
        "regioncode": "MIA",
    },
    "40": {
        "DCID": "40",
        "name": "Singapore",
        "country": "SG",
        "continent": "Asia",
        "state": "",
        "regioncode": "SGP",
    },
}
# For matching image by id
VULTR_COMPUTE_INSTANCE_IMAGES = {
    "127": {
        "OSID": 127,
        "name": "CentOS 6 x64",
        "arch": "x64",
        "family": "centos",
        "windows": False,
    },
    "147": {
        "OSID": 147,
        "name": "CentOS 6 i386",
        "arch": "i386",
        "family": "centos",
        "windows": False,
    },
    "167": {
        "OSID": 167,
        "name": "CentOS 7 x64",
        "arch": "x64",
        "family": "centos",
        "windows": False,
    },
    "381": {
        "OSID": 381,
        "name": "CentOS 7 SELinux x64",
        "arch": "x64",
        "family": "centos",
        "windows": False,
    },
    "362": {
        "OSID": 362,
        "name": "CentOS 8 x64",
        "arch": "x64",
        "family": "centos",
        "windows": False,
    },
    "401": {
        "OSID": 401,
        "name": "CentOS 8 Stream x64",
        "arch": "x64",
        "family": "centos",
        "windows": False,
    },
    "215": {
        "OSID": 215,
        "name": "Ubuntu 16.04 x64",
        "arch": "x64",
        "family": "ubuntu",
        "windows": False,
    },
    "216": {
        "OSID": 216,
        "name": "Ubuntu 16.04 i386",
        "arch": "i386",
        "family": "ubuntu",
        "windows": False,
    },
    "270": {
        "OSID": 270,
        "name": "Ubuntu 18.04 x64",
        "arch": "x64",
        "family": "ubuntu",
        "windows": False,
    },
    "387": {
        "OSID": 387,
        "name": "Ubuntu 20.04 x64",
        "arch": "x64",
        "family": "ubuntu",
        "windows": False,
    },
    "194": {
        "OSID": 194,
        "name": "Debian 8 i386 (jessie)",
        "arch": "i386",
        "family": "debian",
        "windows": False,
    },
    "244": {
        "OSID": 244,
        "name": "Debian 9 x64 (stretch)",
        "arch": "x64",
        "family": "debian",
        "windows": False,
    },
    "352": {
        "OSID": 352,
        "name": "Debian 10 x64 (buster)",
        "arch": "x64",
        "family": "debian",
        "windows": False,
    },
    "230": {
        "OSID": 230,
        "name": "FreeBSD 11 x64",
        "arch": "x64",
        "family": "freebsd",
        "windows": False,
    },
    "327": {
        "OSID": 327,
        "name": "FreeBSD 12 x64",
        "arch": "x64",
        "family": "freebsd",
        "windows": False,
    },
    "366": {
        "OSID": 366,
        "name": "OpenBSD 6.6 x64",
        "arch": "x64",
        "family": "openbsd",
        "windows": False,
    },
    "394": {
        "OSID": 394,
        "name": "OpenBSD 6.7 x64",
        "arch": "x64",
        "family": "openbsd",
        "windows": False,
    },
    "391": {
        "OSID": 391,
        "name": "Fedora CoreOS",
        "arch": "x64",
        "family": "fedora-coreos",
        "windows": False,
    },
    "367": {
        "OSID": 367,
        "name": "Fedora 31 x64",
        "arch": "x64",
        "family": "fedora",
        "windows": False,
    },
    "389": {
        "OSID": 389,
        "name": "Fedora 32 x64",
        "arch": "x64",
        "family": "fedora",
        "windows": False,
    },
    "124": {
        "OSID": 124,
        "name": "Windows 2012 R2 x64",
        "arch": "x64",
        "family": "windows",
        "windows": False,
    },
    "240": {
        "OSID": 240,
        "name": "Windows 2016 x64",
        "arch": "x64",
        "family": "windows",
        "windows": False,
    },
    "159": {
        "OSID": 159,
        "name": "Custom",
        "arch": "x64",
        "family": "iso",
        "windows": False,
    },
    "164": {
        "OSID": 164,
        "name": "Snapshot",
        "arch": "x64",
        "family": "snapshot",
        "windows": False,
    },
    "180": {
        "OSID": 180,
        "name": "Backup",
        "arch": "x64",
        "family": "backup",
        "windows": False,
    },
    "186": {
        "OSID": 186,
        "name": "Application",
        "arch": "x64",
        "family": "application",
        "windows": False,
    },
}
VULTR_COMPUTE_INSTANCE_SIZES = {
    "201": {
        "VPSPLANID": "201",
        "name": "1024 MB RAM,25 GB SSD,1.00 TB BW",
        "vcpu_count": "1",
        "ram": "1024",
        "disk": "25",
        "bandwidth": "1.00",
        "bandwidth_gb": "1024",
        "price_per_month": "5.00",
        "plan_type": "SSD",
        "windows": False,
    },
    "202": {
        "VPSPLANID": "202",
        "name": "2048 MB RAM,55 GB SSD,2.00 TB BW",
        "vcpu_count": "1",
        "ram": "2048",
        "disk": "55",
        "bandwidth": "2.00",
        "bandwidth_gb": "2048",
        "price_per_month": "10.00",
        "plan_type": "SSD",
        "windows": False,
    },
    "203": {
        "VPSPLANID": "203",
        "name": "4096 MB RAM,80 GB SSD,3.00 TB BW",
        "vcpu_count": "2",
        "ram": "4096",
        "disk": "80",
        "bandwidth": "3.00",
        "bandwidth_gb": "3072",
        "price_per_month": "20.00",
        "plan_type": "SSD",
        "windows": False,
    },
    "204": {
        "VPSPLANID": "204",
        "name": "8192 MB RAM,160 GB SSD,4.00 TB BW",
        "vcpu_count": "4",
        "ram": "8192",
        "disk": "160",
        "bandwidth": "4.00",
        "bandwidth_gb": "4096",
        "price_per_month": "40.00",
        "plan_type": "SSD",
        "windows": False,
    },
    "205": {
        "VPSPLANID": "205",
        "name": "16384 MB RAM,320 GB SSD,5.00 TB BW",
        "vcpu_count": "6",
        "ram": "16384",
        "disk": "320",
        "bandwidth": "5.00",
        "bandwidth_gb": "5120",
        "price_per_month": "80.00",
        "plan_type": "SSD",
        "windows": False,
    },
    "206": {
        "VPSPLANID": "206",
        "name": "32768 MB RAM,640 GB SSD,6.00 TB BW",
        "vcpu_count": "8",
        "ram": "32768",
        "disk": "640",
        "bandwidth": "6.00",
        "bandwidth_gb": "6144",
        "price_per_month": "160.00",
        "plan_type": "SSD",
        "windows": False,
    },
    "207": {
        "VPSPLANID": "207",
        "name": "65536 MB RAM,1280 GB SSD,10.00 TB BW",
        "vcpu_count": "16",
        "ram": "65536",
        "disk": "1280",
        "bandwidth": "10.00",
        "bandwidth_gb": "10240",
        "price_per_month": "320.00",
        "plan_type": "SSD",
        "windows": False,
    },
    "208": {
        "VPSPLANID": "208",
        "name": "98304 MB RAM,1600 GB SSD,15.00 TB BW",
        "vcpu_count": "24",
        "ram": "98304",
        "disk": "1600",
        "bandwidth": "15.00",
        "bandwidth_gb": "15360",
        "price_per_month": "640.00",
        "plan_type": "SSD",
        "windows": False,
    },
    "115": {
        "VPSPLANID": "115",
        "name": "8192 MB RAM,110 GB SSD,10.00 TB BW",
        "vcpu_count": "2",
        "ram": "8192",
        "disk": "110",
        "bandwidth": "10.00",
        "bandwidth_gb": "10240",
        "price_per_month": "60.00",
        "plan_type": "DEDICATED",
        "windows": False,
    },
    "116": {
        "VPSPLANID": "116",
        "name": "16384 MB RAM,2x110 GB SSD,20.00 TB BW",
        "vcpu_count": "4",
        "ram": "16384",
        "disk": "110",
        "bandwidth": "20.00",
        "bandwidth_gb": "20480",
        "price_per_month": "120.00",
        "plan_type": "DEDICATED",
        "windows": False,
    },
    "117": {
        "VPSPLANID": "117",
        "name": "24576 MB RAM,3x110 GB SSD,30.00 TB BW",
        "vcpu_count": "6",
        "ram": "24576",
        "disk": "110",
        "bandwidth": "30.00",
        "bandwidth_gb": "30720",
        "price_per_month": "180.00",
        "plan_type": "DEDICATED",
        "windows": False,
    },
    "118": {
        "VPSPLANID": "118",
        "name": "32768 MB RAM,4x110 GB SSD,40.00 TB BW",
        "vcpu_count": "8",
        "ram": "32768",
        "disk": "110",
        "bandwidth": "40.00",
        "bandwidth_gb": "40960",
        "price_per_month": "240.00",
        "plan_type": "DEDICATED",
        "windows": False,
    },
    "400": {
        "VPSPLANID": "400",
        "name": "1024 MB RAM,32 GB SSD,1.00 TB BW",
        "vcpu_count": "1",
        "ram": "1024",
        "disk": "32",
        "bandwidth": "1.00",
        "bandwidth_gb": "1024",
        "price_per_month": "6.00",
        "plan_type": "HIGHFREQUENCY",
        "windows": False,
    },
    "401": {
        "VPSPLANID": "401",
        "name": "2048 MB RAM,64 GB SSD,2.00 TB BW",
        "vcpu_count": "1",
        "ram": "2048",
        "disk": "64",
        "bandwidth": "2.00",
        "bandwidth_gb": "2048",
        "price_per_month": "12.00",
        "plan_type": "HIGHFREQUENCY",
        "windows": False,
    },
    "402": {
        "VPSPLANID": "402",
        "name": "4096 MB RAM,128 GB SSD,3.00 TB BW",
        "vcpu_count": "2",
        "ram": "4096",
        "disk": "128",
        "bandwidth": "3.00",
        "bandwidth_gb": "3072",
        "price_per_month": "24.00",
        "plan_type": "HIGHFREQUENCY",
        "windows": False,
    },
    "403": {
        "VPSPLANID": "403",
        "name": "8192 MB RAM,256 GB SSD,4.00 TB BW",
        "vcpu_count": "3",
        "ram": "8192",
        "disk": "256",
        "bandwidth": "4.00",
        "bandwidth_gb": "4096",
        "price_per_month": "48.00",
        "plan_type": "HIGHFREQUENCY",
        "windows": False,
    },
    "404": {
        "VPSPLANID": "404",
        "name": "16384 MB RAM,384 GB SSD,5.00 TB BW",
        "vcpu_count": "4",
        "ram": "16384",
        "disk": "384",
        "bandwidth": "5.00",
        "bandwidth_gb": "5120",
        "price_per_month": "96.00",
        "plan_type": "HIGHFREQUENCY",
        "windows": False,
    },
    "405": {
        "VPSPLANID": "405",
        "name": "32768 MB RAM,512 GB SSD,6.00 TB BW",
        "vcpu_count": "8",
        "ram": "32768",
        "disk": "512",
        "bandwidth": "6.00",
        "bandwidth_gb": "6144",
        "price_per_month": "192.00",
        "plan_type": "HIGHFREQUENCY",
        "windows": False,
    },
    "406": {
        "VPSPLANID": "406",
        "name": "49152 MB RAM,768 GB SSD,8.00 TB BW",
        "vcpu_count": "12",
        "ram": "49152",
        "disk": "768",
        "bandwidth": "8.00",
        "bandwidth_gb": "8192",
        "price_per_month": "256.00",
        "plan_type": "HIGHFREQUENCY",
        "windows": False,
    },
}


class rate_limited:
    """
    Decorator for retrying Vultr calls that are rate-limited.

    :param int sleep: Seconds to sleep after being rate-limited.
    :param int retries: Number of retries.
    """

    def __init__(self, sleep=0.5, retries=1):
        self.sleep = sleep
        self.retries = retries

    def __call__(self, call):
        """
        Run ``call`` method until it's not rate-limited.

        The method is invoked while it returns 503 Service Unavailable or the
        allowed number of retries is reached.

        :param callable call: Method to be decorated.
        """

        def wrapper(*args, **kwargs):
            last_exception = None

            for _ in range(self.retries + 1):
                try:
                    return call(*args, **kwargs)
                except ServiceUnavailableError as e:
                    last_exception = e
                    time.sleep(self.sleep)  # hit by rate limit, let's sleep

            if last_exception:
                raise last_exception  # pylint: disable=raising-bad-type

        update_wrapper(wrapper, call)
        return wrapper


class VultrResponse(JsonResponse):
    def parse_error(self):
        if self.status == httplib.OK:
            body = self.parse_body()
            return body
        elif self.status == httplib.FORBIDDEN:
            raise InvalidCredsError(self.body)
        elif self.status == httplib.SERVICE_UNAVAILABLE:
            raise ServiceUnavailableError(self.body)
        else:
            raise LibcloudError(self.body)


class SSHKey:
    def __init__(self, id, name, pub_key):
        self.id = id
        self.name = name
        self.pub_key = pub_key

    def __repr__(self):
        return ("<SSHKey: id=%s, name=%s, pub_key=%s>") % (
            self.id,
            self.name,
            self.pub_key,
        )


class VultrConnection(ConnectionKey):
    """
    Connection class for the Vultr driver.
    """

    host = "api.vultr.com"
    responseCls = VultrResponse
    unauthenticated_endpoints = {  # {action: methods}
        "/v1/app/list": ["GET"],
        "/v1/os/list": ["GET"],
        "/v1/plans/list": ["GET"],
        "/v1/plans/list_vc2": ["GET"],
        "/v1/plans/list_vdc2": ["GET"],
        "/v1/regions/availability": ["GET"],
        "/v1/regions/list": ["GET"],
    }

    def add_default_headers(self, headers):
        """
        Adds ``API-Key`` default header.

        :return: Updated headers.
        :rtype: dict
        """

        if self.require_api_key():
            headers.update({"API-Key": self.key})
        return headers

    def encode_data(self, data):
        return urlencode(data)

    @rate_limited()
    def get(self, url):
        return self.request(url)

    @rate_limited()
    def post(self, url, data):
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        return self.request(url, data=data, headers=headers, method="POST")

    def require_api_key(self):
        """
        Check whether this call (method + action) must be authenticated.

        :return: True if ``API-Key`` header required, False otherwise.
        :rtype: bool
        """

        try:
            return self.method not in self.unauthenticated_endpoints[self.action]
        except KeyError:
            return True


class VultrNodeDriverHelper:
    """
    VultrNode helper class.
    """

    def handle_extra(self, extra_keys, data):
        extra = {}
        for key in extra_keys:
            if key in data:
                extra[key] = data[key]
        return extra


class VultrNodeDriver(NodeDriver):
    type = Provider.VULTR
    name = "Vultr"
    website = "https://www.vultr.com"

    def __new__(
        cls,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        api_version=DEFAULT_API_VERSION,
        region=None,
        **kwargs,
    ):
        if cls is VultrNodeDriver:
            if api_version == "1":
                cls = VultrNodeDriverV1
            elif api_version == "2":
                cls = VultrNodeDriverV2
            else:
                raise NotImplementedError(
                    "No Vultr driver found for API version: %s" % (api_version)
                )
        return super().__new__(cls)


class VultrNodeDriverV1(VultrNodeDriver):
    """
    VultrNode node driver.
    """

    connectionCls = VultrConnection

    NODE_STATE_MAP = {"pending": NodeState.PENDING, "active": NodeState.RUNNING}

    EX_CREATE_YES_NO_ATTRIBUTES = [
        "enable_ipv6",
        "enable_private_network",
        "auto_backups",
        "notify_activate",
        "ddos_protection",
    ]

    EX_CREATE_ID_ATTRIBUTES = {
        "iso_id": "ISOID",
        "script_id": "SCRIPTID",
        "snapshot_id": "SNAPSHOTID",
        "app_id": "APPID",
    }

    EX_CREATE_ATTRIBUTES = [
        "ipxe_chain_url",
        "label",
        "userdata",
        "reserved_ip_v4",
        "hostname",
        "tag",
    ]
    EX_CREATE_ATTRIBUTES.extend(EX_CREATE_YES_NO_ATTRIBUTES)
    EX_CREATE_ATTRIBUTES.extend(EX_CREATE_ID_ATTRIBUTES.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._helper = VultrNodeDriverHelper()

    def list_nodes(self):
        return self._list_resources("/v1/server/list", self._to_node)

    def list_key_pairs(self):
        """
        List all the available SSH keys.
        :return: Available SSH keys.
        :rtype: ``list`` of :class:`SSHKey`
        """
        return self._list_resources("/v1/sshkey/list", self._to_ssh_key)

    def create_key_pair(self, name, public_key=""):
        """
        Create a new SSH key.
        :param name: Name of the new SSH key
        :type name: ``str``

        :key public_key: Public part of the new SSH key
        :type name: ``str``

        :return: True on success
        :rtype: ``bool``
        """
        params = {"name": name, "ssh_key": public_key}
        res = self.connection.post("/v1/sshkey/create", params)
        return res.status == httplib.OK

    def delete_key_pair(self, key_pair):
        """
        Delete an SSH key.
        :param key_pair: The SSH key to delete
        :type key_pair: :class:`SSHKey`

        :return: True on success
        :rtype: ``bool``
        """
        params = {"SSHKEYID": key_pair.id}
        res = self.connection.post("/v1/sshkey/destroy", params)
        return res.status == httplib.OK

    def list_locations(self):
        return self._list_resources("/v1/regions/list", self._to_location)

    def list_sizes(self):
        return self._list_resources("/v1/plans/list", self._to_size)

    def list_images(self):
        return self._list_resources("/v1/os/list", self._to_image)

    # pylint: disable=too-many-locals
    def create_node(self, name, size, image, location, ex_ssh_key_ids=None, ex_create_attr=None):
        """
        Create a node

        :param name: Name for the new node
        :type name: ``str``

        :param size: Size of the new node
        :type size: :class:`NodeSize`

        :param image: Image for the new node
        :type image: :class:`NodeImage`

        :param location: Location of the new node
        :type location: :class:`NodeLocation`

        :param ex_ssh_key_ids: IDs of the SSH keys to initialize
        :type ex_sshkeyid: ``list`` of ``str``

        :param ex_create_attr: Extra attributes for node creation
        :type ex_create_attr: ``dict``

        The `ex_create_attr` parameter can include the following dictionary
        key and value pairs:

        * `ipxe_chain_url`: ``str`` for specifying URL to boot via IPXE
        * `iso_id`: ``str`` the ID of a specific ISO to mount,
          only meaningful with the `Custom` `NodeImage`
        * `script_id`: ``int`` ID of a startup script to execute on boot,
          only meaningful when the `NodeImage` is not `Custom`
        * 'snapshot_id`: ``str`` Snapshot ID to restore for the initial
          installation, only meaningful with the `Snapshot` `NodeImage`
        * `enable_ipv6`: ``bool`` Whether an IPv6 subnet should be assigned
        * `enable_private_network`: ``bool`` Whether private networking
          support should be added
        * `label`: ``str`` Text label to be shown in the control panel
        * `auto_backups`: ``bool`` Whether automatic backups should be enabled
        * `app_id`: ``int`` App ID to launch if launching an application,
          only meaningful when the `NodeImage` is `Application`
        * `userdata`: ``str`` Base64 encoded cloud-init user-data
        * `notify_activate`: ``bool`` Whether an activation email should be
          sent when the server is ready
        * `ddos_protection`: ``bool`` Whether DDOS protection should be enabled
        * `reserved_ip_v4`: ``str`` IP address of the floating IP to use as
          the main IP of this server
        * `hostname`: ``str`` The hostname to assign to this server
        * `tag`: ``str`` The tag to assign to this server

        :return: The newly created node.
        :rtype: :class:`Node`

        """
        params = {
            "DCID": location.id,
            "VPSPLANID": size.id,
            "OSID": image.id,
            "label": name,
        }

        if ex_ssh_key_ids is not None:
            params["SSHKEYID"] = ",".join(ex_ssh_key_ids)

        ex_create_attr = ex_create_attr or {}
        for key, value in ex_create_attr.items():
            if key in self.EX_CREATE_ATTRIBUTES:
                if key in self.EX_CREATE_YES_NO_ATTRIBUTES:
                    params[key] = "yes" if value else "no"
                else:
                    if key in self.EX_CREATE_ID_ATTRIBUTES:
                        key = self.EX_CREATE_ID_ATTRIBUTES[key]
                    params[key] = value

        result = self.connection.post("/v1/server/create", params)
        if result.status != httplib.OK:
            return False

        subid = result.object["SUBID"]

        retry_count = 3
        created_node = None

        for _ in range(retry_count):
            try:
                nodes = self.list_nodes()
                created_node = [n for n in nodes if n.id == subid][0]
            except IndexError:
                time.sleep(1)
            else:
                break

        return created_node

    def reboot_node(self, node):
        params = {"SUBID": node.id}
        res = self.connection.post("/v1/server/reboot", params)

        return res.status == httplib.OK

    def destroy_node(self, node):
        params = {"SUBID": node.id}
        res = self.connection.post("/v1/server/destroy", params)

        return res.status == httplib.OK

    def _list_resources(self, url, tranform_func):
        data = self.connection.get(url).object
        sorted_key = sorted(data)
        return [tranform_func(data[key]) for key in sorted_key]

    def _to_node(self, data):
        if "status" in data:
            state = self.NODE_STATE_MAP.get(data["status"], NodeState.UNKNOWN)
            if state == NodeState.RUNNING and data["power_status"] != "running":
                state = NodeState.STOPPED
        else:
            state = NodeState.UNKNOWN

        if "main_ip" in data and data["main_ip"] is not None:
            public_ips = [data["main_ip"]]
        else:
            public_ips = []
        # simple check that we have ip address in value
        if len(data["internal_ip"]) > 0:
            private_ips = [data["internal_ip"]]
        else:
            private_ips = []
        created_at = parse_date(data["date_created"])

        # response ordering
        extra_keys = [
            "location",  # Location name
            "default_password",
            "pending_charges",
            "cost_per_month",
            "current_bandwidth_gb",
            "allowed_bandwidth_gb",
            "netmask_v4",
            "gateway_v4",
            "power_status",
            "server_state",
            "v6_networks",
            # TODO: Does we really need kvm_url?
            "kvm_url",
            "auto_backups",
            "tag",
            # "OSID",  # Operating system to use. See v1/os/list.
            "APPID",
            "FIREWALLGROUPID",
        ]
        extra = self._helper.handle_extra(extra_keys, data)

        resolve_data = VULTR_COMPUTE_INSTANCE_IMAGES.get(data["OSID"])
        if resolve_data:
            image = self._to_image(resolve_data)
        else:
            image = None

        resolve_data = VULTR_COMPUTE_INSTANCE_SIZES.get(data["VPSPLANID"])
        if resolve_data:
            size = self._to_size(resolve_data)
        else:
            size = None

        # resolve_data = VULTR_COMPUTE_INSTANCE_LOCATIONS.get(data['DCID'])
        # if resolve_data:
        #     location = self._to_location(resolve_data)
        # extra['location'] = location

        node = Node(
            id=data["SUBID"],
            name=data["label"],
            state=state,
            public_ips=public_ips,
            private_ips=private_ips,
            image=image,
            size=size,
            extra=extra,
            created_at=created_at,
            driver=self,
        )

        return node

    def _to_location(self, data):
        extra_keys = [
            "continent",
            "state",
            "ddos_protection",
            "block_storage",
            "regioncode",
        ]
        extra = self._helper.handle_extra(extra_keys, data)

        return NodeLocation(
            id=data["DCID"],
            name=data["name"],
            country=data["country"],
            extra=extra,
            driver=self,
        )

    def _to_size(self, data):
        extra_keys = [
            "vcpu_count",
            "plan_type",
            "available_locations",
        ]
        extra = self._helper.handle_extra(extra_keys, data)

        # backward compatibility
        if extra.get("vcpu_count").isdigit():
            extra["vcpu_count"] = int(extra["vcpu_count"])

        ram = int(data["ram"])
        disk = int(data["disk"])
        # NodeSize accepted int instead float
        bandwidth = int(float(data["bandwidth"]))
        price = float(data["price_per_month"])
        return NodeSize(
            id=data["VPSPLANID"],
            name=data["name"],
            ram=ram,
            disk=disk,
            bandwidth=bandwidth,
            price=price,
            extra=extra,
            driver=self,
        )

    def _to_image(self, data):
        extra_keys = ["arch", "family"]
        extra = self._helper.handle_extra(extra_keys, data)
        return NodeImage(id=data["OSID"], name=data["name"], extra=extra, driver=self)

    def _to_ssh_key(self, data):
        return SSHKey(id=data["SSHKEYID"], name=data["name"], pub_key=data["ssh_key"])


class VultrNodeDriverV2(VultrNodeDriver):
    """
    Vultr API v2 NodeDriver.
    """

    connectionCls = VultrConnectionV2
    NODE_STATE_MAP = {
        "active": NodeState.RUNNING,
        "halted": NodeState.STOPPED,
        "rebooting": NodeState.REBOOTING,
        "resizing": NodeState.RECONFIGURING,
        "pending": NodeState.PENDING,
    }

    VOLUME_STATE_MAP = {
        "active": StorageVolumeState.AVAILABLE,
        "pending": StorageVolumeState.CREATING,
    }

    SNAPSHOT_STATE_MAP = {
        "complete": VolumeSnapshotState.AVAILABLE,
        "pending": VolumeSnapshotState.CREATING,
    }

    def list_nodes(self, ex_list_bare_metals: bool = True) -> List[Node]:
        """List all nodes.

        :keyword ex_list_bare_metals: Whether to fetch bare metal nodes.
        :type    ex_list_bare_metals: ``bool``

        :return:  list of node objects
        :rtype: ``list`` of :class: `Node`
        """
        data = self._paginated_request("/v2/instances", "instances")
        nodes = [self._to_node(item) for item in data]

        if ex_list_bare_metals:
            nodes += self.ex_list_bare_metal_nodes()
        return nodes

    def create_node(
        self,
        name: str,
        size: NodeSize,
        location: NodeLocation,
        image: Optional[NodeImage] = None,
        ex_ssh_key_ids: Optional[List[str]] = None,
        ex_private_network_ids: Optional[List[str]] = None,
        ex_snapshot: Union[VultrNodeSnapshot, str, None] = None,
        ex_enable_ipv6: bool = False,
        ex_backups: bool = False,
        ex_userdata: Optional[str] = None,
        ex_ddos_protection: bool = False,
        ex_enable_private_network: bool = False,
        ex_ipxe_chain_url: Optional[str] = None,
        ex_iso_id: Optional[str] = None,
        ex_script_id: Optional[str] = None,
        ex_image_id: Optional[str] = None,
        ex_activation_email: bool = False,
        ex_hostname: Optional[str] = None,
        ex_tag: Optional[str] = None,
        ex_firewall_group_id: Optional[str] = None,
        ex_reserved_ipv4: Optional[str] = None,
        ex_persistent_pxe: bool = False,
    ) -> Node:
        """Create a new node.

        :param name: The new node's name.
        :type name: ``str``

        :param size: The size to use to create the node.
        :type size: :class: `NodeSize`

        :param location: The location to provision the node.
        :type location: :class: `NodeLocation`

        :keyword image:  The image to use to provision the node.
        :type    image:  :class: `NodeImage`

        :keyword ex_ssh_key_ids: List of SSH keys to install on this node.
        :type    ex_ssh_key_ids: ``list`` of ``str``

        :keyword ex_private_network_ids: The network ids to attach to node.
                                         This parameter takes precedence over
                                         ex_enable_private_network (VPS only)
        :type    ex_private_network_ids: ``list`` of ``str``

        :keyword ex_snapshot: The snapshot to use when deploying the node.
                                 Mutually exclusive with image,
        :type    ex_snapshot: :class: `VultrNodeSnapshot` or ``str``

        :keyword ex_enable_ipv6: Whether to enable IPv6.
        :type    ex_enable_ipv6: ``bool``

        :keyword ex_backups: Enable automatic backups for the node. (VPS only)
        :type    ex_backups: ``bool``

        :keyword ex_userdata: String containing user data
        :type    ex_userdata: ``str``

        :keyword ex_ddos_protection: Enable DDoS protection (VPS only)
        :type    ex_ddos_protection: ``bool``

        :keyword ex_enable_private_network: Enable private networking.
                                            Mutually exclusive with
                                             ex_private_network_ids.
                                            (VPS only)
        :type    ex_enable_private_network: ``bool``

        :keyword ex_ipxe_chain_url: The URL location of the iPXE chainloader
                                    (VPS only)
        :type    ex_ipxe_chain_url: ``str``

        :keyword ex_iso_id: The ISO id to use when deploying this node.
                            (VPS only)
        :type    ex_iso_id: ``str``

        :keyword ex_script_id: The startup script id to use when deploying
                               this node.
        :type    ex_script_id: ``str``

        :keyword ex_image_id: The Application image_id to use when deploying
                              this node.
        :type    ex_image_id: ``str``

        :keyword ex_activation_email: Notify by email after deployment.
        :type    ex_activation_email: ``bool``

        :keyword ex_hostname: The hostname to use when deploying this node.
        :type    ex_hostname: ``str``

        :keyword ex_tag: The user-supplied tag.
        :type    ex_tag: ``str``

        :keyword ex_firewall_group_id: The Firewall Group id to attach to
                                       this node. (VPS only)
        :type    ex_firewall_group_id: ``str``

        :keyword ex_reserved_ipv4: Id of the floating IP to use as the
                                   main IP of this node.
        :type    ex_reserved_ipv4: ``str``

        :keyword ex_persistent_pxe: Enable persistent PXE (Bare Metal only)
        :type    ex_persistent_pxe: ``bool``
        """
        data = {
            "label": name,
            "region": location.id,
            "plan": size.id,
            "enable_ipv6": ex_enable_ipv6,
            "activation_email": ex_activation_email,
        }

        if image:
            data["os_id"] = image.id

        if ex_ssh_key_ids:
            data["sshkey_id"] = ex_ssh_key_ids

        if ex_snapshot:
            try:
                data["snapshot_id"] = ex_snapshot.id
            except AttributeError:
                data["snapshot_id"] = ex_snapshot

        if ex_userdata:
            data["user_data"] = base64.b64encode(bytes(ex_userdata, "utf-8")).decode("utf-8")

        if ex_script_id:
            data["script_id"] = ex_script_id

        if ex_image_id:
            data["image_id"] = ex_image_id

        if ex_hostname:
            data["hostname"] = ex_hostname

        if ex_reserved_ipv4:
            data["reserved_ipv4"] = ex_reserved_ipv4

        if ex_tag:
            data["tag"] = ex_tag

        if self._is_bare_metal(size):
            if ex_persistent_pxe:
                data["persistent_pxe"] = ex_persistent_pxe
            resp = self.connection.request("/v2/bare-metals", data=json.dumps(data), method="POST")
            return self._to_node(resp.object["bare_metal"])
        else:
            if ex_private_network_ids:
                data["attach_private_network"] = ex_private_network_ids

            if ex_enable_private_network:
                data["enable_private_network"] = ex_enable_private_network

            if ex_ipxe_chain_url:
                data["ipxe_chain_url"] = ex_ipxe_chain_url

            if ex_iso_id:
                data["iso_id"] = ex_iso_id

            if ex_ddos_protection:
                data["ddos_protection"] = ex_ddos_protection

            if ex_firewall_group_id:
                data["firewall_group_id"] = ex_firewall_group_id

            if ex_backups:
                data["backups"] = "enabled" if ex_backups is True else "disabled"

            resp = self.connection.request("/v2/instances", data=json.dumps(data), method="POST")
            return self._to_node(resp.object["instance"])

    def reboot_node(self, node: Node) -> bool:
        """Reboot the given node.

        :param node: The node to be rebooted.
        :type node: :class: `Node`

        :rtype: ``bool``
        """
        if self._is_bare_metal(node.size):
            return self.ex_reboot_bare_metal_node(node)

        resp = self.connection.request("/v2/instances/%s/reboot" % node.id, method="POST")

        return resp.success()

    def start_node(self, node: Node) -> bool:
        """Start the given node.

        :param node: The node to be started.
        :type node: :class: `Node`

        :rtype: ``bool``
        """
        if self._is_bare_metal(node.size):
            return self.ex_start_bare_metal_node(node)

        resp = self.connection.request("/v2/instances/%s/start" % node.id, method="POST")

        return resp.success()

    def stop_node(self, node: Node) -> bool:
        """Stop the given node.

        :param node: The node to be stopped.
        :type node: :class: `Node`

        :rtype: ``bool``
        """
        if self._is_bare_metal(node.size):
            return self.ex_stop_bare_metal_node(node)

        return self.ex_stop_nodes([node])

    def destroy_node(self, node: Node) -> bool:
        """Destroy the given node.

        :param node: The node to be destroyed.
        :type node: :class: `Node`

        :rtype: ``bool``
        """
        if self._is_bare_metal(node.size):
            return self.ex_destroy_bare_metal_node(node)

        resp = self.connection.request("/v2/instances/%s" % node.id, method="DELETE")

        return resp.success()

    def list_sizes(self, ex_list_bare_metals: bool = True) -> List[NodeSize]:
        """List available node sizes.

        :keyword ex_list_bare_metals: Whether to fetch bare metal sizes.
        :type    ex_list_bare_metals: ``bool``

        :rtype: ``list`` of :class: `NodeSize`
        """
        data = self._paginated_request("/v2/plans", "plans")
        sizes = [self._to_size(item) for item in data]

        if ex_list_bare_metals:
            sizes += self.ex_list_bare_metal_sizes()
        return sizes

    def list_images(self) -> List[NodeImage]:
        """List available node images.

        :rtype: ``list`` of :class: `NodeImage`
        """
        data = self._paginated_request("/v2/os", "os")
        return [self._to_image(item) for item in data]

    def list_locations(self) -> List[NodeLocation]:
        """List available node locations.

        :rtype: ``list`` of :class: `NodeLocation`
        """
        data = self._paginated_request("/v2/regions", "regions")
        return [self._to_location(item) for item in data]

    def list_volumes(self) -> List[StorageVolume]:
        """List storage volumes.

        :rtype: ``list`` of :class:`StorageVolume`
        """
        data = self._paginated_request("/v2/blocks", "blocks")
        return [self._to_volume(item) for item in data]

    def create_volume(
        self,
        size: int,
        name: str,
        location: Union[NodeLocation, str],
    ) -> StorageVolume:
        """Create a new volume.

        :param size: Size of the volume in gigabytes.\
        Size may range between 10 and 10000.
        :type size: ``int``

        :param name: Name of the volume to be created.
        :type name: ``str``

        :param location: Which data center to create the volume in.
        :type location: :class:`NodeLocation` or ``str``

        :return: The newly created volume.
        :rtype: :class:`StorageVolume`
        """

        data = {
            "label": name,
            "size_gb": size,
        }
        try:
            data["region"] = location.id
        except AttributeError:
            data["region"] = location

        resp = self.connection.request("/v2/blocks", data=json.dumps(data), method="POST")
        return self._to_volume(resp.object["block"])

    def attach_volume(
        self,
        node: Node,
        volume: StorageVolume,
        ex_live: bool = True,
    ) -> bool:
        """Attaches volume to node.

        :param node: Node to attach volume to.
        :type node: :class:`Node`

        :param volume: Volume to attach.
        :type volume: :class:`StorageVolume`

        :param ex_live: Attach the volume without restarting the node.
        :type ex_live: ``bool``

        :rytpe: ``bool``
        """

        data = {
            "instance_id": node.id,
            "live": ex_live,
        }
        resp = self.connection.request(
            "/v2/blocks/%s/attach" % volume.id, data=json.dumps(data), method="POST"
        )

        return resp.success()

    def detach_volume(
        self,
        volume: StorageVolume,
        ex_live: bool = True,
    ) -> bool:
        """Detaches a volume from a node.

        :param volume: Volume to be detached
        :type volume: :class:`StorageVolume`

        :param ex_live: Detach the volume without restarting the node.
        :type ex_live: ``bool``

        :rtype: ``bool``
        """
        data = {"live": ex_live}

        resp = self.connection.request(
            "/v2/blocks/%s/detach" % volume.id, data=json.dumps(data), method="POST"
        )

        return resp.success()

    def destroy_volume(self, volume: StorageVolume) -> bool:
        """Destroys a storage volume.

        :param volume: Volume to be destroyed
        :type  volume: :class:`StorageVolume`

        :rtype: ``bool``
        """

        resp = self.connection.request("/v2/blocks/%s" % volume.id, method="DELETE")

        return resp.success()

    def list_key_pairs(self) -> List[KeyPair]:
        """List all the available SSH key pair objects.

        :rtype: ``list`` of :class:`KeyPair`
        """
        data = self._paginated_request("/v2/ssh-keys", "ssh_keys")
        return [self._to_key_pair(item) for item in data]

    def get_key_pair(self, key_id: str) -> KeyPair:
        """Retrieve a single key pair.

        :param key_id: ID of the key pair to retrieve.
        :type  key_id: ``str``

        :rtype: :class: `KeyPair`
        """
        resp = self.connection.request("/v2/ssh-keys/%s" % key_id)
        return self._to_key_pair(resp.object["ssh_key"])

    def import_key_pair_from_string(self, name: str, key_material: str) -> KeyPair:
        """Import a new public key from string.

        :param name: Key pair name.
        :type  name: ``str``

        :param key_material: Public key material.
        :type  key_material: ``str``

        :rtype: :class: `KeyPair`
        """
        data = {
            "name": name,
            "ssh_key": key_material,
        }
        resp = self.connection.request("/v2/ssh-keys", data=json.dumps(data), method="POST")
        return self._to_key_pair(resp.object["ssh_key"])

    def delete_key_pair(self, key_pair: KeyPair) -> bool:
        """Delete existing key pair.

        :param key_pair: The key pair object to delete.
        :type key_pair: :class:`.KeyPair`

        :rtype: ``bool``
        """

        resp = self.connection.request("/v2/ssh-keys/%s" % key_pair.extra["id"], method="DELETE")

        return resp.success()

    def ex_list_bare_metal_nodes(self) -> List[Node]:
        """List all bare metal nodes.

        :return:  list of node objects
        :rtype: ``list`` of :class: `Node`
        """
        data = self._paginated_request("/v2/bare-metals", "bare_metals")
        return [self._to_node(item) for item in data]

    def ex_reboot_bare_metal_node(self, node: Node) -> bool:
        """Reboot the given bare metal node.

        :param node: The bare metal node to be rebooted.
        :type node: :class: `Node`

        :rtype: ``bool``
        """
        resp = self.connection.request("/v2/bare-metals/%s/reboot" % node.id, method="POST")

        return resp.success()

    def ex_resize_node(self, node: Node, size: NodeSize) -> bool:
        """Change size for the given node, only applicable for VPS nodes.

        :param node: The node to be resized.
        :type node: :class: `Node`

        :param size: The new size.
        :type size: :class: `NodeSize`
        """
        data = {"plan": size.id}
        resp = self.connection.request(
            "/v2/instances/%s" % node.id, data=json.dumps(data), method="PATCH"
        )
        return self._to_node(resp.object["instance"])

    def ex_start_bare_metal_node(self, node: Node) -> bool:
        """Start the given bare metal node.

        :param node: The bare metal node to be started.
        :type node: :class: `Node`

        :rtype: ``bool``
        """
        resp = self.connection.request("/v2/bare-metals/%s/start" % node.id, method="POST")

        return resp.success()

    def ex_stop_bare_metal_node(self, node: Node) -> bool:
        """Stop the given bare metal node.

        :param node: The bare metal node to be stopped.
        :type node: :class: `Node`

        :rtype: ``bool``
        """
        resp = self.connection.request("/v2/bare-metals/%s/halt" % node.id, method="POST")

        return resp.success()

    def ex_destroy_bare_metal_node(self, node: Node) -> bool:
        """Destroy the given bare metal node.

        :param node: The bare metal node to be destroyed.
        :type node: :class: `Node`

        :rtype: ``bool``
        """
        resp = self.connection.request("/v2/bare-metals/%s" % node.id, method="DELETE")

        return resp.success()

    def ex_get_node(self, node_id: str) -> Node:
        """Retrieve a node object.

        :param node_id: ID of the node to retrieve.
        :type snapshot_id: ``str``

        :rtype: :class: `Node`
        """
        resp = self.connection.request("/v2/instances/%s" % node_id)
        return self._to_node(resp.object["instance"])

    def ex_stop_nodes(self, nodes: List[Node]) -> bool:
        """Stops all the nodes given.

        : param nodes: A list of the nodes to stop.
        : type node: ``list`` of: class `Node`

        : rtype: ``bool``
        """

        data = {"instance_ids": [node.id for node in nodes]}
        resp = self.connection.request("/v2/instances/halt", data=json.dumps(data), method="POST")

        return resp.success()

    def ex_list_bare_metal_sizes(self) -> List[NodeSize]:
        """List bare metal sizes.

        :rtype: ``list`` of :class: `NodeSize`
        """
        data = self._paginated_request("/v2/plans-metal", "plans_metal")
        return [self._to_size(item) for item in data]

    def ex_list_snapshots(self) -> List[VultrNodeSnapshot]:
        """List node snapshots.

        :rtype: ``list`` of :class: `VultrNodeSnapshot`
        """
        data = self._paginated_request("/v2/snapshots", "snapshots")
        return [self._to_snapshot(item) for item in data]

    def ex_get_snapshot(self, snapshot_id: str) -> VultrNodeSnapshot:
        """Retrieve a snapshot.

        :param snapshot_id: ID of the snapshot to retrieve.
        :type snapshot_id: ``str``

        :rtype: :class: `VultrNodeSnapshot`
        """
        resp = self.connection.request("/v2/snapshots/%s" % snapshot_id)
        return self._to_snapshot(resp.object["snapshot"])

    def ex_create_snapshot(
        self, node: Node, description: Optional[str] = None
    ) -> VultrNodeSnapshot:
        """Create snapshot from a node.

        :param node: Node to create the snapshot from.
        :type node: :class: `Node`

        :keyword description: A description of the snapshot.
        :type    description: ``str``

        :rtype: :class: `VultrNodeSnapshot`
        """
        data = {
            "instance_id": node.id,
        }
        if description:
            data["description"] = description

        resp = self.connection.request("/v2/snapshots", data=json.dumps(data), method="POST")

        return self._to_snapshot(resp.object["snapshot"])

    def ex_delete_snapshot(self, snapshot: VultrNodeSnapshot) -> bool:
        """Delete the given snapshot.

        :param snapshot: The snapshot to delete.
        :type node: :class:`VultrNodeSnapshot`

        :rtype: ``bool``
        """

        resp = self.connection.request("/v2/snapshots/%s" % snapshot.id, method="DELETE")

        return resp.success()

    def ex_list_networks(self) -> List[VultrNetwork]:
        """List all private networks.

        :rtype: ``list`` of :class: `VultrNetwork`
        """

        data = self._paginated_request("/v2/private-networks", "networks")
        return [self._to_network(item) for item in data]

    def ex_create_network(
        self,
        cidr_block: str,
        location: Union[NodeLocation, str],
        description: Optional[str] = None,
    ) -> VultrNetwork:
        """Create a private network.

        :param cidr_block: The CIDR block assigned to the network.
        :type  cidr_block: ``str``

        :param location: The location to create the network.
        :type  location: :class: `NodeLocation` or ``str``

        :keyword description: A description of the private network.
        :type    description: ``str``

        :rtype: :class: `VultrNetwork`
        """
        subnet, subnet_mask = cidr_block.split("/")
        data = {
            "v4_subnet": subnet,
            "v4_subnet_mask": int(subnet_mask),
        }

        try:
            data["region"] = location.id
        except AttributeError:
            data["region"] = location

        if description:
            data["description"] = description

        resp = self.connection.request("/v2/private-networks", data=json.dumps(data), method="POST")

        return self._to_network(resp.object["network"])

    def ex_get_network(self, network_id: str) -> VultrNetwork:
        """Retrieve a private network.

        :param network_id: ID of the network to retrieve.
        :type network_id: ``str``

        :rtype: :class: `VultrNetwork`
        """

        resp = self.connection.request("/v2/private-networks/%s" % network_id)
        return self._to_network(resp.object["network"])

    def ex_destroy_network(self, network: VultrNetwork) -> bool:
        """Delete a private network.

        :param network: The network to destroy.
        :type  network: :class: `VultrNetwork`

        :rtype: ``bool``
        """
        resp = self.connection.request("/v2/private-networks/%s" % network.id, method="DELETE")

        return resp.success()

    def ex_list_available_sizes_for_location(
        self,
        location: NodeLocation,
    ) -> List[str]:
        """Get a list of available sizes for the given location.

        :param location: The location to get available sizes for.
        :type location: :class: `NodeLocation`

        :return:  A list of available size IDs for the given location.
        :rtype: ``list`` of ``str``
        """
        resp = self.connection.request("/v2/regions/%s/availability" % location.id)
        return resp.object["available_plans"]

    def ex_get_volume(self, volume_id: str) -> StorageVolume:
        """Retrieve a single volume.

        :param volume_id: The ID of the volume to fetch.
        :type volume_id: ``str``

        :rtype :class: `StorageVolume`
        :return: StorageVolume instance on success.
        """
        resp = self.connection.request("/v2/blocks/%s" % volume_id)

        return self._to_volume(resp.object["block"])

    def ex_resize_volume(self, volume: StorageVolume, size: int) -> bool:
        """Resize a volume.

        :param volume: The volume to resize.
        :type volume: :class:`StorageVolume`

        :param size: The new volume size in GBs.\
        Size may range between 10 and 10000.
        :type size: ``int``

        :rtype: ``bool``
        """
        data = {
            "label": volume.name,
            "size_gb": size,
        }

        resp = self.connection.request(
            "/v2/blocks/%s" % volume.id, data=json.dumps(data), method="PATCH"
        )
        return resp.success()

    def _is_bare_metal(self, size: Union[NodeSize, str]) -> bool:
        try:
            size_id = size.id
        except AttributeError:
            size_id = size

        return size_id.startswith("vbm")

    def _to_node(self, data: Dict[str, Any]) -> Node:
        id_ = data["id"]
        name = data["label"]
        public_ips = data["main_ip"].split() + data["v6_main_ip"].split()
        size = data["plan"]
        image = str(data["os_id"])
        created_at = data["date_created"]
        is_bare_metal = self._is_bare_metal(size)
        extra = {
            "location": data["region"],
            "ram": data["ram"],
            "disk": data["disk"],
            "netmask_v4": data["netmask_v4"],
            "gateway_v4": data["gateway_v4"],
            "v6_network": data["v6_network"],
            "v6_network_size": data["v6_network_size"],
            "app_id": data["app_id"],
            "image_id": data["image_id"],
            "features": data["features"],
            "tag": data["tag"],
            "os": data["os"],
            "is_bare_metal": is_bare_metal,
        }
        if is_bare_metal:
            state = self._get_node_state(data["status"])
            extra["cpu_count"] = data["cpu_count"]
            extra["mac_address"] = data["mac_address"]
            private_ips = None
        else:
            state = self._get_node_state(data["status"], power_state=data["power_status"])
            extra["vcpu_count"] = data["vcpu_count"]
            extra["allowed_bandwidth"] = data["allowed_bandwidth"]
            extra["power_status"] = data["power_status"]
            extra["server_status"] = data["server_status"]
            extra["firewall_group_id"] = data["firewall_group_id"]
            private_ips = data["internal_ip"].split()

        return Node(
            id=id_,
            name=name,
            state=state,
            public_ips=public_ips,
            private_ips=private_ips,
            driver=self,
            size=size,
            image=image,
            extra=extra,
            created_at=created_at,
        )

    def _to_volume(self, data: Dict[str, Any]) -> StorageVolume:
        id_ = data["id"]
        name = data["label"]
        size = data["size_gb"]
        try:
            state = self.VOLUME_STATE_MAP[data["status"]]
        except KeyError:
            state = StorageVolumeState.UNKNOWN
        extra = {
            "date_created": data["date_created"],
            "cost": data["cost"],
            "location": data["region"],
            "attached_to_instance": data["attached_to_instance"],
            "mount_id": data["mount_id"],
        }
        return StorageVolume(id=id_, name=name, size=size, driver=self, state=state, extra=extra)

    def _get_node_state(
        self,
        state: str,
        power_state: Optional[str] = None,
    ) -> NodeState:
        try:
            state = self.NODE_STATE_MAP[state]
        except KeyError:
            state = NodeState.UNKNOWN

        if power_state is None:
            return state

        if state == NodeState.RUNNING and power_state != "running":
            state = NodeState.STOPPED
        return state

    def _to_key_pair(self, data: Dict[str, Any]) -> KeyPair:
        name = data["name"]
        public_key = data["ssh_key"]
        # requires cryptography module
        try:
            fingerprint = get_pubkey_openssh_fingerprint(public_key)
        except RuntimeError:
            fingerprint = None
        extra = {
            "id": data["id"],
            "date_created": data["date_created"],
        }
        return KeyPair(
            name=name,
            public_key=public_key,
            fingerprint=fingerprint,
            driver=self,
            extra=extra,
        )

    def _to_location(self, data: Dict[str, Any]) -> NodeLocation:
        id_ = data["id"]
        name = data["city"]
        country = data["country"]
        extra = {
            "continent": data["continent"],
            "option": data["options"],
        }
        return NodeLocation(id=id_, name=name, country=country, driver=self, extra=extra)

    def _to_image(self, data: Dict[str, Any]) -> NodeImage:
        id_ = data["id"]
        name = data["name"]
        extra = {
            "arch": data["arch"],
            "family": data["family"],
        }
        return NodeImage(id=id_, name=name, driver=self, extra=extra)

    def _to_size(self, data: Dict[str, Any]) -> NodeSize:
        id_ = data["id"]
        ram = data["ram"]
        disk = data["disk"]
        bandwidth = data["bandwidth"]
        price = data["monthly_cost"]
        is_bare_metal = self._is_bare_metal(id_)
        extra = {
            "locations": data["locations"],
            "type": data["type"],
            "disk_count": data["disk_count"],
            "is_bare_metal": is_bare_metal,
        }

        # VPS and bare metal sizes have different fields
        if is_bare_metal is False:
            extra["vcpu_count"] = data["vcpu_count"]
        else:
            extra["cpu_count"] = data["cpu_count"]
            extra["cpu_model"] = data["cpu_model"]
            extra["cpu_threads"] = data["cpu_threads"]

        return NodeSize(
            id=id_,
            name=id_,
            ram=ram,
            disk=disk,
            bandwidth=bandwidth,
            price=price,
            driver=self,
            extra=extra,
        )

    def _to_network(self, data: Dict[str, Any]) -> VultrNetwork:
        id_ = data["id"]
        cidr_block = "{}/{}".format(data["v4_subnet"], data["v4_subnet_mask"])
        location = data["region"]
        extra = {
            "description": data["description"],
            "date_created": data["date_created"],
        }
        return VultrNetwork(id=id_, cidr_block=cidr_block, location=location, extra=extra)

    def _to_snapshot(self, data: Dict[str, Any]) -> VultrNodeSnapshot:
        id_ = data["id"]
        created = data["date_created"]
        # Size is returned in bytes, convert to GBs
        size = data["size"] / 1024 / 1024 / 1024
        try:
            state = self.SNAPSHOT_STATE_MAP[data["status"]]
        except KeyError:
            state = VolumeSnapshotState.UNKNOWN
        extra = {
            "description": data["description"],
            "os_id": data["os_id"],
            "app_id": data["app_id"],
        }
        return VultrNodeSnapshot(
            id=id_, size=size, created=created, state=state, extra=extra, driver=self
        )

    def _paginated_request(
        self, url: str, key: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Perform multiple calls to get the full list of items when
        the API responses are paginated.

        :param url: API endpoint
        :type url: ``str``

        :param key: Result object key
        :type key: ``str``

        :param params: Request parameters
        :type params: ``dict``

        :return: ``list`` of API response objects
        :rtype: ``list``
        """
        params = params if params is not None else {}
        resp = self.connection.request(url, params=params).object
        data = list(resp.get(key, []))
        objects = data
        while True:
            next_page = resp["meta"]["links"]["next"]
            if next_page:
                params["cursor"] = next_page
                resp = self.connection.request(url, params=params).object
                data = list(resp.get(key, []))
                objects.extend(data)
            else:
                return objects
