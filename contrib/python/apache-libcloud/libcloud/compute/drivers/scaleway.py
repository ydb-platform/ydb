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
Scaleway Driver
"""

import copy

from libcloud.utils.py3 import httplib
from libcloud.common.base import JsonResponse, ConnectionUserAndKey
from libcloud.common.types import ProviderError
from libcloud.compute.base import (
    Node,
    KeyPair,
    NodeSize,
    NodeImage,
    NodeDriver,
    NodeLocation,
    StorageVolume,
    VolumeSnapshot,
)
from libcloud.compute.types import NodeState, VolumeSnapshotState
from libcloud.utils.iso8601 import parse_date
from libcloud.compute.providers import Provider

try:
    import simplejson as json
except ImportError:
    import json


__all__ = ["ScalewayResponse", "ScalewayConnection", "ScalewayNodeDriver"]

SCALEWAY_API_HOSTS = {
    "default": "cp-par1.scaleway.com",
    "account": "account.scaleway.com",
    "par1": "cp-par1.scaleway.com",
    "ams1": "cp-ams1.scaleway.com",
}

# The API doesn't give location info, so we provide it ourselves, instead.
SCALEWAY_LOCATION_DATA = [
    {"id": "par1", "name": "Paris 1", "country": "FR"},
    {"id": "ams1", "name": "Amsterdam 1", "country": "NL"},
]


class ScalewayResponse(JsonResponse):
    valid_response_codes = [
        httplib.OK,
        httplib.ACCEPTED,
        httplib.CREATED,
        httplib.NO_CONTENT,
    ]

    def parse_error(self):
        return super().parse_error()["message"]

    def success(self):
        return self.status in self.valid_response_codes


class ScalewayConnection(ConnectionUserAndKey):
    """
    Connection class for the Scaleway driver.
    """

    host = SCALEWAY_API_HOSTS["default"]
    allow_insecure = False
    responseCls = ScalewayResponse

    def request(
        self,
        action,
        params=None,
        data=None,
        headers=None,
        method="GET",
        raw=False,
        stream=False,
        region=None,
    ):
        if region:
            old_host = self.host
            self.host = SCALEWAY_API_HOSTS[
                region.id if isinstance(region, NodeLocation) else region
            ]
            if not self.host == old_host:
                self.connect()

        return super().request(action, params, data, headers, method, raw, stream)

    def _request_paged(
        self,
        action,
        params=None,
        data=None,
        headers=None,
        method="GET",
        raw=False,
        stream=False,
        region=None,
    ):
        if params is None:
            params = {}

        if isinstance(params, dict):
            params["per_page"] = 100
        elif isinstance(params, list):
            params.append(("per_page", 100))  # pylint: disable=no-member

        results = self.request(action, params, data, headers, method, raw, stream, region).object

        links = self.connection.getresponse().links
        while links and "next" in links:
            next = self.request(
                links["next"]["url"],
                data=data,
                headers=headers,
                method=method,
                raw=raw,
                stream=stream,
            ).object
            links = self.connection.getresponse().links
            merged = {root: child + next[root] for root, child in list(results.items())}
            results = merged

        return results

    def add_default_headers(self, headers):
        """
        Add headers that are necessary for every request
        """
        headers["X-Auth-Token"] = self.key
        headers["Content-Type"] = "application/json"
        return headers


def _to_lib_size(size):
    return int(size / 1000 / 1000 / 1000)


def _to_api_size(size):
    return int(size * 1000 * 1000 * 1000)


class ScalewayNodeDriver(NodeDriver):
    """
    Scaleway Node Driver Class

    This is the primary driver for interacting with Scaleway.  It contains all
    of the standard libcloud methods that Scaleway's API supports.
    """

    type = Provider.SCALEWAY
    connectionCls = ScalewayConnection
    name = "Scaleway"
    website = "https://www.scaleway.com/"

    SNAPSHOT_STATE_MAP = {
        "snapshotting": VolumeSnapshotState.CREATING,
        "available": VolumeSnapshotState.AVAILABLE,
        "error": VolumeSnapshotState.ERROR,
    }

    def list_locations(self):
        """
        List data centers available.

        :return: list of node location objects
        :rtype: ``list`` of :class:`.NodeLocation`
        """
        return [
            NodeLocation(driver=self, **copy.deepcopy(location))
            for location in SCALEWAY_LOCATION_DATA
        ]

    def list_sizes(self, region=None):
        """
        List available VM sizes.

        :param region: The region in which to list sizes
        (if None, use default region specified in __init__)
        :type region: :class:`.NodeLocation`

        :return: list of node size objects
        :rtype: ``list`` of :class:`.NodeSize`
        """
        response = self.connection._request_paged("/products/servers", region=region)
        sizes = response["servers"]

        response = self.connection._request_paged("/products/servers/availability", region=region)
        availability = response["servers"]

        return sorted(
            (self._to_size(name, sizes[name], availability[name]) for name in sizes),
            key=lambda x: x.name,
        )

    def _to_size(self, name, size, availability):
        min_disk = (
            _to_lib_size(size["volumes_constraint"]["min_size"] or 0)
            if size["volumes_constraint"]
            else 25
        )
        max_disk = (
            _to_lib_size(size["volumes_constraint"]["max_size"] or 0)
            if size["volumes_constraint"]
            else min_disk
        )

        extra = {
            "cores": size["ncpus"],
            "monthly": size["monthly_price"],
            "arch": size["arch"],
            "baremetal": size["baremetal"],
            "availability": availability["availability"],
            "max_disk": max_disk,
            "internal_bandwidth": int(
                (size["network"]["sum_internal_bandwidth"] or 0) / (1024 * 1024)
            ),
            "ipv6": size["network"]["ipv6_support"],
            "alt_names": size["alt_names"],
        }

        return NodeSize(
            id=name,
            name=name,
            ram=int((size["ram"] or 0) / (1024 * 1024)),
            disk=min_disk,
            bandwidth=int((size["network"]["sum_internet_bandwidth"] or 0) / (1024 * 1024)),
            price=size["hourly_price"],
            driver=self,
            extra=extra,
        )

    def list_images(self, region=None):
        """
        List available VM images.

        :param region: The region in which to list images
        (if None, use default region specified in __init__)
        :type region: :class:`.NodeLocation`

        :return: list of image objects
        :rtype: ``list`` of :class:`.NodeImage`
        """
        response = self.connection._request_paged("/images", region=region)
        images = response["images"]
        return [self._to_image(image) for image in images]

    def create_image(self, node, name, region=None):
        """
        Create a VM image from an existing node's root volume.

        :param node: The node from which to create the image
        :type node: :class:`.Node`

        :param name: The name to give the image
        :type name: ``str``

        :param region: The region in which to create the image
        (if None, use default region specified in __init__)
        :type region: :class:`.NodeLocation`

        :return: the newly created image object
        :rtype: :class:`.NodeImage`
        """
        data = {
            "organization": self.key,
            "name": name,
            "arch": node.extra["arch"],
            "root_volume": node.extra["volumes"]["0"]["id"],
        }
        response = self.connection.request(
            "/images", data=json.dumps(data), region=region, method="POST"
        )
        image = response.object["image"]
        return self._to_image(image)

    def delete_image(self, node_image, region=None):
        """
        Delete a VM image.

        :param node_image: The image to delete
        :type node_image: :class:`.NodeImage`

        :param region: The region in which to find/delete the image
        (if None, use default region specified in __init__)
        :type region: :class:`.NodeLocation`

        :return: True if the image was deleted, otherwise False
        :rtype: ``bool``
        """
        return self.connection.request(
            "/images/%s" % node_image.id, region=region, method="DELETE"
        ).success()

    def get_image(self, image_id, region=None):
        """
        Retrieve a specific VM image.

        :param image_id: The id of the image to retrieve
        :type image_id: ``int``

        :param region: The region in which to create the image
        (if None, use default region specified in __init__)
        :type region: :class:`.NodeLocation`

        :return: the requested image object
        :rtype: :class:`.NodeImage`
        """
        response = self.connection.request("/images/%s" % image_id, region=region)
        image = response.object["image"]
        return self._to_image(image)

    def _to_image(self, image):
        extra = {
            "arch": image["arch"],
            "size": _to_lib_size(image.get("root_volume", {}).get("size", 0)) or 50,
            "creation_date": parse_date(image["creation_date"]),
            "modification_date": parse_date(image["modification_date"]),
            "organization": image["organization"],
        }
        return NodeImage(id=image["id"], name=image["name"], driver=self, extra=extra)

    def list_nodes(self, region=None):
        """
        List all nodes.

        :param region: The region in which to look for nodes
        (if None, use default region specified in __init__)
        :type region: :class:`.NodeLocation`

        :return: list of node objects
        :rtype: ``list`` of :class:`.Node`
        """
        response = self.connection._request_paged("/servers", region=region)
        servers = response["servers"]
        return [self._to_node(server) for server in servers]

    def _to_node(self, server):
        public_ip = server["public_ip"]
        private_ip = server["private_ip"]
        location = server["location"] or {}
        return Node(
            id=server["id"],
            name=server["name"],
            state=NodeState.fromstring(server["state"]),
            public_ips=[public_ip["address"]] if public_ip else [],
            private_ips=[private_ip] if private_ip else [],
            driver=self,
            extra={
                "volumes": server["volumes"],
                "tags": server["tags"],
                "arch": server["arch"],
                "organization": server["organization"],
                "region": location.get("zone_id", "par1"),
            },
            created_at=parse_date(server["creation_date"]),
        )

    def create_node(self, name, size, image, ex_volumes=None, ex_tags=None, region=None):
        """
        Create a new node.

        :param name: The name to give the node
        :type name: ``str``

        :param size: The size of node to create
        :type size: :class:`.NodeSize`

        :param image: The image to create the node with
        :type image: :class:`.NodeImage`

        :param ex_volumes: Additional volumes to create the node with
        :type ex_volumes: ``dict`` of :class:`.StorageVolume`s

        :param ex_tags: Tags to assign to the node
        :type ex_tags: ``list`` of ``str``

        :param region: The region in which to create the node
        (if None, use default region specified in __init__)
        :type region: :class:`.NodeLocation`

        :return: the newly created node object
        :rtype: :class:`.Node`
        """
        data = {
            "name": name,
            "organization": self.key,
            "image": image.id,
            "volumes": ex_volumes or {},
            "commercial_type": size.id,
            "tags": ex_tags or [],
        }

        allocate_space = image.extra.get("size", 50)
        for volume in data["volumes"]:
            allocate_space += _to_lib_size(volume["size"])

        while allocate_space < size.disk:
            if size.disk - allocate_space > 150:
                bump = 150
            else:
                bump = size.disk - allocate_space

            vol_num = len(data["volumes"]) + 1
            data["volumes"][str(vol_num)] = {
                "name": "%s-%d" % (name, vol_num),
                "organization": self.key,
                "size": _to_api_size(bump),
                "volume_type": "l_ssd",
            }
            allocate_space += bump

        if allocate_space > size.extra.get("max_disk", size.disk):
            range = (
                "of %dGB" % size.disk
                if size.extra.get("max_disk", size.disk) == size.disk
                else "between %dGB and %dGB" % (size.extra.get("max_disk", size.disk), size.disk)
            )
            raise ProviderError(
                value=(
                    "%s only supports a total volume size %s; tried %dGB"
                    % (size.id, range, allocate_space)
                ),
                http_code=400,
                driver=self,
            )

        response = self.connection.request(
            "/servers", data=json.dumps(data), region=region, method="POST"
        )
        server = response.object["server"]
        node = self._to_node(server)
        node.extra["region"] = (region.id if isinstance(region, NodeLocation) else region) or "par1"

        # Scaleway doesn't start servers by default, let's do it
        self._action(node.id, "poweron")

        return node

    def _action(self, server_id, action, region=None):
        return self.connection.request(
            "/servers/%s/action" % server_id,
            region=region,
            data=json.dumps({"action": action}),
            method="POST",
        ).success()

    def reboot_node(self, node):
        """
        Reboot a node.

        :param node: The node to be rebooted
        :type node: :class:`Node`

        :return: True if the reboot was successful, otherwise False
        :rtype: ``bool``
        """
        return self._action(node.id, "reboot")

    def destroy_node(self, node):
        """
        Destroy a node.

        :param node: The node to be destroyed
        :type node: :class:`Node`

        :return: True if the destroy was successful, otherwise False
        :rtype: ``bool``
        """
        return self._action(node.id, "terminate")

    def list_volumes(self, region=None):
        """
        Return a list of volumes.

        :param region: The region in which to look for volumes
        (if None, use default region specified in __init__)
        :type region: :class:`.NodeLocation`

        :return: A list of volume objects.
        :rtype: ``list`` of :class:`StorageVolume`
        """
        response = self.connection._request_paged("/volumes", region=region)
        volumes = response["volumes"]
        return [self._to_volume(volume) for volume in volumes]

    def _to_volume(self, volume):
        extra = {
            "organization": volume["organization"],
            "volume_type": volume["volume_type"],
            "creation_date": parse_date(volume["creation_date"]),
            "modification_date": parse_date(volume["modification_date"]),
        }
        return StorageVolume(
            id=volume["id"],
            name=volume["name"],
            size=_to_lib_size(volume["size"]),
            driver=self,
            extra=extra,
        )

    def list_volume_snapshots(self, volume, region=None):
        """
        List snapshots for a storage volume.

        @inherits :class:`NodeDriver.list_volume_snapshots`

        :param region: The region in which to look for snapshots
        (if None, use default region specified in __init__)
        :type region: :class:`.NodeLocation`
        """
        response = self.connection._request_paged("/snapshots", region=region)
        snapshots = filter(lambda s: s["base_volume"]["id"] == volume.id, response["snapshots"])
        return [self._to_snapshot(snapshot) for snapshot in snapshots]

    def _to_snapshot(self, snapshot):
        state = self.SNAPSHOT_STATE_MAP.get(snapshot["state"], VolumeSnapshotState.UNKNOWN)
        extra = {
            "organization": snapshot["organization"],
            "volume_type": snapshot["volume_type"],
        }
        return VolumeSnapshot(
            id=snapshot["id"],
            driver=self,
            size=_to_lib_size(snapshot["size"]),
            created=parse_date(snapshot["creation_date"]),
            state=state,
            extra=extra,
        )

    def create_volume(self, size, name, region=None):
        """
        Create a new volume.

        :param size: Size of volume in gigabytes.
        :type size: ``int``

        :param name: Name of the volume to be created.
        :type name: ``str``

        :param region: The region in which to create the volume
        (if None, use default region specified in __init__)
        :type region: :class:`.NodeLocation`

        :return: The newly created volume.
        :rtype: :class:`StorageVolume`
        """
        data = {
            "name": name,
            "organization": self.key,
            "volume_type": "l_ssd",
            "size": _to_api_size(size),
        }
        response = self.connection.request(
            "/volumes", region=region, data=json.dumps(data), method="POST"
        )
        volume = response.object["volume"]
        return self._to_volume(volume)

    def create_volume_snapshot(self, volume, name, region=None):
        """
        Create snapshot from volume.

        :param volume: The volume to create a snapshot from
        :type volume: :class`StorageVolume`

        :param name: The name to give the snapshot
        :type name: ``str``

        :param region: The region in which to create the snapshot
        (if None, use default region specified in __init__)
        :type region: :class:`.NodeLocation`

        :return: The newly created snapshot.
        :rtype: :class:`VolumeSnapshot`
        """
        data = {"name": name, "organization": self.key, "volume_id": volume.id}
        response = self.connection.request(
            "/snapshots", region=region, data=json.dumps(data), method="POST"
        )
        snapshot = response.object["snapshot"]
        return self._to_snapshot(snapshot)

    def destroy_volume(self, volume, region=None):
        """
        Destroys a storage volume.

        :param volume: Volume to be destroyed
        :type volume: :class:`StorageVolume`

        :param region: The region in which to look for the volume
        (if None, use default region specified in __init__)
        :type region: :class:`.NodeLocation`

        :return: True if the destroy was successful, otherwise False
        :rtype: ``bool``
        """
        return self.connection.request(
            "/volumes/%s" % volume.id, region=region, method="DELETE"
        ).success()

    def destroy_volume_snapshot(self, snapshot, region=None):
        """
        Dostroy a volume snapshot

        :param snapshot: volume snapshot to destroy
        :type snapshot: class:`VolumeSnapshot`

        :param region: The region in which to look for the snapshot
        (if None, use default region specified in __init__)
        :type region: :class:`.NodeLocation`

        :return: True if the destroy was successful, otherwise False
        :rtype: ``bool``
        """
        return self.connection.request(
            "/snapshots/%s" % snapshot.id, region=region, method="DELETE"
        ).success()

    def list_key_pairs(self):
        """
        List all the available SSH keys.

        :return: Available SSH keys.
        :rtype: ``list`` of :class:`KeyPair`
        """
        response = self.connection.request("/users/%s" % (self._get_user_id()), region="account")
        keys = response.object["user"]["ssh_public_keys"]
        return [
            KeyPair(
                name=" ".join(key["key"].split(" ")[2:]),
                public_key=" ".join(key["key"].split(" ")[:2]),
                fingerprint=key["fingerprint"],
                driver=self,
            )
            for key in keys
        ]

    def import_key_pair_from_string(self, name, key_material):
        """
        Import a new public key from string.

        :param name: Key pair name.
        :type name: ``str``

        :param key_material: Public key material.
        :type key_material: ``str``

        :return: Imported key pair object.
        :rtype: :class:`KeyPair`
        """
        new_key = KeyPair(
            name=name,
            public_key=" ".join(key_material.split(" ")[:2]),
            fingerprint=None,
            driver=self,
        )
        keys = [key for key in self.list_key_pairs() if not key.name == name]
        keys.append(new_key)
        return self._save_keys(keys)

    def delete_key_pair(self, key_pair):
        """
        Delete an existing key pair.

        :param key_pair: Key pair object.
        :type key_pair: :class:`KeyPair`

        :return:   True of False based on success of Keypair deletion
        :rtype:    ``bool``
        """
        keys = [key for key in self.list_key_pairs() if not key.name == key_pair.name]
        return self._save_keys(keys)

    def _get_user_id(self):
        response = self.connection.request("/tokens/%s" % self.secret, region="account")
        return response.object["token"]["user_id"]

    def _save_keys(self, keys):
        data = {
            "ssh_public_keys": [{"key": "{} {}".format(key.public_key, key.name)} for key in keys]
        }
        response = self.connection.request(
            "/users/%s" % (self._get_user_id()),
            region="account",
            method="PATCH",
            data=json.dumps(data),
        )
        return response.success()
