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
Ovh driver
"""
from libcloud.utils.py3 import httplib
from libcloud.common.ovh import API_ROOT, OvhConnection
from libcloud.compute.base import (
    Node,
    NodeSize,
    NodeImage,
    NodeDriver,
    NodeLocation,
    StorageVolume,
    VolumeSnapshot,
)
from libcloud.compute.types import Provider, StorageVolumeState, VolumeSnapshotState
from libcloud.compute.drivers.openstack import OpenStackKeyPair, OpenStackNodeDriver


class OvhNodeDriver(NodeDriver):
    """
    Libcloud driver for the Ovh API

    For more information on the Ovh API, read the official reference:

        https://api.ovh.com/console/
    """

    type = Provider.OVH
    name = "Ovh"
    website = "https://www.ovh.com/"
    connectionCls = OvhConnection
    features = {"create_node": ["ssh_key"]}
    api_name = "ovh"

    NODE_STATE_MAP = OpenStackNodeDriver.NODE_STATE_MAP
    VOLUME_STATE_MAP = OpenStackNodeDriver.VOLUME_STATE_MAP
    SNAPSHOT_STATE_MAP = OpenStackNodeDriver.SNAPSHOT_STATE_MAP

    def __init__(self, key, secret, ex_project_id, ex_consumer_key=None, region=None):
        """
        Instantiate the driver with the given API credentials.

        :param key: Your application key (required)
        :type key: ``str``

        :param secret: Your application secret (required)
        :type secret: ``str``

        :param ex_project_id: Your project ID
        :type ex_project_id: ``str``

        :param ex_consumer_key: Your consumer key (required)
        :type ex_consumer_key: ``str``

        :param region: The datacenter to connect to (optional)
        :type region: ``str``

        :rtype: ``None``
        """
        self.region = region
        self.project_id = ex_project_id
        self.consumer_key = ex_consumer_key
        NodeDriver.__init__(self, key, secret, ex_consumer_key=ex_consumer_key, region=region)

    def _get_project_action(self, suffix):
        base_url = "{}/cloud/project/{}/".format(API_ROOT, self.project_id)

        return base_url + suffix

    @classmethod
    def list_regions(cls):
        return ["eu", "ca"]

    def list_nodes(self, location=None):
        """
        List all nodes.

        :keyword location: Location (region) used as filter
        :type    location: :class:`NodeLocation`

        :return: List of node objects
        :rtype: ``list`` of :class:`Node`
        """
        action = self._get_project_action("instance")
        data = {}

        if location:
            data["region"] = location.id
        response = self.connection.request(action, data=data)

        return self._to_nodes(response.object)

    def ex_get_node(self, node_id):
        """
        Get a individual node.

        :keyword node_id: Node's ID
        :type    node_id: ``str``

        :return: Created node
        :rtype  : :class:`Node`
        """
        action = self._get_project_action("instance/%s" % node_id)
        response = self.connection.request(action, method="GET")

        return self._to_node(response.object)

    def create_node(self, name, image, size, location, ex_keyname=None):
        """
        Create a new node

        :keyword name: Name of created node
        :type    name: ``str``

        :keyword image: Image used for node
        :type    image: :class:`NodeImage`

        :keyword size: Size (flavor) used for node
        :type    size: :class:`NodeSize`

        :keyword location: Location (region) where to create node
        :type    location: :class:`NodeLocation`

        :keyword ex_keyname: Name of SSH key used
        :type    ex_keyname: ``str``

        :return: Created node
        :rtype : :class:`Node`
        """
        action = self._get_project_action("instance")
        data = {
            "name": name,
            "imageId": image.id,
            "flavorId": size.id,
            "region": location.id,
        }

        if ex_keyname:
            key_id = self.get_key_pair(ex_keyname, location).extra["id"]
            data["sshKeyId"] = key_id
        response = self.connection.request(action, data=data, method="POST")

        return self._to_node(response.object)

    def destroy_node(self, node):
        action = self._get_project_action("instance/%s" % node.id)
        self.connection.request(action, method="DELETE")

        return True

    def list_sizes(self, location=None):
        action = self._get_project_action("flavor")
        params = {}

        if location:
            params["region"] = location.id
        response = self.connection.request(action, params=params)

        return self._to_sizes(response.object)

    def ex_get_size(self, size_id):
        """
        Get an individual size (flavor).

        :keyword size_id: Size's ID
        :type    size_id: ``str``

        :return: Size
        :rtype: :class:`NodeSize`
        """
        action = self._get_project_action("flavor/%s" % size_id)
        response = self.connection.request(action)

        return self._to_size(response.object)

    def list_images(self, location=None, ex_size=None):
        """
        List available images

        :keyword location: Location (region) used as filter
        :type    location: :class:`NodeLocation`

        :keyword ex_size: Exclude images which are incompatible with given size
        :type    ex_size: :class:`NodeImage`

        :return: List of images
        :rtype  : ``list`` of :class:`NodeImage`
        """
        action = self._get_project_action("image")
        params = {}

        if location:
            params["region"] = location.id

        if ex_size:
            params["flavorId"] = ex_size.id
        response = self.connection.request(action, params=params)

        return self._to_images(response.object)

    def get_image(self, image_id):
        action = self._get_project_action("image/%s" % image_id)
        response = self.connection.request(action)

        return self._to_image(response.object)

    def list_locations(self):
        action = self._get_project_action("region")
        data = self.connection.request(action)

        return self._to_locations(data.object)

    def list_key_pairs(self, ex_location=None):
        """
        List available SSH public keys.

        :keyword ex_location: Location (region) used as filter
        :type    ex_location: :class:`NodeLocation`

        :return: Public keys
        :rtype: ``list``of :class:`KeyPair`
        """
        action = self._get_project_action("sshkey")
        params = {}

        if ex_location:
            params["region"] = ex_location.id
        response = self.connection.request(action, params=params)

        return self._to_key_pairs(response.object)

    def get_key_pair(self, name, ex_location=None):
        """
        Get an individual SSH public key by its name and location.

        :param name: Name of the key pair to retrieve.
        :type name: ``str``

        :keyword ex_location: Key's region
        :type ex_location: :class:`NodeLocation`

        :return: Public key
        :rtype: :class:`KeyPair`
        """
        # Keys are indexed with ID
        keys = [key for key in self.list_key_pairs(ex_location) if key.name == name]

        if not keys:
            raise Exception("No key named '%s'" % name)

        return keys[0]

    def import_key_pair_from_string(self, name, key_material, ex_location):
        """
        Import a new public key from string.

        :param name: Key pair name.
        :type name: ``str``

        :param key_material: Public key material.
        :type key_material: ``str``

        :param ex_location: Location where to store the key
        :type ex_location: :class:`NodeLocation`

        :return: Imported key pair object.
        :rtype: :class:`KeyPair`
        """
        action = self._get_project_action("sshkey")
        data = {"name": name, "publicKey": key_material, "region": ex_location.id}
        response = self.connection.request(action, data=data, method="POST")

        return self._to_key_pair(response.object)

    def delete_key_pair(self, key_pair):
        action = self._get_project_action("sshkey/%s" % key_pair.extra["id"])
        params = {"keyId": key_pair.extra["id"]}
        self.connection.request(action, params=params, method="DELETE")

        return True

    def create_volume(
        self,
        size,
        name,
        location,
        snapshot=None,
        ex_volume_type="classic",
        ex_description=None,
    ):
        """
        Create a volume.

        :param size: Size of volume to create (in GB).
        :type size: ``int``

        :param name: Name of volume to create
        :type name: ``str``

        :param location: Location to create the volume in
        :type location: :class:`NodeLocation` or ``None``

        :param snapshot:  Snapshot from which to create the new
                          volume.  (optional)
        :type snapshot: :class:`.VolumeSnapshot`

        :keyword ex_volume_type: ``'classic'`` or ``'high-speed'``
        :type ex_volume_type: ``str``

        :keyword ex_description: Optional description of volume
        :type ex_description: str

        :return:  Storage Volume object
        :rtype:   :class:`StorageVolume`
        """
        action = self._get_project_action("volume")
        data = {
            "name": name,
            "region": location.id,
            "size": size,
            "type": ex_volume_type,
        }

        if ex_description:
            data["description"] = ex_description
        response = self.connection.request(action, data=data, method="POST")

        return self._to_volume(response.object)

    def destroy_volume(self, volume):
        action = self._get_project_action("volume/%s" % volume.id)
        self.connection.request(action, method="DELETE")

        return True

    def list_volumes(self, ex_location=None):
        """
        Return a list of volumes.

        :keyword ex_location: Location used to filter
        :type ex_location: :class:`NodeLocation` or ``None``

        :return: A list of volume objects.
        :rtype: ``list`` of :class:`StorageVolume`
        """
        action = self._get_project_action("volume")
        data = {}

        if ex_location:
            data["region"] = ex_location.id
        response = self.connection.request(action, data=data)

        return self._to_volumes(response.object)

    def ex_get_volume(self, volume_id):
        """
        Return a Volume object based on a volume ID.

        :param  volume_id: The ID of the volume
        :type   volume_id: ``int``

        :return:  A StorageVolume object for the volume
        :rtype:   :class:`StorageVolume`
        """
        action = self._get_project_action("volume/%s" % volume_id)
        response = self.connection.request(action)

        return self._to_volume(response.object)

    def attach_volume(self, node, volume, device=None):
        """
        Attach a volume to a node.

        :param node: Node where to attach volume
        :type node: :class:`Node`

        :param volume: The ID of the volume
        :type volume: :class:`StorageVolume`

        :param device: Unused parameter

        :return: True or False representing operation successful
        :rtype:   ``bool``
        """
        action = self._get_project_action("volume/%s/attach" % volume.id)
        data = {"instanceId": node.id, "volumeId": volume.id}
        self.connection.request(action, data=data, method="POST")

        return True

    def detach_volume(self, volume, ex_node=None):
        """
        Detach a volume to a node.

        :param volume: The ID of the volume
        :type volume: :class:`StorageVolume`

        :param ex_node: Node to detach from (optional if volume is attached
                        to only one node)
        :type ex_node: :class:`Node`

        :return: True or False representing operation successful
        :rtype:   ``bool``

        :raises: Exception: If ``ex_node`` is not provided and more than one
                            node is attached to the volume
        """
        action = self._get_project_action("volume/%s/detach" % volume.id)

        if ex_node is None:
            if len(volume.extra["attachedTo"]) != 1:
                err_msg = (
                    "Volume '%s' has more or less than one attached" "nodes, you must specify one."
                )
                raise Exception(err_msg)
            ex_node = self.ex_get_node(volume.extra["attachedTo"][0])
        data = {"instanceId": ex_node.id}
        self.connection.request(action, data=data, method="POST")

        return True

    def ex_list_snapshots(self, location=None):
        """
        List all snapshots.

        :keyword location: Location used to filter
        :type location: :class:`NodeLocation` or ``None``

        :rtype: ``list`` of :class:`VolumeSnapshot`
        """
        action = self._get_project_action("volume/snapshot")
        params = {}

        if location:
            params["region"] = location.id
        response = self.connection.request(action, params=params)

        return self._to_snapshots(response.object)

    def ex_get_volume_snapshot(self, snapshot_id):
        """
        Returns a single volume snapshot.

        :param snapshot_id: Node to run the task on.
        :type snapshot_id: ``str``

        :rtype :class:`.VolumeSnapshot`:
        :return: Volume snapshot.
        """
        action = self._get_project_action("volume/snapshot/%s" % snapshot_id)
        response = self.connection.request(action)

        return self._to_snapshot(response.object)

    def list_volume_snapshots(self, volume):
        action = self._get_project_action("volume/snapshot")
        params = {"region": volume.extra["region"]}
        response = self.connection.request(action, params=params)
        snapshots = self._to_snapshots(response.object)

        return [snap for snap in snapshots if snap.extra["volume_id"] == volume.id]

    def create_volume_snapshot(self, volume, name=None, ex_description=None):
        """
        Create snapshot from volume

        :param volume: Instance of `StorageVolume`
        :type  volume: `StorageVolume`

        :param name: Name of snapshot (optional)
        :type  name: `str` | `NoneType`

        :param ex_description: Description of the snapshot (optional)
        :type  ex_description: `str` | `NoneType`

        :rtype: :class:`VolumeSnapshot`
        """
        action = self._get_project_action("volume/%s/snapshot/" % volume.id)
        data = {}

        if name:
            data["name"] = name

        if ex_description:
            data["description"] = ex_description
        response = self.connection.request(action, data=data, method="POST")

        return self._to_snapshot(response.object)

    def destroy_volume_snapshot(self, snapshot):
        action = self._get_project_action("volume/snapshot/%s" % snapshot.id)
        response = self.connection.request(action, method="DELETE")

        return response.status == httplib.OK

    def ex_get_pricing(self, size_id, subsidiary="US"):
        action = "%s/cloud/subsidiaryPrice" % (API_ROOT)
        params = {"flavorId": size_id, "ovhSubsidiary": subsidiary}
        pricing = self.connection.request(action, params=params).object["instances"][0]

        return {
            "hourly": pricing["price"]["value"],
            "monthly": pricing["monthlyPrice"]["value"],
        }

    def _to_volume(self, obj):
        extra = obj.copy()
        extra.pop("id")
        extra.pop("name")
        extra.pop("size")
        state = self.VOLUME_STATE_MAP.get(obj.pop("status", None), StorageVolumeState.UNKNOWN)

        return StorageVolume(
            id=obj["id"],
            name=obj["name"],
            size=obj["size"],
            state=state,
            extra=extra,
            driver=self,
        )

    def _to_volumes(self, objs):
        return [self._to_volume(obj) for obj in objs]

    def _to_location(self, obj):
        location = self.connectionCls.LOCATIONS[obj]

        return NodeLocation(driver=self, **location)

    def _to_locations(self, objs):
        return [self._to_location(obj) for obj in objs]

    def _to_node(self, obj):
        extra = obj.copy()

        if "ipAddresses" in extra:
            public_ips = [ip["ip"] for ip in extra["ipAddresses"]]
        else:
            public_ips = []
        del extra["id"]
        del extra["name"]

        return Node(
            id=obj["id"],
            name=obj["name"],
            state=self.NODE_STATE_MAP[obj["status"]],
            public_ips=public_ips,
            private_ips=[],
            driver=self,
            extra=extra,
        )

    def _to_nodes(self, objs):
        return [self._to_node(obj) for obj in objs]

    def _to_size(self, obj):
        extra = {"vcpus": obj["vcpus"], "type": obj["type"], "region": obj["region"]}

        return NodeSize(
            id=obj["id"],
            name=obj["name"],
            ram=obj["ram"],
            disk=obj["disk"],
            bandwidth=obj["outboundBandwidth"],
            price=None,
            driver=self,
            extra=extra,
        )

    def _to_sizes(self, objs):
        return [self._to_size(obj) for obj in objs]

    def _to_image(self, obj):
        extra = {"region": obj["region"], "visibility": obj["visibility"]}

        return NodeImage(id=obj["id"], name=obj["name"], driver=self, extra=extra)

    def _to_images(self, objs):
        return [self._to_image(obj) for obj in objs]

    def _to_key_pair(self, obj):
        extra = {"regions": obj["regions"], "id": obj["id"]}

        return OpenStackKeyPair(
            name=obj["name"],
            public_key=obj["publicKey"],
            driver=self,
            fingerprint=None,
            extra=extra,
        )

    def _to_key_pairs(self, objs):
        return [self._to_key_pair(obj) for obj in objs]

    def _to_snapshot(self, obj):
        extra = {
            "volume_id": obj["volumeId"],
            "region": obj["region"],
            "description": obj["description"],
            "status": obj["status"],
        }
        state = self.SNAPSHOT_STATE_MAP.get(obj["status"], VolumeSnapshotState.UNKNOWN)
        snapshot = VolumeSnapshot(
            id=obj["id"],
            driver=self,
            size=obj["size"],
            extra=extra,
            created=obj["creationDate"],
            state=state,
            name=obj["name"],
        )

        return snapshot

    def _to_snapshots(self, objs):
        return [self._to_snapshot(obj) for obj in objs]

    def _ex_connection_class_kwargs(self):
        return {"ex_consumer_key": self.consumer_key, "region": self.region}
