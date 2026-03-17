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
Driver for Microsoft Azure Resource Manager (ARM) Virtual Machines provider.

http://azure.microsoft.com/en-us/services/virtual-machines/
"""

import os
import time
import base64
import binascii

from libcloud.utils import iso8601
from libcloud.utils.py3 import parse_qs, urlparse, basestring
from libcloud.common.types import LibcloudError
from libcloud.compute.base import (
    Node,
    NodeSize,
    NodeImage,
    NodeDriver,
    NodeLocation,
    StorageVolume,
    NodeAuthSSHKey,
    VolumeSnapshot,
    NodeAuthPassword,
)
from libcloud.compute.types import NodeState, StorageVolumeState, VolumeSnapshotState
from libcloud.storage.types import ObjectDoesNotExistError
from libcloud.common.azure_arm import AzureResourceManagementConnection
from libcloud.common.exceptions import BaseHTTPError
from libcloud.compute.providers import Provider
from libcloud.storage.drivers.azure_blobs import AzureBlobsStorageDriver

RESOURCE_API_VERSION = "2016-04-30-preview"
DISK_API_VERSION = "2023-01-02"
IMAGES_API_VERSION = "2015-06-15"
INSTANCE_VIEW_API_VERSION = "2015-06-15"
IP_API_VERSION = "2019-06-01"
LOCATIONS_API_VERSION = "2015-01-01"
NIC_API_VERSION = "2018-06-01"
NSG_API_VERSION = "2016-09-01"
RATECARD_API_VERSION = "2016-08-31-preview"
RESOURCE_GROUP_API_VERSION = "2016-09-01"
SNAPSHOT_API_VERSION = "2016-04-30-preview"
STORAGE_ACCOUNT_API_VERSION = "2015-05-01-preview"
SUBNET_API_VERSION = "2015-06-15"
TAG_API_VERSION = "2018-06-01"
VIRTUAL_NETWORK_API_VERSION = "2018-06-01"
VM_API_VERSION = "2021-11-01"
VM_EXTENSION_API_VERSION = "2015-06-15"
VM_SIZE_API_VERSION = "2015-06-15"  # this API is deprecated

# If pagination code in the list_nodes() method has still not completed after this mount of
# seconds, we will break early from while True loop to avoid infinite loop under edge conditions.
# Keep in mind that we want this timeout relatively high since each `_to_node()` method call which
# is called for each node can result in additional HTTP requests (to retrieve power state, nics,
# etc).
LIST_NODES_PAGINATION_TIMEOUT = 300

# Pagination timeout for list_volumes() method.
LIST_VOLUMES_PAGINATION_TIMEOUT = 300


class AzureImage(NodeImage):
    """Represents a Marketplace node image that an Azure VM can boot from."""

    def __init__(self, version, sku, offer, publisher, location, driver):
        self.publisher = publisher
        self.offer = offer
        self.sku = sku
        self.version = version
        self.location = location
        urn = "{}:{}:{}:{}".format(self.publisher, self.offer, self.sku, self.version)
        name = "{} {} {} {}".format(self.publisher, self.offer, self.sku, self.version)
        super().__init__(urn, name, driver)

    def __repr__(self):
        return ("<AzureImage: id=%s, name=%s, location=%s>") % (
            self.id,
            self.name,
            self.location,
        )


class AzureVhdImage(NodeImage):
    """Represents a VHD node image that an Azure VM can boot from."""

    def __init__(self, storage_account, blob_container, name, driver):
        urn = "https://{}.blob{}/{}/{}".format(
            storage_account,
            driver.connection.storage_suffix,
            blob_container,
            name,
        )
        super().__init__(urn, name, driver)

    def __repr__(self):
        return ("<AzureVhdImage: id=%s, name=%s>") % (self.id, self.name)


class AzureComputeGalleryImage(NodeImage):
    """Represents a Compute Gallery image that an Azure VM can boot from."""

    def __init__(self, subscription_id, resource_group, gallery, name, driver):
        id = (
            "/subscriptions/%s/resourceGroups/%s/"
            "providers/Microsoft.Compute/galleries/%s/images/%s"
            % (
                subscription_id,
                resource_group,
                gallery,
                name,
            )
        )
        super().__init__(id, name, driver)

    def __repr__(self):
        return ("<AzureComputeGalleryImage: id=%s, name=%s>") % (self.id, self.name)


class AzureResourceGroup:
    """Represent an Azure resource group."""

    def __init__(self, id, name, location, extra):
        self.id = id
        self.name = name
        self.location = location
        self.extra = extra

    def __repr__(self):
        return ("<AzureResourceGroup: id=%s, name=%s, location=%s ...>") % (
            self.id,
            self.name,
            self.location,
        )


class AzureNetworkSecurityGroup:
    """Represent an Azure network security group."""

    def __init__(self, id, name, location, extra):
        self.id = id
        self.name = name
        self.location = location
        self.extra = extra

    def __repr__(self):
        return ("<AzureNetworkSecurityGroup: id=%s, name=%s, location=%s ...>") % (
            self.id,
            self.name,
            self.location,
        )


class AzureNetwork:
    """Represent an Azure virtual network."""

    def __init__(self, id, name, location, extra):
        self.id = id
        self.name = name
        self.location = location
        self.extra = extra

    def __repr__(self):
        return ("<AzureNetwork: id=%s, name=%s, location=%s ...>") % (
            self.id,
            self.name,
            self.location,
        )


class AzureSubnet:
    """Represents a subnet of an Azure virtual network."""

    def __init__(self, id, name, extra):
        self.id = id
        self.name = name
        self.extra = extra

    def __repr__(self):
        return ("<AzureSubnet: id=%s, name=%s ...>") % (self.id, self.name)


class AzureNic:
    """Represents an Azure virtual network interface controller (NIC)."""

    def __init__(self, id, name, location, extra):
        self.id = id
        self.name = name
        self.location = location
        self.extra = extra

    def __repr__(self):
        return ("<AzureNic: id=%s, name=%s ...>") % (self.id, self.name)


class AzureIPAddress:
    """Represents an Azure public IP address resource."""

    def __init__(self, id, name, extra):
        self.id = id
        self.name = name
        self.extra = extra

    def __repr__(self):
        return ("<AzureIPAddress: id=%s, name=%s ...>") % (self.id, self.name)


class AzureNodeDriver(NodeDriver):
    """Compute node driver for Azure Resource Manager."""

    connectionCls = AzureResourceManagementConnection
    name = "Azure Virtual machines"
    website = "http://azure.microsoft.com/en-us/services/virtual-machines/"
    type = Provider.AZURE_ARM
    features = {"create_node": ["ssh_key", "password"]}

    # The API doesn't provide state or country information, so fill it in.
    # Information from https://azure.microsoft.com/en-us/regions/
    _location_to_country = {
        "centralus": "Iowa, USA",
        "eastus": "Virginia, USA",
        "eastus2": "Virginia, USA",
        "usgoviowa": "Iowa, USA",
        "usgovvirginia": "Virginia, USA",
        "northcentralus": "Illinois, USA",
        "southcentralus": "Texas, USA",
        "westus": "California, USA",
        "northeurope": "Ireland",
        "westeurope": "Netherlands",
        "eastasia": "Hong Kong",
        "southeastasia": "Singapore",
        "japaneast": "Tokyo, Japan",
        "japanwest": "Osaka, Japan",
        "brazilsouth": "Sao Paulo State, Brazil",
        "australiaeast": "New South Wales, Australia",
        "australiasoutheast": "Victoria, Australia",
    }

    SNAPSHOT_STATE_MAP = {
        "creating": VolumeSnapshotState.CREATING,
        "updating": VolumeSnapshotState.UPDATING,
        "succeeded": VolumeSnapshotState.AVAILABLE,
        "failed": VolumeSnapshotState.ERROR,
    }

    def __init__(
        self,
        tenant_id,
        subscription_id,
        key,
        secret,
        secure=True,
        host=None,
        port=None,
        api_version=None,
        region=None,
        **kwargs,
    ):
        self.tenant_id = tenant_id
        self.subscription_id = subscription_id
        self.cloud_environment = kwargs.get("cloud_environment")
        super().__init__(
            key=key,
            secret=secret,
            secure=secure,
            host=host,
            port=port,
            api_version=api_version,
            region=region,
            **kwargs,
        )
        if self.region is not None:
            loc_id = self.region.lower().replace(" ", "")
            country = self._location_to_country.get(loc_id)
            self.default_location = NodeLocation(loc_id, self.region, country, self)
        else:
            self.default_location = None

    def list_locations(self):
        """
        List data centers available with the current subscription.

        :return: list of node location objects
        :rtype: ``list`` of :class:`.NodeLocation`
        """

        action = "/subscriptions/%s/providers/Microsoft.Compute" % (self.subscription_id)
        r = self.connection.request(action, params={"api-version": LOCATIONS_API_VERSION})

        for rt in r.object["resourceTypes"]:
            if rt["resourceType"] == "virtualMachines":
                return [self._to_location(location) for location in rt["locations"]]

        return []

    def list_sizes(self, location=None):
        """
        List available VM sizes.

        :param location: The location at which to list sizes
        (if None, use default location specified as 'region' in __init__)
        :type location: :class:`.NodeLocation`

        :return: list of node size objects
        :rtype: ``list`` of :class:`.NodeSize`
        """

        if location is None:
            if self.default_location:
                location = self.default_location
            else:
                raise ValueError("location is required.")
        action = "/subscriptions/%s/providers/Microsoft" ".Compute/locations/%s/vmSizes" % (
            self.subscription_id,
            location.id,
        )
        r = self.connection.request(action, params={"api-version": VM_SIZE_API_VERSION})
        return [self._to_node_size(d) for d in r.object["value"]]

    def list_images(
        self,
        location=None,
        ex_publisher=None,
        ex_offer=None,
        ex_sku=None,
        ex_version=None,
    ):
        """
        List available VM images to boot from.

        :param location: The location at which to list images
        (if None, use default location specified as 'region' in __init__)
        :type location: :class:`.NodeLocation`

        :param ex_publisher: Filter by publisher, or None to list
        all publishers.
        :type ex_publisher: ``str``

        :param ex_offer: Filter by offer, or None to list all offers.
        :type ex_offer: ``str``

        :param ex_sku: Filter by sku, or None to list all skus.
        :type ex_sku: ``str``

        :param ex_version: Filter by version, or None to list all versions.
        :type ex_version: ``str``

        :return: list of node image objects.
        :rtype: ``list`` of :class:`.AzureImage`
        """

        images = []

        if location is None:
            locations = [self.default_location]
        else:
            locations = [location]

        for loc in locations:
            if not ex_publisher:
                publishers = self.ex_list_publishers(loc)
            else:
                publishers = [
                    (
                        "/subscriptions/%s/providers/Microsoft"
                        ".Compute/locations/%s/publishers/%s"
                        % (self.subscription_id, loc.id, ex_publisher),
                        ex_publisher,
                    )
                ]

            for pub in publishers:
                if not ex_offer:
                    offers = self.ex_list_offers(pub[0])
                else:
                    offers = [
                        (
                            "{}/artifacttypes/vmimage/offers/{}".format(pub[0], ex_offer),
                            ex_offer,
                        )
                    ]

                for off in offers:
                    if not ex_sku:
                        skus = self.ex_list_skus(off[0])
                    else:
                        skus = [("{}/skus/{}".format(off[0], ex_sku), ex_sku)]

                    for sku in skus:
                        if not ex_version:
                            versions = self.ex_list_image_versions(sku[0])
                        else:
                            versions = [("{}/versions/{}".format(sku[0], ex_version), ex_version)]

                        for v in versions:
                            images.append(
                                AzureImage(
                                    v[1],
                                    sku[1],
                                    off[1],
                                    pub[1],
                                    loc.id,
                                    self.connection.driver,
                                )
                            )
        return images

    def get_image(self, image_id, location=None):
        """Returns a single node image from a provider.

        :param image_id: Either an image urn in the form
        `Publisher:Offer:Sku:Version` or a Azure blob store URI in the form
        `http://storageaccount.blob.core.windows.net/container/image.vhd`
        pointing to a VHD file.
        :type image_id: ``str``

        :param location: The location at which to search for the image
        (if None, use default location specified as 'region' in __init__)
        :type location: :class:`.NodeLocation`

        :rtype :class:`.AzureImage`: or :class:`.AzureVhdImage`:
        :return: AzureImage or AzureVhdImage instance on success.

        """

        if image_id.startswith("http"):
            (storageAccount, blobContainer, blob) = _split_blob_uri(image_id)
            return AzureVhdImage(storageAccount, blobContainer, blob, self)
        else:
            (ex_publisher, ex_offer, ex_sku, ex_version) = image_id.split(":")
            i = self.list_images(location, ex_publisher, ex_offer, ex_sku, ex_version)
            return i[0] if i else None

    def list_nodes(self, ex_resource_group=None, ex_fetch_nic=True, ex_fetch_power_state=True):
        """
        List all nodes.

        :param ex_resource_group: The resource group to list all nodes from.
        :type ex_resource_group: ``str``

        :param ex_fetch_nic: Fetch NIC resources in order to get
        IP address information for nodes.  If True, requires an extra API
        call for each NIC of each node.  If False, IP addresses will not
        be returned.
        :type ex_fetch_nic: ``bool``

        :param ex_fetch_power_state: Fetch node power state.  If True, requires
        an extra API call for each node.  If False, node state
        will be returned based on provisioning state only.
        :type ex_fetch_power_state: ``bool``

        :return:  list of node objects
        :rtype: ``list`` of :class:`.Node`

        NOTE: With the default arguments, the function may result in M * (1 + (N * 3)) HTTP
        requests where M is number of API pages and N is number of nodes returned per page.
        """
        if ex_resource_group:
            action = (
                "/subscriptions/%s/resourceGroups/%s/"
                "providers/Microsoft.Compute/virtualMachines"
                % (self.subscription_id, ex_resource_group)
            )
        else:
            action = "/subscriptions/%s/providers/Microsoft.Compute/" "virtualMachines" % (
                self.subscription_id
            )
        params = {"api-version": VM_API_VERSION}

        now_ts = int(time.time())
        deadline_ts = now_ts + LIST_NODES_PAGINATION_TIMEOUT

        nodes = []
        while time.time() < deadline_ts:
            r = self.connection.request(action, params=params)
            nodes.extend(
                self._to_node(n, fetch_nic=ex_fetch_nic, fetch_power_state=ex_fetch_power_state)
                for n in r.object["value"]
            )
            if not r.object.get("nextLink"):
                # No next page
                break
            parsed_next_link = urlparse.urlparse(r.object["nextLink"])
            params.update({k: v[0] for k, v in parse_qs(parsed_next_link.query).items()})
            action = parsed_next_link.path
        return nodes

    def create_node(
        self,
        name,
        size,
        image,
        auth,
        ex_resource_group,
        ex_storage_account=None,
        ex_blob_container="vhds",
        location=None,
        ex_user_name="azureuser",
        ex_network=None,
        ex_subnet=None,
        ex_nic=None,
        ex_tags={},
        ex_customdata="",
        ex_use_managed_disks=False,
        ex_disk_size=None,
        ex_storage_account_type="Standard_LRS",
        ex_os_disk_delete=False,
    ):
        """Create a new node instance. This instance will be started
        automatically.

        This driver supports the ``ssh_key`` feature flag for ``created_node``
        so you can upload a public key into the new instance::

            >>> from libcloud.compute.drivers.azure_arm import AzureNodeDriver
            >>> driver = AzureNodeDriver(...)
            >>> auth = NodeAuthSSHKey('pubkey data here')
            >>> node = driver.create_node("test_node", auth=auth)

        This driver also supports the ``password`` feature flag for
        ``create_node``
        so you can set a password::

            >>> driver = AzureNodeDriver(...)
            >>> auth = NodeAuthPassword('mysecretpassword')
            >>> node = driver.create_node("test_node", auth=auth, ...)

        If you don't provide the ``auth`` argument libcloud will assign
        a password:

            >>> driver = AzureNodeDriver(...)
            >>> node = driver.create_node("test_node", ...)
            >>> password = node.extra["properties"] \
                           ["osProfile"]["adminPassword"]

        :param name:   String with a name for this new node (required)
        :type name:   ``str``

        :param size:   The size of resources allocated to this node.
                            (required)
        :type size:   :class:`.NodeSize`

        :param image:  OS Image to boot on node (required)
        :type image:  :class:`.AzureImage` or :class:`.AzureVhdImage` or :class:`.AzureComputeGalleryImage`

        :param location: Which data center to create a node in.
        (if None, use default location specified as 'region' in __init__)
        :type location: :class:`.NodeLocation`

        :param auth:   Initial authentication information for the node
                            (optional)
        :type auth:   :class:`.NodeAuthSSHKey` or :class:`NodeAuthPassword`

        :param ex_resource_group:  The resource group in which to create the
        node
        :type ex_resource_group: ``str``

        :param ex_storage_account:  The storage account id in which to store
        the node's disk image.
        Note: when booting from a user image (AzureVhdImage)
        the source image and the node image must use the same storage account.
        :type ex_storage_account: ``str``

        :param ex_blob_container:  The name of the blob container on the
        storage account in which to store the node's disk image (optional,
        default "vhds")
        :type ex_blob_container: ``str``

        :param ex_user_name:  User name for the initial admin user
        (optional, default "azureuser")
        :type ex_user_name: ``str``

        :param ex_network: The virtual network the node will be attached to.
        Must provide either `ex_network` (to create a default NIC for the
        node on the given network) or `ex_nic` (to supply the NIC explicitly).
        :type ex_network: ``str``

        :param ex_subnet: If ex_network is provided, the subnet of the
        virtual network the node will be attached to.  Optional, default
        is the "default" subnet.
        :type ex_subnet: ``str``

        :param ex_nic: A virtual NIC to attach to this Node, from
        `ex_create_network_interface` or `ex_get_nic`.
        Must provide either `ex_nic` (to supply the NIC explicitly) or
        ex_network (to create a default NIC for the node on the
        given network).
        :type ex_nic: :class:`AzureNic`

        :param ex_tags: Optional tags to associate with this node.
        :type ex_tags: ``dict``

        :param ex_customdata: Custom data that will
            be placed in the file /var/lib/waagent/CustomData
            https://azure.microsoft.com/en-us/documentation/ \
            articles/virtual-machines-how-to-inject-custom-data/
        :type ex_customdata: ``str``

        :param ex_use_managed_disks: Enable this feature to have Azure
            automatically manage the availability of disks to provide data
            redundancy and fault tolerance, without creating and managing
            storage accounts on your own. Managed disks may not be available
            in all regions (default False).
        :type ex_use_managed_disks: ``bool``

        :param ex_disk_size: Custom OS disk size in GB
        :type ex_disk_size: ``int``

        :param ex_storage_account_type: The Storage Account type,
            ``Standard_LRS``(HDD disks) or ``Premium_LRS``(SSD disks).
        :type ex_storage_account_type: str

        :param ex_os_disk_delete: Enable this feature to have Azure
            automatically delete the OS disk when you delete the VM.
        :type ex_os_disk_delete: ``bool``

        :return: The newly created node.
        :rtype: :class:`.Node`
        """
        if not ex_use_managed_disks and ex_storage_account is None:
            raise ValueError("ex_use_managed_disks is False, " "must provide ex_storage_account")

        if location is None:
            location = self.default_location
        if ex_nic is None:
            if ex_network is None:
                raise ValueError("Must provide either ex_network or ex_nic")
            if ex_subnet is None:
                ex_subnet = "default"

            subnet_id = (
                "/subscriptions/%s/resourceGroups/%s/providers"
                "/Microsoft.Network/virtualnetworks/%s/subnets/%s"
                % (self.subscription_id, ex_resource_group, ex_network, ex_subnet)
            )
            subnet = AzureSubnet(subnet_id, ex_subnet, {})
            ex_nic = self.ex_create_network_interface(
                name + "-nic", subnet, ex_resource_group, location
            )

        auth = self._get_and_check_auth(auth)

        target = (
            "/subscriptions/%s/resourceGroups/%s/providers"
            "/Microsoft.Compute/virtualMachines/%s"
            % (self.subscription_id, ex_resource_group, name)
        )

        if isinstance(image, AzureVhdImage):
            instance_vhd = self._get_instance_vhd(
                name=name,
                ex_resource_group=ex_resource_group,
                ex_storage_account=ex_storage_account,
                ex_blob_container=ex_blob_container,
            )
            storage_profile = {
                "osDisk": {
                    "name": name,
                    "osType": "linux",
                    "caching": "ReadWrite",
                    "createOption": "FromImage",
                    "image": {"uri": image.id},
                    "vhd": {"uri": instance_vhd},
                }
            }
            if ex_os_disk_delete:
                storage_profile["osDisk"]["deleteOption"] = "Delete"
            if ex_use_managed_disks:
                raise LibcloudError(
                    "Creating managed OS disk from %s image " "type is not supported." % type(image)
                )
        elif isinstance(image, AzureImage) or isinstance(image, AzureComputeGalleryImage):
            if isinstance(image, AzureImage):
                imageReference = {
                    "publisher": image.publisher,
                    "offer": image.offer,
                    "sku": image.sku,
                    "version": image.version,
                }
            else:
                imageReference = {"id": image.id}
            storage_profile = {
                "imageReference": imageReference,
                "osDisk": {
                    "name": name,
                    "osType": "linux",
                    "caching": "ReadWrite",
                    "createOption": "FromImage",
                },
            }
            if ex_os_disk_delete:
                storage_profile["osDisk"]["deleteOption"] = "Delete"
            if ex_use_managed_disks:
                storage_profile["osDisk"]["managedDisk"] = {
                    "storageAccountType": ex_storage_account_type
                }
            else:
                instance_vhd = self._get_instance_vhd(
                    name=name,
                    ex_resource_group=ex_resource_group,
                    ex_storage_account=ex_storage_account,
                    ex_blob_container=ex_blob_container,
                )
                storage_profile["osDisk"]["vhd"] = {"uri": instance_vhd}
        else:
            raise LibcloudError(
                "Unknown image type %s, expected one of AzureImage, "
                "AzureVhdImage, AzureComputeGalleryImage." % type(image)
            )

        data = {
            "id": target,
            "name": name,
            "type": "Microsoft.Compute/virtualMachines",
            "location": location.id,
            "tags": ex_tags,
            "properties": {
                "hardwareProfile": {"vmSize": size.id},
                "storageProfile": storage_profile,
                "osProfile": {"computerName": name},
                "networkProfile": {"networkInterfaces": [{"id": ex_nic.id}]},
            },
        }

        if ex_disk_size:
            data["properties"]["storageProfile"]["osDisk"].update({"diskSizeGB": ex_disk_size})

        if ex_customdata:
            data["properties"]["osProfile"]["customData"] = base64.b64encode(ex_customdata)

        data["properties"]["osProfile"]["adminUsername"] = ex_user_name

        if isinstance(auth, NodeAuthSSHKey):
            data["properties"]["osProfile"]["adminPassword"] = binascii.hexlify(
                os.urandom(20)
            ).decode("utf-8")
            data["properties"]["osProfile"]["linuxConfiguration"] = {
                "disablePasswordAuthentication": "true",
                "ssh": {
                    "publicKeys": [
                        {
                            "path": "/home/%s/.ssh/authorized_keys" % (ex_user_name),
                            "keyData": auth.pubkey,  # pylint: disable=no-member
                        }
                    ]
                },
            }
        elif isinstance(auth, NodeAuthPassword):
            data["properties"]["osProfile"]["linuxConfiguration"] = {
                "disablePasswordAuthentication": "false"
            }
            data["properties"]["osProfile"]["adminPassword"] = auth.password
        else:
            raise ValueError("Must provide NodeAuthSSHKey or NodeAuthPassword in auth")

        r = self.connection.request(
            target,
            params={"api-version": VM_API_VERSION},
            data=data,
            method="PUT",
        )

        node = self._to_node(r.object)
        node.size = size
        node.image = image
        return node

    def reboot_node(self, node):
        """
        Reboot a node.

        :param node: The node to be rebooted
        :type node: :class:`.Node`

        :return: True if the reboot was successful, otherwise False
        :rtype: ``bool``
        """

        target = "%s/restart" % node.id
        try:
            self.connection.request(target, params={"api-version": VM_API_VERSION}, method="POST")
            return True
        except BaseHTTPError as h:
            if h.code == 202:
                return True
            else:
                return False

    def destroy_node(
        self,
        node,
        ex_destroy_nic=True,
        ex_destroy_vhd=True,
        ex_poll_qty=10,
        ex_poll_wait=10,
        ex_destroy_os_disk=True,
    ):
        """
        Destroy a node.

        :param node: The node to be destroyed
        :type node: :class:`.Node`

        :param ex_destroy_nic: Destroy the NICs associated with
        this node (default True).
        :type node: ``bool``

        :param ex_destroy_vhd: Destroy the OS disk blob associated with
        this node (default True).
        :type node: ``bool``

        :param ex_poll_qty: Number of retries checking if the node
        is gone, destroying the NIC or destroying the VHD (default 10).
        :type node: ``int``

        :param ex_poll_wait: Delay in seconds between retries (default 10).
        :type node: ``int``

        :param ex_destroy_os_disk: Destroy the OS disk associated with
        this node either if it is a managed disk (default True).
        :type node: ``bool``

        :return: True if the destroy was successful, raises exception
        otherwise.
        :rtype: ``bool``
        """

        do_node_polling = ex_destroy_nic or ex_destroy_vhd or ex_destroy_os_disk

        # This returns a 202 (Accepted) which means that the delete happens
        # asynchronously.
        # If returns 404, we may be retrying a previous destroy_node call that
        # failed to clean up its related resources, so it isn't taken as a
        # failure.
        try:
            self.connection.request(
                node.id, params={"api-version": VM_API_VERSION}, method="DELETE"
            )
        except BaseHTTPError as h:
            if h.code == 202:
                pass
            elif h.code == 204:
                # Returns 204 if node already deleted.
                do_node_polling = False
            else:
                raise

        # Poll until the node actually goes away (otherwise attempt to delete
        # NIC, VHD and OS Disk will fail with "resource in use" errors).
        retries = ex_poll_qty
        while do_node_polling and retries > 0:
            try:
                time.sleep(ex_poll_wait)
                self.connection.request(node.id, params={"api-version": VM_API_VERSION})
                retries -= 1
            except BaseHTTPError as h:
                if h.code in (204, 404):
                    # Node is gone
                    break
                else:
                    raise

        # Optionally clean up the network
        # interfaces that were attached to this node.
        interfaces = node.extra["properties"]["networkProfile"]["networkInterfaces"]
        if ex_destroy_nic:
            for nic in interfaces:
                retries = ex_poll_qty
                while retries > 0:
                    try:
                        self.ex_destroy_nic(self._to_nic(nic))
                        break
                    except BaseHTTPError as h:
                        retries -= 1
                        if h.code == 400 and h.message.startswith("[NicInUse]") and retries > 0:
                            time.sleep(ex_poll_wait)
                        else:
                            raise

        # Optionally clean up OS disk VHD.
        vhd = node.extra["properties"]["storageProfile"]["osDisk"].get("vhd")
        if ex_destroy_vhd and vhd is not None:
            retries = ex_poll_qty
            resourceGroup = node.id.split("/")[4]
            while retries > 0:
                try:
                    if self._ex_delete_old_vhd(resourceGroup, vhd["uri"]):
                        break
                    # Unfortunately lease errors usually result in it returning
                    # "False" with no more information.  Need to wait and try
                    # again.
                except LibcloudError as e:
                    retries -= 1
                    if "LeaseIdMissing" in str(e) and retries > 0:
                        # Unfortunately lease errors
                        # (which occur if the vhd blob
                        # hasn't yet been released by the VM being destroyed)
                        # get raised as plain
                        # LibcloudError.  Wait a bit and try again.
                        time.sleep(ex_poll_wait)
                    else:
                        raise
                time.sleep(10)

        # Optionally destroy the OS managed disk
        managed_disk = (
            node.extra.get("properties", {})
            .get("storageProfile", {})
            .get("osDisk", {})
            .get("managedDisk")
        )
        if ex_destroy_os_disk and managed_disk is not None:
            managed_disk_id = managed_disk["id"]
            try:
                self.connection.request(
                    managed_disk_id, params={"api-version": DISK_API_VERSION}, method="DELETE"
                )
            except BaseHTTPError as h:
                if h.code not in (202, 204):
                    # 202 or 204 mean destroy is accepted (but deferred) or already destroyed
                    raise

        return True

    def create_volume(
        self,
        size,
        name,
        location=None,
        snapshot=None,
        ex_resource_group=None,
        ex_sku_name=None,
        ex_tags=None,
        ex_zones=None,
        ex_iops=None,
        ex_throughput=None,
    ):
        """
        Create a new managed volume.

        :param size: Size of volume in gigabytes.
        :type size: ``int``

        :param name: Name of the volume to be created.
        :type name: ``str``

        :param location: Which data center to create a volume in. (required)
        :type location: :class:`NodeLocation`

        :param snapshot: Snapshot from which to create the new volume.
        :type snapshot: :class:`VolumeSnapshot`

        :param ex_resource_group: The name of resource group in which to
            create the volume. (required)
        :type ex_resource_group: ``str``

        :param ex_sku_name: The Disk SKU name. Refer to the API reference for
            options.
        :type ex_sku_name: ``str``

        :param ex_tags: Optional tags to associate with this resource.
        :type ex_tags: ``dict``

        :param ex_zones: The list of availability zones to create the volume
            in. Options are any or all of ["1", "2", "3"]. (optional)
        :type ex_zones: ``list`` of ``str``

        :param ex_iops: The max IOPS this volume is capable of.
        :type ex_iops: ``int``

        :param ex_throughput: The max throughput of this volume in MBps.
        :type ex_throughput: ``int``

        :return: The newly created volume.
        :rtype: :class:`StorageVolume`
        """
        if location is None:
            raise ValueError("Must provide `location` value.")

        if ex_resource_group is None:
            raise ValueError("Must provide `ex_resource_group` value.")

        action = (
            "/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
            "/providers/Microsoft.Compute/disks/{volume_name}"
        ).format(
            subscription_id=self.subscription_id,
            resource_group=ex_resource_group,
            volume_name=name,
        )
        tags = ex_tags if ex_tags is not None else {}

        creation_data = (
            {"createOption": "Empty"}
            if snapshot is None
            else {"createOption": "Copy", "sourceUri": snapshot.id}
        )
        data = {
            "location": location.id,
            "tags": tags,
            "properties": {"creationData": creation_data, "diskSizeGB": size},
        }
        if ex_sku_name is not None:
            data["sku"] = {"name": ex_sku_name}

        if ex_zones is not None:
            data["zones"] = ex_zones

        if ex_iops is not None:
            data["properties"]["diskIopsReadWrite"] = ex_iops

        if ex_throughput is not None:
            data["properties"]["diskMBpsReadWrite"] = ex_throughput

        response = self.connection.request(
            action,
            method="PUT",
            params={"api-version": DISK_API_VERSION},
            data=data,
        )

        return self._to_volume(response.object, name=name, ex_resource_group=ex_resource_group)

    def list_volumes(self, ex_resource_group=None):
        """
        Lists all the disks under a resource group or subscription.

        :param ex_resource_group: The identifier of your subscription
            where the managed disks are located.
        :type ex_resource_group: ``str``

        :rtype: list of :class:`StorageVolume`
        """
        if ex_resource_group:
            action = (
                "/subscriptions/{subscription_id}/resourceGroups"
                "/{resource_group}/providers/Microsoft.Compute/disks"
            )
        else:
            action = "/subscriptions/{subscription_id}" "/providers/Microsoft.Compute/disks"

        action = action.format(
            subscription_id=self.subscription_id, resource_group=ex_resource_group
        )
        params = {"api-version": DISK_API_VERSION}

        now_ts = int(time.time())
        deadline_ts = now_ts + LIST_VOLUMES_PAGINATION_TIMEOUT

        volumes = []
        while time.time() < deadline_ts:
            response = self.connection.request(action, method="GET", params=params)
            volumes.extend(self._to_volume(volume) for volume in response.object["value"])
            if not response.object.get("nextLink"):
                # No next page
                break
            parsed_next_link = urlparse.urlparse(response.object["nextLink"])
            params.update({k: v[0] for k, v in parse_qs(parsed_next_link.query).items()})
            action = parsed_next_link.path
        return volumes

    def attach_volume(
        self,
        node,
        volume,
        ex_lun=None,
        ex_vhd_uri=None,
        ex_vhd_create=False,
        **ex_kwargs,
    ):
        """
        Attach a volume to node.

        :param node: A node to attach volume.
        :type node: :class:`Node`

        :param volume: A volume to attach.
        :type volume: :class:`StorageVolume`

        :param ex_lun: Specifies the logical unit number (LUN) location for
            the data drive in the virtual machine. Each data disk must have
            a unique LUN.
        :type ex_lun: ``int``

        :param ex_vhd_uri: Attach old-style unmanaged disk from VHD
            blob. (optional)
        :type ex_vhd_uri: ``str``

        :param ex_vhd_create: Create a new VHD blob for unmanaged disk.
            (optional)
        :type ex_vhd_create: ``bool``

        :rtype: ``bool``
        """
        action = node.extra["id"]
        location = node.extra["location"]
        disks = node.extra["properties"]["storageProfile"]["dataDisks"]

        if ex_lun is None:
            # find the smallest unused logical unit number
            used_luns = [disk["lun"] for disk in disks]
            free_luns = [lun for lun in range(0, 64) if lun not in used_luns]
            if len(free_luns) > 0:
                ex_lun = free_luns[0]
            else:
                raise LibcloudError("No LUN available to attach new disk.")

        if ex_vhd_uri is not None:
            new_disk = {
                "name": volume.name,
                "diskSizeGB": volume.size,
                "lun": ex_lun,
                "createOption": "empty" if ex_vhd_create else "attach",
                "vhd": {"uri": ex_vhd_uri},
            }
        else:
            # attach existing managed disk
            new_disk = {
                "lun": ex_lun,
                "createOption": "attach",
                "managedDisk": {"id": volume.id},
            }

        disks.append(new_disk)
        self.connection.request(
            action,
            method="PUT",
            params={"api-version": VM_API_VERSION},
            data={
                "properties": {"storageProfile": {"dataDisks": disks}},
                "location": location,
            },
        )
        return True

    def ex_resize_volume(self, volume, new_size, resource_group):
        """
        Resize a volume.

        :param volume: A volume to resize.
        :type volume: :class:`StorageVolume`

        :param new_size: The new size to resize the volume to in Gib.
        :type new_size: ``int``

        :param resource_group: The name of the resource group in which to
            create the volume.
        :type resource_group: ``str``

        """
        action = (
            "/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
            "/providers/Microsoft.Compute/disks/{volume_name}"
        ).format(
            subscription_id=self.subscription_id,
            resource_group=resource_group,
            volume_name=volume.name,
        )

        data = {
            "location": volume.extra["location"],
            "properties": {
                "diskSizeGB": new_size,
                "creationData": volume.extra["properties"]["creationData"],
            },
        }

        response = self.connection.request(
            action,
            method="PUT",
            params={"api-version": DISK_API_VERSION},
            data=data,
        )

        return self._to_volume(response.object, name=volume.name, ex_resource_group=resource_group)

    def detach_volume(self, volume, ex_node=None):
        """
        Detach a managed volume from a node.
        """
        if ex_node is None:
            raise ValueError("Must provide `ex_node` value.")

        action = ex_node.extra["id"]
        location = ex_node.extra["location"]
        disks = ex_node.extra["properties"]["storageProfile"]["dataDisks"]

        # remove volume from `properties.storageProfile.dataDisks`
        disks[:] = [
            disk
            for disk in disks
            if disk.get("name") != volume.name
            and disk.get("managedDisk", {}).get("id") != volume.id
        ]

        self.connection.request(
            action,
            method="PUT",
            params={"api-version": VM_API_VERSION},
            data={
                "properties": {"storageProfile": {"dataDisks": disks}},
                "location": location,
            },
        )
        return True

    def destroy_volume(self, volume):
        """
        Delete a volume.
        """
        self.ex_delete_resource(volume)
        return True

    def create_volume_snapshot(
        self, volume, name=None, location=None, ex_resource_group=None, ex_tags=None
    ):
        """
        Create snapshot from volume.

        :param volume: Instance of ``StorageVolume``.
        :type volume: :class`StorageVolume`

        :param name: Name of snapshot. (required)
        :type name: ``str``

        :param location: Which data center to create a volume in. (required)
        :type location: :class:`NodeLocation`

        :param ex_resource_group: The name of resource group in which to
            create the snapshot. (required)
        :type ex_resource_group: ``str``

        :param ex_tags: Optional tags to associate with this resource.
        :type ex_tags: ``dict``

        :rtype: :class:`VolumeSnapshot`
        """
        if name is None:
            raise ValueError("Must provide `name` value")
        if location is None:
            raise ValueError("Must provide `location` value")
        if ex_resource_group is None:
            raise ValueError("Must provide `ex_resource_group` value")

        snapshot_id = (
            "/subscriptions/{subscription_id}"
            "/resourceGroups/{resource_group}"
            "/providers/Microsoft.Compute"
            "/snapshots/{snapshot_name}"
        ).format(
            subscription_id=self.subscription_id,
            resource_group=ex_resource_group,
            snapshot_name=name,
        )
        tags = ex_tags if ex_tags is not None else {}

        data = {
            "location": location.id,
            "tags": tags,
            "properties": {
                "creationData": {"createOption": "Copy", "sourceUri": volume.id},
            },
        }
        response = self.connection.request(
            snapshot_id,
            method="PUT",
            data=data,
            params={"api-version": SNAPSHOT_API_VERSION},
        )

        return self._to_snapshot(response.object, name=name, ex_resource_group=ex_resource_group)

    def list_volume_snapshots(self, volume):
        return [
            snapshot
            for snapshot in self.list_snapshots()
            if snapshot.extra["source_id"] == volume.id
        ]

    def list_snapshots(self, ex_resource_group=None):
        """
        Lists all the snapshots under a resource group or subscription.

        :param ex_resource_group: The identifier of your subscription
            where the managed snapshots are located (optional).
        :type ex_resource_group: ``str``

        :rtype: list of :class:`VolumeSnapshot`
        """
        if ex_resource_group:
            action = (
                "/subscriptions/{subscription_id}/resourceGroups"
                "/{resource_group}/providers/Microsoft.Compute/snapshots"
            )
        else:
            action = "/subscriptions/{subscription_id}" "/providers/Microsoft.Compute/snapshots"

        action = action.format(
            subscription_id=self.subscription_id, resource_group=ex_resource_group
        )

        response = self.connection.request(
            action, method="GET", params={"api-version": SNAPSHOT_API_VERSION}
        )
        return [self._to_snapshot(snap) for snap in response.object["value"]]

    def destroy_volume_snapshot(self, snapshot):
        """
        Delete a snapshot.
        """
        self.ex_delete_resource(snapshot)
        return True

    def _to_volume(self, volume_obj, name=None, ex_resource_group=None):
        """
        Parse the JSON element and return a StorageVolume object.

        :param volume_obj: A volume object from an azure response.
        :type volume_obj: ``dict``

        :param name: An optional name for the volume.
        :type name: ``str``

        :param ex_resource_group: An optional resource group for the volume.
        :type ex_resource_group: ``str``

        :rtype: :class:`StorageVolume`
        """
        volume_id = volume_obj.get("id")
        volume_name = volume_obj.get("name")
        extra = dict(volume_obj)
        properties = extra["properties"]
        size = properties.get("diskSizeGB")
        if size is not None:
            size = int(size)

        provisioning_state = properties.get("provisioningState", "").lower()
        disk_state = properties.get("diskState", "").lower()

        if provisioning_state == "creating":
            state = StorageVolumeState.CREATING
        elif provisioning_state == "updating":
            state = StorageVolumeState.UPDATING
        elif provisioning_state == "succeeded":
            if disk_state in ("attached", "reserved", "activesas"):
                state = StorageVolumeState.INUSE
            elif disk_state == "unattached":
                state = StorageVolumeState.AVAILABLE
            else:
                state = StorageVolumeState.UNKNOWN
        else:
            state = StorageVolumeState.UNKNOWN

        if volume_id is None and ex_resource_group is not None and name is not None:
            volume_id = (
                "/subscriptions/{subscription_id}"
                "/resourceGroups/{resource_group}"
                "/providers/Microsoft.Compute/disks/{volume_name}"
            ).format(
                subscription_id=self.subscription_id,
                resource_group=ex_resource_group,
                volume_name=name,
            )
        if volume_name is None and name is not None:
            volume_name = name

        return StorageVolume(
            id=volume_id,
            name=volume_name,
            size=size,
            driver=self,
            state=state,
            extra=extra,
        )

    def _to_snapshot(self, snapshot_obj, name=None, ex_resource_group=None):
        """
        Parse the JSON element and return a VolumeSnapshot object.

        :param snapshot_obj: A snapshot object from an azure response.
        :type snapshot_obj: ``dict``

        :param name: An optional name for the volume.
        :type name: ``str``

        :param ex_resource_group: An optional resource group for the volume.
        :type ex_resource_group: ``str``

        :rtype: :class:`VolumeSnapshot`
        """
        snapshot_id = snapshot_obj.get("id")
        name = snapshot_obj.get("name", name)
        properties = snapshot_obj["properties"]
        size = properties.get("diskSizeGB")
        if size is not None:
            size = int(size)
        extra = dict(snapshot_obj)
        extra["source_id"] = properties["creationData"]["sourceUri"]
        if "/providers/Microsoft.Compute/disks/" in extra["source_id"]:
            extra["volume_id"] = extra["source_id"]
        state = self.SNAPSHOT_STATE_MAP.get(
            properties.get("provisioningState", "").lower(), VolumeSnapshotState.UNKNOWN
        )
        try:
            created_at = iso8601.parse_date(properties.get("timeCreated"))
        except (TypeError, ValueError, iso8601.ParseError):
            created_at = None

        if snapshot_id is None and ex_resource_group is not None and name is not None:
            snapshot_id = (
                "/subscriptions/{subscription_id}"
                "/resourceGroups/{resource_group}"
                "/providers/Microsoft.Compute/snapshots/{snapshot_name}"
            ).format(
                subscription_id=self.subscription_id,
                resource_group=ex_resource_group,
                snapshot_name=name,
            )

        return VolumeSnapshot(
            snapshot_id,
            name=name,
            size=size,
            driver=self,
            state=state,
            extra=extra,
            created=created_at,
        )

    def ex_delete_resource(self, resource):
        """
        Delete a resource.
        """
        if not isinstance(resource, basestring):
            resource = resource.id
        r = self.connection.request(
            resource,
            method="DELETE",
            params={"api-version": RESOURCE_API_VERSION},
        )
        return r.status in [200, 202, 204]

    def ex_get_ratecard(self, offer_durable_id, currency="USD", locale="en-US", region="US"):
        """
        Get rate card

        :param offer_durable_id: ID of the offer applicable for this
        user account. (e.g. "0026P")
        See http://azure.microsoft.com/en-us/support/legal/offer-details/
        :type offer_durable_id: str

        :param currency: Desired currency for the response (default: "USD")
        :type currency: ``str``

        :param locale: Locale (default: "en-US")
        :type locale: ``str``

        :param region: Region (two-letter code) (default: "US")
        :type region: ``str``

        :return: A dictionary of rates whose ID's correspond to nothing at all
        :rtype: ``dict``
        """

        action = "/subscriptions/%s/providers/Microsoft.Commerce/" "RateCard" % (
            self.subscription_id,
        )
        params = {
            "api-version": RATECARD_API_VERSION,
            "$filter": "OfferDurableId eq 'MS-AZR-%s' and "
            "Currency eq '%s' and "
            "Locale eq '%s' and "
            "RegionInfo eq '%s'" % (offer_durable_id, currency, locale, region),
        }
        r = self.connection.request(action, params=params)
        return r.object

    def ex_list_publishers(self, location=None):
        """
        List node image publishers.

        :param location: The location at which to list publishers
        (if None, use default location specified as 'region' in __init__)
        :type location: :class:`.NodeLocation`

        :return: A list of tuples in the form
        ("publisher id", "publisher name")
        :rtype: ``list``
        """

        if location is None:
            if self.default_location:
                location = self.default_location
            else:
                raise ValueError("location is required.")

        action = "/subscriptions/%s/providers/Microsoft.Compute/" "locations/%s/publishers" % (
            self.subscription_id,
            location.id,
        )
        r = self.connection.request(action, params={"api-version": IMAGES_API_VERSION})
        return [(p["id"], p["name"]) for p in r.object]

    def ex_list_offers(self, publisher):
        """
        List node image offers from a publisher.

        :param publisher: The complete resource path to a publisher
        (as returned by `ex_list_publishers`)
        :type publisher: ``str``

        :return: A list of tuples in the form
        ("offer id", "offer name")
        :rtype: ``list``
        """

        action = "%s/artifacttypes/vmimage/offers" % (publisher)
        r = self.connection.request(action, params={"api-version": IMAGES_API_VERSION})
        return [(p["id"], p["name"]) for p in r.object]

    def ex_list_skus(self, offer):
        """
        List node image skus in an offer.

        :param offer: The complete resource path to an offer (as returned by
        `ex_list_offers`)
        :type offer: ``str``

        :return: A list of tuples in the form
        ("sku id", "sku name")
        :rtype: ``list``
        """

        action = "%s/skus" % offer
        r = self.connection.request(action, params={"api-version": IMAGES_API_VERSION})
        return [(sku["id"], sku["name"]) for sku in r.object]

    def ex_list_image_versions(self, sku):
        """
        List node image versions in a sku.

        :param sku: The complete resource path to a sku (as returned by
        `ex_list_skus`)
        :type publisher: ``str``

        :return: A list of tuples in the form
        ("version id", "version name")
        :rtype: ``list``
        """

        action = "%s/versions" % (sku)
        r = self.connection.request(action, params={"api-version": IMAGES_API_VERSION})
        return [(img["id"], img["name"]) for img in r.object]

    def ex_list_resource_groups(self):
        """
        List resource groups.

        :return: A list of resource groups.
        :rtype: ``list`` of :class:`.AzureResourceGroup`
        """

        action = "/subscriptions/%s/resourceGroups/" % (self.subscription_id)
        r = self.connection.request(action, params={"api-version": RESOURCE_GROUP_API_VERSION})
        return [
            AzureResourceGroup(grp["id"], grp["name"], grp["location"], grp["properties"])
            for grp in r.object["value"]
        ]

    def ex_list_network_security_groups(self, resource_group):
        """
        List network security groups.

        :param resource_group: List security groups in a specific resource
        group.
        :type resource_group: ``str``

        :return: A list of network security groups.
        :rtype: ``list`` of :class:`.AzureNetworkSecurityGroup`
        """

        action = (
            "/subscriptions/%s/resourceGroups/%s/providers/"
            "Microsoft.Network/networkSecurityGroups" % (self.subscription_id, resource_group)
        )
        r = self.connection.request(action, params={"api-version": NSG_API_VERSION})
        return [
            AzureNetworkSecurityGroup(net["id"], net["name"], net["location"], net["properties"])
            for net in r.object["value"]
        ]

    def ex_create_network_security_group(self, name, resource_group, location=None):
        """
        Update tags on any resource supporting tags.

        :param name: Name of the network security group to create
        :type name: ``str``

        :param resource_group: The resource group to create the network
        security group in
        :type resource_group: ``str``

        :param location: The location at which to create the network security
        group (if None, use default location specified as 'region' in __init__)
        :type location: :class:`.NodeLocation`
        """

        if location is None:
            if self.default_location:
                location = self.default_location
            else:
                raise ValueError("location is required.")

        target = (
            "/subscriptions/%s/resourceGroups/%s/"
            "providers/Microsoft.Network/networkSecurityGroups/%s"
            % (self.subscription_id, resource_group, name)
        )
        data = {
            "location": location.id,
        }
        self.connection.request(
            target, params={"api-version": NSG_API_VERSION}, data=data, method="PUT"
        )

    def ex_delete_network_security_group(self, name, resource_group, location=None):
        """
        Update tags on any resource supporting tags.

        :param name: Name of the network security group to delete
        :type name: ``str``

        :param resource_group: The resource group to create the network
        security group in
        :type resource_group: ``str``

        :param location: The location at which to create the network security
        group (if None, use default location specified as 'region' in __init__)
        :type location: :class:`.NodeLocation`
        """

        if location is None:
            if self.default_location:
                location = self.default_location
            else:
                raise ValueError("location is required.")

        target = (
            "/subscriptions/%s/resourceGroups/%s/"
            "providers/Microsoft.Network/networkSecurityGroups/%s"
            % (self.subscription_id, resource_group, name)
        )
        data = {
            "location": location.id,
        }
        self.connection.request(
            target, params={"api-version": NSG_API_VERSION}, data=data, method="DELETE"
        )

    def ex_list_networks(self):
        """
        List virtual networks.

        :return: A list of virtual networks.
        :rtype: ``list`` of :class:`.AzureNetwork`
        """

        action = "/subscriptions/%s/providers/" "Microsoft.Network/virtualnetworks" % (
            self.subscription_id
        )
        r = self.connection.request(action, params={"api-version": VIRTUAL_NETWORK_API_VERSION})
        return [
            AzureNetwork(net["id"], net["name"], net["location"], net["properties"])
            for net in r.object["value"]
        ]

    def ex_list_subnets(self, network):
        """
        List subnets of a virtual network.

        :param network: The virtual network containing the subnets.
        :type network: :class:`.AzureNetwork`

        :return: A list of subnets.
        :rtype: ``list`` of :class:`.AzureSubnet`
        """

        action = "%s/subnets" % (network.id)
        r = self.connection.request(action, params={"api-version": SUBNET_API_VERSION})
        return [AzureSubnet(net["id"], net["name"], net["properties"]) for net in r.object["value"]]

    def ex_list_nics(self, resource_group=None):
        """
        List available virtual network interface controllers
        in a resource group

        :param resource_group: List NICS in a specific resource group
            containing the NICs(optional).
        :type resource_group: ``str``

        :return: A list of NICs.
        :rtype: ``list`` of :class:`.AzureNic`
        """
        if resource_group is None:
            action = (
                "/subscriptions/%s/providers/Microsoft.Network"
                "/networkInterfaces" % self.subscription_id
            )
        else:
            action = (
                "/subscriptions/%s/resourceGroups/%s/providers"
                "/Microsoft.Network/networkInterfaces" % (self.subscription_id, resource_group)
            )
        r = self.connection.request(action, params={"api-version": NIC_API_VERSION})
        return [self._to_nic(net) for net in r.object["value"]]

    def ex_get_nic(self, id):
        """
        Fetch information about a NIC.

        :param id: The complete resource path to the NIC resource.
        :type id: ``str``

        :return: The NIC object
        :rtype: :class:`.AzureNic`
        """

        r = self.connection.request(id, params={"api-version": NIC_API_VERSION})
        return self._to_nic(r.object)

    def ex_update_nic_properties(self, network_interface, resource_group, properties):
        """
        Update the properties of an already existing virtual network
        interface (NIC).

        :param network_interface: The NIC to update.
        :type network_interface: :class:`.AzureNic`

        :param resource_group: The resource group to check the ip address in.
        :type resource_group: ``str``

        :param properties: The dictionary of the NIC's properties
        :type properties: ``dict``

        :return: The NIC object
        :rtype: :class:`.AzureNic`
        """

        target = (
            "/subscriptions/%s/resourceGroups/%s/providers"
            "/Microsoft.Network/networkInterfaces/%s"
            % (self.subscription_id, resource_group, network_interface.name)
        )

        data = {
            "properties": properties,
            "location": network_interface.location,
        }

        r = self.connection.request(
            target, params={"api-version": NIC_API_VERSION}, data=data, method="PUT"
        )
        return AzureNic(
            r.object["id"],
            r.object["name"],
            r.object["location"],
            r.object["properties"],
        )

    def ex_update_network_profile_of_node(self, node, network_profile):
        """
        Update the network profile of a node. This method can be used to
        attach or detach a NIC to a node.

        :param node: A node to attach the network interface to.
        :type node: :class:`Node`

        :param network_profile: The new network profile to update.
        :type network_profile: ``dict``
        """
        action = node.extra["id"]
        location = node.extra["location"]
        self.connection.request(
            action,
            method="PUT",
            params={"api-version": VM_API_VERSION},
            data={
                "id": node.id,
                "name": node.name,
                "type": "Microsoft.Compute/virtualMachines",
                "location": location,
                "properties": {"networkProfile": network_profile},
            },
        )

    def ex_destroy_nic(self, nic):
        """
        Destroy a NIC.

        :param nic: The NIC to destroy.
        :type nic: ``.AzureNic``

        :return: True on success
        :rtype: ``bool``
        """

        try:
            self.connection.request(
                nic.id, params={"api-version": NIC_API_VERSION}, method="DELETE"
            )
            return True
        except BaseHTTPError as h:
            if h.code in (202, 204):
                # Deletion is accepted (but deferred), or NIC is already
                # deleted
                return True
            else:
                raise

    def ex_check_ip_address_availability(self, resource_group, network, ip_address):
        """
        Checks whether a private IP address is available for use. Also
        returns an object that contains the available IPs in the subnet.

        :param resource_group: The resource group to check the ip address in.
        :type resource_group: ``str``

        :param network: The virtual network.
        :type network: :class:`.AzureNetwork`

        :param ip_address: The private IP address to be verified.
        :type ip_address: ``str``
        """
        params = {"api-version": VIRTUAL_NETWORK_API_VERSION}
        action = (
            "/subscriptions/%s/resourceGroups/%s/providers"
            "/Microsoft.Network/virtualNetworks/%s/"
            "CheckIPAddressAvailability" % (self.subscription_id, resource_group, network.name)
        )
        if ip_address is not None:
            params["ipAddress"] = ip_address
        r = self.connection.request(action, params=params)
        return r.object

    def ex_get_node(self, id):
        """
        Fetch information about a node.

        :param id: The complete resource path to the node resource.
        :type id: ``str``

        :return: The Node object
        :rtype: :class:`.Node`
        """

        r = self.connection.request(id, params={"api-version": VM_API_VERSION})
        return self._to_node(r.object)

    def ex_get_volume(self, id):
        """
        Fetch information about a volume.

        :param id: The complete resource path to the volume resource.
        :type id: ``str``

        :return: The StorageVolume object
        :rtype: :class:`.StorageVolume`
        """

        r = self.connection.request(id, params={"api-version": DISK_API_VERSION})
        return self._to_volume(r.object)

    def ex_get_snapshot(self, id):
        """
        Fetch information about a snapshot.

        :param id: The complete resource path to the snapshot resource.
        :type id: ``str``

        :return: The VolumeSnapshot object
        :rtype: :class:`.VolumeSnapshot`
        """

        r = self.connection.request(id, params={"api-version": SNAPSHOT_API_VERSION})
        return self._to_snapshot(r.object)

    def ex_get_public_ip(self, id):
        """
        Fetch information about a public IP resource.

        :param id: The complete resource path to the public IP resource.
        :type id: ``str`

        :return: The public ip object
        :rtype: :class:`.AzureIPAddress`
        """

        r = self.connection.request(id, params={"api-version": IP_API_VERSION})
        return self._to_ip_address(r.object)

    def ex_list_public_ips(self, resource_group):
        """
        List public IP resources.

        :param resource_group: List public IPs in a specific resource group.
        :type resource_group: ``str``

        :return: List of public ip objects
        :rtype: ``list`` of :class:`.AzureIPAddress`
        """

        action = (
            "/subscriptions/%s/resourceGroups/%s/"
            "providers/Microsoft.Network/publicIPAddresses" % (self.subscription_id, resource_group)
        )
        r = self.connection.request(action, params={"api-version": IP_API_VERSION})
        return [self._to_ip_address(net) for net in r.object["value"]]

    def ex_create_public_ip(
        self, name, resource_group, location=None, public_ip_allocation_method=None
    ):
        """
        Create a public IP resources.

        :param name: Name of the public IP resource
        :type name: ``str``

        :param resource_group: The resource group to create the public IP
        :type resource_group: ``str``

        :param location: The location at which to create the public IP
        (if None, use default location specified as 'region' in __init__)
        :type location: :class:`.NodeLocation`

        :param public_ip_allocation_method: Call ex_create_public_ip with
        public_ip_allocation_method="Static" to create a static public
        IP address
        :type public_ip_allocation_method: ``str``

        :return: The newly created public ip object
        :rtype: :class:`.AzureIPAddress`
        """

        if location is None:
            if self.default_location:
                location = self.default_location
            else:
                raise ValueError("location is required.")

        target = (
            "/subscriptions/%s/resourceGroups/%s/"
            "providers/Microsoft.Network/publicIPAddresses/%s"
            % (self.subscription_id, resource_group, name)
        )
        data = {
            "location": location.id,
            "tags": {},
            "properties": {"publicIPAllocationMethod": "Dynamic"},
        }

        if public_ip_allocation_method == "Static":
            data["properties"]["publicIPAllocationMethod"] = "Static"

        r = self.connection.request(
            target, params={"api-version": IP_API_VERSION}, data=data, method="PUT"
        )
        return self._to_ip_address(r.object)

    def ex_delete_public_ip(self, public_ip):
        """
        Delete a public ip address resource.

        :param public_ip: Public ip address resource to delete
        :type public_ip: `.AzureIPAddress`
        """
        # NOTE: This operation requires API version 2018-11-01 so
        # "ex_delete_resource" won't work for deleting an IP address
        resource = public_ip.id
        r = self.connection.request(
            resource,
            method="DELETE",
            params={"api-version": IP_API_VERSION},
        )

        return r.status in [200, 202, 204]

    def ex_create_network_interface(
        self, name, subnet, resource_group, location=None, public_ip=None
    ):
        """
        Create a virtual network interface (NIC).

        :param name: Name of the NIC resource
        :type name: ``str``

        :param subnet: The subnet to attach the NIC
        :type subnet: :class:`.AzureSubnet`

        :param resource_group: The resource group to create the NIC
        :type resource_group: ``str``

        :param location: The location at which to create the NIC
        (if None, use default location specified as 'region' in __init__)
        :type location: :class:`.NodeLocation`

        :param public_ip: Associate a public IP resource with this NIC
        (optional).
        :type public_ip: :class:`.AzureIPAddress`

        :return: The newly created NIC
        :rtype: :class:`.AzureNic`
        """

        if location is None:
            if self.default_location:
                location = self.default_location
            else:
                raise ValueError("location is required.")

        target = (
            "/subscriptions/%s/resourceGroups/%s/providers"
            "/Microsoft.Network/networkInterfaces/%s" % (self.subscription_id, resource_group, name)
        )

        data = {
            "location": location.id,
            "tags": {},
            "properties": {
                "ipConfigurations": [
                    {
                        "name": "myip1",
                        "properties": {
                            "subnet": {"id": subnet.id},
                            "privateIPAllocationMethod": "Dynamic",
                        },
                    }
                ]
            },
        }

        if public_ip:
            ip_config = data["properties"]["ipConfigurations"][0]
            ip_config["properties"]["publicIPAddress"] = {"id": public_ip.id}

        r = self.connection.request(
            target, params={"api-version": NIC_API_VERSION}, data=data, method="PUT"
        )
        return AzureNic(
            r.object["id"],
            r.object["name"],
            r.object["location"],
            r.object["properties"],
        )

    def ex_create_tags(self, resource, tags, replace=False):
        """
        Update tags on any resource supporting tags.

        :param resource: The resource to update.
        :type resource: ``str`` or Azure object with an ``id`` attribute.

        :param tags: The tags to set.
        :type tags: ``dict``

        :param replace: If true, replace all tags with the new tags.
        If false (default) add or update tags.
        :type replace: ``bool``
        """

        if not isinstance(resource, basestring):
            resource = resource.id
        r = self.connection.request(resource, params={"api-version": TAG_API_VERSION})
        if replace:
            r.object["tags"] = tags
        else:
            r.object["tags"].update(tags)
        self.connection.request(
            resource,
            data={"tags": r.object["tags"]},
            params={"api-version": TAG_API_VERSION},
            method="PATCH",
        )

    def ex_create_additional_capabilities(
        self,
        node,
        additional_capabilities,
        resource_group,
    ):
        """
        Set the additional capabilities on a stopped node.

        :param node: The node to be updated
        :type node: :class:`.Node`

        :param ex_additional_capabilities: Optional additional capabilities
            allowing Ultra SSD and hibernation on this node.
        :type ex_additional_capabilities: ``dict``

        :param resource_group: The resource group of the node to be updated
        :type resource_group: ``str``

        :return: True if the update was successful, otherwise False
        :rtype: ``bool``
        """

        target = (
            "/subscriptions/%s/resourceGroups/%s/providers"
            "/Microsoft.Compute/virtualMachines/%s"
            % (self.subscription_id, resource_group, node.name)
        )

        data = {
            "location": node.extra["location"],
            "properties": {"additionalCapabilities": additional_capabilities},
        }

        r = self.connection.request(
            target,
            data=data,
            params={"api-version": VM_API_VERSION},
            method="PUT",
        )

        return r.status in [200, 202, 204]

    def start_node(self, node):
        """
        Start a stopped node.

        :param node: The node to be started
        :type node: :class:`.Node`
        """

        target = "%s/start" % node.id
        r = self.connection.request(target, params={"api-version": VM_API_VERSION}, method="POST")
        return r.object

    def stop_node(self, node, ex_deallocate=True):
        """
        Stop a running node.

        :param node: The node to be stopped
        :type node: :class:`.Node`

        :param deallocate: If True (default) stop and then deallocate the node
        (release the hardware allocated to run the node).  If False, stop the
        node but maintain the hardware allocation.  If the node is not
        deallocated, the subscription will continue to be billed as if it
        were running.
        :type deallocate: ``bool``
        """
        if ex_deallocate:
            target = "%s/deallocate" % node.id
        else:
            target = "%s/powerOff" % node.id
        r = self.connection.request(target, params={"api-version": VM_API_VERSION}, method="POST")
        return r.object

    def ex_start_node(self, node):
        # NOTE: This method is here for backward compatibility reasons after
        # this method was promoted to be part of the standard compute API in
        # Libcloud v2.7.0
        return self.start_node(node=node)

    def ex_stop_node(self, node, deallocate=True):
        # NOTE: This method is here for backward compatibility reasons after
        # this method was promoted to be part of the standard compute API in
        # Libcloud v2.7.0
        return self.stop_node(node=node, ex_deallocate=deallocate)

    def ex_resize_node(self, node, size):
        """
        Resize the node to a different machine size.

        :param node: Node to resize
        :type node: :class:`Node`

        :param size: New size for this machine
        :type node: :class:`NodeSize`
        """
        target = "%s/resize" % node.id
        data = {"size": size.id}
        r = self.connection.request(
            target, params={"api-version": VM_API_VERSION}, data=data, method="POST"
        )
        return r.object

    def ex_get_storage_account_keys(self, resource_group, storage_account):
        """
        Get account keys required to access to a storage account
        (using AzureBlobsStorageDriver).

        :param resource_group: The resource group
            containing the storage account
        :type resource_group: ``str``

        :param storage_account: Storage account to access
        :type storage_account: ``str``

        :return: The account keys, in the form `{"key1": "XXX", "key2": "YYY"}`
        :rtype: ``.dict``
        """

        action = (
            "/subscriptions/%s/resourceGroups/%s/"
            "providers/Microsoft.Storage/storageAccounts/%s/listKeys"
            % (self.subscription_id, resource_group, storage_account)
        )

        r = self.connection.request(
            action, params={"api-version": STORAGE_ACCOUNT_API_VERSION}, method="POST"
        )
        return r.object

    def ex_run_command(
        self,
        node,
        command,
        filerefs=[],
        timestamp=0,
        storage_account_name=None,
        storage_account_key=None,
        location=None,
    ):
        """
        Run a command on the node as root.

        Does not require ssh to log in,
        uses Windows Azure Agent (waagent) running
        on the node.

        :param node: The node on which to run the command.
        :type node: :class:``.Node``

        :param command: The actual command to run.  Note this is parsed
        into separate arguments according to shell quoting rules but is
        executed directly as a subprocess, not a shell command.
        :type command: ``str``

        :param filerefs: Optional files to fetch by URI from Azure blob store
        (must provide storage_account_name and storage_account_key),
        or regular HTTP.
        :type command: ``list`` of ``str``

        :param location: The location of the virtual machine
        (if None, use default location specified as 'region' in __init__)
        :type location: :class:`.NodeLocation`

        :param storage_account_name: The storage account
            from which to fetch files in `filerefs`
        :type storage_account_name: ``str``

        :param storage_account_key: The storage key to
            authorize to the blob store.
        :type storage_account_key: ``str``

        :type: ``list`` of :class:`.NodeLocation`

        """

        if location is None:
            if self.default_location:
                location = self.default_location
            else:
                raise ValueError("location is required.")

        name = "init"

        target = node.id + "/extensions/" + name

        data = {
            "location": location.id,
            "name": name,
            "properties": {
                "publisher": "Microsoft.OSTCExtensions",
                "type": "CustomScriptForLinux",
                "typeHandlerVersion": "1.3",
                "settings": {
                    "fileUris": filerefs,
                    "commandToExecute": command,
                    "timestamp": timestamp,
                },
            },
        }

        if storage_account_name and storage_account_key:
            data["properties"]["protectedSettings"] = {
                "storageAccountName": storage_account_name,
                "storageAccountKey": storage_account_key,
            }

        r = self.connection.request(
            target,
            params={"api-version": VM_EXTENSION_API_VERSION},
            data=data,
            method="PUT",
        )
        return r.object

    def _ex_delete_old_vhd(self, resource_group, uri):
        try:
            (storageAccount, blobContainer, blob) = _split_blob_uri(uri)
            keys = self.ex_get_storage_account_keys(resource_group, storageAccount)
            blobdriver = AzureBlobsStorageDriver(
                storageAccount,
                keys["key1"],
                host="{}.blob{}".format(storageAccount, self.connection.storage_suffix),
            )
            return blobdriver.delete_object(blobdriver.get_object(blobContainer, blob))
        except ObjectDoesNotExistError:
            return True

    def _ex_connection_class_kwargs(self):
        kwargs = super()._ex_connection_class_kwargs()
        kwargs["tenant_id"] = self.tenant_id
        kwargs["subscription_id"] = self.subscription_id
        kwargs["cloud_environment"] = self.cloud_environment
        return kwargs

    def _fetch_power_state(self, data):
        state = NodeState.UNKNOWN
        try:
            action = "%s/InstanceView" % (data["id"])
            r = self.connection.request(action, params={"api-version": INSTANCE_VIEW_API_VERSION})
            for status in r.object["statuses"]:
                if status["code"] in ["ProvisioningState/creating"]:
                    state = NodeState.PENDING
                    break
                elif status["code"] == "ProvisioningState/deleting":
                    state = NodeState.TERMINATED
                    break
                elif status["code"].startswith("ProvisioningState/failed"):
                    state = NodeState.ERROR
                    break
                elif status["code"] == "ProvisioningState/updating":
                    state = NodeState.UPDATING
                    break
                elif status["code"] == "ProvisioningState/succeeded":
                    pass

                if status["code"] == "PowerState/deallocated":
                    state = NodeState.STOPPED
                    break
                elif status["code"] == "PowerState/stopped":
                    state = NodeState.PAUSED
                    break
                elif status["code"] == "PowerState/deallocating":
                    state = NodeState.PENDING
                    break
                elif status["code"] == "PowerState/running":
                    state = NodeState.RUNNING
        except BaseHTTPError:
            pass
        return state

    def _to_node(self, data, fetch_nic=True, fetch_power_state=True):
        private_ips = []
        public_ips = []
        nics = data["properties"]["networkProfile"]["networkInterfaces"]
        if fetch_nic:
            for nic in nics:
                try:
                    n = self.ex_get_nic(nic["id"])
                    priv = n.extra["ipConfigurations"][0]["properties"].get("privateIPAddress")
                    if priv:
                        private_ips.append(priv)
                    pub = n.extra["ipConfigurations"][0]["properties"].get("publicIPAddress")
                    if pub:
                        pub_addr = self.ex_get_public_ip(pub["id"])
                        addr = pub_addr.extra.get("ipAddress")
                        if addr:
                            public_ips.append(addr)
                except BaseHTTPError:
                    pass

        state = NodeState.UNKNOWN
        if fetch_power_state:
            state = self._fetch_power_state(data)
        else:
            ps = data["properties"]["provisioningState"].lower()
            if ps == "creating":
                state = NodeState.PENDING
            elif ps == "deleting":
                state = NodeState.TERMINATED
            elif ps == "failed":
                state = NodeState.ERROR
            elif ps == "updating":
                state = NodeState.UPDATING
            elif ps == "succeeded":
                state = NodeState.RUNNING

        node = Node(
            data["id"],
            data["name"],
            state,
            public_ips,
            private_ips,
            driver=self.connection.driver,
            extra=data,
        )

        return node

    def _to_node_size(self, data):
        return NodeSize(
            id=data["name"],
            name=data["name"],
            ram=data["memoryInMB"],
            # convert to disk from MB to GB
            disk=data["resourceDiskSizeInMB"] / 1024,
            bandwidth=0,
            price=0,
            driver=self.connection.driver,
            extra={
                "numberOfCores": data["numberOfCores"],
                "osDiskSizeInMB": data["osDiskSizeInMB"],
                "maxDataDiskCount": data["maxDataDiskCount"],
            },
        )

    def _to_nic(self, data):
        return AzureNic(data["id"], data.get("name"), data.get("location"), data.get("properties"))

    def _to_ip_address(self, data):
        return AzureIPAddress(data["id"], data["name"], data["properties"])

    def _to_location(self, loc):
        # XXX for some reason the API returns location names like
        # "East US" instead of "eastus" which is what is actually needed
        # for other API calls, so do a name->id fixup.
        loc_id = loc.lower().replace(" ", "")
        return NodeLocation(
            loc_id, loc, self._location_to_country.get(loc_id), self.connection.driver
        )

    def _get_instance_vhd(
        self, name, ex_resource_group, ex_storage_account, ex_blob_container="vhds"
    ):
        n = 0
        errors = []
        while n < 10:
            try:
                instance_vhd = "https://%s.blob%s" "/%s/%s-os_%i.vhd" % (
                    ex_storage_account,
                    self.connection.storage_suffix,
                    ex_blob_container,
                    name,
                    n,
                )
                if self._ex_delete_old_vhd(ex_resource_group, instance_vhd):
                    # We were able to remove it or it doesn't exist,
                    # so we can use it.
                    return instance_vhd
            except LibcloudError as lce:
                errors.append(str(lce))
            n += 1
        raise LibcloudError(
            "Unable to find a name for a VHD to use for "
            "instance in 10 tries, errors were:\n  - %s" % ("\n  - ".join(errors))
        )


def _split_blob_uri(uri):
    uri = uri.split("/")
    storage_account = uri[2].split(".")[0]
    blob_container = uri[3]
    blob_name = "/".join(uri[4:])
    return storage_account, blob_container, blob_name
