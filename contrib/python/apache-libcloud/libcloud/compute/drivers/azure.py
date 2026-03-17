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
Driver for Microsoft Azure Virtual Machines service.

http://azure.microsoft.com/en-us/services/virtual-machines/
"""

import re
import copy
import time
import base64
import random
import collections
from xml.dom import minidom
from datetime import datetime
from xml.sax.saxutils import escape as xml_escape

from libcloud.utils.py3 import ET, httplib, urlparse
from libcloud.utils.py3 import urlquote as url_quote
from libcloud.utils.py3 import _real_unicode, ensure_string
from libcloud.utils.misc import ReprMixin
from libcloud.common.azure import AzureRedirectException, AzureServiceManagementConnection
from libcloud.common.types import LibcloudError
from libcloud.compute.base import (
    Node,
    NodeSize,
    NodeImage,
    NodeDriver,
    NodeLocation,
    StorageVolume,
)
from libcloud.compute.types import NodeState
from libcloud.compute.providers import Provider

HTTPSConnection = httplib.HTTPSConnection

_str = str
_unicode_type = str


AZURE_SERVICE_MANAGEMENT_HOST = "management.core.windows.net"
X_MS_VERSION = "2013-08-01"

WINDOWS_SERVER_REGEX = re.compile(r"Win|SQL|SharePoint|Visual|Dynamics|DynGP|BizTalk")

"""
Sizes must be hardcoded because Microsoft doesn't provide an API to fetch them
From http://msdn.microsoft.com/en-us/library/windowsazure/dn197896.aspx

Prices are for Linux instances in East US data center. To see what pricing will
actually be, visit:
http://azure.microsoft.com/en-gb/pricing/details/virtual-machines/
"""
AZURE_COMPUTE_INSTANCE_TYPES = {
    "A0": {
        "id": "ExtraSmall",
        "name": "Extra Small Instance",
        "ram": 768,
        "disk": 127,
        "bandwidth": None,
        "price": "0.0211",
        "max_data_disks": 1,
        "cores": "Shared",
    },
    "A1": {
        "id": "Small",
        "name": "Small Instance",
        "ram": 1792,
        "disk": 127,
        "bandwidth": None,
        "price": "0.0633",
        "max_data_disks": 2,
        "cores": 1,
    },
    "A2": {
        "id": "Medium",
        "name": "Medium Instance",
        "ram": 3584,
        "disk": 127,
        "bandwidth": None,
        "price": "0.1266",
        "max_data_disks": 4,
        "cores": 2,
    },
    "A3": {
        "id": "Large",
        "name": "Large Instance",
        "ram": 7168,
        "disk": 127,
        "bandwidth": None,
        "price": "0.2531",
        "max_data_disks": 8,
        "cores": 4,
    },
    "A4": {
        "id": "ExtraLarge",
        "name": "Extra Large Instance",
        "ram": 14336,
        "disk": 127,
        "bandwidth": None,
        "price": "0.5062",
        "max_data_disks": 16,
        "cores": 8,
    },
    "A5": {
        "id": "A5",
        "name": "Memory Intensive Instance",
        "ram": 14336,
        "disk": 127,
        "bandwidth": None,
        "price": "0.2637",
        "max_data_disks": 4,
        "cores": 2,
    },
    "A6": {
        "id": "A6",
        "name": "A6 Instance",
        "ram": 28672,
        "disk": 127,
        "bandwidth": None,
        "price": "0.5273",
        "max_data_disks": 8,
        "cores": 4,
    },
    "A7": {
        "id": "A7",
        "name": "A7 Instance",
        "ram": 57344,
        "disk": 127,
        "bandwidth": None,
        "price": "1.0545",
        "max_data_disks": 16,
        "cores": 8,
    },
    "A8": {
        "id": "A8",
        "name": "A8 Instance",
        "ram": 57344,
        "disk": 127,
        "bandwidth": None,
        "price": "2.0774",
        "max_data_disks": 16,
        "cores": 8,
    },
    "A9": {
        "id": "A9",
        "name": "A9 Instance",
        "ram": 114688,
        "disk": 127,
        "bandwidth": None,
        "price": "4.7137",
        "max_data_disks": 16,
        "cores": 16,
    },
    "A10": {
        "id": "A10",
        "name": "A10 Instance",
        "ram": 57344,
        "disk": 127,
        "bandwidth": None,
        "price": "1.2233",
        "max_data_disks": 16,
        "cores": 8,
    },
    "A11": {
        "id": "A11",
        "name": "A11 Instance",
        "ram": 114688,
        "disk": 127,
        "bandwidth": None,
        "price": "2.1934",
        "max_data_disks": 16,
        "cores": 16,
    },
    "D1": {
        "id": "Standard_D1",
        "name": "D1 Faster Compute Instance",
        "ram": 3584,
        "disk": 127,
        "bandwidth": None,
        "price": "0.0992",
        "max_data_disks": 2,
        "cores": 1,
    },
    "D2": {
        "id": "Standard_D2",
        "name": "D2 Faster Compute Instance",
        "ram": 7168,
        "disk": 127,
        "bandwidth": None,
        "price": "0.1983",
        "max_data_disks": 4,
        "cores": 2,
    },
    "D3": {
        "id": "Standard_D3",
        "name": "D3 Faster Compute Instance",
        "ram": 14336,
        "disk": 127,
        "bandwidth": None,
        "price": "0.3965",
        "max_data_disks": 8,
        "cores": 4,
    },
    "D4": {
        "id": "Standard_D4",
        "name": "D4 Faster Compute Instance",
        "ram": 28672,
        "disk": 127,
        "bandwidth": None,
        "price": "0.793",
        "max_data_disks": 16,
        "cores": 8,
    },
    "D11": {
        "id": "Standard_D11",
        "name": "D11 Faster Compute Instance",
        "ram": 14336,
        "disk": 127,
        "bandwidth": None,
        "price": "0.251",
        "max_data_disks": 4,
        "cores": 2,
    },
    "D12": {
        "id": "Standard_D12",
        "name": "D12 Faster Compute Instance",
        "ram": 28672,
        "disk": 127,
        "bandwidth": None,
        "price": "0.502",
        "max_data_disks": 8,
        "cores": 4,
    },
    "D13": {
        "id": "Standard_D13",
        "name": "D13 Faster Compute Instance",
        "ram": 57344,
        "disk": 127,
        "bandwidth": None,
        "price": "0.9038",
        "max_data_disks": 16,
        "cores": 8,
    },
    "D14": {
        "id": "Standard_D14",
        "name": "D14 Faster Compute Instance",
        "ram": 114688,
        "disk": 127,
        "bandwidth": None,
        "price": "1.6261",
        "max_data_disks": 32,
        "cores": 16,
    },
}

_KNOWN_SERIALIZATION_XFORMS = {
    "include_apis": "IncludeAPIs",
    "message_id": "MessageId",
    "content_md5": "Content-MD5",
    "last_modified": "Last-Modified",
    "cache_control": "Cache-Control",
    "account_admin_live_email_id": "AccountAdminLiveEmailId",
    "service_admin_live_email_id": "ServiceAdminLiveEmailId",
    "subscription_id": "SubscriptionID",
    "fqdn": "FQDN",
    "private_id": "PrivateID",
    "os_virtual_hard_disk": "OSVirtualHardDisk",
    "logical_disk_size_in_gb": "LogicalDiskSizeInGB",
    "logical_size_in_gb": "LogicalSizeInGB",
    "os": "OS",
    "persistent_vm_downtime_info": "PersistentVMDowntimeInfo",
    "copy_id": "CopyId",
    "os_disk_configuration": "OSDiskConfiguration",
    "is_dns_programmed": "IsDnsProgrammed",
}


class AzureNodeDriver(NodeDriver):
    connectionCls = AzureServiceManagementConnection
    name = "Azure Virtual machines"
    website = "http://azure.microsoft.com/en-us/services/virtual-machines/"
    type = Provider.AZURE

    _instance_types = AZURE_COMPUTE_INSTANCE_TYPES
    _blob_url = ".blob.core.windows.net"
    features = {"create_node": ["password"]}
    service_location = collections.namedtuple(
        "service_location", ["is_affinity_group", "service_location"]
    )

    NODE_STATE_MAP = {
        "RoleStateUnknown": NodeState.UNKNOWN,
        "CreatingVM": NodeState.PENDING,
        "StartingVM": NodeState.PENDING,
        "Provisioning": NodeState.PENDING,
        "CreatingRole": NodeState.PENDING,
        "StartingRole": NodeState.PENDING,
        "ReadyRole": NodeState.RUNNING,
        "BusyRole": NodeState.PENDING,
        "StoppingRole": NodeState.PENDING,
        "StoppingVM": NodeState.PENDING,
        "DeletingVM": NodeState.PENDING,
        "StoppedVM": NodeState.STOPPED,
        "RestartingRole": NodeState.REBOOTING,
        "CyclingRole": NodeState.TERMINATED,
        "FailedStartingRole": NodeState.TERMINATED,
        "FailedStartingVM": NodeState.TERMINATED,
        "UnresponsiveRole": NodeState.TERMINATED,
        "StoppedDeallocated": NodeState.TERMINATED,
    }

    def __init__(self, subscription_id=None, key_file=None, **kwargs):
        """
        subscription_id contains the Azure subscription id in the form of GUID
        key_file contains the Azure X509 certificate in .pem form
        """
        self.subscription_id = subscription_id
        self.key_file = key_file
        self.follow_redirects = kwargs.get("follow_redirects", True)
        super().__init__(self.subscription_id, self.key_file, secure=True, **kwargs)

    def list_sizes(self):
        """
        Lists all sizes

        :rtype: ``list`` of :class:`NodeSize`
        """
        sizes = []

        for _, values in self._instance_types.items():
            node_size = self._to_node_size(copy.deepcopy(values))
            sizes.append(node_size)

        return sizes

    def list_images(self, location=None):
        """
        Lists all images

        :rtype: ``list`` of :class:`NodeImage`
        """
        data = self._perform_get(self._get_image_path(), Images)

        custom_image_data = self._perform_get(self._get_vmimage_path(), VMImages)

        images = [self._to_image(i) for i in data]
        images.extend(self._vm_to_image(j) for j in custom_image_data)

        if location is not None:
            images = [image for image in images if location in image.extra["location"]]

        return images

    def list_locations(self):
        """
        Lists all locations

        :rtype: ``list`` of :class:`NodeLocation`
        """
        data = self._perform_get("/" + self.subscription_id + "/locations", Locations)

        return [self._to_location(location) for location in data]

    def list_nodes(self, ex_cloud_service_name):
        """
        List all nodes

        ex_cloud_service_name parameter is used to scope the request
        to a specific Cloud Service. This is a required parameter as
        nodes cannot exist outside of a Cloud Service nor be shared
        between a Cloud Service within Azure.

        :param      ex_cloud_service_name: Cloud Service name
        :type       ex_cloud_service_name: ``str``

        :rtype: ``list`` of :class:`Node`
        """
        response = self._perform_get(
            self._get_hosted_service_path(ex_cloud_service_name) + "?embed-detail=True",
            None,
        )
        self.raise_for_response(response, 200)

        data = self._parse_response(response, HostedService)

        vips = None

        if len(data.deployments) > 0 and data.deployments[0].virtual_ips is not None:
            vips = [vip.address for vip in data.deployments[0].virtual_ips]

        try:
            return [
                self._to_node(n, ex_cloud_service_name, vips)
                for n in data.deployments[0].role_instance_list
            ]
        except IndexError:
            return []

    def reboot_node(self, node, ex_cloud_service_name=None, ex_deployment_slot=None):
        """
        Reboots a node.

        ex_cloud_service_name parameter is used to scope the request
        to a specific Cloud Service. This is a required parameter as
        nodes cannot exist outside of a Cloud Service nor be shared
        between a Cloud Service within Azure.

        :param      ex_cloud_service_name: Cloud Service name
        :type       ex_cloud_service_name: ``str``

        :param      ex_deployment_slot: Options are "production" (default)
                                         or "Staging". (Optional)
        :type       ex_deployment_slot: ``str``

        :rtype: ``bool``
        """
        if ex_cloud_service_name is None:
            if node.extra is not None:
                ex_cloud_service_name = node.extra.get("ex_cloud_service_name")

        if not ex_cloud_service_name:
            raise ValueError("ex_cloud_service_name is required.")

        if not ex_deployment_slot:
            ex_deployment_slot = "Production"

        _deployment_name = self._get_deployment(
            service_name=ex_cloud_service_name, deployment_slot=ex_deployment_slot
        ).name

        try:
            response = self._perform_post(
                self._get_deployment_path_using_name(ex_cloud_service_name, _deployment_name)
                + "/roleinstances/"
                + _str(node.id)
                + "?comp=reboot",
                "",
            )

            self.raise_for_response(response, 202)

            if self._parse_response_for_async_op(response):
                return True
            else:
                return False
        except Exception:
            return False

    def list_volumes(self, node=None):
        """
        Lists volumes of the disks in the image repository that are
        associated with the specified subscription.

        Pass Node object to scope the list of volumes to a single
        instance.

        :rtype: ``list`` of :class:`StorageVolume`
        """

        data = self._perform_get(self._get_disk_path(), Disks)
        volumes = [self._to_volume(volume=v, node=node) for v in data]
        return volumes

    def create_node(
        self,
        name,
        size,
        image,
        ex_cloud_service_name,
        ex_storage_service_name=None,
        ex_new_deployment=False,
        ex_deployment_slot="Production",
        ex_deployment_name=None,
        ex_admin_user_id="azureuser",
        ex_custom_data=None,
        ex_virtual_network_name=None,
        ex_network_config=None,
        auth=None,
        **kwargs,
    ):
        """
        Create Azure Virtual Machine

        Reference: http://bit.ly/1fIsCb7
        [www.windowsazure.com/en-us/documentation/]

        We default to:

        + 3389/TCP - RDP - 1st Microsoft instance.
        + RANDOM/TCP - RDP - All succeeding Microsoft instances.

        + 22/TCP - SSH - 1st Linux instance
        + RANDOM/TCP - SSH - All succeeding Linux instances.

        The above replicates the standard behavior of the Azure UI.
        You can retrieve the assigned ports to each instance by
        using the following private function:

        _get_endpoint_ports(service_name)
        Returns public,private port key pair.

        @inherits: :class:`NodeDriver.create_node`

        :keyword     image: The image to use when creating this node
        :type        image:  `NodeImage`

        :keyword     size: The size of the instance to create
        :type        size: `NodeSize`

        :keyword     ex_cloud_service_name: Required.
                     Name of the Azure Cloud Service.
        :type        ex_cloud_service_name:  ``str``

        :keyword     ex_storage_service_name: Optional:
                     Name of the Azure Storage Service.
        :type        ex_storage_service_name:  ``str``

        :keyword     ex_new_deployment: Optional. Tells azure to create a
                                        new deployment rather than add to an
                                        existing one.
        :type        ex_new_deployment: ``boolean``

        :keyword     ex_deployment_slot: Optional: Valid values: production|
                                         staging.
                                         Defaults to production.
        :type        ex_deployment_slot:  ``str``

        :keyword     ex_deployment_name: Optional. The name of the
                                         deployment.
                                         If this is not passed in we default
                                         to using the Cloud Service name.
        :type        ex_deployment_name: ``str``

        :type        ex_custom_data: ``str``
        :keyword     ex_custom_data: Optional script or other data which is
                                     injected into the VM when it's beginning
                                     provisioned.

        :keyword     ex_admin_user_id: Optional. Defaults to 'azureuser'.
        :type        ex_admin_user_id:  ``str``

        :keyword     ex_virtual_network_name: Optional. If this is not passed
                                              in no virtual network is used.
        :type        ex_virtual_network_name:  ``str``

        :keyword     ex_network_config: Optional. The ConfigurationSet to use
                                        for network configuration
        :type        ex_network_config:  `ConfigurationSet`

        """
        # TODO: Refactor this method to make it more readable, split it into
        # multiple smaller methods
        auth = self._get_and_check_auth(auth)
        password = auth.password

        if not isinstance(size, NodeSize):
            raise ValueError("Size must be an instance of NodeSize")

        if not isinstance(image, NodeImage):
            raise ValueError("Image must be an instance of NodeImage, " "produced by list_images()")

        # Retrieve a list of currently available nodes for the provided cloud
        # service
        node_list = self.list_nodes(ex_cloud_service_name=ex_cloud_service_name)

        if ex_network_config is None:
            network_config = ConfigurationSet()
        else:
            network_config = ex_network_config
        network_config.configuration_set_type = "NetworkConfiguration"

        # Base64 encode custom data if provided
        if ex_custom_data:
            ex_custom_data = self._encode_base64(data=ex_custom_data)

        # We do this because we need to pass a Configuration to the
        # method. This will be either Linux or Windows.
        if WINDOWS_SERVER_REGEX.search(image.id, re.I):
            machine_config = WindowsConfigurationSet(
                computer_name=name,
                admin_password=password,
                admin_user_name=ex_admin_user_id,
            )

            machine_config.domain_join = None

            if not node_list or ex_new_deployment:
                port = "3389"
            else:
                port = random.randint(41952, 65535)
                endpoints = self._get_deployment(
                    service_name=ex_cloud_service_name,
                    deployment_slot=ex_deployment_slot,
                )

                for instances in endpoints.role_instance_list:
                    ports = [ep.public_port for ep in instances.instance_endpoints]

                    while port in ports:
                        port = random.randint(41952, 65535)

            endpoint = ConfigurationSetInputEndpoint(
                name="Remote Desktop",
                protocol="tcp",
                port=port,
                local_port="3389",
                load_balanced_endpoint_set_name=None,
                enable_direct_server_return=False,
            )
        else:
            if not node_list or ex_new_deployment:
                port = "22"
            else:
                port = random.randint(41952, 65535)
                endpoints = self._get_deployment(
                    service_name=ex_cloud_service_name,
                    deployment_slot=ex_deployment_slot,
                )

                for instances in endpoints.role_instance_list:
                    ports = []
                    if instances.instance_endpoints is not None:
                        for ep in instances.instance_endpoints:
                            ports += [ep.public_port]

                    while port in ports:
                        port = random.randint(41952, 65535)

            endpoint = ConfigurationSetInputEndpoint(
                name="SSH",
                protocol="tcp",
                port=port,
                local_port="22",
                load_balanced_endpoint_set_name=None,
                enable_direct_server_return=False,
            )
            machine_config = LinuxConfigurationSet(
                name, ex_admin_user_id, password, False, ex_custom_data
            )

        network_config.input_endpoints.items.append(endpoint)

        _storage_location = self._get_cloud_service_location(service_name=ex_cloud_service_name)

        if ex_storage_service_name is None:
            ex_storage_service_name = ex_cloud_service_name
            ex_storage_service_name = re.sub(
                r"[\W_-]+", "", ex_storage_service_name.lower(), flags=re.UNICODE
            )

            if self._is_storage_service_unique(service_name=ex_storage_service_name):
                self._create_storage_account(
                    service_name=ex_storage_service_name,
                    location=_storage_location.service_location,
                    is_affinity_group=_storage_location.is_affinity_group,
                )

        # OK, bit annoying here. You must create a deployment before
        # you can create an instance; however, the deployment function
        # creates the first instance, but all subsequent instances
        # must be created using the add_role function.
        #
        # So, yeah, annoying.
        if not node_list or ex_new_deployment:
            # This is the first node in this cloud service.

            if not ex_deployment_name:
                ex_deployment_name = ex_cloud_service_name

            vm_image_id = None
            disk_config = None

            if image.extra.get("vm_image", False):
                vm_image_id = image.id
                #  network_config = None
            else:
                blob_url = "http://%s.blob.core.windows.net" % (ex_storage_service_name)

                # Azure's pattern in the UI.
                disk_name = "{}-{}-{}.vhd".format(
                    ex_cloud_service_name,
                    name,
                    time.strftime("%Y-%m-%d"),
                )

                media_link = "{}/vhds/{}".format(blob_url, disk_name)

                disk_config = OSVirtualHardDisk(image.id, media_link)

            response = self._perform_post(
                self._get_deployment_path_using_name(ex_cloud_service_name),
                AzureXmlSerializer.virtual_machine_deployment_to_xml(
                    ex_deployment_name,
                    ex_deployment_slot,
                    name,
                    name,
                    machine_config,
                    disk_config,
                    "PersistentVMRole",
                    network_config,
                    None,
                    None,
                    size.id,
                    ex_virtual_network_name,
                    vm_image_id,
                ),
            )
            self.raise_for_response(response, 202)
            self._ex_complete_async_azure_operation(response)
        else:
            _deployment_name = self._get_deployment(
                service_name=ex_cloud_service_name, deployment_slot=ex_deployment_slot
            ).name

            vm_image_id = None
            disk_config = None

            if image.extra.get("vm_image", False):
                vm_image_id = image.id
                #  network_config = None
            else:
                blob_url = "http://%s.blob.core.windows.net" % (ex_storage_service_name)
                disk_name = "{}-{}-{}.vhd".format(
                    ex_cloud_service_name,
                    name,
                    time.strftime("%Y-%m-%d"),
                )
                media_link = "{}/vhds/{}".format(blob_url, disk_name)
                disk_config = OSVirtualHardDisk(image.id, media_link)

            path = self._get_role_path(ex_cloud_service_name, _deployment_name)
            body = AzureXmlSerializer.add_role_to_xml(
                name,  # role_name
                machine_config,  # system_config
                disk_config,  # os_virtual_hard_disk
                "PersistentVMRole",  # role_type
                network_config,  # network_config
                None,  # availability_set_name
                None,  # data_virtual_hard_disks
                vm_image_id,  # vm_image
                size.id,  # role_size
            )

            response = self._perform_post(path, body)
            self.raise_for_response(response, 202)
            self._ex_complete_async_azure_operation(response)

        return Node(
            id=name,
            name=name,
            state=NodeState.PENDING,
            public_ips=[],
            private_ips=[],
            driver=self.connection.driver,
            extra={"ex_cloud_service_name": ex_cloud_service_name},
        )

    def destroy_node(self, node, ex_cloud_service_name=None, ex_deployment_slot="Production"):
        """
        Remove Azure Virtual Machine

        This removes the instance, but does not
        remove the disk. You will need to use destroy_volume.
        Azure sometimes has an issue where it will hold onto
        a blob lease for an extended amount of time.

        :keyword     ex_cloud_service_name: Required.
                     Name of the Azure Cloud Service.
        :type        ex_cloud_service_name:  ``str``

        :keyword     ex_deployment_slot: Optional: The name of the deployment
                                         slot. If this is not passed in we
                                         default to production.
        :type        ex_deployment_slot:  ``str``
        """

        if not isinstance(node, Node):
            raise ValueError("A libcloud Node object is required.")

        if ex_cloud_service_name is None and node.extra is not None:
            ex_cloud_service_name = node.extra.get("ex_cloud_service_name")

        if not ex_cloud_service_name:
            raise ValueError("Unable to get ex_cloud_service_name from Node.")

        _deployment = self._get_deployment(
            service_name=ex_cloud_service_name, deployment_slot=ex_deployment_slot
        )

        _deployment_name = _deployment.name

        _server_deployment_count = len(_deployment.role_instance_list)

        if _server_deployment_count > 1:
            path = self._get_role_path(ex_cloud_service_name, _deployment_name, node.id)
        else:
            path = self._get_deployment_path_using_name(ex_cloud_service_name, _deployment_name)

        path += "?comp=media"

        self._perform_delete(path)

        return True

    def ex_list_cloud_services(self):
        return self._perform_get(self._get_hosted_service_path(), HostedServices)

    def ex_create_cloud_service(self, name, location, description=None, extended_properties=None):
        """
        Create an azure cloud service.

        :param      name: Name of the service to create
        :type       name: ``str``

        :param      location: Standard azure location string
        :type       location: ``str``

        :param      description: Optional description
        :type       description: ``str``

        :param      extended_properties: Optional extended_properties
        :type       extended_properties: ``dict``

        :rtype: ``bool``
        """

        response = self._perform_cloud_service_create(
            self._get_hosted_service_path(),
            AzureXmlSerializer.create_hosted_service_to_xml(
                name,
                self._encode_base64(name),
                description,
                location,
                None,
                extended_properties,
            ),
        )

        self.raise_for_response(response, 201)

        return True

    def ex_destroy_cloud_service(self, name):
        """
        Delete an azure cloud service.

        :param      name: Name of the cloud service to destroy.
        :type       name: ``str``

        :rtype: ``bool``
        """
        response = self._perform_cloud_service_delete(self._get_hosted_service_path(name))

        self.raise_for_response(response, 200)

        return True

    def ex_add_instance_endpoints(self, node, endpoints, ex_deployment_slot="Production"):
        all_endpoints = [
            {
                "name": endpoint.name,
                "protocol": endpoint.protocol,
                "port": endpoint.public_port,
                "local_port": endpoint.local_port,
            }
            for endpoint in node.extra["instance_endpoints"]
        ]

        all_endpoints.extend(endpoints)
        # pylint: disable=assignment-from-no-return
        result = self.ex_set_instance_endpoints(node, all_endpoints, ex_deployment_slot)
        return result

    def ex_set_instance_endpoints(self, node, endpoints, ex_deployment_slot="Production"):
        """
        For example::

            endpoint = ConfigurationSetInputEndpoint(
                name='SSH',
                protocol='tcp',
                port=port,
                local_port='22',
                load_balanced_endpoint_set_name=None,
                enable_direct_server_return=False
            )
            {
                'name': 'SSH',
                'protocol': 'tcp',
                'port': port,
                'local_port': '22'
            }
        """
        ex_cloud_service_name = node.extra["ex_cloud_service_name"]
        vm_role_name = node.name

        network_config = ConfigurationSet()
        network_config.configuration_set_type = "NetworkConfiguration"

        for endpoint in endpoints:
            new_endpoint = ConfigurationSetInputEndpoint(**endpoint)
            network_config.input_endpoints.items.append(new_endpoint)

        _deployment_name = self._get_deployment(
            service_name=ex_cloud_service_name, deployment_slot=ex_deployment_slot
        ).name

        response = self._perform_put(
            self._get_role_path(ex_cloud_service_name, _deployment_name, vm_role_name),
            AzureXmlSerializer.add_role_to_xml(
                None,  # role_name
                None,  # system_config
                None,  # os_virtual_hard_disk
                "PersistentVMRole",  # role_type
                network_config,  # network_config
                None,  # availability_set_name
                None,  # data_virtual_hard_disks
                None,  # vm_image
                None,  # role_size
            ),
        )

        self.raise_for_response(response, 202)

    def ex_create_storage_service(
        self,
        name,
        location,
        description=None,
        affinity_group=None,
        extended_properties=None,
    ):
        """
        Create an azure storage service.

        :param      name: Name of the service to create
        :type       name: ``str``

        :param      location: Standard azure location string
        :type       location: ``str``

        :param      description: (Optional) Description of storage service.
        :type       description: ``str``

        :param      affinity_group: (Optional) Azure affinity group.
        :type       affinity_group: ``str``

        :param      extended_properties: (Optional) Additional configuration
                                         options support by Azure.
        :type       extended_properties: ``dict``

        :rtype: ``bool``
        """

        response = self._perform_storage_service_create(
            self._get_storage_service_path(),
            AzureXmlSerializer.create_storage_service_to_xml(
                service_name=name,
                label=self._encode_base64(name),
                description=description,
                location=location,
                affinity_group=affinity_group,
                extended_properties=extended_properties,
            ),
        )

        self.raise_for_response(response, 202)

        return True

    def ex_destroy_storage_service(self, name):
        """
        Destroy storage service. Storage service must not have any active
        blobs. Sometimes Azure likes to hold onto volumes after they are
        deleted for an inordinate amount of time, so sleep before calling
        this method after volume deletion.

        :param name: Name of storage service.
        :type  name: ``str``

        :rtype: ``bool``
        """

        response = self._perform_storage_service_delete(self._get_storage_service_path(name))
        self.raise_for_response(response, 200)

        return True

    """
    Functions not implemented
    """

    def create_volume_snapshot(self):
        raise NotImplementedError("You cannot create snapshots of " "Azure VMs at this time.")

    def attach_volume(self):
        raise NotImplementedError("attach_volume is not supported " "at this time.")

    def create_volume(self):
        raise NotImplementedError("create_volume is not supported " "at this time.")

    def detach_volume(self):
        raise NotImplementedError("detach_volume is not supported " "at this time.")

    def destroy_volume(self):
        raise NotImplementedError("destroy_volume is not supported " "at this time.")

    """
    Private Functions
    """

    def _perform_cloud_service_create(self, path, data):
        request = AzureHTTPRequest()
        request.method = "POST"
        request.host = AZURE_SERVICE_MANAGEMENT_HOST
        request.path = path
        request.body = data
        request.path, request.query = self._update_request_uri_query(request)
        request.headers = self._update_management_header(request)
        response = self._perform_request(request)

        return response

    def _perform_cloud_service_delete(self, path):
        request = AzureHTTPRequest()
        request.method = "DELETE"
        request.host = AZURE_SERVICE_MANAGEMENT_HOST
        request.path = path
        request.path, request.query = self._update_request_uri_query(request)
        request.headers = self._update_management_header(request)
        response = self._perform_request(request)

        return response

    def _perform_storage_service_create(self, path, data):
        request = AzureHTTPRequest()
        request.method = "POST"
        request.host = AZURE_SERVICE_MANAGEMENT_HOST
        request.path = path
        request.body = data
        request.path, request.query = self._update_request_uri_query(request)
        request.headers = self._update_management_header(request)
        response = self._perform_request(request)

        return response

    def _perform_storage_service_delete(self, path):
        request = AzureHTTPRequest()
        request.method = "DELETE"
        request.host = AZURE_SERVICE_MANAGEMENT_HOST
        request.path = path
        request.path, request.query = self._update_request_uri_query(request)
        request.headers = self._update_management_header(request)
        response = self._perform_request(request)

        return response

    def _to_node(self, data, ex_cloud_service_name=None, virtual_ips=None):
        """
        Convert the data from a Azure response object into a Node
        """

        remote_desktop_port = ""
        ssh_port = ""
        public_ips = virtual_ips or []

        if data.instance_endpoints is not None:
            if len(data.instance_endpoints) >= 1:
                public_ips = [data.instance_endpoints[0].vip]

            for port in data.instance_endpoints:
                if port.name == "Remote Desktop":
                    remote_desktop_port = port.public_port

                if port.name == "SSH":
                    ssh_port = port.public_port

        return Node(
            id=data.role_name,
            name=data.role_name,
            state=self.NODE_STATE_MAP.get(data.instance_status, NodeState.UNKNOWN),
            public_ips=public_ips,
            private_ips=[data.ip_address],
            driver=self.connection.driver,
            extra={
                "instance_endpoints": data.instance_endpoints,
                "remote_desktop_port": remote_desktop_port,
                "ssh_port": ssh_port,
                "power_state": data.power_state,
                "instance_size": data.instance_size,
                "ex_cloud_service_name": ex_cloud_service_name,
            },
        )

    def _to_location(self, data):
        """
        Convert the data from a Azure response object into a location
        """
        country = data.display_name

        if "Asia" in data.display_name:
            country = "Asia"

        if "Europe" in data.display_name:
            country = "Europe"

        if "US" in data.display_name:
            country = "US"

        if "Japan" in data.display_name:
            country = "Japan"

        if "Brazil" in data.display_name:
            country = "Brazil"

        vm_role_sizes = data.compute_capabilities.virtual_machines_role_sizes

        return AzureNodeLocation(
            id=data.name,
            name=data.display_name,
            country=country,
            driver=self.connection.driver,
            available_services=data.available_services,
            virtual_machine_role_sizes=vm_role_sizes,
        )

    def _to_node_size(self, data):
        """
        Convert the AZURE_COMPUTE_INSTANCE_TYPES into NodeSize
        """
        return NodeSize(
            id=data["id"],
            name=data["name"],
            ram=data["ram"],
            disk=data["disk"],
            bandwidth=data["bandwidth"],
            price=data["price"],
            driver=self.connection.driver,
            extra={"max_data_disks": data["max_data_disks"], "cores": data["cores"]},
        )

    def _to_image(self, data):
        return NodeImage(
            id=data.name,
            name=data.label,
            driver=self.connection.driver,
            extra={
                "os": data.os,
                "category": data.category,
                "description": data.description,
                "location": data.location,
                "affinity_group": data.affinity_group,
                "media_link": data.media_link,
                "vm_image": False,
            },
        )

    def _vm_to_image(self, data):
        return NodeImage(
            id=data.name,
            name=data.label,
            driver=self.connection.driver,
            extra={
                "os": data.os_disk_configuration.os,
                "category": data.category,
                "location": data.location,
                "media_link": data.os_disk_configuration.media_link,
                "affinity_group": data.affinity_group,
                "deployment_name": data.deployment_name,
                "vm_image": True,
            },
        )

    def _to_volume(self, volume, node):
        extra = {
            "affinity_group": volume.affinity_group,
            "os": volume.os,
            "location": volume.location,
            "media_link": volume.media_link,
            "source_image_name": volume.source_image_name,
        }

        role_name = getattr(volume.attached_to, "role_name", None)
        hosted_service_name = getattr(volume.attached_to, "hosted_service_name", None)

        deployment_name = getattr(volume.attached_to, "deployment_name", None)

        if role_name is not None:
            extra["role_name"] = role_name

        if hosted_service_name is not None:
            extra["hosted_service_name"] = hosted_service_name

        if deployment_name is not None:
            extra["deployment_name"] = deployment_name

        if node:
            if role_name is not None and role_name == node.id:
                return StorageVolume(
                    id=volume.name,
                    name=volume.name,
                    size=int(volume.logical_disk_size_in_gb),
                    driver=self.connection.driver,
                    extra=extra,
                )
        else:
            return StorageVolume(
                id=volume.name,
                name=volume.name,
                size=int(volume.logical_disk_size_in_gb),
                driver=self.connection.driver,
                extra=extra,
            )

    def _get_deployment(self, **kwargs):
        _service_name = kwargs["service_name"]
        _deployment_slot = kwargs["deployment_slot"]

        response = self._perform_get(
            self._get_deployment_path_using_slot(_service_name, _deployment_slot), None
        )

        self.raise_for_response(response, 200)

        return self._parse_response(response, Deployment)

    def _get_cloud_service_location(self, service_name=None):
        if not service_name:
            raise ValueError("service_name is required.")

        res = self._perform_get(
            "%s?embed-detail=False" % (self._get_hosted_service_path(service_name)),
            HostedService,
        )

        _affinity_group = res.hosted_service_properties.affinity_group
        _cloud_service_location = res.hosted_service_properties.location

        if _affinity_group is not None and _affinity_group != "":
            return self.service_location(True, _affinity_group)
        elif _cloud_service_location is not None:
            return self.service_location(False, _cloud_service_location)
        else:
            return None

    def _is_storage_service_unique(self, service_name=None):
        if not service_name:
            raise ValueError("service_name is required.")

        _check_availability = self._perform_get(
            "%s/operations/isavailable/%s%s"
            % (self._get_storage_service_path(), _str(service_name), ""),
            AvailabilityResponse,
        )

        self.raise_for_response(_check_availability, 200)

        return _check_availability.result

    def _create_storage_account(self, **kwargs):
        if kwargs["is_affinity_group"] is True:
            response = self._perform_post(
                self._get_storage_service_path(),
                AzureXmlSerializer.create_storage_service_input_to_xml(
                    kwargs["service_name"],
                    kwargs["service_name"],
                    self._encode_base64(kwargs["service_name"]),
                    kwargs["location"],
                    None,  # Location
                    True,  # geo_replication_enabled
                    None,  # extended_properties
                ),
            )

            self.raise_for_response(response, 202)

        else:
            response = self._perform_post(
                self._get_storage_service_path(),
                AzureXmlSerializer.create_storage_service_input_to_xml(
                    kwargs["service_name"],
                    kwargs["service_name"],
                    self._encode_base64(kwargs["service_name"]),
                    None,  # Affinity Group
                    kwargs["location"],  # Location
                    True,  # geo_replication_enabled
                    None,  # extended_properties
                ),
            )

            self.raise_for_response(response, 202)

        # We need to wait for this to be created before we can
        # create the storage container and the instance.
        self._ex_complete_async_azure_operation(response, "create_storage_account")

    def _get_operation_status(self, request_id):
        return self._perform_get(
            "/" + self.subscription_id + "/operations/" + _str(request_id), Operation
        )

    def _perform_get(self, path, response_type):
        request = AzureHTTPRequest()
        request.method = "GET"
        request.host = AZURE_SERVICE_MANAGEMENT_HOST
        request.path = path
        request.path, request.query = self._update_request_uri_query(request)
        request.headers = self._update_management_header(request)
        response = self._perform_request(request)

        if response_type is not None:
            return self._parse_response(response, response_type)

        return response

    def _perform_post(self, path, body, response_type=None):
        request = AzureHTTPRequest()
        request.method = "POST"
        request.host = AZURE_SERVICE_MANAGEMENT_HOST
        request.path = path
        request.body = ensure_string(self._get_request_body(body))
        request.path, request.query = self._update_request_uri_query(request)
        request.headers = self._update_management_header(request)
        response = self._perform_request(request)

        return response

    def _perform_put(self, path, body, response_type=None):
        request = AzureHTTPRequest()
        request.method = "PUT"
        request.host = AZURE_SERVICE_MANAGEMENT_HOST
        request.path = path
        request.body = ensure_string(self._get_request_body(body))
        request.path, request.query = self._update_request_uri_query(request)
        request.headers = self._update_management_header(request)
        response = self._perform_request(request)

        return response

    def _perform_delete(self, path):
        request = AzureHTTPRequest()
        request.method = "DELETE"
        request.host = AZURE_SERVICE_MANAGEMENT_HOST
        request.path = path
        request.path, request.query = self._update_request_uri_query(request)
        request.headers = self._update_management_header(request)
        response = self._perform_request(request)

        self.raise_for_response(response, 202)

    def _perform_request(self, request):
        try:
            return self.connection.request(
                action=request.path,
                data=request.body,
                headers=request.headers,
                method=request.method,
            )
        except AzureRedirectException as e:
            parsed_url = urlparse.urlparse(e.location)
            request.host = parsed_url.netloc
            return self._perform_request(request)
        except Exception as e:
            raise e

    def _update_request_uri_query(self, request):
        """
        pulls the query string out of the URI and moves it into
        the query portion of the request object.  If there are already
        query parameters on the request the parameters in the URI will
        appear after the existing parameters
        """
        if "?" in request.path:
            request.path, _, query_string = request.path.partition("?")
            if query_string:
                query_params = query_string.split("&")
                for query in query_params:
                    if "=" in query:
                        name, _, value = query.partition("=")
                        request.query.append((name, value))

        request.path = url_quote(request.path, "/()$=',")

        # add encoded queries to request.path.
        if request.query:
            request.path += "?"
            for name, value in request.query:
                if value is not None:
                    request.path += "{}={}{}".format(name, url_quote(value, "/()$=',"), "&")
            request.path = request.path[:-1]

        return request.path, request.query

    def _update_management_header(self, request):
        """
        Add additional headers for management.
        """

        if request.method in ["PUT", "POST", "MERGE", "DELETE"]:
            request.headers["Content-Length"] = str(len(request.body))

        # append additional headers base on the service
        #  request.headers.append(('x-ms-version', X_MS_VERSION))

        # if it is not GET or HEAD request, must set content-type.
        if request.method not in ["GET", "HEAD"]:
            for key in request.headers:
                if "content-type" == key.lower():
                    break
            else:
                request.headers["Content-Type"] = "application/xml"

        return request.headers

    def _parse_response(self, response, return_type):
        """
        Parse the HTTPResponse's body and fill all the data into a class of
        return_type.
        """

        return self._parse_response_body_from_xml_text(response=response, return_type=return_type)

    def _parse_response_body_from_xml_text(self, response, return_type):
        """
        parse the xml and fill all the data into a class of return_type
        """
        respbody = response.body

        doc = minidom.parseString(respbody)
        return_obj = return_type()
        for node in self._get_child_nodes(doc, return_type.__name__):
            self._fill_data_to_return_object(node, return_obj)

        # Note: We always explicitly assign status code to the custom return
        # type object
        return_obj.status = response.status

        return return_obj

    def _get_child_nodes(self, node, tag_name):
        return [
            childNode
            for childNode in node.getElementsByTagName(tag_name)
            if childNode.parentNode == node
        ]

    def _fill_data_to_return_object(self, node, return_obj):
        members = dict(vars(return_obj))
        for name, value in members.items():
            if isinstance(value, _ListOf):
                setattr(
                    return_obj,
                    name,
                    self._fill_list_of(node, value.list_type, value.xml_element_name),
                )
            elif isinstance(value, ScalarListOf):
                setattr(
                    return_obj,
                    name,
                    self._fill_scalar_list_of(
                        node,
                        value.list_type,
                        self._get_serialization_name(name),
                        value.xml_element_name,
                    ),
                )
            elif isinstance(value, _DictOf):
                setattr(
                    return_obj,
                    name,
                    self._fill_dict_of(
                        node,
                        self._get_serialization_name(name),
                        value.pair_xml_element_name,
                        value.key_xml_element_name,
                        value.value_xml_element_name,
                    ),
                )
            elif isinstance(value, WindowsAzureData):
                setattr(
                    return_obj,
                    name,
                    self._fill_instance_child(node, name, value.__class__),
                )
            elif isinstance(value, dict):
                setattr(
                    return_obj,
                    name,
                    self._fill_dict(node, self._get_serialization_name(name)),
                )
            elif isinstance(value, _Base64String):
                value = self._fill_data_minidom(node, name, "")
                if value is not None:
                    value = self._decode_base64_to_text(value)
                # always set the attribute,
                # so we don't end up returning an object
                # with type _Base64String
                setattr(return_obj, name, value)
            else:
                value = self._fill_data_minidom(node, name, value)
                if value is not None:
                    setattr(return_obj, name, value)

    def _fill_list_of(self, xmldoc, element_type, xml_element_name):
        xmlelements = self._get_child_nodes(xmldoc, xml_element_name)
        return [
            self._parse_response_body_from_xml_node(xmlelement, element_type)
            for xmlelement in xmlelements
        ]

    def _parse_response_body_from_xml_node(self, node, return_type):
        """
        parse the xml and fill all the data into a class of return_type
        """
        return_obj = return_type()
        self._fill_data_to_return_object(node, return_obj)

        return return_obj

    def _fill_scalar_list_of(self, xmldoc, element_type, parent_xml_element_name, xml_element_name):
        xmlelements = self._get_child_nodes(xmldoc, parent_xml_element_name)

        if xmlelements:
            xmlelements = self._get_child_nodes(xmlelements[0], xml_element_name)
            return [self._get_node_value(xmlelement, element_type) for xmlelement in xmlelements]

    def _get_node_value(self, xmlelement, data_type):
        value = xmlelement.firstChild.nodeValue
        if data_type is datetime:
            return self._to_datetime(value)
        elif data_type is bool:
            return value.lower() != "false"
        else:
            return data_type(value)

    def _get_serialization_name(self, element_name):
        """
        Converts a Python name into a serializable name.
        """

        known = _KNOWN_SERIALIZATION_XFORMS.get(element_name)
        if known is not None:
            return known

        if element_name.startswith("x_ms_"):
            return element_name.replace("_", "-")

        if element_name.endswith("_id"):
            element_name = element_name.replace("_id", "ID")

        for name in ["content_", "last_modified", "if_", "cache_control"]:
            if element_name.startswith(name):
                element_name = element_name.replace("_", "-_")

        return "".join(name.capitalize() for name in element_name.split("_"))

    def _fill_dict_of(
        self,
        xmldoc,
        parent_xml_element_name,
        pair_xml_element_name,
        key_xml_element_name,
        value_xml_element_name,
    ):
        return_obj = {}

        xmlelements = self._get_child_nodes(xmldoc, parent_xml_element_name)

        if xmlelements:
            xmlelements = self._get_child_nodes(xmlelements[0], pair_xml_element_name)
            for pair in xmlelements:
                keys = self._get_child_nodes(pair, key_xml_element_name)
                values = self._get_child_nodes(pair, value_xml_element_name)
                if keys and values:
                    key = keys[0].firstChild.nodeValue
                    value = values[0].firstChild.nodeValue
                    return_obj[key] = value

        return return_obj

    def _fill_instance_child(self, xmldoc, element_name, return_type):
        """
        Converts a child of the current dom element to the specified type.
        """
        xmlelements = self._get_child_nodes(xmldoc, self._get_serialization_name(element_name))

        if not xmlelements:
            return None

        return_obj = return_type()
        self._fill_data_to_return_object(xmlelements[0], return_obj)

        return return_obj

    def _fill_dict(self, xmldoc, element_name):
        xmlelements = self._get_child_nodes(xmldoc, element_name)

        if xmlelements:
            return_obj = {}
            for child in xmlelements[0].childNodes:
                if child.firstChild:
                    return_obj[child.nodeName] = child.firstChild.nodeValue
            return return_obj

    def _encode_base64(self, data):
        if isinstance(data, _unicode_type):
            data = data.encode("utf-8")
        encoded = base64.b64encode(data)
        return encoded.decode("utf-8")

    def _decode_base64_to_bytes(self, data):
        if isinstance(data, _unicode_type):
            data = data.encode("utf-8")
        return base64.b64decode(data)

    def _decode_base64_to_text(self, data):
        decoded_bytes = self._decode_base64_to_bytes(data)
        return decoded_bytes.decode("utf-8")

    def _fill_data_minidom(self, xmldoc, element_name, data_member):
        xmlelements = self._get_child_nodes(xmldoc, self._get_serialization_name(element_name))

        if not xmlelements or not xmlelements[0].childNodes:
            return None

        value = xmlelements[0].firstChild.nodeValue

        if data_member is None:
            return value
        elif isinstance(data_member, datetime):
            return self._to_datetime(value)
        elif type(data_member) is bool:
            return value.lower() != "false"
        elif type(data_member) is str:
            return _real_unicode(value)
        else:
            return type(data_member)(value)

    def _to_datetime(self, strtime):
        return datetime.strptime(strtime, "%Y-%m-%dT%H:%M:%S.%f")

    def _get_request_body(self, request_body):
        if request_body is None:
            return b""

        if isinstance(request_body, WindowsAzureData):
            request_body = self._convert_class_to_xml(request_body)

        if isinstance(request_body, bytes):
            return request_body

        if isinstance(request_body, _unicode_type):
            return request_body.encode("utf-8")

        request_body = str(request_body)
        if isinstance(request_body, _unicode_type):
            return request_body.encode("utf-8")

        return request_body

    def _convert_class_to_xml(self, source, xml_prefix=True):
        root = ET.Element()
        doc = self._construct_element_tree(source, root)

        result = ensure_string(ET.tostring(doc, encoding="utf-8", method="xml"))
        return result

    def _construct_element_tree(self, source, etree):
        if source is None:
            return ET.Element()

        if isinstance(source, list):
            for value in source:
                etree.append(self._construct_element_tree(value, etree))

        elif isinstance(source, WindowsAzureData):
            class_name = source.__class__.__name__
            etree.append(ET.Element(class_name))

            for name, value in vars(source).items():
                if value is not None:
                    if isinstance(value, list) or isinstance(value, WindowsAzureData):
                        etree.append(self._construct_element_tree(value, etree))
                    else:
                        ele = ET.Element(self._get_serialization_name(name))
                        ele.text = xml_escape(str(value))
                        etree.append(ele)

            etree.append(ET.Element(class_name))
        return etree

    def _parse_response_for_async_op(self, response):
        if response is None:
            return None

        result = AsynchronousOperationResult()
        if response.headers:
            for name, value in response.headers.items():
                if name.lower() == "x-ms-request-id":
                    result.request_id = value

        return result

    def _get_deployment_path_using_name(self, service_name, deployment_name=None):
        components = ["services/hostedservices/", _str(service_name), "/deployments"]
        resource = "".join(components)
        return self._get_path(resource, deployment_name)

    def _get_path(self, resource, name):
        path = "/" + self.subscription_id + "/" + resource
        if name is not None:
            path += "/" + _str(name)
        return path

    def _get_image_path(self, image_name=None):
        return self._get_path("services/images", image_name)

    def _get_vmimage_path(self, image_name=None):
        return self._get_path("services/vmimages", image_name)

    def _get_hosted_service_path(self, service_name=None):
        return self._get_path("services/hostedservices", service_name)

    def _get_deployment_path_using_slot(self, service_name, slot=None):
        return self._get_path(
            "services/hostedservices/%s/deploymentslots" % (_str(service_name)), slot
        )

    def _get_disk_path(self, disk_name=None):
        return self._get_path("services/disks", disk_name)

    def _get_role_path(self, service_name, deployment_name, role_name=None):
        components = [
            "services/hostedservices/",
            _str(service_name),
            "/deployments/",
            deployment_name,
            "/roles",
        ]
        resource = "".join(components)
        return self._get_path(resource, role_name)

    def _get_storage_service_path(self, service_name=None):
        return self._get_path("services/storageservices", service_name)

    def _ex_complete_async_azure_operation(self, response=None, operation_type="create_node"):
        request_id = self._parse_response_for_async_op(response)
        operation_status = self._get_operation_status(request_id.request_id)

        timeout = 60 * 5
        waittime = 0
        interval = 5

        while operation_status.status == "InProgress" and waittime < timeout:
            operation_status = self._get_operation_status(request_id)
            if operation_status.status == "Succeeded":
                break

            waittime += interval
            time.sleep(interval)

        if operation_status.status == "Failed":
            raise LibcloudError(
                "Message: Async request for operation %s has failed" % operation_type,
                driver=self.connection.driver,
            )

    def raise_for_response(self, response, valid_response):
        if response.status != valid_response:
            values = (response.error, response.body, response.status)
            message = "Message: %s, Body: %s, Status code: %s" % (values)
            raise LibcloudError(message, driver=self)


"""
XML Serializer

Borrowed from the Azure SDK for Python which is licensed under Apache 2.0.

https://github.com/Azure/azure-sdk-for-python
"""


def _lower(text):
    return text.lower()


class AzureXmlSerializer:
    @staticmethod
    def create_storage_service_input_to_xml(
        service_name,
        description,
        label,
        affinity_group,
        location,
        geo_replication_enabled,
        extended_properties,
    ):
        return AzureXmlSerializer.doc_from_data(
            "CreateStorageServiceInput",
            [
                ("ServiceName", service_name),
                ("Description", description),
                ("Label", label),
                ("AffinityGroup", affinity_group),
                ("Location", location),
                ("GeoReplicationEnabled", geo_replication_enabled, _lower),
            ],
            extended_properties,
        )

    @staticmethod
    def update_storage_service_input_to_xml(
        description, label, geo_replication_enabled, extended_properties
    ):
        return AzureXmlSerializer.doc_from_data(
            "UpdateStorageServiceInput",
            [
                ("Description", description),
                ("Label", label, AzureNodeDriver._encode_base64),
                ("GeoReplicationEnabled", geo_replication_enabled, _lower),
            ],
            extended_properties,
        )

    @staticmethod
    def regenerate_keys_to_xml(key_type):
        return AzureXmlSerializer.doc_from_data("RegenerateKeys", [("KeyType", key_type)])

    @staticmethod
    def update_hosted_service_to_xml(label, description, extended_properties):
        return AzureXmlSerializer.doc_from_data(
            "UpdateHostedService",
            [
                ("Label", label, AzureNodeDriver._encode_base64),
                ("Description", description),
            ],
            extended_properties,
        )

    @staticmethod
    def create_hosted_service_to_xml(
        service_name,
        label,
        description,
        location,
        affinity_group=None,
        extended_properties=None,
    ):
        if affinity_group:
            return AzureXmlSerializer.doc_from_data(
                "CreateHostedService",
                [
                    ("ServiceName", service_name),
                    ("Label", label),
                    ("Description", description),
                    ("AffinityGroup", affinity_group),
                ],
                extended_properties,
            )

        return AzureXmlSerializer.doc_from_data(
            "CreateHostedService",
            [
                ("ServiceName", service_name),
                ("Label", label),
                ("Description", description),
                ("Location", location),
            ],
            extended_properties,
        )

    @staticmethod
    def create_storage_service_to_xml(
        service_name,
        label,
        description,
        location,
        affinity_group,
        extended_properties=None,
    ):
        return AzureXmlSerializer.doc_from_data(
            "CreateStorageServiceInput",
            [
                ("ServiceName", service_name),
                ("Label", label),
                ("Description", description),
                ("Location", location),
                ("AffinityGroup", affinity_group),
            ],
            extended_properties,
        )

    @staticmethod
    def create_deployment_to_xml(
        name,
        package_url,
        label,
        configuration,
        start_deployment,
        treat_warnings_as_error,
        extended_properties,
    ):
        return AzureXmlSerializer.doc_from_data(
            "CreateDeployment",
            [
                ("Name", name),
                ("PackageUrl", package_url),
                ("Label", label, AzureNodeDriver._encode_base64),
                ("Configuration", configuration),
                ("StartDeployment", start_deployment, _lower),
                ("TreatWarningsAsError", treat_warnings_as_error, _lower),
            ],
            extended_properties,
        )

    @staticmethod
    def swap_deployment_to_xml(production, source_deployment):
        return AzureXmlSerializer.doc_from_data(
            "Swap",
            [("Production", production), ("SourceDeployment", source_deployment)],
        )

    @staticmethod
    def update_deployment_status_to_xml(status):
        return AzureXmlSerializer.doc_from_data("UpdateDeploymentStatus", [("Status", status)])

    @staticmethod
    def change_deployment_to_xml(configuration, treat_warnings_as_error, mode, extended_properties):
        return AzureXmlSerializer.doc_from_data(
            "ChangeConfiguration",
            [
                ("Configuration", configuration),
                ("TreatWarningsAsError", treat_warnings_as_error, _lower),
                ("Mode", mode),
            ],
            extended_properties,
        )

    @staticmethod
    def upgrade_deployment_to_xml(
        mode,
        package_url,
        configuration,
        label,
        role_to_upgrade,
        force,
        extended_properties,
    ):
        return AzureXmlSerializer.doc_from_data(
            "UpgradeDeployment",
            [
                ("Mode", mode),
                ("PackageUrl", package_url),
                ("Configuration", configuration),
                ("Label", label, AzureNodeDriver._encode_base64),
                ("RoleToUpgrade", role_to_upgrade),
                ("Force", force, _lower),
            ],
            extended_properties,
        )

    @staticmethod
    def rollback_upgrade_to_xml(mode, force):
        return AzureXmlSerializer.doc_from_data(
            "RollbackUpdateOrUpgrade", [("Mode", mode), ("Force", force, _lower)]
        )

    @staticmethod
    def walk_upgrade_domain_to_xml(upgrade_domain):
        return AzureXmlSerializer.doc_from_data(
            "WalkUpgradeDomain", [("UpgradeDomain", upgrade_domain)]
        )

    @staticmethod
    def certificate_file_to_xml(data, certificate_format, password):
        return AzureXmlSerializer.doc_from_data(
            "CertificateFile",
            [
                ("Data", data),
                ("CertificateFormat", certificate_format),
                ("Password", password),
            ],
        )

    @staticmethod
    def create_affinity_group_to_xml(name, label, description, location):
        return AzureXmlSerializer.doc_from_data(
            "CreateAffinityGroup",
            [
                ("Name", name),
                ("Label", label, AzureNodeDriver._encode_base64),
                ("Description", description),
                ("Location", location),
            ],
        )

    @staticmethod
    def update_affinity_group_to_xml(label, description):
        return AzureXmlSerializer.doc_from_data(
            "UpdateAffinityGroup",
            [
                ("Label", label, AzureNodeDriver._encode_base64),
                ("Description", description),
            ],
        )

    @staticmethod
    def subscription_certificate_to_xml(public_key, thumbprint, data):
        return AzureXmlSerializer.doc_from_data(
            "SubscriptionCertificate",
            [
                ("SubscriptionCertificatePublicKey", public_key),
                ("SubscriptionCertificateThumbprint", thumbprint),
                ("SubscriptionCertificateData", data),
            ],
        )

    @staticmethod
    def os_image_to_xml(label, media_link, name, os):
        return AzureXmlSerializer.doc_from_data(
            "OSImage",
            [("Label", label), ("MediaLink", media_link), ("Name", name), ("OS", os)],
        )

    @staticmethod
    def data_virtual_hard_disk_to_xml(
        host_caching,
        disk_label,
        disk_name,
        lun,
        logical_disk_size_in_gb,
        media_link,
        source_media_link,
    ):
        return AzureXmlSerializer.doc_from_data(
            "DataVirtualHardDisk",
            [
                ("HostCaching", host_caching),
                ("DiskLabel", disk_label),
                ("DiskName", disk_name),
                ("Lun", lun),
                ("LogicalDiskSizeInGB", logical_disk_size_in_gb),
                ("MediaLink", media_link),
                ("SourceMediaLink", source_media_link),
            ],
        )

    @staticmethod
    def disk_to_xml(has_operating_system, label, media_link, name, os):
        return AzureXmlSerializer.doc_from_data(
            "Disk",
            [
                ("HasOperatingSystem", has_operating_system, _lower),
                ("Label", label),
                ("MediaLink", media_link),
                ("Name", name),
                ("OS", os),
            ],
        )

    @staticmethod
    def restart_role_operation_to_xml():
        xml = ET.Element("OperationType")
        xml.text = "RestartRoleOperation"
        doc = AzureXmlSerializer.doc_from_xml("RestartRoleOperation", xml)
        result = ensure_string(ET.tostring(doc, encoding="utf-8"))
        return result

    @staticmethod
    def shutdown_role_operation_to_xml():
        xml = ET.Element("OperationType")
        xml.text = "ShutdownRoleOperation"
        doc = AzureXmlSerializer.doc_from_xml("ShutdownRoleOperation", xml)
        result = ensure_string(ET.tostring(doc, encoding="utf-8"))
        return result

    @staticmethod
    def start_role_operation_to_xml():
        xml = ET.Element("OperationType")
        xml.text = "StartRoleOperation"
        doc = AzureXmlSerializer.doc_from_xml("StartRoleOperation", xml)
        result = ensure_string(ET.tostring(doc, encoding="utf-8"))
        return result

    @staticmethod
    def windows_configuration_to_xml(configuration, xml):
        AzureXmlSerializer.data_to_xml(
            [("ConfigurationSetType", configuration.configuration_set_type)], xml
        )
        AzureXmlSerializer.data_to_xml([("ComputerName", configuration.computer_name)], xml)
        AzureXmlSerializer.data_to_xml([("AdminPassword", configuration.admin_password)], xml)
        AzureXmlSerializer.data_to_xml(
            [
                (
                    "ResetPasswordOnFirstLogon",
                    configuration.reset_password_on_first_logon,
                    _lower,
                )
            ],
            xml,
        )

        AzureXmlSerializer.data_to_xml(
            [
                (
                    "EnableAutomaticUpdates",
                    configuration.enable_automatic_updates,
                    _lower,
                )
            ],
            xml,
        )

        AzureXmlSerializer.data_to_xml([("TimeZone", configuration.time_zone)], xml)

        if configuration.domain_join is not None:
            domain = ET.xml("DomainJoin")  # pylint: disable=no-member
            creds = ET.xml("Credentials")  # pylint: disable=no-member
            domain.appemnd(creds)
            xml.append(domain)

            AzureXmlSerializer.data_to_xml(
                [("Domain", configuration.domain_join.credentials.domain)], creds
            )

            AzureXmlSerializer.data_to_xml(
                [("Username", configuration.domain_join.credentials.username)], creds
            )
            AzureXmlSerializer.data_to_xml(
                [("Password", configuration.domain_join.credentials.password)], creds
            )

            AzureXmlSerializer.data_to_xml(
                [("JoinDomain", configuration.domain_join.join_domain)], domain
            )

            AzureXmlSerializer.data_to_xml(
                [("MachineObjectOU", configuration.domain_join.machine_object_ou)],
                domain,
            )

        if configuration.stored_certificate_settings is not None:
            cert_settings = ET.Element("StoredCertificateSettings")
            xml.append(cert_settings)
            for cert in configuration.stored_certificate_settings:
                cert_setting = ET.Element("CertificateSetting")
                cert_settings.append(cert_setting)

                cert_setting.append(
                    AzureXmlSerializer.data_to_xml([("StoreLocation", cert.store_location)])
                )
                AzureXmlSerializer.data_to_xml([("StoreName", cert.store_name)], cert_setting)
                AzureXmlSerializer.data_to_xml([("Thumbprint", cert.thumbprint)], cert_setting)

        AzureXmlSerializer.data_to_xml([("AdminUsername", configuration.admin_user_name)], xml)
        return xml

    @staticmethod
    def linux_configuration_to_xml(configuration, xml):
        AzureXmlSerializer.data_to_xml(
            [("ConfigurationSetType", configuration.configuration_set_type)], xml
        )
        AzureXmlSerializer.data_to_xml([("HostName", configuration.host_name)], xml)
        AzureXmlSerializer.data_to_xml([("UserName", configuration.user_name)], xml)
        AzureXmlSerializer.data_to_xml([("UserPassword", configuration.user_password)], xml)
        AzureXmlSerializer.data_to_xml(
            [
                (
                    "DisableSshPasswordAuthentication",
                    configuration.disable_ssh_password_authentication,
                    _lower,
                )
            ],
            xml,
        )

        if configuration.ssh is not None:
            ssh = ET.Element("SSH")
            pkeys = ET.Element("PublicKeys")
            kpairs = ET.Element("KeyPairs")
            ssh.append(pkeys)
            ssh.append(kpairs)
            xml.append(ssh)

            for key in configuration.ssh.public_keys:
                pkey = ET.Element("PublicKey")
                pkeys.append(pkey)
                AzureXmlSerializer.data_to_xml([("Fingerprint", key.fingerprint)], pkey)
                AzureXmlSerializer.data_to_xml([("Path", key.path)], pkey)

            for key in configuration.ssh.key_pairs:
                kpair = ET.Element("KeyPair")
                kpairs.append(kpair)
                AzureXmlSerializer.data_to_xml([("Fingerprint", key.fingerprint)], kpair)
                AzureXmlSerializer.data_to_xml([("Path", key.path)], kpair)

        if configuration.custom_data is not None:
            AzureXmlSerializer.data_to_xml([("CustomData", configuration.custom_data)], xml)

        return xml

    @staticmethod
    def network_configuration_to_xml(configuration, xml):
        AzureXmlSerializer.data_to_xml(
            [("ConfigurationSetType", configuration.configuration_set_type)], xml
        )

        input_endpoints = ET.Element("InputEndpoints")
        xml.append(input_endpoints)

        for endpoint in configuration.input_endpoints:
            input_endpoint = ET.Element("InputEndpoint")
            input_endpoints.append(input_endpoint)

            AzureXmlSerializer.data_to_xml(
                [
                    (
                        "LoadBalancedEndpointSetName",
                        endpoint.load_balanced_endpoint_set_name,
                    )
                ],
                input_endpoint,
            )

            AzureXmlSerializer.data_to_xml([("LocalPort", endpoint.local_port)], input_endpoint)
            AzureXmlSerializer.data_to_xml([("Name", endpoint.name)], input_endpoint)
            AzureXmlSerializer.data_to_xml([("Port", endpoint.port)], input_endpoint)

            if (
                endpoint.load_balancer_probe.path
                or endpoint.load_balancer_probe.port
                or endpoint.load_balancer_probe.protocol
            ):
                load_balancer_probe = ET.Element("LoadBalancerProbe")
                input_endpoint.append(load_balancer_probe)
                AzureXmlSerializer.data_to_xml(
                    [("Path", endpoint.load_balancer_probe.path)], load_balancer_probe
                )
                AzureXmlSerializer.data_to_xml(
                    [("Port", endpoint.load_balancer_probe.port)], load_balancer_probe
                )
                AzureXmlSerializer.data_to_xml(
                    [("Protocol", endpoint.load_balancer_probe.protocol)],
                    load_balancer_probe,
                )

            AzureXmlSerializer.data_to_xml([("Protocol", endpoint.protocol)], input_endpoint)
            AzureXmlSerializer.data_to_xml(
                [
                    (
                        "EnableDirectServerReturn",
                        endpoint.enable_direct_server_return,
                        _lower,
                    )
                ],
                input_endpoint,
            )

        subnet_names = ET.Element("SubnetNames")
        xml.append(subnet_names)
        for name in configuration.subnet_names:
            AzureXmlSerializer.data_to_xml([("SubnetName", name)], subnet_names)

        return xml

    @staticmethod
    def role_to_xml(
        availability_set_name,
        data_virtual_hard_disks,
        network_configuration_set,
        os_virtual_hard_disk,
        vm_image_name,
        role_name,
        role_size,
        role_type,
        system_configuration_set,
        xml,
    ):
        AzureXmlSerializer.data_to_xml([("RoleName", role_name)], xml)
        AzureXmlSerializer.data_to_xml([("RoleType", role_type)], xml)

        config_sets = ET.Element("ConfigurationSets")
        xml.append(config_sets)

        if system_configuration_set is not None:
            config_set = ET.Element("ConfigurationSet")
            config_sets.append(config_set)

            if isinstance(system_configuration_set, WindowsConfigurationSet):
                AzureXmlSerializer.windows_configuration_to_xml(
                    system_configuration_set, config_set
                )
            elif isinstance(system_configuration_set, LinuxConfigurationSet):
                AzureXmlSerializer.linux_configuration_to_xml(system_configuration_set, config_set)

        if network_configuration_set is not None:
            config_set = ET.Element("ConfigurationSet")
            config_sets.append(config_set)

            AzureXmlSerializer.network_configuration_to_xml(network_configuration_set, config_set)

        if availability_set_name is not None:
            AzureXmlSerializer.data_to_xml([("AvailabilitySetName", availability_set_name)], xml)

        if data_virtual_hard_disks is not None:
            vhds = ET.Element("DataVirtualHardDisks")
            xml.append(vhds)

            for hd in data_virtual_hard_disks:
                vhd = ET.Element("DataVirtualHardDisk")
                vhds.append(vhd)
                AzureXmlSerializer.data_to_xml([("HostCaching", hd.host_caching)], vhd)
                AzureXmlSerializer.data_to_xml([("DiskLabel", hd.disk_label)], vhd)
                AzureXmlSerializer.data_to_xml([("DiskName", hd.disk_name)], vhd)
                AzureXmlSerializer.data_to_xml([("Lun", hd.lun)], vhd)
                AzureXmlSerializer.data_to_xml(
                    [("LogicalDiskSizeInGB", hd.logical_disk_size_in_gb)], vhd
                )
                AzureXmlSerializer.data_to_xml([("MediaLink", hd.media_link)], vhd)

        if os_virtual_hard_disk is not None:
            hd = ET.Element("OSVirtualHardDisk")
            xml.append(hd)
            AzureXmlSerializer.data_to_xml([("HostCaching", os_virtual_hard_disk.host_caching)], hd)
            AzureXmlSerializer.data_to_xml([("DiskLabel", os_virtual_hard_disk.disk_label)], hd)
            AzureXmlSerializer.data_to_xml([("DiskName", os_virtual_hard_disk.disk_name)], hd)
            AzureXmlSerializer.data_to_xml([("MediaLink", os_virtual_hard_disk.media_link)], hd)
            AzureXmlSerializer.data_to_xml(
                [("SourceImageName", os_virtual_hard_disk.source_image_name)], hd
            )

        if vm_image_name is not None:
            AzureXmlSerializer.data_to_xml([("VMImageName", vm_image_name)], xml)

        if role_size is not None:
            AzureXmlSerializer.data_to_xml([("RoleSize", role_size)], xml)

        return xml

    @staticmethod
    def add_role_to_xml(
        role_name,
        system_configuration_set,
        os_virtual_hard_disk,
        role_type,
        network_configuration_set,
        availability_set_name,
        data_virtual_hard_disks,
        vm_image_name,
        role_size,
    ):
        doc = AzureXmlSerializer.doc_from_xml("PersistentVMRole")
        xml = AzureXmlSerializer.role_to_xml(
            availability_set_name,
            data_virtual_hard_disks,
            network_configuration_set,
            os_virtual_hard_disk,
            vm_image_name,
            role_name,
            role_size,
            role_type,
            system_configuration_set,
            doc,
        )
        result = ensure_string(ET.tostring(xml, encoding="utf-8"))
        return result

    @staticmethod
    def update_role_to_xml(
        role_name,
        os_virtual_hard_disk,
        role_type,
        network_configuration_set,
        availability_set_name,
        data_virtual_hard_disks,
        vm_image_name,
        role_size,
    ):
        doc = AzureXmlSerializer.doc_from_xml("PersistentVMRole")
        AzureXmlSerializer.role_to_xml(
            availability_set_name,
            data_virtual_hard_disks,
            network_configuration_set,
            os_virtual_hard_disk,
            vm_image_name,
            role_name,
            role_size,
            role_type,
            None,
            doc,
        )

        result = ensure_string(ET.tostring(doc, encoding="utf-8"))
        return result

    @staticmethod
    def capture_role_to_xml(
        post_capture_action,
        target_image_name,
        target_image_label,
        provisioning_configuration,
    ):
        xml = AzureXmlSerializer.data_to_xml([("OperationType", "CaptureRoleOperation")])
        AzureXmlSerializer.data_to_xml([("PostCaptureAction", post_capture_action)], xml)

        if provisioning_configuration is not None:
            provisioning_config = ET.Element("ProvisioningConfiguration")
            xml.append(provisioning_config)

            if isinstance(provisioning_configuration, WindowsConfigurationSet):
                AzureXmlSerializer.windows_configuration_to_xml(
                    provisioning_configuration, provisioning_config
                )
            elif isinstance(provisioning_configuration, LinuxConfigurationSet):
                AzureXmlSerializer.linux_configuration_to_xml(
                    provisioning_configuration, provisioning_config
                )

        AzureXmlSerializer.data_to_xml([("TargetImageLabel", target_image_label)], xml)
        AzureXmlSerializer.data_to_xml([("TargetImageName", target_image_name)], xml)
        doc = AzureXmlSerializer.doc_from_xml("CaptureRoleOperation", xml)
        result = ensure_string(ET.tostring(doc, encoding="utf-8"))
        return result

    @staticmethod
    def virtual_machine_deployment_to_xml(
        deployment_name,
        deployment_slot,
        label,
        role_name,
        system_configuration_set,
        os_virtual_hard_disk,
        role_type,
        network_configuration_set,
        availability_set_name,
        data_virtual_hard_disks,
        role_size,
        virtual_network_name,
        vm_image_name,
    ):
        doc = AzureXmlSerializer.doc_from_xml("Deployment")
        AzureXmlSerializer.data_to_xml([("Name", deployment_name)], doc)
        AzureXmlSerializer.data_to_xml([("DeploymentSlot", deployment_slot)], doc)
        AzureXmlSerializer.data_to_xml([("Label", label)], doc)

        role_list = ET.Element("RoleList")
        role = ET.Element("Role")
        role_list.append(role)
        doc.append(role_list)

        AzureXmlSerializer.role_to_xml(
            availability_set_name,
            data_virtual_hard_disks,
            network_configuration_set,
            os_virtual_hard_disk,
            vm_image_name,
            role_name,
            role_size,
            role_type,
            system_configuration_set,
            role,
        )

        if virtual_network_name is not None:
            doc.append(
                AzureXmlSerializer.data_to_xml([("VirtualNetworkName", virtual_network_name)])
            )

        result = ensure_string(ET.tostring(doc, encoding="utf-8"))
        return result

    @staticmethod
    def data_to_xml(data, xml=None):
        """
        Creates an xml fragment from the specified data.
           data: Array of tuples, where first: xml element name
                                        second: xml element text
                                        third: conversion function
        """

        for element in data:
            name = element[0]
            val = element[1]
            if len(element) > 2:
                converter = element[2]
            else:
                converter = None

            if val is not None:
                if converter is not None:
                    text = _str(converter(_str(val)))
                else:
                    text = _str(val)

                entry = ET.Element(name)
                entry.text = text
                if xml is not None:
                    xml.append(entry)
                else:
                    return entry
        return xml

    @staticmethod
    def doc_from_xml(document_element_name, inner_xml=None):
        """
        Wraps the specified xml in an xml root element with default azure
        namespaces
        """
        # Note: Namespaces don't work consistency in Python 2 and 3.
        """
        nsmap = {
            None: "http://www.w3.org/2001/XMLSchema-instance",
            "i": "http://www.w3.org/2001/XMLSchema-instance"
        }

        xml.attrib["xmlns:i"] = "http://www.w3.org/2001/XMLSchema-instance"
        xml.attrib["xmlns"] = "http://schemas.microsoft.com/windowsazure"
        """
        xml = ET.Element(document_element_name)
        xml.set("xmlns", "http://schemas.microsoft.com/windowsazure")

        if inner_xml is not None:
            xml.append(inner_xml)

        return xml

    @staticmethod
    def doc_from_data(document_element_name, data, extended_properties=None):
        doc = AzureXmlSerializer.doc_from_xml(document_element_name)
        AzureXmlSerializer.data_to_xml(data, doc)
        if extended_properties is not None:
            doc.append(
                AzureXmlSerializer.extended_properties_dict_to_xml_fragment(extended_properties)
            )

        result = ensure_string(ET.tostring(doc, encoding="utf-8"))
        return result

    @staticmethod
    def extended_properties_dict_to_xml_fragment(extended_properties):
        if extended_properties is not None and len(extended_properties) > 0:
            xml = ET.Element("ExtendedProperties")
            for key, val in extended_properties.items():
                extended_property = ET.Element("ExtendedProperty")
                name = ET.Element("Name")
                name.text = _str(key)
                value = ET.Element("Value")
                value.text = _str(val)

                extended_property.append(name)
                extended_property.append(value)
                xml.append(extended_property)

            return xml


"""
Data Classes

Borrowed from the Azure SDK for Python.
"""


class WindowsAzureData:
    """
    This is the base of data class.
    It is only used to check whether it is instance or not.
    """

    pass


class WindowsAzureDataTypedList(WindowsAzureData):
    list_type = None
    xml_element_name = None

    def __init__(self):
        self.items = _ListOf(self.list_type, self.xml_element_name)

    def __iter__(self):
        return iter(self.items)

    def __len__(self):
        return len(self.items)

    def __getitem__(self, index):
        return self.items[index]


class OSVirtualHardDisk(WindowsAzureData):
    def __init__(
        self,
        source_image_name=None,
        media_link=None,
        host_caching=None,
        disk_label=None,
        disk_name=None,
    ):
        self.source_image_name = source_image_name
        self.media_link = media_link
        self.host_caching = host_caching
        self.disk_label = disk_label
        self.disk_name = disk_name
        self.os = ""  # undocumented, not used when adding a role


class LinuxConfigurationSet(WindowsAzureData):
    def __init__(
        self,
        host_name=None,
        user_name=None,
        user_password=None,
        disable_ssh_password_authentication=None,
        custom_data=None,
    ):
        self.configuration_set_type = "LinuxProvisioningConfiguration"
        self.host_name = host_name
        self.user_name = user_name
        self.user_password = user_password
        self.disable_ssh_password_authentication = disable_ssh_password_authentication
        self.ssh = SSH()
        self.custom_data = custom_data


class WindowsConfigurationSet(WindowsAzureData):
    def __init__(
        self,
        computer_name=None,
        admin_password=None,
        reset_password_on_first_logon=None,
        enable_automatic_updates=None,
        time_zone=None,
        admin_user_name=None,
    ):
        self.configuration_set_type = "WindowsProvisioningConfiguration"
        self.computer_name = computer_name
        self.admin_password = admin_password
        self.reset_password_on_first_logon = reset_password_on_first_logon
        self.enable_automatic_updates = enable_automatic_updates
        self.time_zone = time_zone
        self.admin_user_name = admin_user_name
        self.domain_join = DomainJoin()
        self.stored_certificate_settings = StoredCertificateSettings()


class DomainJoin(WindowsAzureData):
    def __init__(self):
        self.credentials = Credentials()
        self.join_domain = ""
        self.machine_object_ou = ""


class Credentials(WindowsAzureData):
    def __init__(self):
        self.domain = ""
        self.username = ""
        self.password = ""


class CertificateSetting(WindowsAzureData):
    """
    Initializes a certificate setting.

    thumbprint:
        Specifies the thumbprint of the certificate to be provisioned. The
        thumbprint must specify an existing service certificate.
    store_name:
        Specifies the name of the certificate store from which retrieve
        certificate.
    store_location:
        Specifies the target certificate store location on the virtual machine
        The only supported value is LocalMachine.
    """

    def __init__(self, thumbprint="", store_name="", store_location=""):
        self.thumbprint = thumbprint
        self.store_name = store_name
        self.store_location = store_location


class StoredCertificateSettings(WindowsAzureDataTypedList):
    list_type = CertificateSetting

    _repr_attributes = ["items"]


class SSH(WindowsAzureData):
    def __init__(self):
        self.public_keys = PublicKeys()
        self.key_pairs = KeyPairs()


class PublicKey(WindowsAzureData):
    def __init__(self, fingerprint="", path=""):
        self.fingerprint = fingerprint
        self.path = path


class PublicKeys(WindowsAzureDataTypedList):
    list_type = PublicKey

    _repr_attributes = ["items"]


class AzureKeyPair(WindowsAzureData):
    def __init__(self, fingerprint="", path=""):
        self.fingerprint = fingerprint
        self.path = path


class KeyPairs(WindowsAzureDataTypedList):
    list_type = AzureKeyPair

    _repr_attributes = ["items"]


class LoadBalancerProbe(WindowsAzureData):
    def __init__(self):
        self.path = ""
        self.port = ""
        self.protocol = ""


class ConfigurationSet(WindowsAzureData):
    def __init__(self):
        self.configuration_set_type = ""
        self.role_type = ""
        self.input_endpoints = ConfigurationSetInputEndpoints()
        self.subnet_names = ScalarListOf(str, "SubnetName")


class ConfigurationSets(WindowsAzureDataTypedList):
    list_type = ConfigurationSet

    _repr_attributes = ["items"]


class ConfigurationSetInputEndpoint(WindowsAzureData):
    def __init__(
        self,
        name="",
        protocol="",
        port="",
        local_port="",
        load_balanced_endpoint_set_name="",
        enable_direct_server_return=False,
    ):
        self.enable_direct_server_return = enable_direct_server_return
        self.load_balanced_endpoint_set_name = load_balanced_endpoint_set_name
        self.local_port = local_port
        self.name = name
        self.port = port
        self.load_balancer_probe = LoadBalancerProbe()
        self.protocol = protocol


class ConfigurationSetInputEndpoints(WindowsAzureDataTypedList):
    list_type = ConfigurationSetInputEndpoint
    xml_element_name = "InputEndpoint"

    _repr_attributes = ["items"]


class Location(WindowsAzureData):
    def __init__(self):
        self.name = ""
        self.display_name = ""
        self.available_services = ScalarListOf(str, "AvailableService")
        self.compute_capabilities = ComputeCapability()


class Locations(WindowsAzureDataTypedList):
    list_type = Location

    _repr_attributes = ["items"]


class ComputeCapability(WindowsAzureData):
    def __init__(self):
        self.virtual_machines_role_sizes = ScalarListOf(str, "RoleSize")


class VirtualMachinesRoleSizes(WindowsAzureData):
    def __init__(self):
        self.role_size = ScalarListOf(str, "RoleSize")


class OSImage(WindowsAzureData):
    def __init__(self):
        self.affinity_group = ""
        self.category = ""
        self.location = ""
        self.logical_size_in_gb = 0
        self.label = ""
        self.media_link = ""
        self.name = ""
        self.os = ""
        self.eula = ""
        self.description = ""


class Images(WindowsAzureDataTypedList):
    list_type = OSImage

    _repr_attributes = ["items"]


class VMImage(WindowsAzureData):
    def __init__(self):
        self.name = ""
        self.label = ""
        self.category = ""
        self.os_disk_configuration = OSDiskConfiguration()
        self.service_name = ""
        self.deployment_name = ""
        self.role_name = ""
        self.location = ""
        self.affinity_group = ""


class VMImages(WindowsAzureDataTypedList):
    list_type = VMImage

    _repr_attributes = ["items"]


class VirtualIP(WindowsAzureData):
    def __init__(self):
        self.address = ""
        self.is_dns_programmed = ""
        self.name = ""


class VirtualIPs(WindowsAzureDataTypedList):
    list_type = VirtualIP

    _repr_attributes = ["items"]


class HostedService(WindowsAzureData, ReprMixin):
    _repr_attributes = ["service_name", "url"]

    def __init__(self):
        self.url = ""
        self.service_name = ""
        self.hosted_service_properties = HostedServiceProperties()
        self.deployments = Deployments()


class HostedServices(WindowsAzureDataTypedList, ReprMixin):
    list_type = HostedService

    _repr_attributes = ["items"]


class HostedServiceProperties(WindowsAzureData):
    def __init__(self):
        self.description = ""
        self.location = ""
        self.affinity_group = ""
        self.label = _Base64String()
        self.status = ""
        self.date_created = ""
        self.date_last_modified = ""
        self.extended_properties = _DictOf("ExtendedProperty", "Name", "Value")


class Deployment(WindowsAzureData):
    def __init__(self):
        self.name = ""
        self.deployment_slot = ""
        self.private_id = ""
        self.status = ""
        self.label = _Base64String()
        self.url = ""
        self.configuration = _Base64String()
        self.role_instance_list = RoleInstanceList()
        self.upgrade_status = UpgradeStatus()
        self.upgrade_domain_count = ""
        self.role_list = RoleList()
        self.sdk_version = ""
        self.input_endpoint_list = InputEndpoints()
        self.locked = False
        self.rollback_allowed = False
        self.persistent_vm_downtime_info = PersistentVMDowntimeInfo()
        self.created_time = ""
        self.last_modified_time = ""
        self.extended_properties = _DictOf("ExtendedProperty", "Name", "Value")
        self.virtual_ips = VirtualIPs()


class Deployments(WindowsAzureDataTypedList):
    list_type = Deployment

    _repr_attributes = ["items"]


class UpgradeStatus(WindowsAzureData):
    def __init__(self):
        self.upgrade_type = ""
        self.current_upgrade_domain_state = ""
        self.current_upgrade_domain = ""


class RoleInstance(WindowsAzureData):
    def __init__(self):
        self.role_name = ""
        self.instance_name = ""
        self.instance_status = ""
        self.instance_upgrade_domain = 0
        self.instance_fault_domain = 0
        self.instance_size = ""
        self.instance_state_details = ""
        self.instance_error_code = ""
        self.ip_address = ""
        self.instance_endpoints = InstanceEndpoints()
        self.power_state = ""
        self.fqdn = ""
        self.host_name = ""


class RoleInstanceList(WindowsAzureDataTypedList):
    list_type = RoleInstance

    _repr_attributes = ["items"]


class InstanceEndpoint(WindowsAzureData):
    def __init__(self):
        self.name = ""
        self.vip = ""
        self.public_port = ""
        self.local_port = ""
        self.protocol = ""


class InstanceEndpoints(WindowsAzureDataTypedList):
    list_type = InstanceEndpoint

    _repr_attributes = ["items"]


class InputEndpoint(WindowsAzureData):
    def __init__(self):
        self.role_name = ""
        self.vip = ""
        self.port = ""


class InputEndpoints(WindowsAzureDataTypedList):
    list_type = InputEndpoint

    _repr_attributes = ["items"]


class Role(WindowsAzureData):
    def __init__(self):
        self.role_name = ""
        self.os_version = ""


class RoleList(WindowsAzureDataTypedList):
    list_type = Role

    _repr_attributes = ["items"]


class PersistentVMDowntimeInfo(WindowsAzureData):
    def __init__(self):
        self.start_time = ""
        self.end_time = ""
        self.status = ""


class AsynchronousOperationResult(WindowsAzureData):
    def __init__(self, request_id=None):
        self.request_id = request_id


class Disk(WindowsAzureData):
    def __init__(self):
        self.affinity_group = ""
        self.attached_to = AttachedTo()
        self.has_operating_system = ""
        self.is_corrupted = ""
        self.location = ""
        self.logical_disk_size_in_gb = 0
        self.label = ""
        self.media_link = ""
        self.name = ""
        self.os = ""
        self.source_image_name = ""


class Disks(WindowsAzureDataTypedList):
    list_type = Disk

    _repr_attributes = ["items"]


class AttachedTo(WindowsAzureData):
    def __init__(self):
        self.hosted_service_name = ""
        self.deployment_name = ""
        self.role_name = ""


class OperationError(WindowsAzureData):
    def __init__(self):
        self.code = ""
        self.message = ""


class Operation(WindowsAzureData):
    def __init__(self):
        self.id = ""
        self.status = ""
        self.http_status_code = ""
        self.error = OperationError()


class OperatingSystem(WindowsAzureData):
    def __init__(self):
        self.version = ""
        self.label = _Base64String()
        self.is_default = True
        self.is_active = True
        self.family = 0
        self.family_label = _Base64String()


class OSDiskConfiguration(WindowsAzureData):
    def __init__(self):
        self.name = ""
        self.host_caching = ""
        self.os_state = ""
        self.os = ""
        self.media_link = ""
        self.logical_disk_size_in_gb = 0


class OperatingSystems(WindowsAzureDataTypedList):
    list_type = OperatingSystem

    _repr_attributes = ["items"]


class OperatingSystemFamily(WindowsAzureData):
    def __init__(self):
        self.name = ""
        self.label = _Base64String()
        self.operating_systems = OperatingSystems()


class OperatingSystemFamilies(WindowsAzureDataTypedList):
    list_type = OperatingSystemFamily

    _repr_attributes = ["items"]


class Subscription(WindowsAzureData):
    def __init__(self):
        self.subscription_id = ""
        self.subscription_name = ""
        self.subscription_status = ""
        self.account_admin_live_email_id = ""
        self.service_admin_live_email_id = ""
        self.max_core_count = 0
        self.max_storage_accounts = 0
        self.max_hosted_services = 0
        self.current_core_count = 0
        self.current_hosted_services = 0
        self.current_storage_accounts = 0
        self.max_virtual_network_sites = 0
        self.max_local_network_sites = 0
        self.max_dns_servers = 0


class AvailabilityResponse(WindowsAzureData):
    def __init__(self):
        self.result = False


class SubscriptionCertificate(WindowsAzureData):
    def __init__(self):
        self.subscription_certificate_public_key = ""
        self.subscription_certificate_thumbprint = ""
        self.subscription_certificate_data = ""
        self.created = ""


class SubscriptionCertificates(WindowsAzureDataTypedList):
    list_type = SubscriptionCertificate

    _repr_attributes = ["items"]


class AzureHTTPRequest:
    def __init__(self):
        self.host = ""
        self.method = ""
        self.path = ""
        self.query = []  # list of (name, value)
        self.headers = {}  # list of (header name, header value)
        self.body = ""
        self.protocol_override = None


class AzureHTTPResponse:
    def __init__(self, status, message, headers, body):
        self.status = status
        self.message = message
        self.headers = headers
        self.body = body


"""
Helper classes and functions.
"""


class _Base64String(str):
    pass


class _ListOf(list):
    """
    A list which carries with it the type that's expected to go in it.
    Used for deserializaion and construction of the lists
    """

    def __init__(self, list_type, xml_element_name=None):
        self.list_type = list_type
        if xml_element_name is None:
            self.xml_element_name = list_type.__name__
        else:
            self.xml_element_name = xml_element_name
        super().__init__()


class ScalarListOf(list):
    """
    A list of scalar types which carries with it the type that's
    expected to go in it along with its xml element name.
    Used for deserializaion and construction of the lists
    """

    def __init__(self, list_type, xml_element_name):
        self.list_type = list_type
        self.xml_element_name = xml_element_name
        super().__init__()


class _DictOf(dict):
    """
    A dict which carries with it the xml element names for key,val.
    Used for deserializaion and construction of the lists
    """

    def __init__(self, pair_xml_element_name, key_xml_element_name, value_xml_element_name):
        self.pair_xml_element_name = pair_xml_element_name
        self.key_xml_element_name = key_xml_element_name
        self.value_xml_element_name = value_xml_element_name
        super().__init__()


class AzureNodeLocation(NodeLocation):
    # we can also have something in here for available services which is an
    # extra to the API with Azure

    def __init__(self, id, name, country, driver, available_services, virtual_machine_role_sizes):
        super().__init__(id, name, country, driver)
        self.available_services = available_services
        self.virtual_machine_role_sizes = virtual_machine_role_sizes

    def __repr__(self):
        return (
            "<AzureNodeLocation: id=%s, name=%s, country=%s, "
            "driver=%s services=%s virtualMachineRoleSizes=%s >"
        ) % (
            self.id,
            self.name,
            self.country,
            self.driver.name,
            ",".join(self.available_services),
            ",".join(self.virtual_machine_role_sizes),
        )
