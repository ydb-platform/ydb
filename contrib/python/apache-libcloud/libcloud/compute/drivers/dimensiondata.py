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
Dimension Data Driver
"""

import sys

from libcloud.utils.py3 import ET, urlencode, basestring, ensure_string
from libcloud.utils.xml import findall, findtext, fixxpath
from libcloud.compute.base import (
    Node,
    NodeSize,
    NodeImage,
    NodeDriver,
    NodeLocation,
    NodeAuthPassword,
)
from libcloud.compute.types import Provider, NodeState
from libcloud.common.exceptions import BaseHTTPError
from libcloud.common.dimensiondata import (
    SERVER_NS,
    TYPES_URN,
    GENERAL_NS,
    NETWORK_NS,
    API_ENDPOINTS,
    DEFAULT_REGION,
    LooseVersion,
    DimensionDataNic,
    DimensionDataTag,
    DimensionDataPort,
    DimensionDataVlan,
    DimensionDataStatus,
    DimensionDataTagKey,
    DimensionDataNatRule,
    DimensionDataNetwork,
    DimensionDataPortList,
    DimensionDataIpAddress,
    DimensionDataConnection,
    DimensionDataServerDisk,
    NetworkDomainServicePlan,
    DimensionDataAPIException,
    DimensionDataFirewallRule,
    DimensionDataChildPortList,
    DimensionDataIpAddressList,
    DimensionDataNetworkDomain,
    DimensionDataPublicIpBlock,
    DimensionDataFirewallAddress,
    DimensionDataAntiAffinityRule,
    DimensionDataServerVMWareTools,
    DimensionDataChildIpAddressList,
    DimensionDataServerCpuSpecification,
    dd_object_to_id,
)

# Node state map is a dictionary with the keys as tuples
# These tuples represent:
# (<state_of_node_from_didata>, <is node started?>, <action happening>)
NODE_STATE_MAP = {
    ("NORMAL", "false", None): NodeState.STOPPED,
    ("PENDING_CHANGE", "false", None): NodeState.PENDING,
    ("PENDING_CHANGE", "false", "CHANGE_NETWORK_ADAPTER"): NodeState.PENDING,
    ("PENDING_CHANGE", "true", "CHANGE_NETWORK_ADAPTER"): NodeState.PENDING,
    ("PENDING_CHANGE", "false", "EXCHANGE_NIC_VLANS"): NodeState.PENDING,
    ("PENDING_CHANGE", "true", "EXCHANGE_NIC_VLANS"): NodeState.PENDING,
    ("NORMAL", "true", None): NodeState.RUNNING,
    ("PENDING_CHANGE", "true", "START_SERVER"): NodeState.STARTING,
    ("PENDING_ADD", "true", "DEPLOY_SERVER"): NodeState.STARTING,
    ("PENDING_ADD", "true", "DEPLOY_SERVER_WITH_DISK_SPEED"): NodeState.STARTING,
    ("PENDING_CHANGE", "true", "SHUTDOWN_SERVER"): NodeState.STOPPING,
    ("PENDING_CHANGE", "true", "POWER_OFF_SERVER"): NodeState.STOPPING,
    ("PENDING_CHANGE", "true", "REBOOT_SERVER"): NodeState.REBOOTING,
    ("PENDING_CHANGE", "true", "RESET_SERVER"): NodeState.REBOOTING,
    ("PENDING_CHANGE", "true", "RECONFIGURE_SERVER"): NodeState.RECONFIGURING,
}

OBJECT_TO_TAGGING_ASSET_TYPE_MAP = {
    "Node": "SERVER",
    "NodeImage": "CUSTOMER_IMAGE",
    "DimensionDataNetworkDomain": "NETWORK_DOMAIN",
    "DimensionDataVlan": "VLAN",
    "DimensionDataPublicIpBlock": "PUBLIC_IP_BLOCK",
}


class DimensionDataNodeDriver(NodeDriver):
    """
    DimensionData node driver.
    Default api_version is used unless specified.
    """

    selected_region = None
    connectionCls = DimensionDataConnection
    name = "DimensionData"
    website = "http://www.dimensiondata.com/"
    type = Provider.DIMENSIONDATA
    features = {"create_node": ["password"]}
    api_version = 1.0

    def __init__(
        self,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        api_version=None,
        region=DEFAULT_REGION,
        **kwargs,
    ):
        if region not in API_ENDPOINTS and host is None:
            raise ValueError("Invalid region: %s, no host specified" % (region))
        if region is not None:
            self.selected_region = API_ENDPOINTS[region]

        if api_version is not None:
            self.api_version = api_version

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

    def _ex_connection_class_kwargs(self):
        """
        Add the region to the kwargs before the connection is instantiated
        """

        kwargs = super()._ex_connection_class_kwargs()
        kwargs["region"] = self.selected_region
        kwargs["api_version"] = self.api_version
        return kwargs

    def _create_node_mcp1(
        self,
        name,
        image,
        auth,
        ex_description,
        ex_network=None,
        ex_memory_gb=None,
        ex_cpu_specification=None,
        ex_is_started=True,
        ex_primary_dns=None,
        ex_secondary_dns=None,
        **kwargs,
    ):
        """
        Create a new DimensionData node

        :keyword    name:   String with a name for this new node (required)
        :type       name:   ``str``

        :keyword    image:  OS Image to boot on node. (required)
        :type       image:  :class:`NodeImage` or ``str``

        :keyword    auth:   Initial authentication information for the
                            node. (If this is a customer LINUX
                            image auth will be ignored)
        :type       auth:   :class:`NodeAuthPassword` or ``str`` or
                            ``None``

        :keyword    ex_description:  description for this node (required)
        :type       ex_description:  ``str``

        :keyword    ex_network:  Network to create the node within
                                 (required unless using ex_network_domain
                                 or ex_primary_ipv4)

        :type       ex_network: :class:`DimensionDataNetwork` or ``str``

        :keyword    ex_memory_gb:  The amount of memory in GB for the
                                   server
        :type       ex_memory_gb: ``int``

        :keyword    ex_cpu_specification: The spec of CPU to deploy (
                                          optional)
        :type       ex_cpu_specification:
                        :class:`DimensionDataServerCpuSpecification`

        :keyword    ex_is_started:  Start server after creation? default
                                    true (required)
        :type       ex_is_started:  ``bool``

        :keyword    ex_primary_dns: The node's primary DNS

        :type       ex_primary_dns: ``str``

        :keyword    ex_secondary_dns: The node's secondary DNS

        :type       ex_secondary_dns: ``str``

        :return: The newly created :class:`Node`.
        :rtype: :class:`Node`
        """
        password = None
        image_needs_auth = self._image_needs_auth(image)
        if image_needs_auth:
            if isinstance(auth, basestring):
                auth_obj = NodeAuthPassword(password=auth)
                password = auth
            else:
                auth_obj = self._get_and_check_auth(auth)
                password = auth_obj.password

        server_elm = ET.Element("deployServer", {"xmlns": TYPES_URN})
        ET.SubElement(server_elm, "name").text = name
        ET.SubElement(server_elm, "description").text = ex_description
        image_id = self._image_to_image_id(image)
        ET.SubElement(server_elm, "imageId").text = image_id
        ET.SubElement(server_elm, "start").text = str(ex_is_started).lower()
        if password is not None:
            ET.SubElement(server_elm, "administratorPassword").text = password

        if ex_cpu_specification is not None:
            cpu = ET.SubElement(server_elm, "cpu")
            cpu.set("speed", ex_cpu_specification.performance)
            cpu.set("count", str(ex_cpu_specification.cpu_count))
            cpu.set("coresPerSocket", str(ex_cpu_specification.cores_per_socket))

        if ex_memory_gb is not None:
            ET.SubElement(server_elm, "memoryGb").text = str(ex_memory_gb)

        if ex_network is not None:
            network_elm = ET.SubElement(server_elm, "network")
            network_id = self._network_to_network_id(ex_network)
            ET.SubElement(network_elm, "networkId").text = network_id

        if ex_primary_dns:
            dns_elm = ET.SubElement(server_elm, "primaryDns")
            dns_elm.text = ex_primary_dns

        if ex_secondary_dns:
            dns_elm = ET.SubElement(server_elm, "secondaryDns")
            dns_elm.text = ex_secondary_dns

        response = self.connection.request_with_orgId_api_2(
            "server/deployServer", method="POST", data=ET.tostring(server_elm)
        ).object

        node_id = None
        for info in findall(response, "info", TYPES_URN):
            if info.get("name") == "serverId":
                node_id = info.get("value")

        node = self.ex_get_node_by_id(node_id)

        if image_needs_auth:
            if getattr(auth_obj, "generated", False):
                node.extra["password"] = auth_obj.password

        return node

    def create_node(
        self,
        name,
        image,
        auth,
        ex_network_domain=None,
        ex_primary_nic_private_ipv4=None,
        ex_primary_nic_vlan=None,
        ex_primary_nic_network_adapter=None,
        ex_additional_nics=None,
        ex_description=None,
        ex_disks=None,
        ex_cpu_specification=None,
        ex_memory_gb=None,
        ex_is_started=True,
        ex_primary_dns=None,
        ex_secondary_dns=None,
        ex_ipv4_gateway=None,
        ex_microsoft_time_zone=None,
        **kwargs,
    ):
        """
        Create a new DimensionData node in MCP2. However, it is still
        backward compatible for MCP1 for a limited time. Please consider
        using MCP2 datacenter as MCP1 will phase out soon.

        Legacy Create Node for MCP1 datacenter

        >>> from pprint import pprint
        >>> from libcloud.compute.types import Provider
        >>> from libcloud.compute.base import NodeAuthPassword
        >>> from libcloud.compute.providers import get_driver
        >>> import libcloud.security
        >>>
        >>> # Get dimension data driver
        >>> libcloud.security.VERIFY_SSL_CERT = False
        >>> DimensionData = get_driver(Provider.DIMENSIONDATA)
        >>> driver = cls('myusername','mypassword', region='dd-au')
        >>>
        >>> # Password
        >>> root_pw = NodeAuthPassword('password123')
        >>>
        >>> # Get location
        >>> location = driver.ex_get_location_by_id(id='AU1')
        >>>
        >>> # Get network by location
        >>> my_network = driver.list_networks(location=location)[0]
        >>> pprint(my_network)
        >>>
        >>> # Get Image
        >>> images = driver.list_images(location=location)
        >>> image = images[0]
        >>>
        >>> node = driver.create_node(name='test_blah_2', image=image,
        >>>                           auth=root_pw,
        >>>                           ex_description='test3 node',
        >>>                           ex_network=my_network,
        >>>                           ex_is_started=False)
        >>> pprint(node)


        Create Node in MCP2 Data Center

        >>> from pprint import pprint
        >>> from libcloud.compute.types import Provider
        >>> from libcloud.compute.base import NodeAuthPassword
        >>> from libcloud.compute.providers import get_driver
        >>> import libcloud.security
        >>>
        >>> # Get dimension data driver
        >>> libcloud.security.VERIFY_SSL_CERT = True
        >>> cls = get_driver(Provider.DIMENSIONDATA)
        >>> driver = cls('myusername','mypassword', region='dd-au')
        >>>
        >>> # Password
        >>> root_pw = NodeAuthPassword('password123')
        >>>
        >>> # Get location
        >>> location = driver.ex_get_location_by_id(id='AU9')
        >>>
        >>> # Get network domain by location
        >>> networkDomainName = "Baas QA"
        >>> network_domains = driver.ex_list_network_domains(location=location)
        >>> my_network_domain = [d for d in network_domains if d.name ==
                              networkDomainName][0]
        >>>
        >>> vlan = driver.ex_list_vlans(location=location,
        >>>                             network_domain=my_network_domain)[0]
        >>> pprint(vlan)
        >>>
        >>> # Get Image
        >>> images = driver.list_images(location=location)
        >>> image = images[0]
        >>>
        >>> # Create node using vlan instead of private IPv4
        >>> node = driver.create_node(name='test_server_01', image=image,
        >>>                           auth=root_pw,
        >>>                           ex_description='test2 node',
        >>>                           ex_network_domain=my_network_domain,
        >>>                           ex_primary_nic_vlan=vlan,
        >>>                           ex_is_started=False)
        >>>
        >>> # Option: Create node using private IPv4 instead of vlan
        >>> # node = driver.create_node(name='test_server_02', image=image,
        >>> #                           auth=root_pw,
        >>> #                           ex_description='test2 node',
        >>> #                           ex_network_domain=my_network_domain,
        >>> #                           ex_primary_nic_private_ipv4='10.1.1.7',
        >>> #                           ex_is_started=False)
        >>>
        >>> # Option: Create node using by specifying Network Adapter
        >>> # node = driver.create_node(name='test_server_03', image=image,
        >>> #                           auth=root_pw,
        >>> #                           ex_description='test2 node',
        >>> #                           ex_network_domain=my_network_domain,
        >>> #                           ex_primary_nic_vlan=vlan,
        >>> #                           ex_primary_nic_network_adapter='E1000',
        >>> #                           ex_is_started=False)
        >>>

        :keyword    name:   (required) String with a name for this new node
        :type       name:   ``str``

        :keyword    image:  (required) OS Image to boot on node.
        :type       image:  :class:`NodeImage` or ``str``

        :keyword    auth:   Initial authentication information for the
                            node. (If this is a customer LINUX
                            image auth will be ignored)
        :type       auth:   :class:`NodeAuthPassword` or ``str`` or ``None``

        :keyword    ex_description:  (optional) description for this node
        :type       ex_description:  ``str``


        :keyword    ex_network_domain:  (required) Network Domain or Network
                                        Domain ID to create the node
        :type       ex_network_domain: :class:`DimensionDataNetworkDomain`
                                        or ``str``

        :keyword    ex_primary_nic_private_ipv4:  Provide private IPv4. Ignore
                                                  if ex_primary_nic_vlan is
                                                  provided. Use one or the
                                                  other. Not both.
        :type       ex_primary_nic_private_ipv4: :``str``

        :keyword    ex_primary_nic_vlan:  Provide VLAN for the node if
                                          ex_primary_nic_private_ipv4 NOT
                                          provided. One or the other. Not both.
        :type       ex_primary_nic_vlan: :class: DimensionDataVlan or ``str``

        :keyword    ex_primary_nic_network_adapter: (Optional) Default value
                                                    for the Operating System
                                                    will be used if leave
                                                    empty. Example: "E1000".
        :type       ex_primary_nic_network_adapter: :``str``

        :keyword    ex_additional_nics: (optional) List
                                        :class:'DimensionDataNic' or None
        :type       ex_additional_nics: ``list`` of :class:'DimensionDataNic'
                                        or ``str``

        :keyword    ex_memory_gb:  (optional) The amount of memory in GB for
                                   the server Can be used to override the
                                   memory value inherited from the source
                                   Server Image.
        :type       ex_memory_gb: ``int``

        :keyword    ex_cpu_specification: (optional) The spec of CPU to deploy
        :type       ex_cpu_specification:
                        :class:`DimensionDataServerCpuSpecification`

        :keyword    ex_is_started: (required) Start server after creation.
                                   Default is set to true.
        :type       ex_is_started:  ``bool``

        :keyword    ex_primary_dns: (Optional) The node's primary DNS
        :type       ex_primary_dns: ``str``

        :keyword    ex_secondary_dns: (Optional) The node's secondary DNS
        :type       ex_secondary_dns: ``str``

        :keyword    ex_ipv4_gateway: (Optional) IPv4 address in dot-decimal
                                     notation, which will be used as the
                                     Primary NIC gateway instead of the default
                                     gateway assigned by the system. If
                                     ipv4Gateway is provided it does not have
                                     to be on the VLAN of the Primary NIC
                                     but MUST be reachable or the Guest OS
                                     will not be configured correctly.
        :type       ex_ipv4_gateway: ``str``

        :keyword    ex_disks: (optional) Dimensiondata disks. Optional disk
                            elements can be used to define the disk speed
                            that each disk on the Server; inherited from the
                            source Server Image will be deployed to. It is
                            not necessary to include a diskelement for every
                            disk; only those that you wish to set a disk
                            speed value for. Note that scsiId 7 cannot be
                            used.Up to 13 disks can be present in addition to
                            the required OS disk on SCSI ID 0. Refer to
                            https://docs.mcp-services.net/x/UwIu for disk

        :type       ex_disks: List or tuple of :class:'DimensionDataServerDisk`

        :keyword    ex_microsoft_time_zone: (optional) For use with
                    Microsoft Windows source Server Images only. For the exact
                    value to use please refer to the table of time zone
                    indexes in the following Microsoft Technet
                    documentation. If none is supplied, the default time
                    zone for the data center geographic region will be used.
        :type       ex_microsoft_time_zone: `str``


        :return: The newly created :class:`Node`.
        :rtype: :class:`Node`
        """

        # Neither legacy MCP1 network nor MCP2 network domain provided
        if ex_network_domain is None and "ex_network" not in kwargs:
            raise ValueError(
                "You must provide either ex_network_domain "
                "for MCP2 or ex_network for legacy MCP1"
            )

        # Ambiguous parameter provided. Can't determine if it is MCP 1 or 2.
        if ex_network_domain is not None and "ex_network" in kwargs:
            raise ValueError(
                "You can only supply either "
                "ex_network_domain "
                "for MCP2 or ex_network for legacy MCP1"
            )

        # Set ex_is_started to False by default if none bool data type provided
        if not isinstance(ex_is_started, bool):
            ex_is_started = True

        # Handle MCP1 legacy
        if "ex_network" in kwargs:
            new_node = self._create_node_mcp1(
                name=name,
                image=image,
                auth=auth,
                ex_network=kwargs.get("ex_network"),
                ex_description=ex_description,
                ex_memory_gb=ex_memory_gb,
                ex_cpu_specification=ex_cpu_specification,
                ex_is_started=ex_is_started,
                ex_primary_ipv4=ex_primary_nic_private_ipv4,
                ex_disks=ex_disks,
                ex_additional_nics_vlan=kwargs.get("ex_additional_nics_vlan"),
                ex_additional_nics_ipv4=kwargs.get("ex_additional_nics_ipv4"),
                ex_primary_dns=ex_primary_dns,
                ex_secondary_dns=ex_secondary_dns,
            )
        else:
            # Handle MCP2 legacy. CaaS api 2.2 or earlier
            if "ex_vlan" in kwargs:
                ex_primary_nic_vlan = kwargs.get("ex_vlan")

            if "ex_primary_ipv4" in kwargs:
                ex_primary_nic_private_ipv4 = kwargs.get("ex_primary_ipv4")

            additional_nics = []

            if "ex_additional_nics_vlan" in kwargs:
                vlans = kwargs.get("ex_additional_nics_vlan")
                if isinstance(vlans, (list, tuple)):
                    for v in vlans:
                        add_nic = DimensionDataNic(vlan=v)
                        additional_nics.append(add_nic)
                else:
                    raise TypeError("ex_additional_nics_vlan must " "be None or a tuple/list")

            if "ex_additional_nics_ipv4" in kwargs:
                ips = kwargs.get("ex_additional_nics_ipv4")

                if isinstance(ips, (list, tuple)):
                    for ip in ips:
                        add_nic = DimensionDataNic(private_ip_v4=ip)
                        additional_nics.append(add_nic)
                else:
                    if ips is not None:
                        raise TypeError("ex_additional_nics_ipv4 must " "be None or a tuple/list")

            if "ex_additional_nics_vlan" in kwargs or "ex_additional_nics_ipv4" in kwargs:
                ex_additional_nics = additional_nics

            # Handle MCP2 latest. CaaS API 2.3 onwards
            if ex_network_domain is None:
                raise ValueError("ex_network_domain must be specified")

            password = None
            image_needs_auth = self._image_needs_auth(image)
            if image_needs_auth:
                if isinstance(auth, basestring):
                    auth_obj = NodeAuthPassword(password=auth)
                    password = auth
                else:
                    auth_obj = self._get_and_check_auth(auth)
                    password = auth_obj.password

            server_elm = ET.Element("deployServer", {"xmlns": TYPES_URN})
            ET.SubElement(server_elm, "name").text = name
            ET.SubElement(server_elm, "description").text = ex_description
            image_id = self._image_to_image_id(image)
            ET.SubElement(server_elm, "imageId").text = image_id
            ET.SubElement(server_elm, "start").text = str(ex_is_started).lower()
            if password is not None:
                ET.SubElement(server_elm, "administratorPassword").text = password

            if ex_cpu_specification is not None:
                cpu = ET.SubElement(server_elm, "cpu")
                cpu.set("speed", ex_cpu_specification.performance)
                cpu.set("count", str(ex_cpu_specification.cpu_count))
                cpu.set("coresPerSocket", str(ex_cpu_specification.cores_per_socket))

            if ex_memory_gb is not None:
                ET.SubElement(server_elm, "memoryGb").text = str(ex_memory_gb)

            if ex_primary_nic_private_ipv4 is None and ex_primary_nic_vlan is None:
                raise ValueError(
                    "Missing argument. Either "
                    "ex_primary_nic_private_ipv4 or "
                    "ex_primary_nic_vlan "
                    "must be specified."
                )

            if ex_primary_nic_private_ipv4 is not None and ex_primary_nic_vlan is not None:
                raise ValueError(
                    "Either ex_primary_nic_private_ipv4 or "
                    "ex_primary_nic_vlan "
                    "be specified. Not both."
                )

            network_elm = ET.SubElement(server_elm, "networkInfo")

            net_domain_id = self._network_domain_to_network_domain_id(ex_network_domain)

            network_elm.set("networkDomainId", net_domain_id)

            pri_nic = ET.SubElement(network_elm, "primaryNic")

            if ex_primary_nic_private_ipv4 is not None:
                ET.SubElement(pri_nic, "privateIpv4").text = ex_primary_nic_private_ipv4

            if ex_primary_nic_vlan is not None:
                vlan_id = self._vlan_to_vlan_id(ex_primary_nic_vlan)
                ET.SubElement(pri_nic, "vlanId").text = vlan_id

            if ex_primary_nic_network_adapter is not None:
                ET.SubElement(pri_nic, "networkAdapter").text = ex_primary_nic_network_adapter

            if isinstance(ex_additional_nics, (list, tuple)):
                for nic in ex_additional_nics:
                    additional_nic = ET.SubElement(network_elm, "additionalNic")

                    if nic.private_ip_v4 is None and nic.vlan is None:
                        raise ValueError(
                            "Either a vlan or private_ip_v4 "
                            "must be specified for each "
                            "additional nic."
                        )

                    if nic.private_ip_v4 is not None and nic.vlan is not None:
                        raise ValueError(
                            "Either a vlan or private_ip_v4 "
                            "must be specified for each "
                            "additional nic. Not both."
                        )

                    if nic.private_ip_v4 is not None:
                        ET.SubElement(additional_nic, "privateIpv4").text = nic.private_ip_v4

                    if nic.vlan is not None:
                        vlan_id = self._vlan_to_vlan_id(nic.vlan)
                        ET.SubElement(additional_nic, "vlanId").text = vlan_id

                    if nic.network_adapter_name is not None:
                        ET.SubElement(additional_nic, "networkAdapter").text = (
                            nic.network_adapter_name
                        )
            elif ex_additional_nics is not None:
                raise TypeError("ex_additional_NICs must be None or tuple/list")

            if ex_primary_dns:
                dns_elm = ET.SubElement(server_elm, "primaryDns")
                dns_elm.text = ex_primary_dns

            if ex_secondary_dns:
                dns_elm = ET.SubElement(server_elm, "secondaryDns")
                dns_elm.text = ex_secondary_dns

            if ex_ipv4_gateway:
                ET.SubElement(server_elm, "ipv4Gateway").text = ex_ipv4_gateway

            if isinstance(ex_disks, (list, tuple)):
                for disk in ex_disks:
                    disk_elm = ET.SubElement(server_elm, "disk")
                    disk_elm.set("scsiId", disk.scsi_id)
                    disk_elm.set("speed", disk.speed)
            elif ex_disks is not None:
                raise TypeError("ex_disks must be None or tuple/list")

            if ex_microsoft_time_zone:
                ET.SubElement(server_elm, "microsoftTimeZone").text = ex_microsoft_time_zone

            response = self.connection.request_with_orgId_api_2(
                "server/deployServer", method="POST", data=ET.tostring(server_elm)
            ).object

            node_id = None
            for info in findall(response, "info", TYPES_URN):
                if info.get("name") == "serverId":
                    node_id = info.get("value")

            new_node = self.ex_get_node_by_id(node_id)

            if image_needs_auth:
                if getattr(auth_obj, "generated", False):
                    new_node.extra["password"] = auth_obj.password

        return new_node

    def destroy_node(self, node):
        """
        Deletes a node, node must be stopped before deletion


        :keyword node: The node to delete
        :type    node: :class:`Node`

        :rtype: ``bool``
        """
        request_elm = ET.Element("deleteServer", {"xmlns": TYPES_URN, "id": node.id})
        body = self.connection.request_with_orgId_api_2(
            "server/deleteServer", method="POST", data=ET.tostring(request_elm)
        ).object
        response_code = findtext(body, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def reboot_node(self, node):
        """
        Reboots a node by requesting the OS restart via the hypervisor


        :keyword node: The node to reboot
        :type    node: :class:`Node`

        :rtype: ``bool``
        """
        request_elm = ET.Element("rebootServer", {"xmlns": TYPES_URN, "id": node.id})
        body = self.connection.request_with_orgId_api_2(
            "server/rebootServer", method="POST", data=ET.tostring(request_elm)
        ).object
        response_code = findtext(body, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def list_nodes(
        self,
        ex_location=None,
        ex_name=None,
        ex_ipv6=None,
        ex_ipv4=None,
        ex_vlan=None,
        ex_image=None,
        ex_deployed=None,
        ex_started=None,
        ex_state=None,
        ex_network=None,
        ex_network_domain=None,
    ):
        """
        List nodes deployed for your organization.

        :keyword ex_location: Filters the node list to nodes that are
                              located in this location
        :type    ex_location: :class:`NodeLocation` or ``str``

        :keyword ex_name: Filters the node list to nodes that have this name
        :type    ex_name ``str``

        :keyword ex_ipv6: Filters the node list to nodes that have this
                          ipv6 address
        :type    ex_ipv6: ``str``

        :keyword ex_ipv4: Filters the node list to nodes that have this
                          ipv4 address
        :type    ex_ipv4: ``str``

        :keyword ex_vlan: Filters the node list to nodes that are in this VLAN
        :type    ex_vlan: :class:`DimensionDataVlan` or ``str``

        :keyword ex_image: Filters the node list to nodes that have this image
        :type    ex_image: :class:`NodeImage` or ``str``

        :keyword ex_deployed: Filters the node list to nodes that are
                              deployed or not
        :type    ex_deployed: ``bool``

        :keyword ex_started: Filters the node list to nodes that are
                             started or not
        :type    ex_started: ``bool``

        :keyword ex_state: Filters the node list by nodes that are in
                           this state
        :type    ex_state: ``str``

        :keyword ex_network: Filters the node list to nodes in this network
        :type    ex_network: :class:`DimensionDataNetwork` or ``str``

        :keyword ex_network_domain: Filters the node list to nodes in this
                                    network domain
        :type    ex_network_domain: :class:`DimensionDataNetworkDomain`
                                    or ``str``

        :return: a list of `Node` objects
        :rtype: ``list`` of :class:`Node`
        """
        node_list = []
        for nodes in self.ex_list_nodes_paginated(
            location=ex_location,
            name=ex_name,
            ipv6=ex_ipv6,
            ipv4=ex_ipv4,
            vlan=ex_vlan,
            image=ex_image,
            deployed=ex_deployed,
            started=ex_started,
            state=ex_state,
            network=ex_network,
            network_domain=ex_network_domain,
        ):
            node_list.extend(nodes)

        return node_list

    def list_images(self, location=None):
        """
        List images available

        Note:  Currently only returns the default 'base OS images'
               provided by DimensionData. Customer images (snapshots)
               use ex_list_customer_images

        :keyword ex_location: Filters the node list to nodes that are
                              located in this location
        :type    ex_location: :class:`NodeLocation` or ``str``

        :return: List of images available
        :rtype: ``list`` of :class:`NodeImage`
        """
        params = {}
        if location is not None:
            params["datacenterId"] = self._location_to_location_id(location)

        return self._to_images(
            self.connection.request_with_orgId_api_2("image/osImage", params=params).object
        )

    def list_sizes(self, location=None):
        """
        return a list of available sizes
            Currently, the size of the node is dictated by the chosen OS base
            image, they cannot be set explicitly.

        @inherits: :class:`NodeDriver.list_sizes`
        """
        return [
            NodeSize(
                id=1,
                name="default",
                ram=0,
                disk=0,
                bandwidth=0,
                price=0,
                driver=self.connection.driver,
            ),
        ]

    def list_locations(self, ex_id=None):
        """
        List locations (datacenters) available for instantiating servers and
        networks.

        :keyword ex_id: Filters the location list to this id
        :type    ex_id: ``str``

        :return:  List of locations
        :rtype:  ``list`` of :class:`NodeLocation`
        """
        params = {}
        if ex_id is not None:
            params["id"] = ex_id

        return self._to_locations(
            self.connection.request_with_orgId_api_2(
                "infrastructure/datacenter", params=params
            ).object
        )

    def list_networks(self, location=None):
        """
        List networks deployed across all data center locations for your
        organization.  The response includes the location of each network.


        :keyword location: The location
        :type    location: :class:`NodeLocation` or ``str``

        :return: a list of DimensionDataNetwork objects
        :rtype: ``list`` of :class:`DimensionDataNetwork`
        """
        url_ext = ""
        if location is not None:
            url_ext = "/" + self._location_to_location_id(location)

        return self._to_networks(
            self.connection.request_with_orgId_api_1("networkWithLocation%s" % url_ext).object
        )

    def import_image(
        self,
        ovf_package_name,
        name,
        cluster_id=None,
        datacenter_id=None,
        description=None,
        is_guest_os_customization=None,
        tagkey_name_value_dictionaries=None,
    ):
        """
        Import image

        :param ovf_package_name: Image OVF package name
        :type  ovf_package_name: ``str``

        :param name: Image name
        :type  name: ``str``

        :param cluster_id: Provide either cluster_id or datacenter_id
        :type  cluster_id: ``str``

        :param datacenter_id: Provide either cluster_id or datacenter_id
        :type  datacenter_id: ``str``

        :param description: Optional. Description of image
        :type  description: ``str``

        :param is_guest_os_customization: Optional. true for NGOC image
        :type  is_guest_os_customization: ``bool``

        :param tagkey_name_value_dictionaries: Optional tagkey name value dict
        :type  tagkey_name_value_dictionaries: dictionaries

        :return: Return true if successful
        :rtype:  ``bool``
        """

        # Unsupported for version lower than 2.4
        if LooseVersion(self.connection.active_api_version) < LooseVersion("2.4"):
            raise Exception(
                "import image is feature is NOT supported in  " "api version earlier than 2.4"
            )
        elif cluster_id is None and datacenter_id is None:
            raise ValueError("Either cluster_id or datacenter_id must be " "provided")
        elif cluster_id is not None and datacenter_id is not None:
            raise ValueError(
                "Cannot accept both cluster_id and " "datacenter_id. Please provide either one"
            )
        else:
            import_image_elem = ET.Element("urn:importImage", {"xmlns:urn": TYPES_URN})

            ET.SubElement(import_image_elem, "urn:ovfPackage").text = ovf_package_name

            ET.SubElement(import_image_elem, "urn:name").text = name

            if description is not None:
                ET.SubElement(import_image_elem, "urn:description").text = description

            if cluster_id is not None:
                ET.SubElement(import_image_elem, "urn:clusterId").text = cluster_id
            else:
                ET.SubElement(import_image_elem, "urn:datacenterId").text = datacenter_id

            if is_guest_os_customization is not None:
                ET.SubElement(import_image_elem, "urn:guestOsCustomization").text = (
                    is_guest_os_customization
                )

            if len(tagkey_name_value_dictionaries) > 0:
                for k, v in tagkey_name_value_dictionaries.items():
                    tag_elem = ET.SubElement(import_image_elem, "urn:tag")
                    ET.SubElement(tag_elem, "urn:tagKeyName").text = k

                    if v is not None:
                        ET.SubElement(tag_elem, "urn:value").text = v

        response = self.connection.request_with_orgId_api_2(
            "image/importImage", method="POST", data=ET.tostring(import_image_elem)
        ).object

        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def start_node(self, node):
        """
        Powers on an existing deployed server

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        request_elm = ET.Element("startServer", {"xmlns": TYPES_URN, "id": node.id})
        body = self.connection.request_with_orgId_api_2(
            "server/startServer", method="POST", data=ET.tostring(request_elm)
        ).object
        response_code = findtext(body, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def stop_node(self, node):
        """
        This function will attempt to "gracefully" stop a server by
        initiating a shutdown sequence within the guest operating system.
        A successful response on this function means the system has
        successfully passed the request into the operating system.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        request_elm = ET.Element("shutdownServer", {"xmlns": TYPES_URN, "id": node.id})
        body = self.connection.request_with_orgId_api_2(
            "server/shutdownServer", method="POST", data=ET.tostring(request_elm)
        ).object
        response_code = findtext(body, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_list_nodes_paginated(
        self,
        name=None,
        location=None,
        ipv6=None,
        ipv4=None,
        vlan=None,
        image=None,
        deployed=None,
        started=None,
        state=None,
        network=None,
        network_domain=None,
    ):
        """
        Return a generator which yields node lists in pages

        :keyword location: Filters the node list to nodes that are
                           located in this location
        :type    location: :class:`NodeLocation` or ``str``

        :keyword name: Filters the node list to nodes that have this name
        :type    name ``str``

        :keyword ipv6: Filters the node list to nodes that have this
                       ipv6 address
        :type    ipv6: ``str``

        :keyword ipv4: Filters the node list to nodes that have this
                       ipv4 address
        :type    ipv4: ``str``

        :keyword vlan: Filters the node list to nodes that are in this VLAN
        :type    vlan: :class:`DimensionDataVlan` or ``str``

        :keyword image: Filters the node list to nodes that have this image
        :type    image: :class:`NodeImage` or ``str``

        :keyword deployed: Filters the node list to nodes that are
                           deployed or not
        :type    deployed: ``bool``

        :keyword started: Filters the node list to nodes that are
                          started or not
        :type    started: ``bool``

        :keyword state: Filters the node list to nodes that are in
                        this state
        :type    state: ``str``

        :keyword network: Filters the node list to nodes in this network
        :type    network: :class:`DimensionDataNetwork` or ``str``

        :keyword network_domain: Filters the node list to nodes in this
                                 network domain
        :type    network_domain: :class:`DimensionDataNetworkDomain`
                                 or ``str``

        :return: a list of `Node` objects
        :rtype: ``generator`` of `list` of :class:`Node`
        """

        params = {}
        if location is not None:
            params["datacenterId"] = self._location_to_location_id(location)
        if ipv6 is not None:
            params["ipv6"] = ipv6
        if ipv4 is not None:
            params["privateIpv4"] = ipv4
        if state is not None:
            params["state"] = state
        if started is not None:
            params["started"] = started
        if deployed is not None:
            params["deployed"] = deployed
        if name is not None:
            params["name"] = name
        if network_domain is not None:
            params["networkDomainId"] = self._network_domain_to_network_domain_id(network_domain)
        if network is not None:
            params["networkId"] = self._network_to_network_id(network)
        if vlan is not None:
            params["vlanId"] = self._vlan_to_vlan_id(vlan)
        if image is not None:
            params["sourceImageId"] = self._image_to_image_id(image)

        nodes_obj = self._list_nodes_single_page(params)
        yield self._to_nodes(nodes_obj)

        while nodes_obj.get("pageCount") >= nodes_obj.get("pageSize"):
            params["pageNumber"] = int(nodes_obj.get("pageNumber")) + 1
            nodes_obj = self._list_nodes_single_page(params)
            yield self._to_nodes(nodes_obj)

    def ex_start_node(self, node):
        # NOTE: This method is here for backward compatibility reasons after
        # this method was promoted to be part of the standard compute API in
        # Libcloud v2.7.0
        return self.start_node(node=node)

    def ex_shutdown_graceful(self, node):
        return self.stop_node(node=node)

    def ex_power_off(self, node):
        """
        This function will abruptly power-off a server.  Unlike
        ex_shutdown_graceful, success ensures the node will stop but some OS
        and application configurations may be adversely affected by the
        equivalent of pulling the power plug out of the machine.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        request_elm = ET.Element("powerOffServer", {"xmlns": TYPES_URN, "id": node.id})

        try:
            body = self.connection.request_with_orgId_api_2(
                "server/powerOffServer", method="POST", data=ET.tostring(request_elm)
            ).object
            response_code = findtext(body, "responseCode", TYPES_URN)
        except (DimensionDataAPIException, NameError, BaseHTTPError):
            r = self.ex_get_node_by_id(node.id)
            response_code = r.state.upper()

        return response_code in ["IN_PROGRESS", "OK", "STOPPED", "STOPPING"]

    def ex_reset(self, node):
        """
        This function will abruptly reset a server.  Unlike
        reboot_node, success ensures the node will restart but some OS
        and application configurations may be adversely affected by the
        equivalent of pulling the power plug out of the machine.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        request_elm = ET.Element("resetServer", {"xmlns": TYPES_URN, "id": node.id})
        body = self.connection.request_with_orgId_api_2(
            "server/resetServer", method="POST", data=ET.tostring(request_elm)
        ).object
        response_code = findtext(body, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_update_vm_tools(self, node):
        """
        This function triggers an update of the VMware Tools
        software running on the guest OS of a Server.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        request_elm = ET.Element("updateVmwareTools", {"xmlns": TYPES_URN, "id": node.id})
        body = self.connection.request_with_orgId_api_2(
            "server/updateVmwareTools", method="POST", data=ET.tostring(request_elm)
        ).object
        response_code = findtext(body, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_update_node(self, node, name=None, description=None, cpu_count=None, ram_mb=None):
        """
        Update the node, the name, CPU or RAM

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :param      name: The new name (optional)
        :type       name: ``str``

        :param      description: The new description (optional)
        :type       description: ``str``

        :param      cpu_count: The new CPU count (optional)
        :type       cpu_count: ``int``

        :param      ram_mb: The new Memory in MB (optional)
        :type       ram_mb: ``int``

        :rtype: ``bool``
        """
        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if cpu_count is not None:
            data["cpuCount"] = str(cpu_count)
        if ram_mb is not None:
            data["memory"] = str(ram_mb)
        body = self.connection.request_with_orgId_api_1(
            "server/%s" % (node.id), method="POST", data=urlencode(data, True)
        ).object
        response_code = findtext(body, "result", GENERAL_NS)
        return response_code in ["IN_PROGRESS", "SUCCESS"]

    def ex_create_anti_affinity_rule(self, node_list):
        """
        Create an anti affinity rule given a list of nodes
        Anti affinity rules ensure that servers will not reside
        on the same VMware ESX host

        :param node_list: The list of nodes to create a rule for
        :type  node_list: ``list`` of :class:`Node` or
                          ``list`` of ``str``

        :rtype: ``bool``
        """
        if not isinstance(node_list, (list, tuple)):
            raise TypeError("Node list must be a list or a tuple.")
        anti_affinity_xml_request = ET.Element("NewAntiAffinityRule", {"xmlns": SERVER_NS})
        for node in node_list:
            ET.SubElement(anti_affinity_xml_request, "serverId").text = self._node_to_node_id(node)
        result = self.connection.request_with_orgId_api_1(
            "antiAffinityRule",
            method="POST",
            data=ET.tostring(anti_affinity_xml_request),
        ).object
        response_code = findtext(result, "result", GENERAL_NS)
        return response_code in ["IN_PROGRESS", "SUCCESS"]

    def ex_delete_anti_affinity_rule(self, anti_affinity_rule):
        """
        Remove anti affinity rule

        :param anti_affinity_rule: The anti affinity rule to delete
        :type  anti_affinity_rule: :class:`DimensionDataAntiAffinityRule` or
                                   ``str``

        :rtype: ``bool``
        """
        rule_id = self._anti_affinity_rule_to_anti_affinity_rule_id(anti_affinity_rule)
        result = self.connection.request_with_orgId_api_1(
            "antiAffinityRule/%s?delete" % (rule_id), method="GET"
        ).object
        response_code = findtext(result, "result", GENERAL_NS)
        return response_code in ["IN_PROGRESS", "SUCCESS"]

    def ex_list_anti_affinity_rules(
        self,
        network=None,
        network_domain=None,
        node=None,
        filter_id=None,
        filter_state=None,
    ):
        """
        List anti affinity rules for a network, network domain, or node

        :param network: The network to list anti affinity rules for
                        One of network, network_domain, or node is required
        :type  network: :class:`DimensionDataNetwork` or ``str``

        :param network_domain: The network domain to list anti affinity rules
                               One of network, network_domain,
                               or node is required
        :type  network_domain: :class:`DimensionDataNetworkDomain` or ``str``

        :param node: The node to list anti affinity rules for
                     One of network, netwok_domain, or node is required
        :type  node: :class:`Node` or ``str``

        :param filter_id: This will allow you to filter the rules
                          by this node id
        :type  filter_id: ``str``

        :type  filter_state: This will allow you to filter rules by
                             node state (i.e. NORMAL)
        :type  filter_state: ``str``

        :rtype: ``list`` of :class:`DimensionDataAntiAffinityRule`
        """
        not_none_arguments = [key for key in (network, network_domain, node) if key is not None]
        if len(not_none_arguments) != 1:
            raise ValueError("One and ONLY one of network, " "network_domain, or node must be set")

        params = {}
        if network_domain is not None:
            params["networkDomainId"] = self._network_domain_to_network_domain_id(network_domain)
        if network is not None:
            params["networkId"] = self._network_to_network_id(network)
        if node is not None:
            params["serverId"] = self._node_to_node_id(node)
        if filter_id is not None:
            params["id"] = filter_id
        if filter_state is not None:
            params["state"] = filter_state

        paged_result = self.connection.paginated_request_with_orgId_api_2(
            "server/antiAffinityRule", method="GET", params=params
        )

        rules = []
        for result in paged_result:
            rules.extend(self._to_anti_affinity_rules(result))
        return rules

    def ex_attach_node_to_vlan(self, node, vlan=None, private_ipv4=None):
        """
        Attach a node to a VLAN by adding an additional NIC to
        the node on the target VLAN. The IP will be automatically
        assigned based on the VLAN IP network space. Alternatively, provide
        a private IPv4 address instead of VLAN information, and this will
        be assigned to the node on corresponding NIC.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :param      vlan: VLAN to attach the node to
                          (required unless private_ipv4)
        :type       vlan: :class:`DimensionDataVlan`

        :keyword    private_ipv4: Private nic IPv4 Address
                                  (required unless vlan)
        :type       private_ipv4: ``str``

        :rtype: ``bool``
        """
        request = ET.Element("addNic", {"xmlns": TYPES_URN})
        ET.SubElement(request, "serverId").text = node.id
        nic = ET.SubElement(request, "nic")

        if vlan is not None:
            ET.SubElement(nic, "vlanId").text = vlan.id
        elif private_ipv4 is not None:
            ET.SubElement(nic, "privateIpv4").text = private_ipv4
        else:
            raise ValueError("One of vlan or primary_ipv4 " "must be specified")

        response = self.connection.request_with_orgId_api_2(
            "server/addNic", method="POST", data=ET.tostring(request)
        ).object
        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_destroy_nic(self, nic_id):
        """
        Remove a NIC on a node, removing the node from a VLAN

        :param      nic_id: The identifier of the NIC to remove
        :type       nic_id: ``str``

        :rtype: ``bool``
        """
        request = ET.Element("removeNic", {"xmlns": TYPES_URN, "id": nic_id})

        response = self.connection.request_with_orgId_api_2(
            "server/removeNic", method="POST", data=ET.tostring(request)
        ).object
        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_list_networks(self, location=None):
        """
        List networks deployed across all data center locations for your
        organization.  The response includes the location of each network.

        :param location: The target location
        :type  location: :class:`NodeLocation` or ``str``

        :return: a list of DimensionDataNetwork objects
        :rtype: ``list`` of :class:`DimensionDataNetwork`
        """
        return self.list_networks(location=location)

    def ex_create_network(self, location, name, description=None):
        """
        Create a new network in an MCP 1.0 location

        :param   location: The target location (MCP1)
        :type    location: :class:`NodeLocation` or ``str``

        :param   name: The name of the network
        :type    name: ``str``

        :param   description: Additional description of the network
        :type    description: ``str``

        :return: A new instance of `DimensionDataNetwork`
        :rtype:  Instance of :class:`DimensionDataNetwork`
        """
        network_location = self._location_to_location_id(location)

        create_node = ET.Element("NewNetworkWithLocation", {"xmlns": NETWORK_NS})
        ET.SubElement(create_node, "name").text = name
        if description is not None:
            ET.SubElement(create_node, "description").text = description
        ET.SubElement(create_node, "location").text = network_location

        self.connection.request_with_orgId_api_1(
            "networkWithLocation", method="POST", data=ET.tostring(create_node)
        )

        # MCP1 API does not return the ID, but name is unique for location
        network = list(filter(lambda x: x.name == name, self.ex_list_networks(location)))[0]

        return network

    def ex_delete_network(self, network):
        """
        Delete a network from an MCP 1 data center

        :param  network: The network to delete
        :type   network: :class:`DimensionDataNetwork`

        :rtype: ``bool``
        """
        response = self.connection.request_with_orgId_api_1(
            "network/%s?delete" % network.id, method="GET"
        ).object
        response_code = findtext(response, "result", GENERAL_NS)
        return response_code == "SUCCESS"

    def ex_rename_network(self, network, new_name):
        """
        Rename a network in MCP 1 data center

        :param  network: The network to rename
        :type   network: :class:`DimensionDataNetwork`

        :param  new_name: The new name of the network
        :type   new_name: ``str``

        :rtype: ``bool``
        """
        response = self.connection.request_with_orgId_api_1(
            "network/%s" % network.id, method="POST", data="name=%s" % new_name
        ).object
        response_code = findtext(response, "result", GENERAL_NS)
        return response_code == "SUCCESS"

    def ex_get_network_domain(self, network_domain_id):
        """
        Get an individual Network Domain, by identifier

        :param      network_domain_id: The identifier of the network domain
        :type       network_domain_id: ``str``

        :rtype: :class:`DimensionDataNetworkDomain`
        """
        locations = self.list_locations()
        net = self.connection.request_with_orgId_api_2(
            "network/networkDomain/%s" % network_domain_id
        ).object
        return self._to_network_domain(net, locations)

    def ex_list_network_domains(self, location=None, name=None, service_plan=None, state=None):
        """
        List networks domains deployed across all data center locations domain.

        for your organization.
        The response includes the location of each network
        :param      location: Only network domains in the location (optional)
        :type       location: :class:`NodeLocation` or ``str``

        :param      name: Only network domains of this name (optional)
        :type       name: ``str``

        :param      service_plan: Only network domains of this type (optional)
        :type       service_plan: ``str``

        :param      state: Only network domains in this state (optional)
        :type       state: ``str``

        :return: a list of `DimensionDataNetwork` objects
        :rtype: ``list`` of :class:`DimensionDataNetwork`
        """
        params = {}
        if location is not None:
            params["datacenterId"] = self._location_to_location_id(location)
        if name is not None:
            params["name"] = name
        if service_plan is not None:
            params["type"] = service_plan
        if state is not None:
            params["state"] = state

        response = self.connection.request_with_orgId_api_2(
            "network/networkDomain", params=params
        ).object
        return self._to_network_domains(response)

    def ex_create_network_domain(self, location, name, service_plan, description=None):
        """
        Deploy a new network domain to a data center

        :param      location: The data center to list
        :type       location: :class:`NodeLocation` or ``str``

        :param      name: The name of the network domain to create
        :type       name: ``str``

        :param      service_plan: The service plan, either "ESSENTIALS"
            or "ADVANCED"
        :type       service_plan: ``str``

        :param      description: An additional description of
                                 the network domain
        :type       description: ``str``

        :return: an instance of `DimensionDataNetworkDomain`
        :rtype: :class:`DimensionDataNetworkDomain`
        """
        create_node = ET.Element("deployNetworkDomain", {"xmlns": TYPES_URN})
        ET.SubElement(create_node, "datacenterId").text = self._location_to_location_id(location)

        ET.SubElement(create_node, "name").text = name
        if description is not None:
            ET.SubElement(create_node, "description").text = description
        ET.SubElement(create_node, "type").text = service_plan

        response = self.connection.request_with_orgId_api_2(
            "network/deployNetworkDomain", method="POST", data=ET.tostring(create_node)
        ).object

        network_domain_id = None

        for info in findall(response, "info", TYPES_URN):
            if info.get("name") == "networkDomainId":
                network_domain_id = info.get("value")

        return DimensionDataNetworkDomain(
            id=network_domain_id,
            name=name,
            description=description,
            location=location,
            status=NodeState.RUNNING,
            plan=service_plan,
        )

    def ex_update_network_domain(self, network_domain):
        """
        Update the properties of a network domain

        :param      network_domain: The network domain with updated properties
        :type       network_domain: :class:`DimensionDataNetworkDomain`

        :return: an instance of `DimensionDataNetworkDomain`
        :rtype: :class:`DimensionDataNetworkDomain`
        """
        edit_node = ET.Element("editNetworkDomain", {"xmlns": TYPES_URN})
        edit_node.set("id", network_domain.id)
        ET.SubElement(edit_node, "name").text = network_domain.name
        if network_domain.description is not None:
            ET.SubElement(edit_node, "description").text = network_domain.description
        ET.SubElement(edit_node, "type").text = network_domain.plan

        self.connection.request_with_orgId_api_2(
            "network/editNetworkDomain", method="POST", data=ET.tostring(edit_node)
        ).object

        return network_domain

    def ex_delete_network_domain(self, network_domain):
        """
        Delete a network domain

        :param      network_domain: The network domain to delete
        :type       network_domain: :class:`DimensionDataNetworkDomain`

        :rtype: ``bool``
        """
        delete_node = ET.Element("deleteNetworkDomain", {"xmlns": TYPES_URN})
        delete_node.set("id", network_domain.id)
        result = self.connection.request_with_orgId_api_2(
            "network/deleteNetworkDomain", method="POST", data=ET.tostring(delete_node)
        ).object

        response_code = findtext(result, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_create_vlan(
        self,
        network_domain,
        name,
        private_ipv4_base_address,
        description=None,
        private_ipv4_prefix_size=24,
    ):
        """
        Deploy a new VLAN to a network domain

        :param      network_domain: The network domain to add the VLAN to
        :type       network_domain: :class:`DimensionDataNetworkDomain`

        :param      name: The name of the VLAN to create
        :type       name: ``str``

        :param      private_ipv4_base_address: The base IPv4 address
            e.g. 192.168.1.0
        :type       private_ipv4_base_address: ``str``

        :param      description: An additional description of the VLAN
        :type       description: ``str``

        :param      private_ipv4_prefix_size: The size of the IPv4
            address space, e.g 24
        :type       private_ipv4_prefix_size: ``int``

        :return: an instance of `DimensionDataVlan`
        :rtype: :class:`DimensionDataVlan`
        """
        create_node = ET.Element("deployVlan", {"xmlns": TYPES_URN})
        ET.SubElement(create_node, "networkDomainId").text = network_domain.id
        ET.SubElement(create_node, "name").text = name
        if description is not None:
            ET.SubElement(create_node, "description").text = description
        ET.SubElement(create_node, "privateIpv4BaseAddress").text = private_ipv4_base_address
        ET.SubElement(create_node, "privateIpv4PrefixSize").text = str(private_ipv4_prefix_size)

        response = self.connection.request_with_orgId_api_2(
            "network/deployVlan", method="POST", data=ET.tostring(create_node)
        ).object

        vlan_id = None

        for info in findall(response, "info", TYPES_URN):
            if info.get("name") == "vlanId":
                vlan_id = info.get("value")

        return self.ex_get_vlan(vlan_id)

    def ex_get_vlan(self, vlan_id):
        """
        Get a single VLAN, by it's identifier

        :param   vlan_id: The identifier of the VLAN
        :type    vlan_id: ``str``

        :return: an instance of `DimensionDataVlan`
        :rtype: :class:`DimensionDataVlan`
        """
        locations = self.list_locations()
        vlan = self.connection.request_with_orgId_api_2("network/vlan/%s" % vlan_id).object
        return self._to_vlan(vlan, locations)

    def ex_update_vlan(self, vlan):
        """
        Updates the properties of the given VLAN
        Only name and description are updated

        :param      vlan: The VLAN to update
        :type       vlan: :class:`DimensionDataNetworkDomain`

        :return: an instance of `DimensionDataVlan`
        :rtype: :class:`DimensionDataVlan`
        """
        edit_node = ET.Element("editVlan", {"xmlns": TYPES_URN})
        edit_node.set("id", vlan.id)
        ET.SubElement(edit_node, "name").text = vlan.name
        if vlan.description is not None:
            ET.SubElement(edit_node, "description").text = vlan.description

        self.connection.request_with_orgId_api_2(
            "network/editVlan", method="POST", data=ET.tostring(edit_node)
        ).object

        return vlan

    def ex_expand_vlan(self, vlan):
        """
        Expands the VLAN to the prefix size in private_ipv4_range_size
        The expansion will
        not be permitted if the proposed IP space overlaps with an
        already deployed VLANs IP space.

        :param      vlan: The VLAN to update
        :type       vlan: :class:`DimensionDataNetworkDomain`

        :return: an instance of `DimensionDataVlan`
        :rtype: :class:`DimensionDataVlan`
        """
        edit_node = ET.Element("expandVlan", {"xmlns": TYPES_URN})
        edit_node.set("id", vlan.id)
        ET.SubElement(edit_node, "privateIpv4PrefixSize").text = vlan.private_ipv4_range_size

        self.connection.request_with_orgId_api_2(
            "network/expandVlan", method="POST", data=ET.tostring(edit_node)
        ).object

        return vlan

    def ex_delete_vlan(self, vlan):
        """
        Deletes an existing VLAN

        :param      vlan: The VLAN to delete
        :type       vlan: :class:`DimensionDataNetworkDomain`

        :rtype: ``bool``
        """
        delete_node = ET.Element("deleteVlan", {"xmlns": TYPES_URN})
        delete_node.set("id", vlan.id)
        result = self.connection.request_with_orgId_api_2(
            "network/deleteVlan", method="POST", data=ET.tostring(delete_node)
        ).object

        response_code = findtext(result, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_list_vlans(
        self,
        location=None,
        network_domain=None,
        name=None,
        ipv4_address=None,
        ipv6_address=None,
        state=None,
    ):
        """
        List VLANs available, can filter by location and/or network domain

        :param      location: Only VLANs in this location (optional)
        :type       location: :class:`NodeLocation` or ``str``

        :param      network_domain: Only VLANs in this domain (optional)
        :type       network_domain: :class:`DimensionDataNetworkDomain`

        :param      name: Only VLANs with this name (optional)
        :type       name: ``str``

        :param      ipv4_address: Only VLANs with this ipv4 address (optional)
        :type       ipv4_address: ``str``

        :param      ipv6_address: Only VLANs with this ipv6 address  (optional)
        :type       ipv6_address: ``str``

        :param      state: Only VLANs with this state (optional)
        :type       state: ``str``

        :return: a list of DimensionDataVlan objects
        :rtype: ``list`` of :class:`DimensionDataVlan`
        """
        params = {}
        if location is not None:
            params["datacenterId"] = self._location_to_location_id(location)
        if network_domain is not None:
            params["networkDomainId"] = self._network_domain_to_network_domain_id(network_domain)
        if name is not None:
            params["name"] = name
        if ipv4_address is not None:
            params["privateIpv4Address"] = ipv4_address
        if ipv6_address is not None:
            params["ipv6Address"] = ipv6_address
        if state is not None:
            params["state"] = state
        response = self.connection.request_with_orgId_api_2("network/vlan", params=params).object
        return self._to_vlans(response)

    def ex_add_public_ip_block_to_network_domain(self, network_domain):
        add_node = ET.Element("addPublicIpBlock", {"xmlns": TYPES_URN})
        ET.SubElement(add_node, "networkDomainId").text = network_domain.id

        response = self.connection.request_with_orgId_api_2(
            "network/addPublicIpBlock", method="POST", data=ET.tostring(add_node)
        ).object

        block_id = None

        for info in findall(response, "info", TYPES_URN):
            if info.get("name") == "ipBlockId":
                block_id = info.get("value")
        return self.ex_get_public_ip_block(block_id)

    def ex_list_public_ip_blocks(self, network_domain):
        params = {}
        params["networkDomainId"] = network_domain.id

        response = self.connection.request_with_orgId_api_2(
            "network/publicIpBlock", params=params
        ).object
        return self._to_ip_blocks(response)

    def ex_get_public_ip_block(self, block_id):
        locations = self.list_locations()
        block = self.connection.request_with_orgId_api_2(
            "network/publicIpBlock/%s" % block_id
        ).object
        return self._to_ip_block(block, locations)

    def ex_delete_public_ip_block(self, block):
        delete_node = ET.Element("removePublicIpBlock", {"xmlns": TYPES_URN})
        delete_node.set("id", block.id)
        result = self.connection.request_with_orgId_api_2(
            "network/removePublicIpBlock", method="POST", data=ET.tostring(delete_node)
        ).object

        response_code = findtext(result, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_get_node_by_id(self, id):
        node = self.connection.request_with_orgId_api_2("server/server/%s" % id).object
        return self._to_node(node)

    def ex_list_firewall_rules(self, network_domain, page_size=50, page_number=1):
        params = {"pageSize": page_size, "pageNumber": page_number}
        params["networkDomainId"] = self._network_domain_to_network_domain_id(network_domain)

        response = self.connection.request_with_orgId_api_2(
            "network/firewallRule", params=params
        ).object
        return self._to_firewall_rules(response, network_domain)

    def ex_create_firewall_rule(
        self, network_domain, rule, position, position_relative_to_rule=None
    ):
        """
        Creates a firewall rule

        :param network_domain: The network domain in which to create
                                the firewall rule
        :type  network_domain: :class:`DimensionDataNetworkDomain` or ``str``

        :param rule: The rule in which to create
        :type  rule: :class:`DimensionDataFirewallRule`

        :param position: The position in which to create the rule
                         There are two types of positions
                         with position_relative_to_rule arg and without it
                         With: 'BEFORE' or 'AFTER'
                         Without: 'FIRST' or 'LAST'
        :type  position: ``str``

        :param position_relative_to_rule: The rule or rule name in
                                          which to decide positioning by
        :type  position_relative_to_rule:
            :class:`DimensionDataFirewallRule` or ``str``

        :rtype: ``bool``
        """
        positions_without_rule = ("FIRST", "LAST")
        positions_with_rule = ("BEFORE", "AFTER")

        create_node = ET.Element("createFirewallRule", {"xmlns": TYPES_URN})
        ET.SubElement(create_node, "networkDomainId").text = (
            self._network_domain_to_network_domain_id(network_domain)
        )
        ET.SubElement(create_node, "name").text = rule.name
        ET.SubElement(create_node, "action").text = rule.action
        ET.SubElement(create_node, "ipVersion").text = rule.ip_version
        ET.SubElement(create_node, "protocol").text = rule.protocol
        # Setup source port rule
        source = ET.SubElement(create_node, "source")
        if rule.source.address_list_id is not None:
            source_ip = ET.SubElement(source, "ipAddressListId")
            source_ip.text = rule.source.address_list_id
        else:
            source_ip = ET.SubElement(source, "ip")
            if rule.source.any_ip:
                source_ip.set("address", "ANY")
            else:
                source_ip.set("address", rule.source.ip_address)
                if rule.source.ip_prefix_size is not None:
                    source_ip.set("prefixSize", str(rule.source.ip_prefix_size))
        if rule.source.port_list_id is not None:
            source_port = ET.SubElement(source, "portListId")
            source_port.text = rule.source.port_list_id
        else:
            if rule.source.port_begin is not None:
                source_port = ET.SubElement(source, "port")
                source_port.set("begin", rule.source.port_begin)
            if rule.source.port_end is not None:
                source_port.set("end", rule.source.port_end)
        # Setup destination port rule
        dest = ET.SubElement(create_node, "destination")
        if rule.destination.address_list_id is not None:
            dest_ip = ET.SubElement(dest, "ipAddressListId")
            dest_ip.text = rule.destination.address_list_id
        else:
            dest_ip = ET.SubElement(dest, "ip")
            if rule.destination.any_ip:
                dest_ip.set("address", "ANY")
            else:
                dest_ip.set("address", rule.destination.ip_address)
                if rule.destination.ip_prefix_size is not None:
                    dest_ip.set("prefixSize", rule.destination.ip_prefix_size)
        if rule.destination.port_list_id is not None:
            dest_port = ET.SubElement(dest, "portListId")
            dest_port.text = rule.destination.port_list_id
        else:
            if rule.destination.port_begin is not None:
                dest_port = ET.SubElement(dest, "port")
                dest_port.set("begin", rule.destination.port_begin)
            if rule.destination.port_end is not None:
                dest_port.set("end", rule.destination.port_end)
        # Set up positioning of rule
        ET.SubElement(create_node, "enabled").text = str(rule.enabled).lower()
        placement = ET.SubElement(create_node, "placement")
        if position_relative_to_rule is not None:
            if position not in positions_with_rule:
                raise ValueError(
                    "When position_relative_to_rule is specified"
                    " position must be %s" % ", ".join(positions_with_rule)
                )
            if isinstance(position_relative_to_rule, DimensionDataFirewallRule):
                rule_name = position_relative_to_rule.name
            else:
                rule_name = position_relative_to_rule
            placement.set("relativeToRule", rule_name)
        else:
            if position not in positions_without_rule:
                raise ValueError(
                    "When position_relative_to_rule is not"
                    " specified position must be %s" % ", ".join(positions_without_rule)
                )
        placement.set("position", position)

        response = self.connection.request_with_orgId_api_2(
            "network/createFirewallRule", method="POST", data=ET.tostring(create_node)
        ).object

        rule_id = None
        for info in findall(response, "info", TYPES_URN):
            if info.get("name") == "firewallRuleId":
                rule_id = info.get("value")
        rule.id = rule_id
        return rule

    def ex_edit_firewall_rule(self, rule, position, relative_rule_for_position=None):
        """
        Edit a firewall rule

        >>> from pprint import pprint
        >>> from libcloud.compute.types import Provider
        >>> from libcloud.compute.providers import get_driver
        >>> import libcloud.security
        >>>
        >>> # Get dimension data driver
        >>> libcloud.security.VERIFY_SSL_CERT = True
        >>> cls = get_driver(Provider.DIMENSIONDATA)
        >>> driver = cls('myusername','mypassword', region='dd-au')
        >>>
        >>> # Get location
        >>> location = driver.ex_get_location_by_id(id='AU9')
        >>>
        >>> # Get network domain by location
        >>> networkDomainName = "Baas QA"
        >>> network_domains = driver.ex_list_network_domains(location=location)
        >>> my_network_domain = [d for d in network_domains if d.name ==
                              networkDomainName][0]
        >>>
        >>>
        >>> # List firewall rules
        >>> firewall_rules = driver.ex_list_firewall_rules(my_network_domain)
        >>>
        >>> # Get Firewall Rule by name
        >>> pprint("List specific firewall rule by name")
        >>> fire_rule_under_test = (list(filter(lambda x: x.name ==
                                   'My_New_Firewall_Rule', firewall_rules))[0])
        >>> pprint(fire_rule_under_test.source)
        >>> pprint(fire_rule_under_test.destination)
        >>>
        >>> # Edit Firewall
        >>> fire_rule_under_test.destination.address_list_id =
                '5e7c323f-c885-4e4b-9a27-94c44217dbd3'
        >>> fire_rule_under_test.destination.port_list_id =
                'b6557c5a-45fa-4138-89bd-8fe68392691b'
        >>> result = driver.ex_edit_firewall_rule(fire_rule_under_test, 'LAST')
        >>> pprint(result)

        :param rule: (required) The rule in which to create
        :type  rule: :class:`DimensionDataFirewallRule`

        :param position: (required) There are two types of positions
                         with position_relative_to_rule arg and without it
                         With: 'BEFORE' or 'AFTER'
                         Without: 'FIRST' or 'LAST'
        :type  position: ``str``

        :param relative_rule_for_position: (optional) The rule or rule name in
                                           which to decide the relative rule
                                           for positioning.
        :type  relative_rule_for_position:
            :class:`DimensionDataFirewallRule` or ``str``

        :rtype: ``bool``
        """

        positions_without_rule = ("FIRST", "LAST")
        positions_with_rule = ("BEFORE", "AFTER")

        edit_node = ET.Element("editFirewallRule", {"xmlns": TYPES_URN, "id": rule.id})
        ET.SubElement(edit_node, "action").text = rule.action
        ET.SubElement(edit_node, "protocol").text = rule.protocol

        # Source address
        source = ET.SubElement(edit_node, "source")
        if rule.source.address_list_id is not None:
            source_ip = ET.SubElement(source, "ipAddressListId")
            source_ip.text = rule.source.address_list_id
        else:
            source_ip = ET.SubElement(source, "ip")
            if rule.source.any_ip:
                source_ip.set("address", "ANY")
            else:
                source_ip.set("address", rule.source.ip_address)
                if rule.source.ip_prefix_size is not None:
                    source_ip.set("prefixSize", str(rule.source.ip_prefix_size))

        # Setup source port rule
        if rule.source.port_list_id is not None:
            source_port = ET.SubElement(source, "portListId")
            source_port.text = rule.source.port_list_id
        else:
            if rule.source.port_begin is not None:
                source_port = ET.SubElement(source, "port")
                source_port.set("begin", rule.source.port_begin)
            if rule.source.port_end is not None:
                source_port.set("end", rule.source.port_end)
        # Setup destination port rule
        dest = ET.SubElement(edit_node, "destination")
        if rule.destination.address_list_id is not None:
            dest_ip = ET.SubElement(dest, "ipAddressListId")
            dest_ip.text = rule.destination.address_list_id
        else:
            dest_ip = ET.SubElement(dest, "ip")
            if rule.destination.any_ip:
                dest_ip.set("address", "ANY")
            else:
                dest_ip.set("address", rule.destination.ip_address)
                if rule.destination.ip_prefix_size is not None:
                    dest_ip.set("prefixSize", rule.destination.ip_prefix_size)
        if rule.destination.port_list_id is not None:
            dest_port = ET.SubElement(dest, "portListId")
            dest_port.text = rule.destination.port_list_id
        else:
            if rule.destination.port_begin is not None:
                dest_port = ET.SubElement(dest, "port")
                dest_port.set("begin", rule.destination.port_begin)
            if rule.destination.port_end is not None:
                dest_port.set("end", rule.destination.port_end)
        # Set up positioning of rule
        ET.SubElement(edit_node, "enabled").text = str(rule.enabled).lower()
        placement = ET.SubElement(edit_node, "placement")
        if relative_rule_for_position is not None:
            if position not in positions_with_rule:
                raise ValueError(
                    "When position_relative_to_rule is specified"
                    " position must be %s" % ", ".join(positions_with_rule)
                )
            if isinstance(relative_rule_for_position, DimensionDataFirewallRule):
                rule_name = relative_rule_for_position.name
            else:
                rule_name = relative_rule_for_position
            placement.set("relativeToRule", rule_name)
        else:
            if position not in positions_without_rule:
                raise ValueError(
                    "When position_relative_to_rule is not"
                    " specified position must be %s" % ", ".join(positions_without_rule)
                )
        placement.set("position", position)

        response = self.connection.request_with_orgId_api_2(
            "network/editFirewallRule", method="POST", data=ET.tostring(edit_node)
        ).object

        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_get_firewall_rule(self, network_domain, rule_id):
        locations = self.list_locations()
        rule = self.connection.request_with_orgId_api_2("network/firewallRule/%s" % rule_id).object
        return self._to_firewall_rule(rule, locations, network_domain)

    def ex_set_firewall_rule_state(self, rule, state):
        """
        Change the state (enabled or disabled) of a rule

        :param rule: The rule to delete
        :type  rule: :class:`DimensionDataFirewallRule`

        :param state: The desired state enabled (True) or disabled (False)
        :type  state: ``bool``

        :rtype: ``bool``
        """
        update_node = ET.Element("editFirewallRule", {"xmlns": TYPES_URN})
        update_node.set("id", rule.id)
        ET.SubElement(update_node, "enabled").text = str(state).lower()
        result = self.connection.request_with_orgId_api_2(
            "network/editFirewallRule", method="POST", data=ET.tostring(update_node)
        ).object

        response_code = findtext(result, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_delete_firewall_rule(self, rule):
        """
        Delete a firewall rule

        :param rule: The rule to delete
        :type  rule: :class:`DimensionDataFirewallRule`

        :rtype: ``bool``
        """
        update_node = ET.Element("deleteFirewallRule", {"xmlns": TYPES_URN})
        update_node.set("id", rule.id)
        result = self.connection.request_with_orgId_api_2(
            "network/deleteFirewallRule", method="POST", data=ET.tostring(update_node)
        ).object

        response_code = findtext(result, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_create_nat_rule(self, network_domain, internal_ip, external_ip):
        """
        Create a NAT rule

        :param  network_domain: The network domain the rule belongs to
        :type   network_domain: :class:`DimensionDataNetworkDomain`

        :param  internal_ip: The IPv4 address internally
        :type   internal_ip: ``str``

        :param  external_ip: The IPv4 address externally
        :type   external_ip: ``str``

        :rtype: :class:`DimensionDataNatRule`
        """
        create_node = ET.Element("createNatRule", {"xmlns": TYPES_URN})
        ET.SubElement(create_node, "networkDomainId").text = network_domain.id
        ET.SubElement(create_node, "internalIp").text = internal_ip
        ET.SubElement(create_node, "externalIp").text = external_ip
        result = self.connection.request_with_orgId_api_2(
            "network/createNatRule", method="POST", data=ET.tostring(create_node)
        ).object

        rule_id = None
        for info in findall(result, "info", TYPES_URN):
            if info.get("name") == "natRuleId":
                rule_id = info.get("value")

        return DimensionDataNatRule(
            id=rule_id,
            network_domain=network_domain,
            internal_ip=internal_ip,
            external_ip=external_ip,
            status=NodeState.RUNNING,
        )

    def ex_list_nat_rules(self, network_domain):
        """
        Get NAT rules for the network domain

        :param  network_domain: The network domain the rules belongs to
        :type   network_domain: :class:`DimensionDataNetworkDomain`

        :rtype: ``list`` of :class:`DimensionDataNatRule`
        """
        params = {}
        params["networkDomainId"] = network_domain.id

        response = self.connection.request_with_orgId_api_2("network/natRule", params=params).object
        return self._to_nat_rules(response, network_domain)

    def ex_get_nat_rule(self, network_domain, rule_id):
        """
        Get a NAT rule by ID

        :param  network_domain: The network domain the rule belongs to
        :type   network_domain: :class:`DimensionDataNetworkDomain`

        :param  rule_id: The ID of the NAT rule to fetch
        :type   rule_id: ``str``

        :rtype: :class:`DimensionDataNatRule`
        """
        rule = self.connection.request_with_orgId_api_2("network/natRule/%s" % rule_id).object
        return self._to_nat_rule(rule, network_domain)

    def ex_delete_nat_rule(self, rule):
        """
        Delete an existing NAT rule

        :param  rule: The rule to delete
        :type   rule: :class:`DimensionDataNatRule`

        :rtype: ``bool``
        """
        update_node = ET.Element("deleteNatRule", {"xmlns": TYPES_URN})
        update_node.set("id", rule.id)
        result = self.connection.request_with_orgId_api_2(
            "network/deleteNatRule", method="POST", data=ET.tostring(update_node)
        ).object

        response_code = findtext(result, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_get_location_by_id(self, id):
        """
        Get location by ID.

        :param  id: ID of the node location which should be used
        :type   id: ``str``

        :rtype: :class:`NodeLocation`
        """
        location = None
        if id is not None:
            location = self.list_locations(ex_id=id)[0]
        return location

    def ex_wait_for_state(self, state, func, poll_interval=2, timeout=60, *args, **kwargs):
        """
        Wait for the function which returns a instance
        with field status to match

        Keep polling func until one of the desired states is matched

        :param state: Either the desired state (`str`) or a `list` of states
        :type  state: ``str`` or ``list``

        :param  func: The function to call, e.g. ex_get_vlan
        :type   func: ``function``

        :param  poll_interval: The number of seconds to wait between checks
        :type   poll_interval: `int`

        :param  timeout: The total number of seconds to wait to reach a state
        :type   timeout: `int`

        :param  args: The arguments for func
        :type   args: Positional arguments

        :param  kwargs: The arguments for func
        :type   kwargs: Keyword arguments
        """
        return self.connection.wait_for_state(state, func, poll_interval, timeout, *args, **kwargs)

    def ex_enable_monitoring(self, node, service_plan="ESSENTIALS"):
        """
        Enables cloud monitoring on a node

        :param   node: The node to monitor
        :type    node: :class:`Node`

        :param   service_plan: The service plan, one of ESSENTIALS or
                               ADVANCED
        :type    service_plan: ``str``

        :rtype: ``bool``
        """
        update_node = ET.Element("enableServerMonitoring", {"xmlns": TYPES_URN})
        update_node.set("id", node.id)
        ET.SubElement(update_node, "servicePlan").text = service_plan
        result = self.connection.request_with_orgId_api_2(
            "server/enableServerMonitoring",
            method="POST",
            data=ET.tostring(update_node),
        ).object

        response_code = findtext(result, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_update_monitoring_plan(self, node, service_plan="ESSENTIALS"):
        """
        Updates the service plan on a node with monitoring

        :param   node: The node to monitor
        :type    node: :class:`Node`

        :param   service_plan: The service plan, one of ESSENTIALS or
                               ADVANCED
        :type    service_plan: ``str``

        :rtype: ``bool``
        """
        update_node = ET.Element("changeServerMonitoringPlan", {"xmlns": TYPES_URN})
        update_node.set("id", node.id)
        ET.SubElement(update_node, "servicePlan").text = service_plan
        result = self.connection.request_with_orgId_api_2(
            "server/changeServerMonitoringPlan",
            method="POST",
            data=ET.tostring(update_node),
        ).object

        response_code = findtext(result, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_disable_monitoring(self, node):
        """
        Disables cloud monitoring for a node

        :param   node: The node to stop monitoring
        :type    node: :class:`Node`

        :rtype: ``bool``
        """
        update_node = ET.Element("disableServerMonitoring", {"xmlns": TYPES_URN})
        update_node.set("id", node.id)
        result = self.connection.request_with_orgId_api_2(
            "server/disableServerMonitoring",
            method="POST",
            data=ET.tostring(update_node),
        ).object

        response_code = findtext(result, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_add_storage_to_node(self, node, amount, speed="STANDARD", scsi_id=None):
        """
        Add storage to the node

        :param  node: The server to add storage to
        :type   node: :class:`Node`

        :param  amount: The amount of storage to add, in GB
        :type   amount: ``int``

        :param  speed: The disk speed type
        :type   speed: ``str``

        :param  scsi_id: The target SCSI ID (optional)
        :type   scsi_id: ``int``

        :rtype: ``bool``
        """
        update_node = ET.Element("addDisk", {"xmlns": TYPES_URN})
        update_node.set("id", node.id)
        ET.SubElement(update_node, "sizeGb").text = str(amount)
        ET.SubElement(update_node, "speed").text = speed.upper()
        if scsi_id is not None:
            ET.SubElement(update_node, "scsiId").text = str(scsi_id)

        result = self.connection.request_with_orgId_api_2(
            "server/addDisk", method="POST", data=ET.tostring(update_node)
        ).object
        response_code = findtext(result, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_remove_storage_from_node(self, node, scsi_id):
        """
        Remove storage from a node

        :param  node: The server to add storage to
        :type   node: :class:`Node`

        :param  scsi_id: The ID of the disk to remove
        :type   scsi_id: ``str``

        :rtype: ``bool``
        """
        disk = [disk for disk in node.extra["disks"] if disk.scsi_id == scsi_id][0]
        return self.ex_remove_storage(disk.id)

    def ex_remove_storage(self, disk_id):
        """
        Remove storage from a node

        :param  node: The server to add storage to
        :type   node: :class:`Node`

        :param  disk_id: The ID of the disk to remove
        :type   disk_id: ``str``

        :rtype: ``bool``
        """
        remove_disk = ET.Element("removeDisk", {"xmlns": TYPES_URN})
        remove_disk.set("id", disk_id)
        result = self.connection.request_with_orgId_api_2(
            "server/removeDisk", method="POST", data=ET.tostring(remove_disk)
        ).object
        response_code = findtext(result, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_change_storage_speed(self, node, disk_id, speed):
        """
        Change the speed (disk tier) of a disk

        :param  node: The server to change the disk speed of
        :type   node: :class:`Node`

        :param  disk_id: The ID of the disk to change
        :type   disk_id: ``str``

        :param  speed: The disk speed type e.g. STANDARD
        :type   speed: ``str``

        :rtype: ``bool``
        """
        create_node = ET.Element("ChangeDiskSpeed", {"xmlns": SERVER_NS})
        ET.SubElement(create_node, "speed").text = speed
        result = self.connection.request_with_orgId_api_1(
            "server/{}/disk/{}/changeSpeed".format(node.id, disk_id),
            method="POST",
            data=ET.tostring(create_node),
        ).object
        response_code = findtext(result, "result", GENERAL_NS)
        return response_code in ["IN_PROGRESS", "SUCCESS"]

    def ex_change_storage_size(self, node, disk_id, size):
        """
        Change the size of a disk

        :param  node: The server to change the disk of
        :type   node: :class:`Node`

        :param  disk_id: The ID of the disk to resize
        :type   disk_id: ``str``

        :param  size: The disk size in GB
        :type   size: ``int``

        :rtype: ``bool``
        """
        create_node = ET.Element("ChangeDiskSize", {"xmlns": SERVER_NS})
        ET.SubElement(create_node, "newSizeGb").text = str(size)
        result = self.connection.request_with_orgId_api_1(
            "server/{}/disk/{}/changeSize".format(node.id, disk_id),
            method="POST",
            data=ET.tostring(create_node),
        ).object
        response_code = findtext(result, "result", GENERAL_NS)
        return response_code in ["IN_PROGRESS", "SUCCESS"]

    def ex_reconfigure_node(self, node, memory_gb, cpu_count, cores_per_socket, cpu_performance):
        """
        Reconfigure the virtual hardware specification of a node

        :param  node: The server to change
        :type   node: :class:`Node`

        :param  memory_gb: The amount of memory in GB (optional)
        :type   memory_gb: ``int``

        :param  cpu_count: The number of CPU (optional)
        :type   cpu_count: ``int``

        :param  cores_per_socket: Number of CPU cores per socket (optional)
        :type   cores_per_socket: ``int``

        :param  cpu_performance: CPU Performance type (optional)
        :type   cpu_performance: ``str``

        :rtype: ``bool``
        """
        update = ET.Element("reconfigureServer", {"xmlns": TYPES_URN})
        update.set("id", node.id)
        if memory_gb is not None:
            ET.SubElement(update, "memoryGb").text = str(memory_gb)
        if cpu_count is not None:
            ET.SubElement(update, "cpuCount").text = str(cpu_count)
        if cpu_performance is not None:
            ET.SubElement(update, "cpuSpeed").text = cpu_performance
        if cores_per_socket is not None:
            ET.SubElement(update, "coresPerSocket").text = str(cores_per_socket)
        result = self.connection.request_with_orgId_api_2(
            "server/reconfigureServer", method="POST", data=ET.tostring(update)
        ).object
        response_code = findtext(result, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_clone_node_to_image(
        self,
        node,
        image_name,
        image_description=None,
        cluster_id=None,
        is_guest_Os_Customization=None,
        tag_key_id=None,
        tag_value=None,
    ):
        """
        Clone a server into a customer image.

        :param  node: The server to clone
        :type   node: :class:`Node`

        :param  image_name: The name of the clone image
        :type   image_name: ``str``

        :param  description: The description of the image
        :type   description: ``str``

        :rtype: ``bool``
        """
        if image_description is None:
            image_description = ""

        node_id = self._node_to_node_id(node)

        # Version 2.3 and lower
        if LooseVersion(self.connection.active_api_version) < LooseVersion("2.4"):
            response = self.connection.request_with_orgId_api_1(
                "server/{}?clone={}&desc={}".format(node_id, image_name, image_description)
            ).object

        # Version 2.4 and higher
        else:
            clone_server_elem = ET.Element("cloneServer", {"id": node_id, "xmlns": TYPES_URN})

            ET.SubElement(clone_server_elem, "imageName").text = image_name

            if image_description is not None:
                ET.SubElement(clone_server_elem, "description").text = image_description

            if cluster_id is not None:
                ET.SubElement(clone_server_elem, "clusterId").text = cluster_id

            if is_guest_Os_Customization is not None:
                ET.SubElement(clone_server_elem, "guestOsCustomization").text = (
                    is_guest_Os_Customization
                )

            if tag_key_id is not None:
                tag_elem = ET.SubElement(clone_server_elem, "tagById")
                ET.SubElement(tag_elem, "tagKeyId").text = tag_key_id

                if tag_value is not None:
                    ET.SubElement(tag_elem, "value").text = tag_value

            response = self.connection.request_with_orgId_api_2(
                "server/cloneServer", method="POST", data=ET.tostring(clone_server_elem)
            ).object

        # Version 2.3 and lower
        if LooseVersion(self.connection.active_api_version) < LooseVersion("2.4"):
            response_code = findtext(response, "result", GENERAL_NS)
        else:
            response_code = findtext(response, "responseCode", TYPES_URN)

        return response_code in ["IN_PROGRESS", "SUCCESS"]

    def ex_clean_failed_deployment(self, node):
        """
        Removes a node that has failed to deploy

        :param  node: The failed node to clean
        :type   node: :class:`Node` or ``str``
        """
        node_id = self._node_to_node_id(node)
        request_elm = ET.Element("cleanServer", {"xmlns": TYPES_URN, "id": node_id})
        body = self.connection.request_with_orgId_api_2(
            "server/cleanServer", method="POST", data=ET.tostring(request_elm)
        ).object
        response_code = findtext(body, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_list_customer_images(self, location=None):
        """
        Return a list of customer imported images

        :param location: The target location
        :type  location: :class:`NodeLocation` or ``str``

        :rtype: ``list`` of :class:`NodeImage`
        """
        params = {}
        if location is not None:
            params["datacenterId"] = self._location_to_location_id(location)

        return self._to_images(
            self.connection.request_with_orgId_api_2("image/customerImage", params=params).object,
            "customerImage",
        )

    def ex_get_base_image_by_id(self, id):
        """
        Gets a Base image in the Dimension Data Cloud given the id

        :param id: The id of the image
        :type  id: ``str``

        :rtype: :class:`NodeImage`
        """
        image = self.connection.request_with_orgId_api_2("image/osImage/%s" % id).object
        return self._to_image(image)

    def ex_get_customer_image_by_id(self, id):
        """
        Gets a Customer image in the Dimension Data Cloud given the id

        :param id: The id of the image
        :type  id: ``str``

        :rtype: :class:`NodeImage`
        """
        image = self.connection.request_with_orgId_api_2("image/customerImage/%s" % id).object
        return self._to_image(image)

    def ex_get_image_by_id(self, id):
        """
        Gets a Base/Customer image in the Dimension Data Cloud given the id

        Note: This first checks the base image
              If it is not a base image we check if it is a customer image
              If it is not in either of these a DimensionDataAPIException
              is thrown

        :param id: The id of the image
        :type  id: ``str``

        :rtype: :class:`NodeImage`
        """
        try:
            return self.ex_get_base_image_by_id(id)
        except DimensionDataAPIException as e:
            if e.code != "RESOURCE_NOT_FOUND":
                raise e
        return self.ex_get_customer_image_by_id(id)

    def ex_create_tag_key(
        self, name, description=None, value_required=True, display_on_report=True
    ):
        """
        Creates a tag key in the Dimension Data Cloud

        :param name: The name of the tag key (required)
        :type  name: ``str``

        :param description: The description of the tag key
        :type  description: ``str``

        :param value_required: If a value is required for the tag
                               Tags themselves can be just a tag,
                               or be a key/value pair
        :type  value_required: ``bool``

        :param display_on_report: Should this key show up on the usage reports
        :type  display_on_report: ``bool``

        :rtype: ``bool``
        """
        create_tag_key = ET.Element("createTagKey", {"xmlns": TYPES_URN})
        ET.SubElement(create_tag_key, "name").text = name
        if description is not None:
            ET.SubElement(create_tag_key, "description").text = description
        ET.SubElement(create_tag_key, "valueRequired").text = str(value_required).lower()
        ET.SubElement(create_tag_key, "displayOnReport").text = str(display_on_report).lower()
        response = self.connection.request_with_orgId_api_2(
            "tag/createTagKey", method="POST", data=ET.tostring(create_tag_key)
        ).object
        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_list_tag_keys(self, id=None, name=None, value_required=None, display_on_report=None):
        """
        List tag keys in the Dimension Data Cloud

        :param id: Filter the list to the id of the tag key
        :type  id: ``str``

        :param name: Filter the list to the name of the tag key
        :type  name: ``str``

        :param value_required: Filter the list to if a value is required
                               for a tag key
        :type  value_required: ``bool``

        :param display_on_report: Filter the list to if the tag key should
                                  show up on usage reports
        :type  display_on_report: ``bool``

        :rtype: ``list`` of :class:`DimensionDataTagKey`
        """
        params = {}
        if id is not None:
            params["id"] = id
        if name is not None:
            params["name"] = name
        if value_required is not None:
            params["valueRequired"] = str(value_required).lower()
        if display_on_report is not None:
            params["displayOnReport"] = str(display_on_report).lower()

        paged_result = self.connection.paginated_request_with_orgId_api_2(
            "tag/tagKey", method="GET", params=params
        )

        tag_keys = []
        for result in paged_result:
            tag_keys.extend(self._to_tag_keys(result))
        return tag_keys

    def ex_get_tag_key_by_id(self, id):
        """
        Get a specific tag key by ID

        :param id: ID of the tag key you want (required)
        :type  id: ``str``

        :rtype: :class:`DimensionDataTagKey`
        """
        tag_key = self.connection.request_with_orgId_api_2("tag/tagKey/%s" % id).object
        return self._to_tag_key(tag_key)

    def ex_get_tag_key_by_name(self, name):
        """
        Get a specific tag key by Name

        :param name: Name of the tag key you want (required)
        :type  name: ``str``

        :rtype: :class:`DimensionDataTagKey`
        """
        tag_keys = self.ex_list_tag_keys(name=name)
        if len(tag_keys) != 1:
            raise ValueError("No tags found with name %s" % name)
        return tag_keys[0]

    def ex_modify_tag_key(
        self,
        tag_key,
        name=None,
        description=None,
        value_required=None,
        display_on_report=None,
    ):
        """
        Modify a specific tag key

        :param tag_key: The tag key you want to modify (required)
        :type  tag_key: :class:`DimensionDataTagKey` or ``str``

        :param name: Set to modify the name of the tag key
        :type  name: ``str``

        :param description: Set to modify the description of the tag key
        :type  description: ``str``

        :param value_required: Set to modify if a value is required for
                               the tag key
        :type  value_required: ``bool``

        :param display_on_report: Set to modify if this tag key should display
                                  on the usage reports
        :type  display_on_report: ``bool``

        :rtype: ``bool``
        """
        tag_key_id = self._tag_key_to_tag_key_id(tag_key)
        modify_tag_key = ET.Element("editTagKey", {"xmlns": TYPES_URN, "id": tag_key_id})
        if name is not None:
            ET.SubElement(modify_tag_key, "name").text = name
        if description is not None:
            ET.SubElement(modify_tag_key, "description").text = description
        if value_required is not None:
            ET.SubElement(modify_tag_key, "valueRequired").text = str(value_required).lower()
        if display_on_report is not None:
            ET.SubElement(modify_tag_key, "displayOnReport").text = str(display_on_report).lower()

        response = self.connection.request_with_orgId_api_2(
            "tag/editTagKey", method="POST", data=ET.tostring(modify_tag_key)
        ).object
        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_remove_tag_key(self, tag_key):
        """
        Modify a specific tag key

        :param tag_key: The tag key you want to remove (required)
        :type  tag_key: :class:`DimensionDataTagKey` or ``str``

        :rtype: ``bool``
        """
        tag_key_id = self._tag_key_to_tag_key_id(tag_key)
        remove_tag_key = ET.Element("deleteTagKey", {"xmlns": TYPES_URN, "id": tag_key_id})
        response = self.connection.request_with_orgId_api_2(
            "tag/deleteTagKey", method="POST", data=ET.tostring(remove_tag_key)
        ).object
        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_apply_tag_to_asset(self, asset, tag_key, value=None):
        """
        Apply a tag to a Dimension Data Asset

        :param asset: The asset to apply a tag to. (required)
        :type  asset: :class:`Node` or :class:`NodeImage` or
                      :class:`DimensionDataNewtorkDomain` or
                      :class:`DimensionDataVlan` or
                      :class:`DimensionDataPublicIpBlock`

        :param tag_key: The tag_key to apply to the asset. (required)
        :type  tag_key: :class:`DimensionDataTagKey` or ``str``

        :param value: The value to be assigned to the tag key
                      This is only required if the :class:`DimensionDataTagKey`
                      requires it
        :type  value: ``str``

        :rtype: ``bool``
        """
        asset_type = self._get_tagging_asset_type(asset)
        tag_key_name = self._tag_key_to_tag_key_name(tag_key)

        apply_tags = ET.Element("applyTags", {"xmlns": TYPES_URN})
        ET.SubElement(apply_tags, "assetType").text = asset_type
        ET.SubElement(apply_tags, "assetId").text = asset.id

        tag_ele = ET.SubElement(apply_tags, "tag")
        ET.SubElement(tag_ele, "tagKeyName").text = tag_key_name
        if value is not None:
            ET.SubElement(tag_ele, "value").text = value

        response = self.connection.request_with_orgId_api_2(
            "tag/applyTags", method="POST", data=ET.tostring(apply_tags)
        ).object
        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_remove_tag_from_asset(self, asset, tag_key):
        """
        Remove a tag from an asset

        :param asset: The asset to remove a tag from. (required)
        :type  asset: :class:`Node` or :class:`NodeImage` or
                      :class:`DimensionDataNewtorkDomain` or
                      :class:`DimensionDataVlan` or
                      :class:`DimensionDataPublicIpBlock`

        :param tag_key: The tag key you want to remove (required)
        :type  tag_key: :class:`DimensionDataTagKey` or ``str``

        :rtype: ``bool``
        """
        asset_type = self._get_tagging_asset_type(asset)
        tag_key_name = self._tag_key_to_tag_key_name(tag_key)

        apply_tags = ET.Element("removeTags", {"xmlns": TYPES_URN})
        ET.SubElement(apply_tags, "assetType").text = asset_type
        ET.SubElement(apply_tags, "assetId").text = asset.id
        ET.SubElement(apply_tags, "tagKeyName").text = tag_key_name
        response = self.connection.request_with_orgId_api_2(
            "tag/removeTags", method="POST", data=ET.tostring(apply_tags)
        ).object
        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_list_tags(
        self,
        asset_id=None,
        asset_type=None,
        location=None,
        tag_key_name=None,
        tag_key_id=None,
        value=None,
        value_required=None,
        display_on_report=None,
    ):
        """
        List tags in the Dimension Data Cloud

        :param asset_id: Filter the list by asset id
        :type  asset_id: ``str``

        :param asset_type: Filter the list by asset type
        :type  asset_type: ``str``

        :param location: Filter the list by the assets location
        :type  location: :class:``NodeLocation`` or ``str``

        :param tag_key_name: Filter the list by a tag key name
        :type  tag_key_name: ``str``

        :param tag_key_id: Filter the list by a tag key id
        :type  tag_key_id: ``str``

        :param value: Filter the list by a tag value
        :type  value: ``str``

        :param value_required: Filter the list to if a value is required
                               for a tag
        :type  value_required: ``bool``

        :param display_on_report: Filter the list to if the tag should
                                  show up on usage reports
        :type  display_on_report: ``bool``

        :rtype: ``list`` of :class:`DimensionDataTag`
        """
        params = {}
        if asset_id is not None:
            params["assetId"] = asset_id
        if asset_type is not None:
            params["assetType"] = asset_type
        if location is not None:
            params["datacenterId"] = self._location_to_location_id(location)
        if tag_key_name is not None:
            params["tagKeyName"] = tag_key_name
        if tag_key_id is not None:
            params["tagKeyId"] = tag_key_id
        if value is not None:
            params["value"] = value
        if value_required is not None:
            params["valueRequired"] = str(value_required).lower()
        if display_on_report is not None:
            params["displayOnReport"] = str(display_on_report).lower()

        paged_result = self.connection.paginated_request_with_orgId_api_2(
            "tag/tag", method="GET", params=params
        )

        tags = []
        for result in paged_result:
            tags.extend(self._to_tags(result))
        return tags

    def ex_summary_usage_report(self, start_date, end_date):
        """
        Get summary usage information

        :param start_date: Start date for the report
        :type  start_date: ``str`` in format YYYY-MM-DD

        :param end_date: End date for the report
        :type  end_date: ``str`` in format YYYY-MM-DD

        :rtype: ``list`` of ``list``
        """
        result = self.connection.raw_request_with_orgId_api_1(
            "report/usage?startDate={}&endDate={}".format(start_date, end_date)
        )
        return self._format_csv(result.response)

    def ex_detailed_usage_report(self, start_date, end_date):
        """
        Get detailed usage information

        :param start_date: Start date for the report
        :type  start_date: ``str`` in format YYYY-MM-DD

        :param end_date: End date for the report
        :type  end_date: ``str`` in format YYYY-MM-DD

        :rtype: ``list`` of ``list``
        """
        result = self.connection.raw_request_with_orgId_api_1(
            "report/usageDetailed?startDate={}&endDate={}".format(start_date, end_date)
        )
        return self._format_csv(result.response)

    def ex_software_usage_report(self, start_date, end_date):
        """
        Get detailed software usage reports

        :param start_date: Start date for the report
        :type  start_date: ``str`` in format YYYY-MM-DD

        :param end_date: End date for the report
        :type  end_date: ``str`` in format YYYY-MM-DD

        :rtype: ``list`` of ``list``
        """
        result = self.connection.raw_request_with_orgId_api_1(
            "report/usageSoftwareUnits?startDate={}&endDate={}".format(start_date, end_date)
        )
        return self._format_csv(result.response)

    def ex_audit_log_report(self, start_date, end_date):
        """
        Get audit log report

        :param start_date: Start date for the report
        :type  start_date: ``str`` in format YYYY-MM-DD

        :param end_date: End date for the report
        :type  end_date: ``str`` in format YYYY-MM-DD

        :rtype: ``list`` of ``list``
        """
        result = self.connection.raw_request_with_orgId_api_1(
            "auditlog?startDate={}&endDate={}".format(start_date, end_date)
        )
        return self._format_csv(result.response)

    def ex_backup_usage_report(self, start_date, end_date, location):
        """
        Get audit log report

        :param start_date: Start date for the report
        :type  start_date: ``str`` in format YYYY-MM-DD

        :param end_date: End date for the report
        :type  end_date: ``str`` in format YYYY-MM-DD

        :keyword location: Filters the node list to nodes that are
                           located in this location
        :type    location: :class:`NodeLocation` or ``str``

        :rtype: ``list`` of ``list``
        """
        datacenter_id = self._location_to_location_id(location)
        result = self.connection.raw_request_with_orgId_api_1(
            "backup/detailedUsageReport?datacenterId=%s&fromDate=%s&toDate=%s"
            % (datacenter_id, start_date, end_date)
        )
        return self._format_csv(result.response)

    def ex_list_ip_address_list(self, ex_network_domain):
        """
        List IP Address List by network domain ID specified

        >>> from pprint import pprint
        >>> from libcloud.compute.types import Provider
        >>> from libcloud.compute.providers import get_driver
        >>> import libcloud.security
        >>>
        >>> # Get dimension data driver
        >>> libcloud.security.VERIFY_SSL_CERT = True
        >>> cls = get_driver(Provider.DIMENSIONDATA)
        >>> driver = cls('myusername','mypassword', region='dd-au')
        >>>
        >>> # Get location
        >>> location = driver.ex_get_location_by_id(id='AU9')
        >>>
        >>> # Get network domain by location
        >>> networkDomainName = "Baas QA"
        >>> network_domains = driver.ex_list_network_domains(location=location)
        >>> my_network_domain = [d for d in network_domains if d.name ==
                              networkDomainName][0]
        >>>
        >>> # List IP Address List of network domain
        >>> ipaddresslist_list = driver.ex_list_ip_address_list(
        >>>     ex_network_domain=my_network_domain)
        >>> pprint(ipaddresslist_list)

        :param  ex_network_domain: The network domain or network domain ID
        :type   ex_network_domain: :class:`DimensionDataNetworkDomain` or 'str'

        :return: a list of DimensionDataIpAddressList objects
        :rtype: ``list`` of :class:`DimensionDataIpAddressList`
        """
        params = {"networkDomainId": self._network_domain_to_network_domain_id(ex_network_domain)}
        response = self.connection.request_with_orgId_api_2(
            "network/ipAddressList", params=params
        ).object
        return self._to_ip_address_lists(response)

    def ex_get_ip_address_list(self, ex_network_domain, ex_ip_address_list_name):
        """
        Get IP Address List by name in network domain specified

        >>> from pprint import pprint
        >>> from libcloud.compute.types import Provider
        >>> from libcloud.compute.providers import get_driver
        >>> import libcloud.security
        >>>
        >>> # Get dimension data driver
        >>> libcloud.security.VERIFY_SSL_CERT = True
        >>> cls = get_driver(Provider.DIMENSIONDATA)
        >>> driver = cls('myusername','mypassword', region='dd-au')
        >>>
        >>> # Get location
        >>> location = driver.ex_get_location_by_id(id='AU9')
        >>>
        >>> # Get network domain by location
        >>> networkDomainName = "Baas QA"
        >>> network_domains = driver.ex_list_network_domains(location=location)
        >>> my_network_domain = [d for d in network_domains if d.name ==
                              networkDomainName][0]
        >>>
        >>> # Get IP Address List by Name
        >>> ipaddresslist_list_by_name = driver.ex_get_ip_address_list(
        >>>     ex_network_domain=my_network_domain,
        >>>     ex_ip_address_list_name='My_IP_AddressList_1')
        >>> pprint(ipaddresslist_list_by_name)


        :param  ex_network_domain: (required) The network domain or network
                                   domain ID in which ipaddresslist resides.
        :type   ex_network_domain: :class:`DimensionDataNetworkDomain` or 'str'

        :param    ex_ip_address_list_name: (required) Get 'IP Address List' by
                                            name
        :type     ex_ip_address_list_name: :``str``

        :return: a list of DimensionDataIpAddressList objects
        :rtype: ``list`` of :class:`DimensionDataIpAddressList`
        """

        ip_address_lists = self.ex_list_ip_address_list(ex_network_domain)
        return list(filter(lambda x: x.name == ex_ip_address_list_name, ip_address_lists))

    def ex_create_ip_address_list(
        self,
        ex_network_domain,
        name,
        description,
        ip_version,
        ip_address_collection,
        child_ip_address_list=None,
    ):
        """
        Create IP Address List. IP Address list.

        >>> from pprint import pprint
        >>> from libcloud.compute.types import Provider
        >>> from libcloud.compute.providers import get_driver
        >>> from libcloud.common.dimensiondata import DimensionDataIpAddress
        >>> import libcloud.security
        >>>
        >>> # Get dimension data driver
        >>> libcloud.security.VERIFY_SSL_CERT = True
        >>> cls = get_driver(Provider.DIMENSIONDATA)
        >>> driver = cls('myusername','mypassword', region='dd-au')
        >>>
        >>> # Get location
        >>> location = driver.ex_get_location_by_id(id='AU9')
        >>>
        >>> # Get network domain by location
        >>> networkDomainName = "Baas QA"
        >>> network_domains = driver.ex_list_network_domains(location=location)
        >>> my_network_domain = [d for d in network_domains if d.name ==
                              networkDomainName][0]
        >>>
        >>> # IP Address collection
        >>> ipAddress_1 = DimensionDataIpAddress(begin='190.2.2.100')
        >>> ipAddress_2 = DimensionDataIpAddress(begin='190.2.2.106',
                                                 end='190.2.2.108')
        >>> ipAddress_3 = DimensionDataIpAddress(begin='190.2.2.0',
                                                 prefix_size='24')
        >>> ip_address_collection = [ipAddress_1, ipAddress_2, ipAddress_3]
        >>>
        >>> # Create IPAddressList
        >>> result = driver.ex_create_ip_address_list(
        >>>     ex_network_domain=my_network_domain,
        >>>     name='My_IP_AddressList_2',
        >>>     ip_version='IPV4',
        >>>     description='Test only',
        >>>     ip_address_collection=ip_address_collection,
        >>>     child_ip_address_list='08468e26-eeb3-4c3d-8ff2-5351fa6d8a04'
        >>> )
        >>>
        >>> pprint(result)


        :param  ex_network_domain: The network domain or network domain ID
        :type   ex_network_domain: :class:`DimensionDataNetworkDomain` or 'str'

        :param    name:  IP Address List Name (required)
        :type      name: :``str``

        :param    description:  IP Address List Description (optional)
        :type      description: :``str``

        :param    ip_version:  IP Version of ip address (required)
        :type      ip_version: :``str``

        :param    ip_address_collection:  List of IP Address. At least one
                                          ipAddress element or one
                                          childIpAddressListId element must
                                          be provided.
        :type      ip_address_collection: :``str``

        :param    child_ip_address_list:  Child IP Address List or id to be
                                          included in this IP Address List.
                                          At least one ipAddress or
                                          one childIpAddressListId
                                          must be provided.
        :type     child_ip_address_list:
                        :class:'DimensionDataChildIpAddressList` or `str``

        :return: a list of DimensionDataIpAddressList objects
        :rtype: ``list`` of :class:`DimensionDataIpAddressList`
        """
        if ip_address_collection is None and child_ip_address_list is None:
            raise ValueError(
                "At least one ipAddress element or one "
                "childIpAddressListId element must be "
                "provided."
            )

        create_ip_address_list = ET.Element("createIpAddressList", {"xmlns": TYPES_URN})
        ET.SubElement(create_ip_address_list, "networkDomainId").text = (
            self._network_domain_to_network_domain_id(ex_network_domain)
        )

        ET.SubElement(create_ip_address_list, "name").text = name

        ET.SubElement(create_ip_address_list, "description").text = description

        ET.SubElement(create_ip_address_list, "ipVersion").text = ip_version

        for ip in ip_address_collection:
            ip_address = ET.SubElement(
                create_ip_address_list,
                "ipAddress",
            )
            ip_address.set("begin", ip.begin)

            if ip.end:
                ip_address.set("end", ip.end)

            if ip.prefix_size:
                ip_address.set("prefixSize", ip.prefix_size)

        if child_ip_address_list is not None:
            ET.SubElement(create_ip_address_list, "childIpAddressListId").text = (
                self._child_ip_address_list_to_child_ip_address_list_id(child_ip_address_list)
            )

        response = self.connection.request_with_orgId_api_2(
            "network/createIpAddressList",
            method="POST",
            data=ET.tostring(create_ip_address_list),
        ).object

        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_edit_ip_address_list(
        self,
        ex_ip_address_list,
        description,
        ip_address_collection,
        child_ip_address_lists=None,
    ):
        """
        Edit IP Address List. IP Address list.

        >>> from pprint import pprint
        >>> from libcloud.compute.types import Provider
        >>> from libcloud.compute.providers import get_driver
        >>> from libcloud.common.dimensiondata import DimensionDataIpAddress
        >>> import libcloud.security
        >>>
        >>> # Get dimension data driver
        >>> libcloud.security.VERIFY_SSL_CERT = True
        >>> cls = get_driver(Provider.DIMENSIONDATA)
        >>> driver = cls('myusername','mypassword', region='dd-au')
        >>>
        >>> # IP Address collection
        >>> ipAddress_1 = DimensionDataIpAddress(begin='190.2.2.100')
        >>> ipAddress_2 = DimensionDataIpAddress(begin='190.2.2.106',
        >>>                                      end='190.2.2.108')
        >>> ipAddress_3 = DimensionDataIpAddress(
        >>>                   begin='190.2.2.0', prefix_size='24')
        >>> ip_address_collection = [ipAddress_1, ipAddress_2, ipAddress_3]
        >>>
        >>> # Edit IP Address List
        >>> ip_address_list_id = '5e7c323f-c885-4e4b-9a27-94c44217dbd3'
        >>> result = driver.ex_edit_ip_address_list(
        >>>      ex_ip_address_list=ip_address_list_id,
        >>>      description="Edit Test",
        >>>      ip_address_collection=ip_address_collection,
        >>>      child_ip_address_lists=None
        >>>      )
        >>> pprint(result)

        :param    ex_ip_address_list:  (required) IpAddressList object or
                                       IpAddressList ID
        :type     ex_ip_address_list: :class:'DimensionDataIpAddressList'
                    or ``str``

        :param    description:  IP Address List Description
        :type      description: :``str``

        :param    ip_address_collection:  List of IP Address
        :type     ip_address_collection: ''list'' of
                                         :class:'DimensionDataIpAddressList'

        :param   child_ip_address_lists:  Child IP Address List or id to be
                                          included in this IP Address List
        :type    child_ip_address_lists:  ``list`` of
                                    :class:'DimensionDataChildIpAddressList'
                                    or ``str``

        :return: a list of DimensionDataIpAddressList objects
        :rtype: ``list`` of :class:`DimensionDataIpAddressList`
        """
        edit_ip_address_list = ET.Element(
            "editIpAddressList",
            {
                "xmlns": TYPES_URN,
                "id": self._ip_address_list_to_ip_address_list_id(ex_ip_address_list),
                "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            },
        )

        ET.SubElement(edit_ip_address_list, "description").text = description

        for ip in ip_address_collection:
            ip_address = ET.SubElement(
                edit_ip_address_list,
                "ipAddress",
            )
            ip_address.set("begin", ip.begin)

            if ip.end:
                ip_address.set("end", ip.end)

            if ip.prefix_size:
                ip_address.set("prefixSize", ip.prefix_size)

        if child_ip_address_lists is not None:
            ET.SubElement(edit_ip_address_list, "childIpAddressListId").text = (
                self._child_ip_address_list_to_child_ip_address_list_id(child_ip_address_lists)
            )
        else:
            ET.SubElement(edit_ip_address_list, "childIpAddressListId", {"xsi:nil": "true"})

        response = self.connection.request_with_orgId_api_2(
            "network/editIpAddressList",
            method="POST",
            data=ET.tostring(edit_ip_address_list),
        ).object

        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_delete_ip_address_list(self, ex_ip_address_list):
        """
        Delete IP Address List by ID

        >>> from pprint import pprint
        >>> from libcloud.compute.types import Provider
        >>> from libcloud.compute.providers import get_driver
        >>> import libcloud.security
        >>>
        >>> # Get dimension data driver
        >>> libcloud.security.VERIFY_SSL_CERT = True
        >>> cls = get_driver(Provider.DIMENSIONDATA)
        >>> driver = cls('myusername','mypassword', region='dd-au')
        >>>
        >>> ip_address_list_id = '5e7c323f-c885-4e4b-9a27-94c44217dbd3'
        >>> result = driver.ex_delete_ip_address_list(ip_address_list_id)
        >>> pprint(result)

        :param    ex_ip_address_list:  IP Address List object or IP Address
                                        List ID (required)
        :type     ex_ip_address_list: :class:'DimensionDataIpAddressList'
                    or ``str``

        :rtype: ``bool``
        """

        delete_ip_address_list = ET.Element(
            "deleteIpAddressList",
            {
                "xmlns": TYPES_URN,
                "id": self._ip_address_list_to_ip_address_list_id(ex_ip_address_list),
            },
        )

        response = self.connection.request_with_orgId_api_2(
            "network/deleteIpAddressList",
            method="POST",
            data=ET.tostring(delete_ip_address_list),
        ).object

        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_list_portlist(self, ex_network_domain):
        """
        List Portlist by network domain ID specified

        >>> from pprint import pprint
        >>> from libcloud.compute.types import Provider
        >>> from libcloud.compute.providers import get_driver
        >>> import libcloud.security
        >>>
        >>> # Get dimension data driver
        >>> libcloud.security.VERIFY_SSL_CERT = True
        >>> cls = get_driver(Provider.DIMENSIONDATA)
        >>> driver = cls('myusername','mypassword', region='dd-au')
        >>>
        >>> # Get location
        >>> location = driver.ex_get_location_by_id(id='AU9')
        >>>
        >>> # Get network domain by location
        >>> networkDomainName = "Baas QA"
        >>> network_domains = driver.ex_list_network_domains(location=location)
        >>> my_network_domain = [d for d in network_domains if d.name ==
        >>>                                               networkDomainName][0]
        >>>
        >>> # List portlist
        >>> portLists = driver.ex_list_portlist(
        >>>     ex_network_domain=my_network_domain)
        >>> pprint(portLists)
        >>>

        :param  ex_network_domain: The network domain or network domain ID
        :type   ex_network_domain: :class:`DimensionDataNetworkDomain` or 'str'

        :return: a list of DimensionDataPortList objects
        :rtype: ``list`` of :class:`DimensionDataPortList`
        """
        params = {"networkDomainId": self._network_domain_to_network_domain_id(ex_network_domain)}
        response = self.connection.request_with_orgId_api_2(
            "network/portList", params=params
        ).object
        return self._to_port_lists(response)

    def ex_get_portlist(self, ex_portlist_id):
        """
        Get Port List

        >>> from pprint import pprint
        >>> from libcloud.compute.types import Provider
        >>> from libcloud.compute.providers import get_driver
        >>> import libcloud.security
        >>>
        >>> # Get dimension data driver
        >>> libcloud.security.VERIFY_SSL_CERT = True
        >>> cls = get_driver(Provider.DIMENSIONDATA)
        >>> driver = cls('myusername','mypassword', region='dd-au')
        >>>
        >>> # Get specific portlist by ID
        >>> portlist_id = '27dd8c66-80ff-496b-9f54-2a3da2fe679e'
        >>> portlist = driver.ex_get_portlist(portlist_id)
        >>> pprint(portlist)

        :param  ex_portlist_id: The ex_port_list or ex_port_list ID
        :type   ex_portlist_id: :class:`DimensionDataNetworkDomain` or 'str'

        :return:  DimensionDataPortList object
        :rtype:  :class:`DimensionDataPort`
        """

        url_path = "network/portList/%s" % ex_portlist_id
        response = self.connection.request_with_orgId_api_2(url_path).object
        return self._to_port_list(response)

    def ex_create_portlist(
        self,
        ex_network_domain,
        name,
        description,
        port_collection,
        child_portlist_list=None,
    ):
        """
        Create Port List.

        >>> from pprint import pprint
        >>> from libcloud.compute.types import Provider
        >>> from libcloud.compute.providers import get_driver
        >>> from libcloud.common.dimensiondata import DimensionDataPort
        >>> import libcloud.security
        >>>
        >>> # Get dimension data driver
        >>> libcloud.security.VERIFY_SSL_CERT = True
        >>> cls = get_driver(Provider.DIMENSIONDATA)
        >>> driver = cls('myusername','mypassword', region='dd-au')
        >>>
        >>> # Get location
        >>> location = driver.ex_get_location_by_id(id='AU9')
        >>>
        >>> # Get network domain by location
        >>> networkDomainName = "Baas QA"
        >>> network_domains = driver.ex_list_network_domains(location=location)
        >>> my_network_domain = [d for d in network_domains if d.name ==
                              networkDomainName][0]
        >>>
        >>> # Port Collection
        >>> port_1 = DimensionDataPort(begin='1000')
        >>> port_2 = DimensionDataPort(begin='1001', end='1003')
        >>> port_collection = [port_1, port_2]
        >>>
        >>> # Create Port List
        >>> new_portlist = driver.ex_create_portlist(
        >>>     ex_network_domain=my_network_domain,
        >>>     name='MyPortListX',
        >>>     description="Test only",
        >>>     port_collection=port_collection,
        >>>     child_portlist_list={'a9cd4984-6ff5-4f93-89ff-8618ab642bb9'}
        >>>     )
        >>> pprint(new_portlist)

        :param    ex_network_domain:  (required) The network domain in
                                       which to create PortList. Provide
                                       networkdomain object or its id.
        :type      ex_network_domain: :``str``

        :param    name:  Port List Name
        :type     name: :``str``

        :param    description:  IP Address List Description
        :type     description: :``str``

        :param    port_collection:  List of Port Address
        :type     port_collection: :``str``

        :param    child_portlist_list:  List of Child Portlist to be
                                        included in this Port List
        :type     child_portlist_list: :``str`` or ''list of
                                         :class:'DimensionDataChildPortList'

        :return: result of operation
        :rtype: ``bool``
        """
        new_port_list = ET.Element("createPortList", {"xmlns": TYPES_URN})
        ET.SubElement(new_port_list, "networkDomainId").text = (
            self._network_domain_to_network_domain_id(ex_network_domain)
        )

        ET.SubElement(new_port_list, "name").text = name

        ET.SubElement(new_port_list, "description").text = description

        for port in port_collection:
            p = ET.SubElement(new_port_list, "port")
            p.set("begin", port.begin)

            if port.end:
                p.set("end", port.end)

        if child_portlist_list is not None:
            for child in child_portlist_list:
                ET.SubElement(new_port_list, "childPortListId").text = (
                    self._child_port_list_to_child_port_list_id(child)
                )

        response = self.connection.request_with_orgId_api_2(
            "network/createPortList", method="POST", data=ET.tostring(new_port_list)
        ).object

        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_edit_portlist(self, ex_portlist, description, port_collection, child_portlist_list=None):
        """
        Edit Port List.

        >>> from pprint import pprint
        >>> from libcloud.compute.types import Provider
        >>> from libcloud.compute.providers import get_driver
        >>> from libcloud.common.dimensiondata import DimensionDataPort
        >>> import libcloud.security
        >>>
        >>> # Get dimension data driver
        >>> libcloud.security.VERIFY_SSL_CERT = True
        >>> cls = get_driver(Provider.DIMENSIONDATA)
        >>> driver = cls('myusername','mypassword', region='dd-au')
        >>>
        >>> # Port Collection
        >>> port_1 = DimensionDataPort(begin='4200')
        >>> port_2 = DimensionDataPort(begin='4201', end='4210')
        >>> port_collection = [port_1, port_2]
        >>>
        >>> # Edit Port List
        >>> editPortlist = driver.ex_get_portlist(
            '27dd8c66-80ff-496b-9f54-2a3da2fe679e')
        >>>
        >>> result = driver.ex_edit_portlist(
        >>>     ex_portlist=editPortlist.id,
        >>>     description="Make Changes in portlist",
        >>>     port_collection=port_collection,
        >>>     child_portlist_list={'a9cd4984-6ff5-4f93-89ff-8618ab642bb9'}
        >>> )
        >>> pprint(result)

        :param    ex_portlist:  Port List to be edited
                                        (required)
        :type      ex_portlist: :``str`` or :class:'DimensionDataPortList'

        :param    description:  Port List Description
        :type      description: :``str``

        :param    port_collection:  List of Ports
        :type      port_collection: :``str``

        :param    child_portlist_list:  Child PortList to be included in
                                          this IP Address List
        :type      child_portlist_list: :``list`` of
                                          :class'DimensionDataChildPortList'
                                          or ''str''

        :return: a list of DimensionDataPortList objects
        :rtype: ``list`` of :class:`DimensionDataPortList`
        """

        existing_port_address_list = ET.Element(
            "editPortList",
            {
                "id": self._port_list_to_port_list_id(ex_portlist),
                "xmlns": TYPES_URN,
                "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            },
        )

        ET.SubElement(existing_port_address_list, "description").text = description

        for port in port_collection:
            p = ET.SubElement(existing_port_address_list, "port")
            p.set("begin", port.begin)

            if port.end:
                p.set("end", port.end)

        if child_portlist_list is not None:
            for child in child_portlist_list:
                ET.SubElement(existing_port_address_list, "childPortListId").text = (
                    self._child_port_list_to_child_port_list_id(child)
                )
        else:
            ET.SubElement(existing_port_address_list, "childPortListId", {"xsi:nil": "true"})

        response = self.connection.request_with_orgId_api_2(
            "network/editPortList",
            method="POST",
            data=ET.tostring(existing_port_address_list),
        ).object

        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_delete_portlist(self, ex_portlist):
        """
        Delete Port List

        >>> from pprint import pprint
        >>> from libcloud.compute.types import Provider
        >>> from libcloud.compute.providers import get_driver
        >>> import libcloud.security
        >>>
        >>> # Get dimension data driver
        >>> libcloud.security.VERIFY_SSL_CERT = True
        >>> cls = get_driver(Provider.DIMENSIONDATA)
        >>> driver = cls('myusername','mypassword', region='dd-au')
        >>>
        >>> # Delete Port List
        >>> portlist_id = '157531ce-77d4-493c-866b-d3d3fc4a912a'
        >>> response = driver.ex_delete_portlist(portlist_id)
        >>> pprint(response)

        :param    ex_portlist:  Port List to be deleted
        :type     ex_portlist: :``str`` or :class:'DimensionDataPortList'

        :rtype: ``bool``
        """

        delete_port_list = ET.Element(
            "deletePortList",
            {"xmlns": TYPES_URN, "id": self._port_list_to_port_list_id(ex_portlist)},
        )

        response = self.connection.request_with_orgId_api_2(
            "network/deletePortList", method="POST", data=ET.tostring(delete_port_list)
        ).object

        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_exchange_nic_vlans(self, nic_id_1, nic_id_2):
        """
        Exchange NIC Vlans

        :param    nic_id_1:  Nic ID 1
        :type     nic_id_1: :``str``

        :param    nic_id_2:  Nic ID 2
        :type     nic_id_2: :``str``

        :rtype: ``bool``
        """

        exchange_elem = ET.Element("urn:exchangeNicVlans", {"xmlns:urn": TYPES_URN})

        ET.SubElement(exchange_elem, "urn:nicId1").text = nic_id_1
        ET.SubElement(exchange_elem, "urn:nicId2").text = nic_id_2

        response = self.connection.request_with_orgId_api_2(
            "server/exchangeNicVlans", method="POST", data=ET.tostring(exchange_elem)
        ).object

        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_change_nic_network_adapter(self, nic_id, network_adapter_name):
        """
        Change network adapter of a NIC on a cloud server

        :param    nic_id:  Nic ID
        :type     nic_id: :``str``

        :param    network_adapter_name:  Network adapter name
        :type     network_adapter_name: :``str``

        :rtype: ``bool``
        """

        change_elem = ET.Element("changeNetworkAdapter", {"nicId": nic_id, "xmlns": TYPES_URN})

        ET.SubElement(change_elem, "networkAdapter").text = network_adapter_name

        response = self.connection.request_with_orgId_api_2(
            "server/changeNetworkAdapter", method="POST", data=ET.tostring(change_elem)
        ).object

        response_code = findtext(response, "responseCode", TYPES_URN)
        return response_code in ["IN_PROGRESS", "OK"]

    def ex_create_node_uncustomized(
        self,
        name,
        image,
        ex_network_domain,
        ex_is_started=True,
        ex_description=None,
        ex_cluster_id=None,
        ex_cpu_specification=None,
        ex_memory_gb=None,
        ex_primary_nic_private_ipv4=None,
        ex_primary_nic_vlan=None,
        ex_primary_nic_network_adapter=None,
        ex_additional_nics=None,
        ex_disks=None,
        ex_tagid_value_pairs=None,
        ex_tagname_value_pairs=None,
    ):
        """
        This MCP 2.0 only function deploys a new Cloud Server from a
        CloudControl compatible Server Image, which does not utilize
        VMware Guest OS Customization process.

        Create Node in MCP2 Data Center

        :keyword    name:   (required) String with a name for this new node
        :type       name:   ``str``

        :keyword    image:  (UUID of the Server Image being used as the target
                            for the new Server deployment. The source Server
                            Image (OS Image or Customer Image) must have
                            osCustomization set to true. See Get/List OS
                            Image(s) and Get/List Customer Image(s).
        :type       image:  :class:`NodeImage` or ``str``


        :keyword    ex_network_domain:  (required) Network Domain or Network
                                        Domain ID to create the node
        :type       ex_network_domain: :class:`DimensionDataNetworkDomain`
                                        or ``str``

        :keyword    ex_description:  (optional) description for this node
        :type       ex_description:  ``str``

        :keyword    ex_cluster_id:  (optional) For multiple cluster
        environments, it is possible to set a destination cluster for the new
        Customer Image. Note that performance of this function is optimal when
        either the Server cluster and destination are the same or when shared
        data storage is in place for the multiple clusters.
        :type       ex_cluster_id:  ``str``


        :keyword    ex_primary_nic_private_ipv4:  Provide private IPv4. Ignore
                                                  if ex_primary_nic_vlan is
                                                  provided. Use one or the
                                                  other. Not both.
        :type       ex_primary_nic_private_ipv4: :``str``

        :keyword    ex_primary_nic_vlan:  Provide VLAN for the node if
                                          ex_primary_nic_private_ipv4 NOT
                                          provided. One or the other. Not both.
        :type       ex_primary_nic_vlan: :class: DimensionDataVlan or ``str``

        :keyword    ex_primary_nic_network_adapter: (Optional) Default value
                                                    for the Operating System
                                                    will be used if leave
                                                    empty. Example: "E1000".
        :type       ex_primary_nic_network_adapter: :``str``

        :keyword    ex_additional_nics: (optional) List
                                        :class:'DimensionDataNic' or None
        :type       ex_additional_nics: ``list`` of :class:'DimensionDataNic'
                                        or ``str``

        :keyword    ex_memory_gb:  (optional) The amount of memory in GB for
                                   the server Can be used to override the
                                   memory value inherited from the source
                                   Server Image.
        :type       ex_memory_gb: ``int``

        :keyword    ex_cpu_specification: (optional) The spec of CPU to deploy
        :type       ex_cpu_specification:
                        :class:`DimensionDataServerCpuSpecification`

        :keyword    ex_is_started: (required) Start server after creation.
                                   Default is set to true.
        :type       ex_is_started:  ``bool``

        :keyword    ex_disks: (optional) Dimensiondata disks. Optional disk
                            elements can be used to define the disk speed
                            that each disk on the Server; inherited from the
                            source Server Image will be deployed to. It is
                            not necessary to include a diskelement for every
                            disk; only those that you wish to set a disk
                            speed value for. Note that scsiId 7 cannot be
                            used.Up to 13 disks can be present in addition to
                            the required OS disk on SCSI ID 0. Refer to
                            https://docs.mcp-services.net/x/UwIu for disk

        :type       ex_disks: List or tuple of :class:'DimensionDataServerDisk`

        :keyword    ex_tagid_value_pairs:
                            (Optional) up to 10 tag elements may be provided.
                            A combination of tagById and tag name cannot be
                            supplied in the same request.
                            Note: ex_tagid_value_pairs and
                            ex_tagname_value_pairs is
                            mutually exclusive. Use one or other.

        :type       ex_tagname_value_pairs: ``dict``.  Value can be None.

        :keyword    ex_tagname_value_pairs:
                            (Optional) up to 10 tag elements may be provided.
                            A combination of tagById and tag name cannot be
                            supplied in the same request.
                            Note: ex_tagid_value_pairs and
                            ex_tagname_value_pairs is
                            mutually exclusive. Use one or other.

        :type       ex_tagname_value_pairs: ``dict```.

        :return: The newly created :class:`Node`.
        :rtype: :class:`Node`
        """

        # Unsupported for version lower than 2.4
        if LooseVersion(self.connection.active_api_version) < LooseVersion("2.4"):
            raise Exception("This feature is NOT supported in  " "earlier api version of 2.4")

        # Default start to true if input is invalid
        if not isinstance(ex_is_started, bool):
            ex_is_started = True
            print("Warning: ex_is_started input value is invalid. Default" "to True")

        server_uncustomized_elm = ET.Element("deployUncustomizedServer", {"xmlns": TYPES_URN})
        ET.SubElement(server_uncustomized_elm, "name").text = name
        ET.SubElement(server_uncustomized_elm, "description").text = ex_description
        image_id = self._image_to_image_id(image)
        ET.SubElement(server_uncustomized_elm, "imageId").text = image_id

        if ex_cluster_id:
            dns_elm = ET.SubElement(server_uncustomized_elm, "primaryDns")
            dns_elm.text = ex_cluster_id

        if ex_is_started is not None:
            ET.SubElement(server_uncustomized_elm, "start").text = str(ex_is_started).lower()

        if ex_cpu_specification is not None:
            cpu = ET.SubElement(server_uncustomized_elm, "cpu")
            cpu.set("speed", ex_cpu_specification.performance)
            cpu.set("count", str(ex_cpu_specification.cpu_count))
            cpu.set("coresPerSocket", str(ex_cpu_specification.cores_per_socket))

        if ex_memory_gb is not None:
            ET.SubElement(server_uncustomized_elm, "memoryGb").text = str(ex_memory_gb)

        if ex_primary_nic_private_ipv4 is None and ex_primary_nic_vlan is None:
            raise ValueError(
                "Missing argument. Either "
                "ex_primary_nic_private_ipv4 or "
                "ex_primary_nic_vlan "
                "must be specified."
            )

        if ex_primary_nic_private_ipv4 is not None and ex_primary_nic_vlan is not None:
            raise ValueError(
                "Either ex_primary_nic_private_ipv4 or "
                "ex_primary_nic_vlan "
                "be specified. Not both."
            )

        network_elm = ET.SubElement(server_uncustomized_elm, "networkInfo")

        net_domain_id = self._network_domain_to_network_domain_id(ex_network_domain)
        network_elm.set("networkDomainId", net_domain_id)

        pri_nic = ET.SubElement(network_elm, "primaryNic")

        if ex_primary_nic_private_ipv4 is not None:
            ET.SubElement(pri_nic, "privateIpv4").text = ex_primary_nic_private_ipv4

        if ex_primary_nic_vlan is not None:
            vlan_id = self._vlan_to_vlan_id(ex_primary_nic_vlan)
            ET.SubElement(pri_nic, "vlanId").text = vlan_id

        if ex_primary_nic_network_adapter is not None:
            ET.SubElement(pri_nic, "networkAdapter").text = ex_primary_nic_network_adapter

        if isinstance(ex_additional_nics, (list, tuple)):
            for nic in ex_additional_nics:
                additional_nic = ET.SubElement(network_elm, "additionalNic")

                if nic.private_ip_v4 is None and nic.vlan is None:
                    raise ValueError(
                        "Either a vlan or private_ip_v4 "
                        "must be specified for each "
                        "additional nic."
                    )

                if nic.private_ip_v4 is not None and nic.vlan is not None:
                    raise ValueError(
                        "Either a vlan or private_ip_v4 "
                        "must be specified for each "
                        "additional nic. Not both."
                    )

                if nic.private_ip_v4 is not None:
                    ET.SubElement(additional_nic, "privateIpv4").text = nic.private_ip_v4

                if nic.vlan is not None:
                    vlan_id = self._vlan_to_vlan_id(nic.vlan)
                    ET.SubElement(additional_nic, "vlanId").text = vlan_id

                if nic.network_adapter_name is not None:
                    ET.SubElement(additional_nic, "networkAdapter").text = nic.network_adapter_name
        elif ex_additional_nics is not None:
            raise TypeError("ex_additional_NICs must be None or tuple/list")

        if isinstance(ex_disks, (list, tuple)):
            for disk in ex_disks:
                disk_elm = ET.SubElement(server_uncustomized_elm, "disk")
                disk_elm.set("scsiId", disk.scsi_id)
                disk_elm.set("speed", disk.speed)
        elif ex_disks is not None:
            raise TypeError("ex_disks must be None or tuple/list")

        # tagid and tagname value pair should not co-exists
        if ex_tagid_value_pairs is not None and ex_tagname_value_pairs is not None:
            raise ValueError(
                "ex_tagid_value_pairs and ex_tagname_value_pairs"
                "is mutually exclusive. Use one or the other."
            )

        # Tag by ID
        if ex_tagid_value_pairs is not None:
            if not isinstance(ex_tagid_value_pairs, dict):
                raise ValueError("ex_tagid_value_pairs must be a dictionary.")

            if sys.version_info[0] < 3:
                tagid_items = ex_tagid_value_pairs.iteritems()
            else:
                tagid_items = ex_tagid_value_pairs.items()

            for k, v in tagid_items:
                tag_elem = ET.SubElement(server_uncustomized_elm, "tagById")
                ET.SubElement(tag_elem, "tagKeyId").text = k

                if v is not None:
                    ET.SubElement(tag_elem, "value").text = v

        if ex_tagname_value_pairs is not None:
            if not isinstance(ex_tagname_value_pairs, dict):
                raise ValueError("ex_tagname_value_pairs must be a dictionary")

            if sys.version_info[0] < 3:
                tags_items = ex_tagname_value_pairs.iteritems()
            else:
                tags_items = ex_tagname_value_pairs.items()

            for k, v in tags_items:
                tag_name_elem = ET.SubElement(server_uncustomized_elm, "tag")
                ET.SubElement(tag_name_elem, "tagKeyName").text = k

                if v is not None:
                    ET.SubElement(tag_name_elem, "value").text = v

        response = self.connection.request_with_orgId_api_2(
            "server/deployUncustomizedServer",
            method="POST",
            data=ET.tostring(server_uncustomized_elm),
        ).object

        node_id = None
        for info in findall(response, "info", TYPES_URN):
            if info.get("name") == "serverId":
                node_id = info.get("value")

        new_node = self.ex_get_node_by_id(node_id)

        return new_node

    def _format_csv(self, http_response):
        text = http_response.read()
        lines = str.splitlines(ensure_string(text))
        return [line.split(",") for line in lines]

    @staticmethod
    def _get_tagging_asset_type(asset):
        objecttype = type(asset)
        if objecttype.__name__ in OBJECT_TO_TAGGING_ASSET_TYPE_MAP:
            return OBJECT_TO_TAGGING_ASSET_TYPE_MAP[objecttype.__name__]
        raise TypeError("Asset type %s cannot be tagged" % objecttype.__name__)

    def _list_nodes_single_page(self, params={}):
        nodes = self.connection.request_with_orgId_api_2("server/server", params=params).object
        return nodes

    def _to_tags(self, object):
        tags = []
        for element in object.findall(fixxpath("tag", TYPES_URN)):
            tags.append(self._to_tag(element))
        return tags

    def _to_tag(self, element):
        tag_key = self._to_tag_key(element, from_tag_api=True)
        return DimensionDataTag(
            asset_type=findtext(element, "assetType", TYPES_URN),
            asset_id=findtext(element, "assetId", TYPES_URN),
            asset_name=findtext(element, "assetName", TYPES_URN),
            datacenter=findtext(element, "datacenterId", TYPES_URN),
            key=tag_key,
            value=findtext(element, "value", TYPES_URN),
        )

    def _to_tag_keys(self, object):
        keys = []
        for element in object.findall(fixxpath("tagKey", TYPES_URN)):
            keys.append(self._to_tag_key(element))
        return keys

    def _to_tag_key(self, element, from_tag_api=False):
        if from_tag_api:
            id = findtext(element, "tagKeyId", TYPES_URN)
            name = findtext(element, "tagKeyName", TYPES_URN)
        else:
            id = element.get("id")
            name = findtext(element, "name", TYPES_URN)

        return DimensionDataTagKey(
            id=id,
            name=name,
            description=findtext(element, "description", TYPES_URN),
            value_required=self._str2bool(findtext(element, "valueRequired", TYPES_URN)),
            display_on_report=self._str2bool(findtext(element, "displayOnReport", TYPES_URN)),
        )

    def _to_images(self, object, el_name="osImage"):
        images = []
        locations = self.list_locations()

        # The CloudControl API will return all images
        # in the current geographic region (even ones in
        # datacenters the user's organisation does not have access to)
        #
        # We therefore need to filter out those images (since we can't
        # get a NodeLocation for them)
        location_ids = {location.id for location in locations}

        for element in object.findall(fixxpath(el_name, TYPES_URN)):
            location_id = element.get("datacenterId")
            if location_id in location_ids:
                images.append(self._to_image(element, locations))

        return images

    def _to_image(self, element, locations=None):
        location_id = element.get("datacenterId")
        if locations is None:
            locations = self.list_locations(location_id)

        location = [loc for loc in locations if loc.id == location_id][0]
        cpu_spec = self._to_cpu_spec(element.find(fixxpath("cpu", TYPES_URN)))

        if LooseVersion(self.connection.active_api_version) > LooseVersion("2.3"):
            os_el = element.find(fixxpath("guest/operatingSystem", TYPES_URN))
        else:
            os_el = element.find(fixxpath("operatingSystem", TYPES_URN))

        if element.tag.endswith("customerImage"):
            is_customer_image = True
        else:
            is_customer_image = False
        extra = {
            "description": findtext(element, "description", TYPES_URN),
            "OS_type": os_el.get("family"),
            "OS_displayName": os_el.get("displayName"),
            "cpu": cpu_spec,
            "memoryGb": findtext(element, "memoryGb", TYPES_URN),
            "osImageKey": findtext(element, "osImageKey", TYPES_URN),
            "created": findtext(element, "createTime", TYPES_URN),
            "location": location,
            "isCustomerImage": is_customer_image,
        }

        return NodeImage(
            id=element.get("id"),
            name=str(findtext(element, "name", TYPES_URN)),
            extra=extra,
            driver=self.connection.driver,
        )

    def _to_nat_rules(self, object, network_domain):
        rules = []
        for element in findall(object, "natRule", TYPES_URN):
            rules.append(self._to_nat_rule(element, network_domain))

        return rules

    def _to_nat_rule(self, element, network_domain):
        return DimensionDataNatRule(
            id=element.get("id"),
            network_domain=network_domain,
            internal_ip=findtext(element, "internalIp", TYPES_URN),
            external_ip=findtext(element, "externalIp", TYPES_URN),
            status=findtext(element, "state", TYPES_URN),
        )

    def _to_anti_affinity_rules(self, object):
        rules = []
        for element in findall(object, "antiAffinityRule", TYPES_URN):
            rules.append(self._to_anti_affinity_rule(element))
        return rules

    def _to_anti_affinity_rule(self, element):
        node_list = []
        for node in findall(element, "serverSummary", TYPES_URN):
            node_list.append(node.get("id"))
        return DimensionDataAntiAffinityRule(id=element.get("id"), node_list=node_list)

    def _to_firewall_rules(self, object, network_domain):
        rules = []
        locations = self.list_locations()
        for element in findall(object, "firewallRule", TYPES_URN):
            rules.append(self._to_firewall_rule(element, locations, network_domain))

        return rules

    def _to_firewall_rule(self, element, locations, network_domain):
        location_id = element.get("datacenterId")
        location = list(filter(lambda x: x.id == location_id, locations))[0]

        return DimensionDataFirewallRule(
            id=element.get("id"),
            network_domain=network_domain,
            name=findtext(element, "name", TYPES_URN),
            action=findtext(element, "action", TYPES_URN),
            ip_version=findtext(element, "ipVersion", TYPES_URN),
            protocol=findtext(element, "protocol", TYPES_URN),
            enabled=findtext(element, "enabled", TYPES_URN),
            source=self._to_firewall_address(element.find(fixxpath("source", TYPES_URN))),
            destination=self._to_firewall_address(element.find(fixxpath("destination", TYPES_URN))),
            location=location,
            status=findtext(element, "state", TYPES_URN),
        )

    def _to_firewall_address(self, element):
        ip = element.find(fixxpath("ip", TYPES_URN))
        port = element.find(fixxpath("port", TYPES_URN))
        port_list = element.find(fixxpath("portList", TYPES_URN))
        address_list = element.find(fixxpath("ipAddressList", TYPES_URN))
        if address_list is None:
            return DimensionDataFirewallAddress(
                any_ip=ip.get("address") == "ANY",
                ip_address=ip.get("address"),
                ip_prefix_size=ip.get("prefixSize"),
                port_begin=port.get("begin") if port is not None else None,
                port_end=port.get("end") if port is not None else None,
                port_list_id=port_list.get("id", None) if port_list is not None else None,
                address_list_id=address_list.get("id") if address_list is not None else None,
            )
        else:
            return DimensionDataFirewallAddress(
                any_ip=False,
                ip_address=None,
                ip_prefix_size=None,
                port_begin=None,
                port_end=None,
                port_list_id=port_list.get("id", None) if port_list is not None else None,
                address_list_id=address_list.get("id") if address_list is not None else None,
            )

    def _to_ip_blocks(self, object):
        blocks = []
        locations = self.list_locations()
        for element in findall(object, "publicIpBlock", TYPES_URN):
            blocks.append(self._to_ip_block(element, locations))

        return blocks

    def _to_ip_block(self, element, locations):
        location_id = element.get("datacenterId")
        location = list(filter(lambda x: x.id == location_id, locations))[0]

        return DimensionDataPublicIpBlock(
            id=element.get("id"),
            network_domain=self.ex_get_network_domain(
                findtext(element, "networkDomainId", TYPES_URN)
            ),
            base_ip=findtext(element, "baseIp", TYPES_URN),
            size=findtext(element, "size", TYPES_URN),
            location=location,
            status=findtext(element, "state", TYPES_URN),
        )

    def _to_networks(self, object):
        networks = []
        locations = self.list_locations()
        for element in findall(object, "network", NETWORK_NS):
            networks.append(self._to_network(element, locations))

        return networks

    def _to_network(self, element, locations):
        multicast = False
        if findtext(element, "multicast", NETWORK_NS) == "true":
            multicast = True

        status = self._to_status(element.find(fixxpath("status", NETWORK_NS)))

        location_id = findtext(element, "location", NETWORK_NS)
        location = list(filter(lambda x: x.id == location_id, locations))[0]

        return DimensionDataNetwork(
            id=findtext(element, "id", NETWORK_NS),
            name=findtext(element, "name", NETWORK_NS),
            description=findtext(element, "description", NETWORK_NS),
            location=location,
            private_net=findtext(element, "privateNet", NETWORK_NS),
            multicast=multicast,
            status=status,
        )

    def _to_network_domains(self, object):
        network_domains = []
        locations = self.list_locations()
        for element in findall(object, "networkDomain", TYPES_URN):
            network_domains.append(self._to_network_domain(element, locations))

        return network_domains

    def _to_network_domain(self, element, locations):
        location_id = element.get("datacenterId")
        location = list(filter(lambda x: x.id == location_id, locations))[0]
        plan = findtext(element, "type", TYPES_URN)
        if plan == "ESSENTIALS":
            plan_type = NetworkDomainServicePlan.ESSENTIALS
        else:
            plan_type = NetworkDomainServicePlan.ADVANCED
        return DimensionDataNetworkDomain(
            id=element.get("id"),
            name=findtext(element, "name", TYPES_URN),
            description=findtext(element, "description", TYPES_URN),
            plan=plan_type,
            location=location,
            status=findtext(element, "state", TYPES_URN),
        )

    def _to_vlans(self, object):
        vlans = []
        locations = self.list_locations()
        for element in findall(object, "vlan", TYPES_URN):
            vlans.append(self._to_vlan(element, locations=locations))

        return vlans

    def _to_vlan(self, element, locations):
        location_id = element.get("datacenterId")
        location = list(filter(lambda x: x.id == location_id, locations))[0]
        ip_range = element.find(fixxpath("privateIpv4Range", TYPES_URN))
        ip6_range = element.find(fixxpath("ipv6Range", TYPES_URN))
        network_domain_el = element.find(fixxpath("networkDomain", TYPES_URN))
        network_domain = self.ex_get_network_domain(network_domain_el.get("id"))
        return DimensionDataVlan(
            id=element.get("id"),
            name=findtext(element, "name", TYPES_URN),
            description=findtext(element, "description", TYPES_URN),
            network_domain=network_domain,
            private_ipv4_range_address=ip_range.get("address"),
            private_ipv4_range_size=int(ip_range.get("prefixSize")),
            ipv6_range_address=ip6_range.get("address"),
            ipv6_range_size=int(ip6_range.get("prefixSize")),
            ipv4_gateway=findtext(element, "ipv4GatewayAddress", TYPES_URN),
            ipv6_gateway=findtext(element, "ipv6GatewayAddress", TYPES_URN),
            location=location,
            status=findtext(element, "state", TYPES_URN),
        )

    def _to_locations(self, object):
        locations = []
        for element in object.findall(fixxpath("datacenter", TYPES_URN)):
            locations.append(self._to_location(element))

        return locations

    def _to_location(self, element):
        loc = NodeLocation(
            id=element.get("id"),
            name=findtext(element, "displayName", TYPES_URN),
            country=findtext(element, "country", TYPES_URN),
            driver=self,
        )
        return loc

    def _to_cpu_spec(self, element):
        return DimensionDataServerCpuSpecification(
            cpu_count=int(element.get("count")),
            cores_per_socket=int(element.get("coresPerSocket")),
            performance=element.get("speed"),
        )

    def _to_vmware_tools(self, element):
        status = None
        if hasattr(element, "runningStatus"):
            status = element.get("runningStatus")

        version_status = None
        if hasattr(element, "version_status"):
            version_status = element.get("version_status")

        api_version = None
        if hasattr(element, "apiVersion"):
            api_version = element.get("apiVersion")

        return DimensionDataServerVMWareTools(
            status=status, version_status=version_status, api_version=api_version
        )

    def _to_disks(self, object):
        disk_elements = object.findall(fixxpath("disk", TYPES_URN))
        return [self._to_disk(el) for el in disk_elements]

    def _to_disk(self, element):
        return DimensionDataServerDisk(
            id=element.get("id"),
            scsi_id=int(element.get("scsiId")),
            size_gb=int(element.get("sizeGb")),
            speed=element.get("speed"),
            state=element.get("state"),
        )

    def _to_nodes(self, object):
        node_elements = object.findall(fixxpath("server", TYPES_URN))
        return [self._to_node(el) for el in node_elements]

    def _to_node(self, element):
        started = findtext(element, "started", TYPES_URN)
        status = self._to_status(element.find(fixxpath("progress", TYPES_URN)))
        dd_state = findtext(element, "state", TYPES_URN)

        node_state = self._get_node_state(dd_state, started, status.action)

        has_network_info = element.find(fixxpath("networkInfo", TYPES_URN)) is not None
        cpu_spec = self._to_cpu_spec(element.find(fixxpath("cpu", TYPES_URN)))
        disks = self._to_disks(element)

        # Vmware Tools

        # Version 2.3 or earlier
        if LooseVersion(self.connection.active_api_version) < LooseVersion("2.4"):
            vmware_tools = self._to_vmware_tools(element.find(fixxpath("vmwareTools", TYPES_URN)))
            operation_system = element.find(fixxpath("operatingSystem", TYPES_URN))
        # Version 2.4 or later
        else:
            # vmtools_elm = fixxpath('guest/vmTools', TYPES_URN)
            vmtools_elm = element.find(fixxpath("guest/vmTools", TYPES_URN))
            if vmtools_elm is not None:
                vmware_tools = self._to_vmware_tools(vmtools_elm)
            else:
                vmware_tools = DimensionDataServerVMWareTools(
                    status=None, version_status=None, api_version=None
                )
            operation_system = element.find(fixxpath("guest/operatingSystem", TYPES_URN))

        extra = {
            "description": findtext(element, "description", TYPES_URN),
            "sourceImageId": findtext(element, "sourceImageId", TYPES_URN),
            "networkId": findtext(element, "networkId", TYPES_URN),
            "networkDomainId": (
                element.find(fixxpath("networkInfo", TYPES_URN)).get("networkDomainId")
                if has_network_info
                else None
            ),
            "datacenterId": element.get("datacenterId"),
            "deployedTime": findtext(element, "createTime", TYPES_URN),
            "cpu": cpu_spec,
            "memoryMb": int(findtext(element, "memoryGb", TYPES_URN)) * 1024,
            "OS_id": operation_system.get("id"),
            "OS_type": operation_system.get("family"),
            "OS_displayName": operation_system.get("displayName"),
            "status": status,
            "disks": disks,
            "vmWareTools": vmware_tools,
        }

        public_ip = findtext(element, "publicIpAddress", TYPES_URN)

        private_ip = (
            element.find(fixxpath("networkInfo/primaryNic", TYPES_URN)).get("privateIpv4")
            if has_network_info
            else element.find(fixxpath("nic", TYPES_URN)).get("privateIpv4")
        )

        extra["ipv6"] = (
            element.find(fixxpath("networkInfo/primaryNic", TYPES_URN)).get("ipv6")
            if has_network_info
            else element.find(fixxpath("nic", TYPES_URN)).get("ipv6")
        )

        n = Node(
            id=element.get("id"),
            name=findtext(element, "name", TYPES_URN),
            state=node_state,
            public_ips=[public_ip] if public_ip is not None else [],
            private_ips=[private_ip] if private_ip is not None else [],
            size=self.list_sizes()[0],
            image=NodeImage(
                extra["sourceImageId"], extra["OS_displayName"], self.connection.driver
            ),
            driver=self.connection.driver,
            extra=extra,
        )
        return n

    def _to_status(self, element):
        if element is None:
            return DimensionDataStatus()
        s = DimensionDataStatus(
            action=findtext(element, "action", TYPES_URN),
            request_time=findtext(element, "requestTime", TYPES_URN),
            user_name=findtext(element, "userName", TYPES_URN),
            number_of_steps=findtext(element, "numberOfSteps", TYPES_URN),
            step_name=findtext(element, "step/name", TYPES_URN),
            step_number=findtext(element, "step_number", TYPES_URN),
            step_percent_complete=findtext(element, "step/percentComplete", TYPES_URN),
            failure_reason=findtext(element, "failureReason", TYPES_URN),
        )
        return s

    def _to_ip_address_lists(self, object):
        ip_address_lists = []
        for element in findall(object, "ipAddressList", TYPES_URN):
            ip_address_lists.append(self._to_ip_address_list(element))

        return ip_address_lists

    def _to_ip_address_list(self, element):
        ipAddresses = []
        for ip in findall(element, "ipAddress", TYPES_URN):
            ipAddresses.append(self._to_ip_address(ip))

        child_ip_address_lists = []
        for child_ip_list in findall(element, "childIpAddressList", TYPES_URN):
            child_ip_address_lists.append(self._to_child_ip_list(child_ip_list))

        return DimensionDataIpAddressList(
            id=element.get("id"),
            name=findtext(element, "name", TYPES_URN),
            description=findtext(element, "description", TYPES_URN),
            ip_version=findtext(element, "ipVersion", TYPES_URN),
            ip_address_collection=ipAddresses,
            state=findtext(element, "state", TYPES_URN),
            create_time=findtext(element, "createTime", TYPES_URN),
            child_ip_address_lists=child_ip_address_lists,
        )

    def _to_child_ip_list(self, element):
        return DimensionDataChildIpAddressList(id=element.get("id"), name=element.get("name"))

    def _to_ip_address(self, element):
        return DimensionDataIpAddress(
            begin=element.get("begin"),
            end=element.get("end"),
            prefix_size=element.get("prefixSize"),
        )

    def _to_port_lists(self, object):
        port_lists = []
        for element in findall(object, "portList", TYPES_URN):
            port_lists.append(self._to_port_list(element))

        return port_lists

    def _to_port_list(self, element):
        ports = []
        for port in findall(element, "port", TYPES_URN):
            ports.append(self._to_port(element=port))

        child_portlist_list = []
        for child in findall(element, "childPortList", TYPES_URN):
            child_portlist_list.append(self._to_child_port_list(element=child))

        return DimensionDataPortList(
            id=element.get("id"),
            name=findtext(element, "name", TYPES_URN),
            description=findtext(element, "description", TYPES_URN),
            port_collection=ports,
            child_portlist_list=child_portlist_list,
            state=findtext(element, "state", TYPES_URN),
            create_time=findtext(element, "createTime", TYPES_URN),
        )

    def _image_needs_auth(self, image):
        if not isinstance(image, NodeImage):
            image = self.ex_get_image_by_id(image)
        if image.extra["isCustomerImage"] and image.extra["OS_type"] == "UNIX":
            return False
        return True

    @staticmethod
    def _to_port(element):
        return DimensionDataPort(begin=element.get("begin"), end=element.get("end"))

    @staticmethod
    def _to_child_port_list(element):
        return DimensionDataChildPortList(id=element.get("id"), name=element.get("name"))

    @staticmethod
    def _get_node_state(state, started, action):
        try:
            return NODE_STATE_MAP[(state, started, action)]
        except KeyError:
            if started == "true":
                return NodeState.RUNNING
            else:
                return NodeState.TERMINATED

    @staticmethod
    def _node_to_node_id(node):
        return dd_object_to_id(node, Node)

    @staticmethod
    def _location_to_location_id(location):
        return dd_object_to_id(location, NodeLocation)

    @staticmethod
    def _vlan_to_vlan_id(vlan):
        return dd_object_to_id(vlan, DimensionDataVlan)

    @staticmethod
    def _image_to_image_id(image):
        return dd_object_to_id(image, NodeImage)

    @staticmethod
    def _network_to_network_id(network):
        return dd_object_to_id(network, DimensionDataNetwork)

    @staticmethod
    def _anti_affinity_rule_to_anti_affinity_rule_id(rule):
        return dd_object_to_id(rule, DimensionDataAntiAffinityRule)

    @staticmethod
    def _network_domain_to_network_domain_id(network_domain):
        return dd_object_to_id(network_domain, DimensionDataNetworkDomain)

    @staticmethod
    def _tag_key_to_tag_key_id(tag_key):
        return dd_object_to_id(tag_key, DimensionDataTagKey)

    @staticmethod
    def _tag_key_to_tag_key_name(tag_key):
        return dd_object_to_id(tag_key, DimensionDataTagKey, id_value="name")

    @staticmethod
    def _ip_address_list_to_ip_address_list_id(ip_addr_list):
        return dd_object_to_id(ip_addr_list, DimensionDataIpAddressList, id_value="id")

    @staticmethod
    def _child_ip_address_list_to_child_ip_address_list_id(child_ip_addr_list):
        return dd_object_to_id(child_ip_addr_list, DimensionDataChildIpAddressList, id_value="id")

    @staticmethod
    def _port_list_to_port_list_id(port_list):
        return dd_object_to_id(port_list, DimensionDataPortList, id_value="id")

    @staticmethod
    def _child_port_list_to_child_port_list_id(child_port_list):
        return dd_object_to_id(child_port_list, DimensionDataChildPortList, id_value="id")

    @staticmethod
    def _str2bool(string):
        return string.lower() in ("true")
