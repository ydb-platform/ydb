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


import base64
import warnings

from libcloud.utils.py3 import b, urlparse
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
from libcloud.compute.types import (
    NodeState,
    LibcloudError,
    StorageVolumeState,
    KeyPairDoesNotExistError,
)
from libcloud.utils.networking import is_private_subnet
from libcloud.common.cloudstack import CloudStackDriverMixIn
from libcloud.compute.providers import Provider


# Utility functions
def transform_int_or_unlimited(value):
    try:
        return int(value)
    except ValueError as e:
        if str(value).lower() == "unlimited":
            return -1

        raise e


"""
Define the extra dictionary for specific resources
"""
RESOURCE_EXTRA_ATTRIBUTES_MAP = {
    "network": {
        "broadcast_domain_type": {
            "key_name": "broadcastdomaintype",
            "transform_func": str,
        },
        "traffic_type": {"key_name": "traffictype", "transform_func": str},
        "zone_name": {"key_name": "zonename", "transform_func": str},
        "network_offering_name": {
            "key_name": "networkofferingname",
            "transform_func": str,
        },
        "network_offeringdisplay_text": {
            "key_name": "networkofferingdisplaytext",
            "transform_func": str,
        },
        "network_offering_availability": {
            "key_name": "networkofferingavailability",
            "transform_func": str,
        },
        "is_system": {"key_name": "issystem", "transform_func": str},
        "state": {"key_name": "state", "transform_func": str},
        "dns1": {"key_name": "dns1", "transform_func": str},
        "dns2": {"key_name": "dns2", "transform_func": str},
        "type": {"key_name": "type", "transform_func": str},
        "acl_type": {"key_name": "acltype", "transform_func": str},
        "subdomain_access": {"key_name": "subdomainaccess", "transform_func": str},
        "network_domain": {"key_name": "networkdomain", "transform_func": str},
        "physical_network_id": {"key_name": "physicalnetworkid", "transform_func": str},
        "can_use_for_deploy": {"key_name": "canusefordeploy", "transform_func": str},
        "gateway": {"key_name": "gateway", "transform_func": str},
        "netmask": {"key_name": "netmask", "transform_func": str},
        "vpc_id": {"key_name": "vpcid", "transform_func": str},
        "project_id": {"key_name": "projectid", "transform_func": str},
    },
    "node": {
        "haenable": {"key_name": "haenable", "transform_func": str},
        "zone_id": {"key_name": "zoneid", "transform_func": str},
        "zone_name": {"key_name": "zonename", "transform_func": str},
        "key_name": {"key_name": "keypair", "transform_func": str},
        "password": {"key_name": "password", "transform_func": str},
        "image_id": {"key_name": "templateid", "transform_func": str},
        "image_name": {"key_name": "templatename", "transform_func": str},
        "template_display_text": {
            "key_name": "templatdisplaytext",
            "transform_func": str,
        },
        "password_enabled": {"key_name": "passwordenabled", "transform_func": str},
        "size_id": {"key_name": "serviceofferingid", "transform_func": str},
        "size_name": {"key_name": "serviceofferingname", "transform_func": str},
        "root_device_id": {"key_name": "rootdeviceid", "transform_func": str},
        "root_device_type": {"key_name": "rootdevicetype", "transform_func": str},
        "hypervisor": {"key_name": "hypervisor", "transform_func": str},
        "project": {"key_name": "project", "transform_func": str},
        "project_id": {"key_name": "projectid", "transform_func": str},
        "nics:": {"key_name": "nic", "transform_func": list},
    },
    "volume": {
        "created": {"key_name": "created", "transform_func": str},
        "device_id": {
            "key_name": "deviceid",
            "transform_func": transform_int_or_unlimited,
        },
        "instance_id": {"key_name": "virtualmachineid", "transform_func": str},
        "serviceoffering_id": {"key_name": "serviceofferingid", "transform_func": str},
        "state": {"key_name": "state", "transform_func": str},
        "volume_type": {"key_name": "type", "transform_func": str},
        "zone_id": {"key_name": "zoneid", "transform_func": str},
        "zone_name": {"key_name": "zonename", "transform_func": str},
    },
    "vpc": {
        "created": {"key_name": "created", "transform_func": str},
        "domain": {"key_name": "domain", "transform_func": str},
        "domain_id": {
            "key_name": "domainid",
            "transform_func": transform_int_or_unlimited,
        },
        "network_domain": {"key_name": "networkdomain", "transform_func": str},
        "state": {"key_name": "state", "transform_func": str},
        "vpc_offering_id": {"key_name": "vpcofferingid", "transform_func": str},
        "zone_name": {"key_name": "zonename", "transform_func": str},
        "zone_id": {"key_name": "zoneid", "transform_func": str},
    },
    "project": {
        "account": {"key_name": "account", "transform_func": str},
        "cpuavailable": {
            "key_name": "cpuavailable",
            "transform_func": transform_int_or_unlimited,
        },
        "cpulimit": {
            "key_name": "cpulimit",
            "transform_func": transform_int_or_unlimited,
        },
        "cputotal": {
            "key_name": "cputotal",
            "transform_func": transform_int_or_unlimited,
        },
        "domain": {"key_name": "domain", "transform_func": str},
        "domainid": {"key_name": "domainid", "transform_func": str},
        "ipavailable": {
            "key_name": "ipavailable",
            "transform_func": transform_int_or_unlimited,
        },
        "iplimit": {
            "key_name": "iplimit",
            "transform_func": transform_int_or_unlimited,
        },
        "iptotal": {
            "key_name": "iptotal",
            "transform_func": transform_int_or_unlimited,
        },
        "memoryavailable": {
            "key_name": "memoryavailable",
            "transform_func": transform_int_or_unlimited,
        },
        "memorylimit": {
            "key_name": "memorylimit",
            "transform_func": transform_int_or_unlimited,
        },
        "memorytotal": {
            "key_name": "memorytotal",
            "transform_func": transform_int_or_unlimited,
        },
        "networkavailable": {
            "key_name": "networkavailable",
            "transform_func": transform_int_or_unlimited,
        },
        "networklimit": {
            "key_name": "networklimit",
            "transform_func": transform_int_or_unlimited,
        },
        "networktotal": {
            "key_name": "networktotal",
            "transform_func": transform_int_or_unlimited,
        },
        "primarystorageavailable": {
            "key_name": "primarystorageavailable",
            "transform_func": transform_int_or_unlimited,
        },
        "primarystoragelimit": {
            "key_name": "primarystoragelimit",
            "transform_func": transform_int_or_unlimited,
        },
        "primarystoragetotal": {
            "key_name": "primarystoragetotal",
            "transform_func": transform_int_or_unlimited,
        },
        "secondarystorageavailable": {
            "key_name": "secondarystorageavailable",
            "transform_func": transform_int_or_unlimited,
        },
        "secondarystoragelimit": {
            "key_name": "secondarystoragelimit",
            "transform_func": transform_int_or_unlimited,
        },
        "secondarystoragetotal": {
            "key_name": "secondarystoragetotal",
            "transform_func": transform_int_or_unlimited,
        },
        "snapshotavailable": {
            "key_name": "snapshotavailable",
            "transform_func": transform_int_or_unlimited,
        },
        "snapshotlimit": {
            "key_name": "snapshotlimit",
            "transform_func": transform_int_or_unlimited,
        },
        "snapshottotal": {
            "key_name": "snapshottotal",
            "transform_func": transform_int_or_unlimited,
        },
        "state": {"key_name": "state", "transform_func": str},
        "tags": {"key_name": "tags", "transform_func": str},
        "templateavailable": {
            "key_name": "templateavailable",
            "transform_func": transform_int_or_unlimited,
        },
        "templatelimit": {
            "key_name": "templatelimit",
            "transform_func": transform_int_or_unlimited,
        },
        "templatetotal": {
            "key_name": "templatetotal",
            "transform_func": transform_int_or_unlimited,
        },
        "vmavailable": {
            "key_name": "vmavailable",
            "transform_func": transform_int_or_unlimited,
        },
        "vmlimit": {
            "key_name": "vmlimit",
            "transform_func": transform_int_or_unlimited,
        },
        "vmrunning": {
            "key_name": "vmrunning",
            "transform_func": transform_int_or_unlimited,
        },
        "vmtotal": {
            "key_name": "vmtotal",
            "transform_func": transform_int_or_unlimited,
        },
        "volumeavailable": {
            "key_name": "volumeavailable",
            "transform_func": transform_int_or_unlimited,
        },
        "volumelimit": {
            "key_name": "volumelimit",
            "transform_func": transform_int_or_unlimited,
        },
        "volumetotal": {
            "key_name": "volumetotal",
            "transform_func": transform_int_or_unlimited,
        },
        "vpcavailable": {
            "key_name": "vpcavailable",
            "transform_func": transform_int_or_unlimited,
        },
        "vpclimit": {
            "key_name": "vpclimit",
            "transform_func": transform_int_or_unlimited,
        },
        "vpctotal": {
            "key_name": "vpctotal",
            "transform_func": transform_int_or_unlimited,
        },
    },
    "nic": {"secondary_ip": {"key_name": "secondaryip", "transform_func": list}},
    "vpngateway": {
        "for_display": {"key_name": "fordisplay", "transform_func": str},
        "project": {"key_name": "project", "transform_func": str},
        "project_id": {"key_name": "projectid", "transform_func": str},
        "removed": {"key_name": "removed", "transform_func": str},
    },
    "vpncustomergateway": {
        "account": {"key_name": "account", "transform_func": str},
        "domain": {"key_name": "domain", "transform_func": str},
        "domain_id": {"key_name": "domainid", "transform_func": str},
        "dpd": {"key_name": "dpd", "transform_func": bool},
        "esp_lifetime": {
            "key_name": "esplifetime",
            "transform_func": transform_int_or_unlimited,
        },
        "ike_lifetime": {
            "key_name": "ikelifetime",
            "transform_func": transform_int_or_unlimited,
        },
        "name": {"key_name": "name", "transform_func": str},
    },
    "vpnconnection": {
        "account": {"key_name": "account", "transform_func": str},
        "domain": {"key_name": "domain", "transform_func": str},
        "domain_id": {"key_name": "domainid", "transform_func": str},
        "for_display": {"key_name": "fordisplay", "transform_func": str},
        "project": {"key_name": "project", "transform_func": str},
        "project_id": {"key_name": "projectid", "transform_func": str},
    },
}


class CloudStackNode(Node):
    """
    Subclass of Node so we can expose our extension methods.
    """

    def ex_allocate_public_ip(self):
        """
        Allocate a public IP and bind it to this node.
        """
        return self.driver.ex_allocate_public_ip(self)

    def ex_release_public_ip(self, address):
        """
        Release a public IP that this node holds.
        """
        return self.driver.ex_release_public_ip(self, address)

    def ex_create_ip_forwarding_rule(self, address, protocol, start_port, end_port=None):
        """
        Add a NAT/firewall forwarding rule for a port or ports.
        """
        return self.driver.ex_create_ip_forwarding_rule(
            node=self,
            address=address,
            protocol=protocol,
            start_port=start_port,
            end_port=end_port,
        )

    def ex_create_port_forwarding_rule(
        self,
        address,
        private_port,
        public_port,
        protocol,
        public_end_port=None,
        private_end_port=None,
        openfirewall=True,
    ):
        """
        Add a port forwarding rule for port or ports.
        """
        return self.driver.ex_create_port_forwarding_rule(
            node=self,
            address=address,
            private_port=private_port,
            public_port=public_port,
            protocol=protocol,
            public_end_port=public_end_port,
            private_end_port=private_end_port,
            openfirewall=openfirewall,
        )

    def ex_delete_ip_forwarding_rule(self, rule):
        """
        Delete a port forwarding rule.
        """
        return self.driver.ex_delete_ip_forwarding_rule(node=self, rule=rule)

    def ex_delete_port_forwarding_rule(self, rule):
        """
        Delete a NAT/firewall rule.
        """
        return self.driver.ex_delete_port_forwarding_rule(node=self, rule=rule)

    def ex_restore(self, template=None):
        """
        Restore virtual machine
        """
        return self.driver.ex_restore(node=self, template=template)

    def ex_change_node_size(self, offering):
        """
        Change virtual machine offering/size
        """
        return self.driver.ex_change_node_size(node=self, offering=offering)

    def ex_start(self):
        """
        Starts a stopped virtual machine.
        """
        return self.driver.ex_start(node=self)

    def ex_stop(self):
        """
        Stops a running virtual machine.
        """
        return self.driver.ex_stop(node=self)


class CloudStackAddress:
    """
    A public IP address.

    :param      id: UUID of the Public IP
    :type       id: ``str``

    :param      address: The public IP address
    :type       address: ``str``

    :param      associated_network_id: The ID of the network where this address
                                        has been associated with
    :type       associated_network_id: ``str``

    :param      vpc_id: VPC the ip belongs to
    :type       vpc_id: ``str``

    :param      virtualmachine_id: The ID of virtual machine this address
                                   is assigned to
    :type       virtualmachine_id: ``str``
    """

    def __init__(
        self,
        id,
        address,
        driver,
        associated_network_id=None,
        vpc_id=None,
        virtualmachine_id=None,
    ):
        self.id = id
        self.address = address
        self.driver = driver
        self.associated_network_id = associated_network_id
        self.vpc_id = vpc_id
        self.virtualmachine_id = virtualmachine_id

    def release(self):
        self.driver.ex_release_public_ip(address=self)

    def __str__(self):
        return self.address

    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.id == other.id


class CloudStackFirewallRule:
    """
    A firewall rule.
    """

    def __init__(
        self,
        id,
        address,
        cidr_list,
        protocol,
        icmp_code=None,
        icmp_type=None,
        start_port=None,
        end_port=None,
    ):
        """
        A Firewall rule.

        @note: This is a non-standard extension API, and only works for
               CloudStack.

        :param      id: Firewall Rule ID
        :type       id: ``int``

        :param      address: External IP address
        :type       address: :class:`CloudStackAddress`

        :param      cidr_list: cidr list
        :type       cidr_list: ``str``

        :param      protocol: TCP/IP Protocol (TCP, UDP)
        :type       protocol: ``str``

        :param      icmp_code: Error code for this icmp message
        :type       icmp_code: ``int``

        :param      icmp_type: Type of the icmp message being sent
        :type       icmp_type: ``int``

        :param      start_port: start of port range
        :type       start_port: ``int``

        :param      end_port: end of port range
        :type       end_port: ``int``

        :rtype: :class:`CloudStackFirewallRule`
        """

        self.id = id
        self.address = address
        self.cidr_list = cidr_list
        self.protocol = protocol
        self.icmp_code = icmp_code
        self.icmp_type = icmp_type
        self.start_port = start_port
        self.end_port = end_port

    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.id == other.id


class CloudStackEgressFirewallRule:
    """
    A egress firewall rule.
    """

    def __init__(
        self,
        id,
        network_id,
        cidr_list,
        protocol,
        icmp_code=None,
        icmp_type=None,
        start_port=None,
        end_port=None,
    ):
        """
        A egress firewall rule.

        @note: This is a non-standard extension API, and only works for
               CloudStack.

        :param      id: Firewall Rule ID
        :type       id: ``int``

        :param      network_id: the id network network for the egress firwall
                    services
        :type       network_id: ``str``

        :param      protocol: TCP/IP Protocol (TCP, UDP)
        :type       protocol: ``str``

        :param      cidr_list: cidr list
        :type       cidr_list: ``str``

        :param      icmp_code: Error code for this icmp message
        :type       icmp_code: ``int``

        :param      icmp_type: Type of the icmp message being sent
        :type       icmp_type: ``int``

        :param      start_port: start of port range
        :type       start_port: ``int``

        :param      end_port: end of port range
        :type       end_port: ``int``

        :rtype: :class:`CloudStackEgressFirewallRule`
        """

        self.id = id
        self.network_id = network_id
        self.cidr_list = cidr_list
        self.protocol = protocol
        self.icmp_code = icmp_code
        self.icmp_type = icmp_type
        self.start_port = start_port
        self.end_port = end_port

    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.id == other.id


class CloudStackIPForwardingRule:
    """
    A NAT/firewall forwarding rule.
    """

    def __init__(self, node, id, address, protocol, start_port, end_port=None):
        """
        A NAT/firewall forwarding rule.

        @note: This is a non-standard extension API, and only works for
               CloudStack.

        :param      node: Node for rule
        :type       node: :class:`Node`

        :param      id: Rule ID
        :type       id: ``int``

        :param      address: External IP address
        :type       address: :class:`CloudStackAddress`

        :param      protocol: TCP/IP Protocol (TCP, UDP)
        :type       protocol: ``str``

        :param      start_port: Start port for the rule
        :type       start_port: ``int``

        :param      end_port: End port for the rule
        :type       end_port: ``int``

        :rtype: :class:`CloudStackIPForwardingRule`
        """
        self.node = node
        self.id = id
        self.address = address
        self.protocol = protocol
        self.start_port = start_port
        self.end_port = end_port

    def delete(self):
        self.node.ex_delete_ip_forwarding_rule(rule=self)

    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.id == other.id


class CloudStackPortForwardingRule:
    """
    A Port forwarding rule for Source NAT.
    """

    def __init__(
        self,
        node,
        rule_id,
        address,
        protocol,
        public_port,
        private_port,
        public_end_port=None,
        private_end_port=None,
        network_id=None,
    ):
        """
        A Port forwarding rule for Source NAT.

        @note: This is a non-standard extension API, and only works for EC2.

        :param      node: Node for rule
        :type       node: :class:`Node`

        :param      rule_id: Rule ID
        :type       rule_id: ``int``

        :param      address: External IP address
        :type       address: :class:`CloudStackAddress`

        :param      protocol: TCP/IP Protocol (TCP, UDP)
        :type       protocol: ``str``

        :param      public_port: External port for rule (or start port if
                                 public_end_port is also provided)
        :type       public_port: ``int``

        :param      private_port: Internal node port for rule (or start port if
                                  public_end_port is also provided)
        :type       private_port: ``int``

        :param      public_end_port: End of external port range
        :type       public_end_port: ``int``

        :param      private_end_port: End of internal port range
        :type       private_end_port: ``int``

        :param      network_id: The network of the vm the Port Forwarding rule
                                will be created for. Required when public Ip
                                address is not associated with any Guest
                                network yet (VPC case)
        :type       network_id: ``str``

        :rtype: :class:`CloudStackPortForwardingRule`
        """
        self.node = node
        self.id = rule_id
        self.address = address
        self.protocol = protocol
        self.public_port = public_port
        self.public_end_port = public_end_port
        self.private_port = private_port
        self.private_end_port = private_end_port

    def delete(self):
        self.node.ex_delete_port_forwarding_rule(rule=self)

    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.id == other.id


class CloudStackNetworkACLList:
    """
    a Network ACL for the given VPC
    """

    def __init__(self, acl_id, name, vpc_id, driver, description=None):
        """
        a Network ACL for the given VPC

        @note: This is a non-standard extension API, and only works for
               Cloudstack.

        :param      acl_id: ACL ID
        :type       acl_id: ``int``

        :param      name: Name of the network ACL List
        :type       name: ``str``

        :param      vpc_id: Id of the VPC associated with this network ACL List
        :type       vpc_id: ``string``

        :param      description: Description of the network ACL List
        :type       description: ``str``

        :rtype: :class:`CloudStackNetworkACLList`
        """

        self.id = acl_id
        self.name = name
        self.vpc_id = vpc_id
        self.driver = driver
        self.description = description

    def __repr__(self):
        return (
            "<CloudStackNetworkACLList: id=%s, name=%s, vpc_id=%s, " "driver=%s, description=%s>"
        ) % (self.id, self.name, self.vpc_id, self.driver.name, self.description)


class CloudStackNetworkACL:
    """
    a ACL rule in the given network (the network has to belong to VPC)
    """

    def __init__(
        self,
        id,
        protocol,
        acl_id,
        action,
        cidr_list,
        start_port,
        end_port,
        traffic_type=None,
    ):
        """
        a ACL rule in the given network (the network has to belong to
        VPC)

        @note: This is a non-standard extension API, and only works for
               Cloudstack.

        :param      id: the ID of the ACL Item
        :type       id ``int``

        :param      protocol: the protocol for the ACL rule. Valid values are
                               TCP/UDP/ICMP/ALL or valid protocol number
        :type       protocol: ``string``

        :param      acl_id: Name of the network ACL List
        :type       acl_id: ``str``

        :param      action: scl entry action, allow or deny
        :type       action: ``string``

        :param      cidr_list: the cidr list to allow traffic from/to
        :type       cidr_list: ``str``

        :param      start_port: the starting port of ACL
        :type       start_port: ``str``

        :param      end_port: the ending port of ACL
        :type       end_port: ``str``

        :param      traffic_type: the traffic type for the ACL,can be Ingress
                                  or Egress, defaulted to Ingress if not
                                  specified
        :type       traffic_type: ``str``

        :rtype: :class:`CloudStackNetworkACL`
        """

        self.id = id
        self.protocol = protocol
        self.acl_id = acl_id
        self.action = action
        self.cidr_list = cidr_list
        self.start_port = start_port
        self.end_port = end_port
        self.traffic_type = traffic_type

    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.id == other.id


class CloudStackDiskOffering:
    """
    A disk offering within CloudStack.
    """

    def __init__(self, id, name, size, customizable):
        self.id = id
        self.name = name
        self.size = size
        self.customizable = customizable

    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.id == other.id


class CloudStackNetwork:
    """
    Class representing a CloudStack Network.
    """

    def __init__(self, displaytext, name, networkofferingid, id, zoneid, driver, extra=None):
        self.displaytext = displaytext
        self.name = name
        self.networkofferingid = networkofferingid
        self.id = id
        self.zoneid = zoneid
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return (
            "<CloudStackNetwork: displaytext=%s, name=%s, "
            "networkofferingid=%s, "
            "id=%s, zoneid=%s, driver=%s>"
        ) % (
            self.displaytext,
            self.name,
            self.networkofferingid,
            self.id,
            self.zoneid,
            self.driver.name,
        )


class CloudStackNetworkOffering:
    """
    Class representing a CloudStack Network Offering.
    """

    def __init__(
        self,
        name,
        display_text,
        guest_ip_type,
        id,
        service_offering_id,
        for_vpc,
        driver,
        extra=None,
    ):
        self.display_text = display_text
        self.name = name
        self.guest_ip_type = guest_ip_type
        self.id = id
        self.service_offering_id = service_offering_id
        self.for_vpc = for_vpc
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return (
            "<CloudStackNetworkOffering: id=%s, name=%s, "
            "display_text=%s, guest_ip_type=%s, service_offering_id=%s, "
            "for_vpc=%s, driver=%s>"
        ) % (
            self.id,
            self.name,
            self.display_text,
            self.guest_ip_type,
            self.service_offering_id,
            self.for_vpc,
            self.driver.name,
        )


class CloudStackNic:
    """
    Class representing a CloudStack Network Interface.
    """

    def __init__(
        self,
        id,
        network_id,
        net_mask,
        gateway,
        ip_address,
        is_default,
        mac_address,
        driver,
        extra=None,
    ):
        self.id = id
        self.network_id = network_id
        self.net_mask = net_mask
        self.gateway = gateway
        self.ip_address = ip_address
        self.is_default = is_default
        self.mac_address = mac_address
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return (
            "<CloudStackNic: id=%s, network_id=%s, "
            "net_mask=%s, gateway=%s, ip_address=%s, "
            "is_default=%s, mac_address=%s, driver%s>"
        ) % (
            self.id,
            self.network_id,
            self.net_mask,
            self.gateway,
            self.ip_address,
            self.is_default,
            self.mac_address,
            self.driver.name,
        )

    def __eq__(self, other):
        return self.__class__ is other.__class__ and self.id == other.id


class CloudStackVPC:
    """
    Class representing a CloudStack VPC.
    """

    def __init__(
        self,
        name,
        vpc_offering_id,
        id,
        cidr,
        driver,
        zone_id=None,
        display_text=None,
        extra=None,
    ):
        self.display_text = display_text
        self.name = name
        self.vpc_offering_id = vpc_offering_id
        self.id = id
        self.zone_id = zone_id
        self.cidr = cidr
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return (
            "<CloudStackVPC: name=%s, vpc_offering_id=%s, id=%s, "
            "cidr=%s, driver=%s, zone_id=%s, display_text=%s>"
        ) % (
            self.name,
            self.vpc_offering_id,
            self.id,
            self.cidr,
            self.driver.name,
            self.zone_id,
            self.display_text,
        )


class CloudStackVPCOffering:
    """
    Class representing a CloudStack VPC Offering.
    """

    def __init__(self, name, display_text, id, driver, extra=None):
        self.name = name
        self.display_text = display_text
        self.id = id
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return ("<CloudStackVPCOffering: name=%s, display_text=%s, " "id=%s, " "driver=%s>") % (
            self.name,
            self.display_text,
            self.id,
            self.driver.name,
        )


class CloudStackVpnGateway:
    """
    Class representing a CloudStack VPN Gateway.
    """

    def __init__(self, id, account, domain, domain_id, public_ip, vpc_id, driver, extra=None):
        self.id = id
        self.account = account
        self.domain = domain
        self.domain_id = domain_id
        self.public_ip = public_ip
        self.vpc_id = vpc_id
        self.driver = driver
        self.extra = extra or {}

    @property
    def vpc(self):
        for vpc in self.driver.ex_list_vpcs():
            if self.vpc_id == vpc.id:
                return vpc

        raise LibcloudError("VPC with id=%s not found" % self.vpc_id)

    def delete(self):
        return self.driver.ex_delete_vpn_gateway(vpn_gateway=self)

    def __repr__(self):
        return (
            "<CloudStackVpnGateway: account=%s, domain=%s, "
            "domain_id=%s, id=%s, public_ip=%s, vpc_id=%s, "
            "driver=%s>"
        ) % (
            self.account,
            self.domain,
            self.domain_id,
            self.id,
            self.public_ip,
            self.vpc_id,
            self.driver.name,
        )


class CloudStackVpnCustomerGateway:
    """
    Class representing a CloudStack VPN Customer Gateway.
    """

    def __init__(
        self,
        id,
        cidr_list,
        esp_policy,
        gateway,
        ike_policy,
        ipsec_psk,
        driver,
        extra=None,
    ):
        self.id = id
        self.cidr_list = cidr_list
        self.esp_policy = esp_policy
        self.gateway = gateway
        self.ike_policy = ike_policy
        self.ipsec_psk = ipsec_psk
        self.driver = driver
        self.extra = extra or {}

    def delete(self):
        return self.driver.ex_delete_vpn_customer_gateway(vpn_customer_gateway=self)

    def __repr__(self):
        return (
            "<CloudStackVpnCustomerGateway: id=%s, cidr_list=%s, "
            "esp_policy=%s, gateway=%s, ike_policy=%s, ipsec_psk=%s, "
            "driver=%s>"
        ) % (
            self.id,
            self.cidr_list,
            self.esp_policy,
            self.gateway,
            self.ike_policy,
            self.ipsec_psk,
            self.driver.name,
        )


class CloudStackVpnConnection:
    """
    Class representing a CloudStack VPN Connection.
    """

    def __init__(
        self,
        id,
        passive,
        vpn_customer_gateway_id,
        vpn_gateway_id,
        state,
        driver,
        extra=None,
    ):
        self.id = id
        self.passive = passive
        self.vpn_customer_gateway_id = vpn_customer_gateway_id
        self.vpn_gateway_id = vpn_gateway_id
        self.state = state
        self.driver = driver
        self.extra = extra or {}

    @property
    def vpn_customer_gateway(self):
        try:
            return self.driver.ex_list_vpn_customer_gateways(id=self.vpn_customer_gateway_id)[0]
        except IndexError:
            raise LibcloudError(
                "VPN Customer Gateway with id=%s not found" % self.vpn_customer_gateway_id
            )

    @property
    def vpn_gateway(self):
        try:
            return self.driver.ex_list_vpn_gateways(id=self.vpn_gateway_id)[0]
        except IndexError:
            raise LibcloudError("VPN Gateway with id=%s not found" % self.vpn_gateway_id)

    def delete(self):
        return self.driver.ex_delete_vpn_connection(vpn_connection=self)

    def __repr__(self):
        return (
            "<CloudStackVpnConnection: id=%s, passive=%s, "
            "vpn_customer_gateway_id=%s, vpn_gateway_id=%s, state=%s, "
            "driver=%s>"
        ) % (
            self.id,
            self.passive,
            self.vpn_customer_gateway_id,
            self.vpn_gateway_id,
            self.state,
            self.driver.name,
        )


class CloudStackRouter:
    """
    Class representing a CloudStack Router.
    """

    def __init__(self, id, name, state, public_ip, vpc_id, driver):
        self.id = id
        self.name = name
        self.state = state
        self.public_ip = public_ip
        self.vpc_id = vpc_id
        self.driver = driver

    def __repr__(self):
        return (
            "<CloudStackRouter: id=%s, name=%s, state=%s, " "public_ip=%s, vpc_id=%s, driver=%s>"
        ) % (
            self.id,
            self.name,
            self.state,
            self.public_ip,
            self.vpc_id,
            self.driver.name,
        )


class CloudStackProject:
    """
    Class representing a CloudStack Project.
    """

    def __init__(self, id, name, display_text, driver, extra=None):
        self.id = id
        self.name = name
        self.display_text = display_text
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return ("<CloudStackProject: id=%s, name=%s, display_text=%s," "driver=%s>") % (
            self.id,
            self.display_text,
            self.name,
            self.driver.name,
        )


class CloudStackAffinityGroup:
    """
    Class representing a CloudStack AffinityGroup.
    """

    def __init__(
        self,
        id,
        account,
        description,
        domain,
        domainid,
        name,
        group_type,
        virtualmachine_ids,
    ):
        """
        A CloudStack Affinity Group.

        @note: This is a non-standard extension API, and only works for
               CloudStack.

        :param      id: CloudStack Affinity Group ID
        :type       id: ``str``

        :param      account: An account for the affinity group. Must be used
                             with domainId.
        :type       account: ``str``

        :param      description: optional description of the affinity group
        :type       description: ``str``

        :param      domain: the domain name of the affinity group
        :type       domain: ``str``

        :param      domainid: domain ID of the account owning the affinity
                              group
        :type       domainid: ``str``

        :param      name: name of the affinity group
        :type       name: ``str``

        :param      group_type: the type of the affinity group
        :type       group_type: :class:`CloudStackAffinityGroupType`

        :param      virtualmachine_ids: virtual machine Ids associated with
                                        this affinity group
        :type       virtualmachine_ids: ``str``

        :rtype:     :class:`CloudStackAffinityGroup`
        """
        self.id = id
        self.account = account
        self.description = description
        self.domain = domain
        self.domainid = domainid
        self.name = name
        self.type = group_type
        self.virtualmachine_ids = virtualmachine_ids

    def __repr__(self):
        return ("<CloudStackAffinityGroup: id=%s, name=%s, type=%s>") % (
            self.id,
            self.name,
            self.type,
        )


class CloudStackAffinityGroupType:
    """
    Class representing a CloudStack AffinityGroupType.
    """

    def __init__(self, type_name):
        """
        A CloudStack Affinity Group Type.

        @note: This is a non-standard extension API, and only works for
               CloudStack.

        :param      type_name: the type of the affinity group
        :type       type_name: ``str``

        :rtype: :class:`CloudStackAffinityGroupType`
        """
        self.type = type_name

    def __repr__(self):
        return ("<CloudStackAffinityGroupType: type=%s>") % self.type


class CloudStackNodeDriver(CloudStackDriverMixIn, NodeDriver):
    """
    Driver for the CloudStack API.

    :cvar host: The host where the API can be reached.
    :cvar path: The path where the API can be reached.
    :cvar async_poll_frequency: How often (in seconds) to poll for async
                                job completion.
    :type async_poll_frequency: ``int``"""

    name = "CloudStack"
    api_name = "cloudstack"
    website = "http://cloudstack.org/"
    type = Provider.CLOUDSTACK

    features = {"create_node": ["generates_password"]}

    NODE_STATE_MAP = {
        "Running": NodeState.RUNNING,
        "Starting": NodeState.REBOOTING,
        "Migrating": NodeState.MIGRATING,
        "Stopped": NodeState.STOPPED,
        "Stopping": NodeState.PENDING,
        "Destroyed": NodeState.TERMINATED,
        "Expunging": NodeState.PENDING,
        "Error": NodeState.TERMINATED,
    }

    VOLUME_STATE_MAP = {
        "Creating": StorageVolumeState.CREATING,
        "Destroying": StorageVolumeState.DELETING,
        "Expunging": StorageVolumeState.DELETING,
        "Destroy": StorageVolumeState.DELETED,
        "Expunged": StorageVolumeState.DELETED,
        "Allocated": StorageVolumeState.AVAILABLE,
        "Ready": StorageVolumeState.AVAILABLE,
        "Snapshotting": StorageVolumeState.BACKUP,
        "UploadError": StorageVolumeState.ERROR,
        "Migrating": StorageVolumeState.MIGRATING,
    }

    def __init__(
        self,
        key,
        secret=None,
        secure=True,
        host=None,
        path=None,
        port=None,
        url=None,
        *args,
        **kwargs,
    ):
        """
        :inherits: :class:`NodeDriver.__init__`

        :param    host: The host where the API can be reached. (required)
        :type     host: ``str``

        :param    path: The path where the API can be reached. (required)
        :type     path: ``str``

        :param url: Full URL to the API endpoint. Mutually exclusive with host
                    and path argument.
        :type url: ``str``
        """
        if url:
            parsed = urlparse.urlparse(url)

            path = parsed.path

            scheme = parsed.scheme
            split = parsed.netloc.split(":")

            if len(split) == 1:
                # No port provided, use the default one
                host = parsed.netloc
                port = 443 if scheme == "https" else 80
            else:
                host = split[0]
                port = int(split[1])
        else:
            host = host if host else self.host
            path = path if path else self.path

        if path is not None:
            self.path = path

        if host is not None:
            self.host = host

        if (self.type == Provider.CLOUDSTACK) and (not host or not path):
            raise Exception(
                "When instantiating CloudStack driver directly "
                "you also need to provide url or host and path "
                "argument"
            )

        super().__init__(key=key, secret=secret, secure=secure, host=host, port=port)

    def list_images(self, location=None):
        args = {"templatefilter": "executable"}
        if location is not None:
            args["zoneid"] = location.id

        imgs = self._sync_request(command="listTemplates", params=args, method="GET")
        images = []
        for img in imgs.get("template", []):
            extra = {
                "hypervisor": img["hypervisor"],
                "format": img["format"],
                "os": img["ostypename"],
                "displaytext": img["displaytext"],
            }

            size = img.get("size", None)
            if size is not None:
                extra.update({"size": img["size"]})

            images.append(
                NodeImage(
                    id=img["id"],
                    name=img["name"],
                    driver=self.connection.driver,
                    extra=extra,
                )
            )
        return images

    def list_locations(self):
        """
        :rtype ``list`` of :class:`NodeLocation`
        """
        locs = self._sync_request("listZones")

        locations = []
        for loc in locs["zone"]:
            location = NodeLocation(str(loc["id"]), loc["name"], "Unknown", self)
            locations.append(location)

        return locations

    def list_nodes(self, project=None, location=None):
        """
        @inherits: :class:`NodeDriver.list_nodes`

        :keyword    project: Limit nodes returned to those configured under
                             the defined project.
        :type       project: :class:`.CloudStackProject`

        :keyword    location: Limit nodes returned to those in the defined
                              location.
        :type       location: :class:`.NodeLocation`

        :rtype: ``list`` of :class:`CloudStackNode`
        """

        args = {}

        if project:
            args["projectid"] = project.id

        if location is not None:
            args["zoneid"] = location.id

        vms = self._sync_request("listVirtualMachines", params=args)
        addrs = self._sync_request("listPublicIpAddresses", params=args)
        port_forwarding_rules = self._sync_request("listPortForwardingRules")
        ip_forwarding_rules = self._sync_request("listIpForwardingRules")

        public_ips_map = {}
        for addr in addrs.get("publicipaddress", []):
            if "virtualmachineid" not in addr:
                continue
            vm_id = str(addr["virtualmachineid"])
            if vm_id not in public_ips_map:
                public_ips_map[vm_id] = {}
            public_ips_map[vm_id][addr["ipaddress"]] = addr["id"]

        nodes = []

        for vm in vms.get("virtualmachine", []):
            public_ips = public_ips_map.get(str(vm["id"]), {}).keys()
            public_ips = list(public_ips)
            node = self._to_node(data=vm, public_ips=public_ips)

            addresses = public_ips_map.get(str(vm["id"]), {}).items()
            addresses = [
                CloudStackAddress(id=address_id, address=address, driver=node.driver)
                for address, address_id in addresses
            ]
            node.extra["ip_addresses"] = addresses

            rules = []
            for addr in addresses:
                for r in ip_forwarding_rules.get("ipforwardingrule", []):
                    if str(r["virtualmachineid"]) == node.id:
                        rule = CloudStackIPForwardingRule(
                            node,
                            r["id"],
                            addr,
                            r["protocol"].upper(),
                            r["startport"],
                            r["endport"],
                        )
                        rules.append(rule)
            node.extra["ip_forwarding_rules"] = rules

            rules = []
            for r in port_forwarding_rules.get("portforwardingrule", []):
                if str(r["virtualmachineid"]) == node.id:
                    addr = [
                        CloudStackAddress(id=a["id"], address=a["ipaddress"], driver=node.driver)
                        for a in addrs.get("publicipaddress", [])
                        if a["ipaddress"] == r["ipaddress"]
                    ]
                    rule = CloudStackPortForwardingRule(
                        node,
                        r["id"],
                        addr[0],
                        r["protocol"].upper(),
                        r["publicport"],
                        r["privateport"],
                        r["publicendport"],
                        r["privateendport"],
                    )
                    if not addr[0].address in node.public_ips:
                        node.public_ips.append(addr[0].address)
                    rules.append(rule)
            node.extra["port_forwarding_rules"] = rules

            nodes.append(node)

        return nodes

    def ex_get_node(self, node_id, project=None):
        """
        Return a Node object based on its ID.

        :param  node_id: The id of the node
        :type   node_id: ``str``

        :keyword    project: Limit node returned to those configured under
                             the defined project.
        :type       project: :class:`.CloudStackProject`

        :rtype: :class:`CloudStackNode`
        """
        list_nodes_args = {"id": node_id}
        list_ips_args = {}
        if project:
            list_nodes_args["projectid"] = project.id
            list_ips_args["projectid"] = project.id
        vms = self._sync_request("listVirtualMachines", params=list_nodes_args)
        if not vms:
            raise Exception("Node '%s' not found" % node_id)
        vm = vms["virtualmachine"][0]
        addrs = self._sync_request("listPublicIpAddresses", params=list_ips_args)

        public_ips = {}
        for addr in addrs.get("publicipaddress", []):
            if "virtualmachineid" not in addr:
                continue
            public_ips[addr["ipaddress"]] = addr["id"]

        node = self._to_node(data=vm, public_ips=list(public_ips.keys()))

        addresses = [
            CloudStackAddress(id=address_id, address=address, driver=node.driver)
            for address, address_id in public_ips.items()
        ]
        node.extra["ip_addresses"] = addresses

        rules = []
        list_fw_rules = {"virtualmachineid": node_id}
        for addr in addresses:
            result = self._sync_request("listIpForwardingRules", params=list_fw_rules)
            for r in result.get("ipforwardingrule", []):
                if str(r["virtualmachineid"]) == node.id:
                    rule = CloudStackIPForwardingRule(
                        node,
                        r["id"],
                        addr,
                        r["protocol"].upper(),
                        r["startport"],
                        r["endport"],
                    )
                    rules.append(rule)
        node.extra["ip_forwarding_rules"] = rules

        rules = []
        public_ips = self.ex_list_public_ips()
        result = self._sync_request("listPortForwardingRules", params=list_fw_rules)
        for r in result.get("portforwardingrule", []):
            if str(r["virtualmachineid"]) == node.id:
                addr = [a for a in public_ips if a.address == r["ipaddress"]]
                rule = CloudStackPortForwardingRule(
                    node,
                    r["id"],
                    addr[0],
                    r["protocol"].upper(),
                    r["publicport"],
                    r["privateport"],
                    r["publicendport"],
                    r["privateendport"],
                )
                if not addr[0].address in node.public_ips:
                    node.public_ips.append(addr[0].address)
                rules.append(rule)
        node.extra["port_forwarding_rules"] = rules
        return node

    def list_sizes(self, location=None):
        """
        :rtype ``list`` of :class:`NodeSize`
        """
        szs = self._sync_request(command="listServiceOfferings", method="GET")
        sizes = []
        for sz in szs["serviceoffering"]:
            extra = {"cpu": sz["cpunumber"]}
            sizes.append(NodeSize(sz["id"], sz["name"], sz["memory"], 0, 0, 0, self, extra=extra))
        return sizes

    def create_node(
        self,
        name,
        size,
        image,
        location=None,
        networks=None,
        project=None,
        diskoffering=None,
        ex_keyname=None,
        ex_userdata=None,
        ex_security_groups=None,
        ex_displayname=None,
        ex_ip_address=None,
        ex_start_vm=False,
        ex_rootdisksize=None,
        ex_affinity_groups=None,
    ):
        """
        Create a new node

        @inherits: :class:`NodeDriver.create_node`

        :keyword    networks: Optional list of networks to launch the server
                              into.
        :type       networks: ``list`` of :class:`.CloudStackNetwork`

        :keyword    project: Optional project to create the new node under.
        :type       project: :class:`.CloudStackProject`

        :keyword    diskoffering:  Optional disk offering to add to the new
                                   node.
        :type       diskoffering:  :class:`.CloudStackDiskOffering`

        :keyword    ex_keyname:  Name of existing keypair
        :type       ex_keyname:  ``str``

        :keyword    ex_userdata: String containing user data
        :type       ex_userdata: ``str``

        :keyword    ex_security_groups: List of security groups to assign to
                                        the node
        :type       ex_security_groups: ``list`` of ``str``

        :keyword    ex_displayname: String containing instance display name
        :type       ex_displayname: ``str``

        :keyword    ex_ip_address: String with ipaddress for the default nic
        :type       ex_ip_address: ``str``

        :keyword    ex_start_vm: Boolean to specify to start VM after creation
                                 Default Cloudstack behaviour is to start a VM,
                                 if not specified.

        :type       ex_start_vm: ``bool``

        :keyword    ex_rootdisksize: String with rootdisksize for the template
        :type       ex_rootdisksize: ``str``

        :keyword    ex_affinity_groups: List of affinity groups to assign to
                                        the node
        :type       ex_affinity_groups: ``list`` of
                                        :class:`.CloudStackAffinityGroup`

        :rtype:     :class:`.CloudStackNode`
        """

        server_params = self._create_args_to_params(
            node=None,
            name=name,
            size=size,
            image=image,
            location=location,
            networks=networks,
            diskoffering=diskoffering,
            ex_keyname=ex_keyname,
            ex_userdata=ex_userdata,
            ex_security_groups=ex_security_groups,
            ex_displayname=ex_displayname,
            ex_ip_address=ex_ip_address,
            ex_start_vm=ex_start_vm,
            ex_rootdisksize=ex_rootdisksize,
            ex_affinity_groups=ex_affinity_groups,
        )

        data = self._async_request(
            command="deployVirtualMachine", params=server_params, method="GET"
        )["virtualmachine"]
        node = self._to_node(data=data)
        return node

    def _create_args_to_params(
        self,
        node,
        name,
        size,
        image,
        location=None,
        networks=None,
        project=None,
        diskoffering=None,
        ex_keyname=None,
        ex_userdata=None,
        ex_security_groups=None,
        ex_displayname=None,
        ex_ip_address=None,
        ex_start_vm=False,
        ex_rootdisksize=None,
        ex_affinity_groups=None,
    ):
        server_params = {}

        if name:
            server_params["name"] = name

        if ex_displayname:
            server_params["displayname"] = ex_displayname

        if size:
            server_params["serviceofferingid"] = size.id

        if image:
            server_params["templateid"] = image.id

        if location:
            server_params["zoneid"] = location.id
        else:
            # Use a default location
            server_params["zoneid"] = self.list_locations()[0].id

        if networks:
            networks = ",".join([str(network.id) for network in networks])
            server_params["networkids"] = networks

        if project:
            server_params["projectid"] = project.id

        if diskoffering:
            server_params["diskofferingid"] = diskoffering.id

        if ex_keyname:
            server_params["keypair"] = ex_keyname

        if ex_userdata:
            ex_userdata = base64.b64encode(b(ex_userdata)).decode("ascii")
            server_params["userdata"] = ex_userdata

        if ex_security_groups:
            ex_security_groups = ",".join(ex_security_groups)
            server_params["securitygroupnames"] = ex_security_groups

        if ex_ip_address:
            server_params["ipaddress"] = ex_ip_address

        if ex_rootdisksize:
            server_params["rootdisksize"] = ex_rootdisksize

        if ex_start_vm is not None:
            server_params["startvm"] = ex_start_vm

        if ex_affinity_groups:
            affinity_group_ids = ",".join(ag.id for ag in ex_affinity_groups)
            server_params["affinitygroupids"] = affinity_group_ids

        return server_params

    def destroy_node(self, node, ex_expunge=False):
        """
        @inherits: :class:`NodeDriver.reboot_node`
        :type node: :class:`CloudStackNode`

        :keyword    ex_expunge: If true is passed, the vm is expunged
                                immediately. False by default.
        :type       ex_expunge: ``bool``

        :rtype: ``bool``
        """

        args = {
            "id": node.id,
        }

        if ex_expunge:
            args["expunge"] = ex_expunge

        self._async_request(command="destroyVirtualMachine", params=args, method="GET")
        return True

    def reboot_node(self, node):
        """
        @inherits: :class:`NodeDriver.reboot_node`
        :type node: :class:`CloudStackNode`

        :rtype: ``bool``
        """
        self._async_request(command="rebootVirtualMachine", params={"id": node.id}, method="GET")
        return True

    def ex_restore(self, node, template=None):
        """
        Restore virtual machine

        :param node: Node to restore
        :type node: :class:`CloudStackNode`

        :param template: Optional new template
        :type  template: :class:`NodeImage`

        :rtype ``str``
        """
        params = {"virtualmachineid": node.id}
        if template:
            params["templateid"] = template.id

        res = self._async_request(command="restoreVirtualMachine", params=params, method="GET")
        return res["virtualmachine"]["templateid"]

    def ex_change_node_size(self, node, offering):
        """
        Change offering/size of a virtual machine

        :param node: Node to change size
        :type node: :class:`CloudStackNode`

        :param offering: The new offering
        :type  offering: :class:`NodeSize`

        :rtype ``str``
        """
        res = self._async_request(
            command="scaleVirtualMachine",
            params={"id": node.id, "serviceofferingid": offering.id},
            method="GET",
        )
        return res["virtualmachine"]["serviceofferingid"]

    def ex_start(self, node):
        """
        Starts/Resumes a stopped virtual machine

        :type node: :class:`CloudStackNode`

        :param id: The ID of the virtual machine (required)
        :type  id: ``str``

        :param hostid: destination Host ID to deploy the VM to
                       parameter available for root admin only
        :type  hostid: ``str``

        :rtype ``str``
        """
        res = self._async_request(
            command="startVirtualMachine", params={"id": node.id}, method="GET"
        )
        return res["virtualmachine"]["state"]

    def ex_stop(self, node):
        """
        Stops/Suspends a running virtual machine

        :param node: Node to stop.
        :type node: :class:`CloudStackNode`

        :rtype: ``str``
        """
        res = self._async_request(
            command="stopVirtualMachine", params={"id": node.id}, method="GET"
        )
        return res["virtualmachine"]["state"]

    def ex_list_disk_offerings(self):
        """
        Fetch a list of all available disk offerings.

        :rtype: ``list`` of :class:`CloudStackDiskOffering`
        """

        diskOfferings = []

        diskOfferResponse = self._sync_request(command="listDiskOfferings", method="GET")
        for diskOfferDict in diskOfferResponse.get("diskoffering", ()):
            diskOfferings.append(
                CloudStackDiskOffering(
                    id=diskOfferDict["id"],
                    name=diskOfferDict["name"],
                    size=diskOfferDict["disksize"],
                    customizable=diskOfferDict["iscustomized"],
                )
            )

        return diskOfferings

    def ex_list_networks(self, project=None):
        """
        List the available networks

        :param  project: Optional project the networks belongs to.
        :type   project: :class:`.CloudStackProject`

        :rtype ``list`` of :class:`CloudStackNetwork`
        """

        args = {}

        if project is not None:
            args["projectid"] = project.id

        res = self._sync_request(command="listNetworks", params=args, method="GET")
        nets = res.get("network", [])

        networks = []
        extra_map = RESOURCE_EXTRA_ATTRIBUTES_MAP["network"]
        for net in nets:
            extra = self._get_extra_dict(net, extra_map)

            if "tags" in net:
                extra["tags"] = self._get_resource_tags(net["tags"])

            networks.append(
                CloudStackNetwork(
                    net["displaytext"],
                    net["name"],
                    net["networkofferingid"],
                    net["id"],
                    net["zoneid"],
                    self,
                    extra=extra,
                )
            )

        return networks

    def ex_list_network_offerings(self):
        """
        List the available network offerings

        :rtype ``list`` of :class:`CloudStackNetworkOffering`
        """
        res = self._sync_request(command="listNetworkOfferings", method="GET")
        netoffers = res.get("networkoffering", [])

        networkofferings = []

        for netoffer in netoffers:
            networkofferings.append(
                CloudStackNetworkOffering(
                    netoffer["name"],
                    netoffer["displaytext"],
                    netoffer["guestiptype"],
                    netoffer["id"],
                    netoffer["serviceofferingid"],
                    netoffer["forvpc"],
                    self,
                )
            )

        return networkofferings

    def ex_create_network(
        self,
        display_text,
        name,
        network_offering,
        location,
        gateway=None,
        netmask=None,
        network_domain=None,
        vpc_id=None,
        project_id=None,
    ):
        """

        Creates a Network, only available in advanced zones.

        :param  display_text: the display text of the network
        :type   display_text: ``str``

        :param  name: the name of the network
        :type   name: ``str``

        :param  network_offering: NetworkOffering object
        :type   network_offering: :class:'CloudStackNetworkOffering`

        :param location: Zone object
        :type  location: :class:`NodeLocation`

        :param  gateway: Optional, the Gateway of this network
        :type   gateway: ``str``

        :param  netmask: Optional, the netmask of this network
        :type   netmask: ``str``

        :param  network_domain: Optional, the DNS domain of the network
        :type   network_domain: ``str``

        :param  vpc_id: Optional, the VPC id the network belongs to
        :type   vpc_id: ``str``

        :param  project_id: Optional, the project id the networks belongs to
        :type   project_id: ``str``

        :rtype: :class:`CloudStackNetwork`

        """

        extra_map = RESOURCE_EXTRA_ATTRIBUTES_MAP["network"]

        args = {
            "displaytext": display_text,
            "name": name,
            "networkofferingid": network_offering.id,
            "zoneid": location.id,
        }

        if gateway is not None:
            args["gateway"] = gateway

        if netmask is not None:
            args["netmask"] = netmask

        if network_domain is not None:
            args["networkdomain"] = network_domain

        if vpc_id is not None:
            args["vpcid"] = vpc_id

        if project_id is not None:
            args["projectid"] = project_id

        """ Cloudstack allows for duplicate network names,
        this should be handled in the code leveraging libcloud
        As there could be use cases for duplicate names.
        e.g. management from ROOT level"""

        # for net in self.ex_list_networks():
        #    if name == net.name:
        #        raise LibcloudError('This network name already exists')

        result = self._sync_request(command="createNetwork", params=args, method="GET")

        result = result["network"]
        extra = self._get_extra_dict(result, extra_map)

        network = CloudStackNetwork(
            display_text,
            name,
            network_offering.id,
            result["id"],
            location.id,
            self,
            extra=extra,
        )

        return network

    def ex_delete_network(self, network, force=None):
        """

        Deletes a Network, only available in advanced zones.

        :param  network: The network
        :type   network: :class: 'CloudStackNetwork'

        :param  force: Force deletion of the network?
        :type   force: ``bool``

        :rtype: ``bool``

        """

        args = {"id": network.id, "forced": force}

        self._async_request(command="deleteNetwork", params=args, method="GET")
        return True

    def ex_list_vpc_offerings(self):
        """
        List the available vpc offerings

        :rtype ``list`` of :class:`CloudStackVPCOffering`
        """
        res = self._sync_request(command="listVPCOfferings", method="GET")
        vpcoffers = res.get("vpcoffering", [])

        vpcofferings = []

        for vpcoffer in vpcoffers:
            vpcofferings.append(
                CloudStackVPCOffering(
                    vpcoffer["name"], vpcoffer["displaytext"], vpcoffer["id"], self
                )
            )

        return vpcofferings

    def ex_list_vpcs(self, project=None):
        """
        List the available VPCs

        :keyword    project: Optional project under which VPCs are present.
        :type       project: :class:`.CloudStackProject`

        :rtype ``list`` of :class:`CloudStackVPC`
        """

        args = {}

        if project is not None:
            args["projectid"] = project.id

        res = self._sync_request(command="listVPCs", params=args, method="GET")
        vpcs = res.get("vpc", [])

        networks = []
        for vpc in vpcs:
            networks.append(
                CloudStackVPC(
                    vpc["name"],
                    vpc["vpcofferingid"],
                    vpc["id"],
                    vpc["cidr"],
                    self,
                    vpc["zoneid"],
                    vpc["displaytext"],
                )
            )

        return networks

    def ex_list_routers(self, vpc_id=None):
        """
        List routers

        :rtype ``list`` of :class:`CloudStackRouter`
        """

        args = {}

        if vpc_id is not None:
            args["vpcid"] = vpc_id

        res = self._sync_request(command="listRouters", params=args, method="GET")
        rts = res.get("router", [])

        routers = []
        for router in rts:
            routers.append(
                CloudStackRouter(
                    router["id"],
                    router["name"],
                    router["state"],
                    router["publicip"],
                    router["vpcid"],
                    self,
                )
            )

        return routers

    def ex_create_vpc(self, cidr, display_text, name, vpc_offering, zone_id, network_domain=None):
        """

        Creates a VPC, only available in advanced zones.

        :param  cidr: the cidr of the VPC. All VPC guest networks' cidrs
                should be within this CIDR

        :type   display_text: ``str``

        :param  display_text: the display text of the VPC
        :type   display_text: ``str``

        :param  name: the name of the VPC
        :type   name: ``str``

        :param  vpc_offering: the ID of the VPC offering
        :type   vpc_offering: :class:'CloudStackVPCOffering`

        :param  zone_id: the ID of the availability zone
        :type   zone_id: ``str``

        :param  network_domain: Optional, the DNS domain of the network
        :type   network_domain: ``str``

        :rtype: :class:`CloudStackVPC`

        """

        extra_map = RESOURCE_EXTRA_ATTRIBUTES_MAP["vpc"]

        args = {
            "cidr": cidr,
            "displaytext": display_text,
            "name": name,
            "vpcofferingid": vpc_offering.id,
            "zoneid": zone_id,
        }

        if network_domain is not None:
            args["networkdomain"] = network_domain

        result = self._sync_request(command="createVPC", params=args, method="GET")

        extra = self._get_extra_dict(result, extra_map)

        vpc = CloudStackVPC(
            name,
            vpc_offering.id,
            result["id"],
            cidr,
            self,
            zone_id,
            display_text,
            extra=extra,
        )

        return vpc

    def ex_delete_vpc(self, vpc):
        """

        Deletes a VPC, only available in advanced zones.

        :param  vpc: The VPC
        :type   vpc: :class: 'CloudStackVPC'

        :rtype: ``bool``

        """

        args = {"id": vpc.id}

        self._async_request(command="deleteVPC", params=args, method="GET")
        return True

    def ex_list_projects(self):
        """
        List the available projects

        :rtype ``list`` of :class:`CloudStackProject`
        """

        res = self._sync_request(command="listProjects", method="GET")
        projs = res.get("project", [])

        projects = []
        extra_map = RESOURCE_EXTRA_ATTRIBUTES_MAP["project"]
        for proj in projs:
            extra = self._get_extra_dict(proj, extra_map)

            if "tags" in proj:
                extra["tags"] = self._get_resource_tags(proj["tags"])

            projects.append(
                CloudStackProject(
                    id=proj["id"],
                    name=proj["name"],
                    display_text=proj["displaytext"],
                    driver=self,
                    extra=extra,
                )
            )

        return projects

    def create_volume(self, size, name, location=None, snapshot=None, ex_volume_type=None):
        """
        Creates a data volume
        Defaults to the first location
        """
        if ex_volume_type is None:
            for diskOffering in self.ex_list_disk_offerings():
                if diskOffering.size == size or diskOffering.customizable:
                    break
            else:
                raise LibcloudError("Disk offering with size=%s not found" % size)
        else:
            for diskOffering in self.ex_list_disk_offerings():
                if diskOffering.name == ex_volume_type:
                    if not diskOffering.customizable:
                        size = diskOffering.size
                    break
            else:
                raise LibcloudError("Volume type with name=%s not found" % ex_volume_type)

        if location is None:
            location = self.list_locations()[0]

        params = {
            "name": name,
            "diskOfferingId": diskOffering.id,
            "zoneId": location.id,
        }

        if diskOffering.customizable:
            params["size"] = size

        requestResult = self._async_request(command="createVolume", params=params, method="GET")

        volumeResponse = requestResult["volume"]

        state = self._to_volume_state(volumeResponse)

        return StorageVolume(
            id=volumeResponse["id"],
            name=name,
            size=size,
            state=state,
            driver=self,
            extra=dict(name=volumeResponse["name"]),
        )

    def destroy_volume(self, volume):
        """
        :rtype: ``bool``
        """
        self._sync_request(command="deleteVolume", params={"id": volume.id}, method="GET")
        return True

    def attach_volume(self, node, volume, device=None):
        """
        @inherits: :class:`NodeDriver.attach_volume`
        :type node: :class:`CloudStackNode`

        :rtype: ``bool``
        """
        # TODO Add handling for device name
        self._async_request(
            command="attachVolume",
            params={"id": volume.id, "virtualMachineId": node.id},
            method="GET",
        )
        return True

    def detach_volume(self, volume):
        """
        :rtype: ``bool``
        """
        self._async_request(command="detachVolume", params={"id": volume.id}, method="GET")
        return True

    def list_volumes(self, node=None):
        """
        List all volumes

        :param node: Only return volumes for the provided node.
        :type node: :class:`CloudStackNode`

        :rtype: ``list`` of :class:`StorageVolume`
        """
        if node:
            volumes = self._sync_request(
                command="listVolumes",
                params={"virtualmachineid": node.id},
                method="GET",
            )
        else:
            volumes = self._sync_request(command="listVolumes", method="GET")

        list_volumes = []

        extra_map = RESOURCE_EXTRA_ATTRIBUTES_MAP["volume"]
        for vol in volumes.get("volume", []):
            extra = self._get_extra_dict(vol, extra_map)

            if "tags" in vol:
                extra["tags"] = self._get_resource_tags(vol["tags"])

            state = self._to_volume_state(vol)

            list_volumes.append(
                StorageVolume(
                    id=vol["id"],
                    name=vol["name"],
                    size=vol["size"],
                    state=state,
                    driver=self,
                    extra=extra,
                )
            )
        return list_volumes

    def ex_get_volume(self, volume_id, project=None):
        """
        Return a StorageVolume object based on its ID.

        :param  volume_id: The id of the volume
        :type   volume_id: ``str``

        :keyword    project: Limit volume returned to those configured under
                             the defined project.
        :type       project: :class:`.CloudStackProject`

        :rtype: :class:`CloudStackNode`
        """
        args = {"id": volume_id}
        if project:
            args["projectid"] = project.id
        volumes = self._sync_request(command="listVolumes", params=args)
        if not volumes:
            raise Exception("Volume '%s' not found" % volume_id)
        vol = volumes["volume"][0]

        extra_map = RESOURCE_EXTRA_ATTRIBUTES_MAP["volume"]
        extra = self._get_extra_dict(vol, extra_map)

        if "tags" in vol:
            extra["tags"] = self._get_resource_tags(vol["tags"])

        state = self._to_volume_state(vol)

        volume = StorageVolume(
            id=vol["id"],
            name=vol["name"],
            state=state,
            size=vol["size"],
            driver=self,
            extra=extra,
        )
        return volume

    def list_key_pairs(self, **kwargs):
        """
        List registered key pairs.

        :param     projectid: list objects by project
        :type      projectid: ``str``

        :param     page: The page to list the keypairs from
        :type      page: ``int``

        :param     keyword: List by keyword
        :type      keyword: ``str``

        :param     listall: If set to false, list only resources
                            belonging to the command's caller;
                            if set to true - list resources that
                            the caller is authorized to see.
                            Default value is false

        :type      listall: ``bool``

        :param     pagesize: The number of results per page
        :type      pagesize: ``int``

        :param     account: List resources by account.
                            Must be used with the domainId parameter
        :type      account: ``str``

        :param     isrecursive: Defaults to false, but if true,
                                lists all resources from
                                the parent specified by the
                                domainId till leaves.
        :type      isrecursive: ``bool``

        :param     fingerprint: A public key fingerprint to look for
        :type      fingerprint: ``str``

        :param     name: A key pair name to look for
        :type      name: ``str``

        :param     domainid: List only resources belonging to
                                     the domain specified
        :type      domainid: ``str``

        :return:   A list of key par objects.
        :rtype:   ``list`` of :class:`libcloud.compute.base.KeyPair`
        """
        extra_args = kwargs.copy()
        res = self._sync_request(command="listSSHKeyPairs", params=extra_args, method="GET")
        key_pairs = res.get("sshkeypair", [])
        key_pairs = self._to_key_pairs(data=key_pairs)
        return key_pairs

    def get_key_pair(self, name):
        """
        Retrieve a single key pair.

        :param name: Name of the key pair to retrieve.
        :type name: ``str``

        :rtype: :class:`.KeyPair`
        """
        params = {"name": name}
        res = self._sync_request(command="listSSHKeyPairs", params=params, method="GET")
        key_pairs = res.get("sshkeypair", [])

        if len(key_pairs) == 0:
            raise KeyPairDoesNotExistError(name=name, driver=self)

        key_pair = self._to_key_pair(data=key_pairs[0])
        return key_pair

    def create_key_pair(self, name, **kwargs):
        """
        Create a new key pair object.

        :param name: Key pair name.
        :type name: ``str``

        :param     name: Name of the keypair (required)
        :type      name: ``str``

        :param     projectid: An optional project for the ssh key
        :type      projectid: ``str``

        :param     domainid: An optional domainId for the ssh key.
                             If the account parameter is used,
                             domainId must also be used.
        :type      domainid: ``str``

        :param     account: An optional account for the ssh key.
                            Must be used with domainId.
        :type      account: ``str``

        :return:   Created key pair object.
        :rtype:    :class:`libcloud.compute.base.KeyPair`
        """
        extra_args = kwargs.copy()

        params = {"name": name}
        params.update(extra_args)

        res = self._sync_request(command="createSSHKeyPair", params=params, method="GET")
        key_pair = self._to_key_pair(data=res["keypair"])
        return key_pair

    def import_key_pair_from_string(self, name, key_material):
        """
        Import a new public key from string.

        :param name: Key pair name.
        :type name: ``str``

        :param key_material: Public key material.
        :type key_material: ``str``

        :return: Imported key pair object.
        :rtype: :class:`libcloud.compute.base.KeyPair`
        """
        res = self._sync_request(
            command="registerSSHKeyPair",
            params={"name": name, "publickey": key_material},
            method="GET",
        )
        key_pair = self._to_key_pair(data=res["keypair"])
        return key_pair

    def delete_key_pair(self, key_pair, **kwargs):
        """
        Delete an existing key pair.

        :param key_pair: Key pair object.
        :type key_pair: :class:`libcloud.compute.base.KeyPair`

        :param     projectid: The project associated with keypair
        :type      projectid: ``str``

        :param     domainid: The domain ID associated with the keypair
        :type      domainid: ``str``

        :param     account: The account associated with the keypair.
                            Must be used with the domainId parameter.
        :type      account: ``str``

        :return:   True of False based on success of Keypair deletion
        :rtype:    ``bool``
        """

        extra_args = kwargs.copy()
        params = {"name": key_pair.name}
        params.update(extra_args)

        res = self._sync_request(command="deleteSSHKeyPair", params=params, method="GET")
        return res["success"] == "true"

    def ex_list_public_ips(self):
        """
        Lists all Public IP Addresses.

        :rtype: ``list`` of :class:`CloudStackAddress`
        """
        ips = []

        res = self._sync_request(command="listPublicIpAddresses", method="GET")

        # Workaround for basic zones
        if not res:
            return ips

        for ip in res["publicipaddress"]:
            ips.append(
                CloudStackAddress(
                    ip["id"],
                    ip["ipaddress"],
                    self,
                    ip.get("associatednetworkid", []),
                    ip.get("vpcid"),
                    ip.get("virtualmachineid"),
                )
            )

        return ips

    def ex_allocate_public_ip(self, vpc_id=None, network_id=None, location=None):
        """
        Allocate a public IP.

        :param vpc_id: VPC the ip belongs to
        :type vpc_id: ``str``

        :param network_id: Network where this IP is connected to.
        :type network_id: ''str''

        :param location: Zone
        :type  location: :class:`NodeLocation`

        :rtype: :class:`CloudStackAddress`
        """

        args = {}

        if location is not None:
            args["zoneid"] = location.id
        else:
            args["zoneid"] = self.list_locations()[0].id

        if vpc_id is not None:
            args["vpcid"] = vpc_id

        if network_id is not None:
            args["networkid"] = network_id

        addr = self._async_request(command="associateIpAddress", params=args, method="GET")
        addr = addr["ipaddress"]
        addr = CloudStackAddress(addr["id"], addr["ipaddress"], self)
        return addr

    def ex_release_public_ip(self, address):
        """
        Release a public IP.

        :param address: CloudStackAddress which should be used
        :type  address: :class:`CloudStackAddress`

        :rtype: ``bool``
        """
        res = self._async_request(
            command="disassociateIpAddress", params={"id": address.id}, method="GET"
        )
        return res["success"]

    def ex_list_firewall_rules(self):
        """
        Lists all Firewall Rules

        :rtype: ``list`` of :class:`CloudStackFirewallRule`
        """
        rules = []
        result = self._sync_request(command="listFirewallRules", method="GET")
        if result != {}:
            public_ips = self.ex_list_public_ips()
            for rule in result["firewallrule"]:
                addr = [a for a in public_ips if a.address == rule["ipaddress"]]

                rules.append(
                    CloudStackFirewallRule(
                        rule["id"],
                        addr[0],
                        rule["cidrlist"],
                        rule["protocol"],
                        rule.get("icmpcode"),
                        rule.get("icmptype"),
                        rule.get("startport"),
                        rule.get("endport"),
                    )
                )

        return rules

    def ex_create_firewall_rule(
        self,
        address,
        cidr_list,
        protocol,
        icmp_code=None,
        icmp_type=None,
        start_port=None,
        end_port=None,
    ):
        """
        Creates a Firewall Rule

        :param      address: External IP address
        :type       address: :class:`CloudStackAddress`

        :param      cidr_list: cidr list
        :type       cidr_list: ``str``

        :param      protocol: TCP/IP Protocol (TCP, UDP)
        :type       protocol: ``str``

        :param      icmp_code: Error code for this icmp message
        :type       icmp_code: ``int``

        :param      icmp_type: Type of the icmp message being sent
        :type       icmp_type: ``int``

        :param      start_port: start of port range
        :type       start_port: ``int``

        :param      end_port: end of port range
        :type       end_port: ``int``

        :rtype: :class:`CloudStackFirewallRule`
        """
        args = {"ipaddressid": address.id, "cidrlist": cidr_list, "protocol": protocol}
        if icmp_code is not None:
            args["icmpcode"] = int(icmp_code)
        if icmp_type is not None:
            args["icmptype"] = int(icmp_type)
        if start_port is not None:
            args["startport"] = int(start_port)
        if end_port is not None:
            args["endport"] = int(end_port)
        result = self._async_request(command="createFirewallRule", params=args, method="GET")
        rule = CloudStackFirewallRule(
            result["firewallrule"]["id"],
            address,
            cidr_list,
            protocol,
            icmp_code,
            icmp_type,
            start_port,
            end_port,
        )
        return rule

    def ex_delete_firewall_rule(self, firewall_rule):
        """
        Remove a Firewall Rule.

        :param firewall_rule: Firewall rule which should be used
        :type  firewall_rule: :class:`CloudStackFirewallRule`

        :rtype: ``bool``
        """
        res = self._async_request(
            command="deleteFirewallRule", params={"id": firewall_rule.id}, method="GET"
        )
        return res["success"]

    def ex_list_egress_firewall_rules(self):
        """
        Lists all egress Firewall Rules

        :rtype: ``list`` of :class:`CloudStackEgressFirewallRule`
        """
        rules = []
        result = self._sync_request(command="listEgressFirewallRules", method="GET")
        for rule in result["firewallrule"]:
            rules.append(
                CloudStackEgressFirewallRule(
                    rule["id"],
                    rule["networkid"],
                    rule["cidrlist"],
                    rule["protocol"],
                    rule.get("icmpcode"),
                    rule.get("icmptype"),
                    rule.get("startport"),
                    rule.get("endport"),
                )
            )

        return rules

    def ex_create_egress_firewall_rule(
        self,
        network_id,
        cidr_list,
        protocol,
        icmp_code=None,
        icmp_type=None,
        start_port=None,
        end_port=None,
    ):
        """
        Creates a Firewall Rule

        :param      network_id: the id network network for the egress firewall
                    services
        :type       network_id: ``str``

        :param      cidr_list: cidr list
        :type       cidr_list: ``str``

        :param      protocol: TCP/IP Protocol (TCP, UDP)
        :type       protocol: ``str``

        :param      icmp_code: Error code for this icmp message
        :type       icmp_code: ``int``

        :param      icmp_type: Type of the icmp message being sent
        :type       icmp_type: ``int``

        :param      start_port: start of port range
        :type       start_port: ``int``

        :param      end_port: end of port range
        :type       end_port: ``int``

        :rtype: :class:`CloudStackEgressFirewallRule`
        """
        args = {"networkid": network_id, "cidrlist": cidr_list, "protocol": protocol}
        if icmp_code is not None:
            args["icmpcode"] = int(icmp_code)
        if icmp_type is not None:
            args["icmptype"] = int(icmp_type)
        if start_port is not None:
            args["startport"] = int(start_port)
        if end_port is not None:
            args["endport"] = int(end_port)

        result = self._async_request(command="createEgressFirewallRule", params=args, method="GET")

        rule = CloudStackEgressFirewallRule(
            result["firewallrule"]["id"],
            network_id,
            cidr_list,
            protocol,
            icmp_code,
            icmp_type,
            start_port,
            end_port,
        )
        return rule

    def ex_delete_egress_firewall_rule(self, firewall_rule):
        """
        Remove a Firewall rule.

        :param egress_firewall_rule: Firewall rule which should be used
        :type  egress_firewall_rule: :class:`CloudStackEgressFirewallRule`

        :rtype: ``bool``
        """
        res = self._async_request(
            command="deleteEgressFirewallRule",
            params={"id": firewall_rule.id},
            method="GET",
        )
        return res["success"]

    def ex_list_port_forwarding_rules(
        self,
        account=None,
        domain_id=None,
        id=None,
        ipaddress_id=None,
        is_recursive=None,
        keyword=None,
        list_all=None,
        network_id=None,
        page=None,
        page_size=None,
        project_id=None,
    ):
        """
        Lists all Port Forwarding Rules

        :param     account: List resources by account.
                            Must be used with the domainId parameter
        :type      account: ``str``

        :param     domain_id: List only resources belonging to
                                     the domain specified
        :type      domain_id: ``str``

        :param     for_display: List resources by display flag (only root
                                admin is eligible to pass this parameter).
        :type      for_display: ``bool``

        :param     id: Lists rule with the specified ID
        :type      id: ``str``

        :param     ipaddress_id: list the rule belonging to
                                this public ip address
        :type      ipaddress_id: ``str``

        :param     is_recursive: Defaults to false, but if true,
                                lists all resources from
                                the parent specified by the
                                domainId till leaves.
        :type      is_recursive: ``bool``

        :param     keyword: List by keyword
        :type      keyword: ``str``

        :param     list_all: If set to false, list only resources
                            belonging to the command's caller;
                            if set to true - list resources that
                            the caller is authorized to see.
                            Default value is false
        :type      list_all: ``bool``

        :param     network_id: list port forwarding rules for certain network
        :type      network_id: ``string``

        :param     page: The page to list the keypairs from
        :type      page: ``int``

        :param     page_size: The number of results per page
        :type      page_size: ``int``

        :param     project_id: list objects by project
        :type      project_id: ``str``

        :rtype: ``list`` of :class:`CloudStackPortForwardingRule`
        """

        args = {}

        if account is not None:
            args["account"] = account

        if domain_id is not None:
            args["domainid"] = domain_id

        if id is not None:
            args["id"] = id

        if ipaddress_id is not None:
            args["ipaddressid"] = ipaddress_id

        if is_recursive is not None:
            args["isrecursive"] = is_recursive

        if keyword is not None:
            args["keyword"] = keyword

        if list_all is not None:
            args["listall"] = list_all

        if network_id is not None:
            args["networkid"] = network_id

        if page is not None:
            args["page"] = page

        if page_size is not None:
            args["pagesize"] = page_size

        if project_id is not None:
            args["projectid"] = project_id

        rules = []
        result = self._sync_request(command="listPortForwardingRules", params=args, method="GET")
        if result != {}:
            public_ips = self.ex_list_public_ips()
            nodes = self.list_nodes()
            for rule in result["portforwardingrule"]:
                node = [n for n in nodes if n.id == str(rule["virtualmachineid"])]
                addr = [a for a in public_ips if a.address == rule["ipaddress"]]
                rules.append(
                    CloudStackPortForwardingRule(
                        node[0],
                        rule["id"],
                        addr[0],
                        rule["protocol"],
                        rule["publicport"],
                        rule["privateport"],
                        rule["publicendport"],
                        rule["privateendport"],
                    )
                )

        return rules

    def ex_create_port_forwarding_rule(
        self,
        node,
        address,
        private_port,
        public_port,
        protocol,
        public_end_port=None,
        private_end_port=None,
        openfirewall=True,
        network_id=None,
    ):
        """
        Creates a Port Forwarding Rule, used for Source NAT

        :param  address: IP address of the Source NAT
        :type   address: :class:`CloudStackAddress`

        :param  private_port: Port of the virtual machine
        :type   private_port: ``int``

        :param  protocol: Protocol of the rule
        :type   protocol: ``str``

        :param  public_port: Public port on the Source NAT address
        :type   public_port: ``int``

        :param  node: The virtual machine
        :type   node: :class:`CloudStackNode`

        :param  network_id: The network of the vm the Port Forwarding rule
                            will be created for. Required when public Ip
                            address is not associated with any Guest
                            network yet (VPC case)
        :type   network_id: ``string``

        :rtype: :class:`CloudStackPortForwardingRule`
        """
        args = {
            "ipaddressid": address.id,
            "protocol": protocol,
            "privateport": int(private_port),
            "publicport": int(public_port),
            "virtualmachineid": node.id,
            "openfirewall": openfirewall,
        }
        if public_end_port:
            args["publicendport"] = int(public_end_port)
        if private_end_port:
            args["privateendport"] = int(private_end_port)
        if network_id:
            args["networkid"] = network_id

        result = self._async_request(command="createPortForwardingRule", params=args, method="GET")
        rule = CloudStackPortForwardingRule(
            node,
            result["portforwardingrule"]["id"],
            address,
            protocol,
            public_port,
            private_port,
            public_end_port,
            private_end_port,
            network_id,
        )
        node.extra["port_forwarding_rules"].append(rule)
        node.public_ips.append(address.address)
        return rule

    def ex_delete_port_forwarding_rule(self, node, rule):
        """
        Remove a Port forwarding rule.

        :param node: Node used in the rule
        :type  node: :class:`CloudStackNode`

        :param rule: Forwarding rule which should be used
        :type  rule: :class:`CloudStackPortForwardingRule`

        :rtype: ``bool``
        """

        node.extra["port_forwarding_rules"].remove(rule)
        node.public_ips.remove(rule.address.address)
        res = self._async_request(
            command="deletePortForwardingRule", params={"id": rule.id}, method="GET"
        )
        return res["success"]

    def ex_list_ip_forwarding_rules(
        self,
        account=None,
        domain_id=None,
        id=None,
        ipaddress_id=None,
        is_recursive=None,
        keyword=None,
        list_all=None,
        page=None,
        page_size=None,
        project_id=None,
        virtualmachine_id=None,
    ):
        """
        Lists all NAT/firewall forwarding rules

        :param     account: List resources by account.
                            Must be used with the domainId parameter
        :type      account: ``str``

        :param     domain_id: List only resources belonging to
                                     the domain specified
        :type      domain_id: ``str``

        :param     id: Lists rule with the specified ID
        :type      id: ``str``

        :param     ipaddress_id: list the rule belonging to
                                this public ip address
        :type      ipaddress_id: ``str``

        :param     is_recursive: Defaults to false, but if true,
                                lists all resources from
                                the parent specified by the
                                domainId till leaves.
        :type      is_recursive: ``bool``

        :param     keyword: List by keyword
        :type      keyword: ``str``

        :param     list_all: If set to false, list only resources
                            belonging to the command's caller;
                            if set to true - list resources that
                            the caller is authorized to see.
                            Default value is false
        :type      list_all: ``bool``

        :param     page: The page to list the keypairs from
        :type      page: ``int``

        :param     page_size: The number of results per page
        :type      page_size: ``int``

        :param     project_id: list objects by project
        :type      project_id: ``str``

        :param     virtualmachine_id: Lists all rules applied to
                                     the specified Vm
        :type      virtualmachine_id: ``str``

        :rtype: ``list`` of :class:`CloudStackIPForwardingRule`
        """

        args = {}

        if account is not None:
            args["account"] = account

        if domain_id is not None:
            args["domainid"] = domain_id

        if id is not None:
            args["id"] = id

        if ipaddress_id is not None:
            args["ipaddressid"] = ipaddress_id

        if is_recursive is not None:
            args["isrecursive"] = is_recursive

        if keyword is not None:
            args["keyword"] = keyword

        if list_all is not None:
            args["listall"] = list_all

        if page is not None:
            args["page"] = page

        if page_size is not None:
            args["pagesize"] = page_size

        if project_id is not None:
            args["projectid"] = project_id

        if virtualmachine_id is not None:
            args["virtualmachineid"] = virtualmachine_id

        result = self._sync_request(command="listIpForwardingRules", params=args, method="GET")

        rules = []
        if result != {}:
            public_ips = self.ex_list_public_ips()
            nodes = self.list_nodes()
            for rule in result["ipforwardingrule"]:
                node = [n for n in nodes if n.id == str(rule["virtualmachineid"])]
                addr = [a for a in public_ips if a.address == rule["ipaddress"]]
                rules.append(
                    CloudStackIPForwardingRule(
                        node[0],
                        rule["id"],
                        addr[0],
                        rule["protocol"],
                        rule["startport"],
                        rule["endport"],
                    )
                )
        return rules

    def ex_create_ip_forwarding_rule(self, node, address, protocol, start_port, end_port=None):
        """
        "Add a NAT/firewall forwarding rule.

        :param      node: Node which should be used
        :type       node: :class:`CloudStackNode`

        :param      address: CloudStackAddress which should be used
        :type       address: :class:`CloudStackAddress`

        :param      protocol: Protocol which should be used (TCP or UDP)
        :type       protocol: ``str``

        :param      start_port: Start port which should be used
        :type       start_port: ``int``

        :param      end_port: End port which should be used
        :type       end_port: ``int``

        :rtype:     :class:`CloudStackForwardingRule`
        """

        protocol = protocol.upper()
        if protocol not in ("TCP", "UDP"):
            return None

        args = {
            "ipaddressid": address.id,
            "protocol": protocol,
            "startport": int(start_port),
        }
        if end_port is not None:
            args["endport"] = int(end_port)

        result = self._async_request(command="createIpForwardingRule", params=args, method="GET")
        result = result["ipforwardingrule"]
        rule = CloudStackIPForwardingRule(
            node, result["id"], address, protocol, start_port, end_port
        )
        node.extra["ip_forwarding_rules"].append(rule)
        return rule

    def ex_delete_ip_forwarding_rule(self, node, rule):
        """
        Remove a NAT/firewall forwarding rule.

        :param node: Node which should be used
        :type  node: :class:`CloudStackNode`

        :param rule: Forwarding rule which should be used
        :type  rule: :class:`CloudStackForwardingRule`

        :rtype: ``bool``
        """

        node.extra["ip_forwarding_rules"].remove(rule)
        self._async_request(command="deleteIpForwardingRule", params={"id": rule.id}, method="GET")
        return True

    def ex_create_network_acllist(self, name, vpc_id, description=None):
        """
        Create an ACL List for a network within a VPC.

        :param name: Name of the network ACL List
        :type  name: ``string``

        :param vpc_id: Id of the VPC associated with this network ACL List
        :type  vpc_id: ``string``

        :param description: Description of the network ACL List
        :type  description: ``string``

        :rtype: :class:`CloudStackNetworkACLList`
        """

        args = {"name": name, "vpcid": vpc_id}
        if description:
            args["description"] = description

        result = self._sync_request(command="createNetworkACLList", params=args, method="GET")

        acl_list = CloudStackNetworkACLList(result["id"], name, vpc_id, self, description)
        return acl_list

    def ex_create_network_acl(
        self,
        protocol,
        acl_id,
        cidr_list,
        start_port,
        end_port,
        action=None,
        traffic_type=None,
    ):
        """
        Creates an ACL rule in the given network (the network has to belong to
        VPC)

        :param      protocol: the protocol for the ACL rule. Valid values are
                    TCP/UDP/ICMP/ALL or valid protocol number
        :type       protocol: ``string``

        :param      acl_id: Name of the network ACL List
        :type       acl_id: ``str``

        :param      cidr_list: the cidr list to allow traffic from/to
        :type       cidr_list: ``str``

        :param      start_port: the starting port of ACL
        :type       start_port: ``str``

        :param      end_port: the ending port of ACL
        :type       end_port: ``str``

        :param      action: scl entry action, allow or deny
        :type       action: ``str``

        :param      traffic_type: the traffic type for the ACL,can be Ingress
                    or Egress, defaulted to Ingress if not specified
        :type       traffic_type: ``str``

        :rtype: :class:`CloudStackNetworkACL`
        """

        args = {
            "protocol": protocol,
            "aclid": acl_id,
            "cidrlist": cidr_list,
            "startport": start_port,
            "endport": end_port,
        }

        if action:
            args["action"] = action
        else:
            action = "allow"

        if traffic_type:
            args["traffictype"] = traffic_type

        result = self._async_request(command="createNetworkACL", params=args, method="GET")

        acl = CloudStackNetworkACL(
            result["networkacl"]["id"],
            protocol,
            acl_id,
            action,
            cidr_list,
            start_port,
            end_port,
            traffic_type,
        )

        return acl

    def ex_list_network_acllists(self):
        """
        Lists all network ACLs

        :rtype: ``list`` of :class:`CloudStackNetworkACLList`
        """
        acllists = []

        result = self._sync_request(command="listNetworkACLLists", method="GET")

        if not result:
            return acllists

        for acllist in result["networkacllist"]:
            acllists.append(
                CloudStackNetworkACLList(
                    acllist["id"],
                    acllist["name"],
                    acllist.get("vpcid", []),
                    self,
                    acllist["description"],
                )
            )

        return acllists

    def ex_replace_network_acllist(self, acl_id, network_id):
        """
        Create an ACL List for a network within a VPC.Replaces ACL associated
        with a Network or private gateway

        :param acl_id: the ID of the network ACL
        :type  acl_id: ``string``

        :param network_id: the ID of the network
        :type  network_id: ``string``

        :rtype: :class:`CloudStackNetworkACLList`
        """

        args = {"aclid": acl_id, "networkid": network_id}

        self._async_request(command="replaceNetworkACLList", params=args, method="GET")

        return True

    def ex_list_network_acl(self):
        """
        Lists all network ACL items

        :rtype: ``list`` of :class:`CloudStackNetworkACL`
        """
        acls = []

        result = self._sync_request(command="listNetworkACLs", method="GET")

        if not result:
            return acls

        for acl in result["networkacl"]:
            acls.append(
                CloudStackNetworkACL(
                    acl["id"],
                    acl["protocol"],
                    acl["aclid"],
                    acl["action"],
                    acl["cidrlist"],
                    acl.get("startport", []),
                    acl.get("endport", []),
                    acl["traffictype"],
                )
            )

        return acls

    def ex_list_keypairs(self, **kwargs):
        """
        List Registered SSH Key Pairs

        :param     projectid: list objects by project
        :type      projectid: ``str``

        :param     page: The page to list the keypairs from
        :type      page: ``int``

        :param     keyword: List by keyword
        :type      keyword: ``str``

        :param     listall: If set to false, list only resources
                            belonging to the command's caller;
                            if set to true - list resources that
                            the caller is authorized to see.
                            Default value is false

        :type      listall: ``bool``

        :param     pagesize: The number of results per page
        :type      pagesize: ``int``

        :param     account: List resources by account.
                            Must be used with the domainId parameter
        :type      account: ``str``

        :param     isrecursive: Defaults to false, but if true,
                                lists all resources from
                                the parent specified by the
                                domainId till leaves.
        :type      isrecursive: ``bool``

        :param     fingerprint: A public key fingerprint to look for
        :type      fingerprint: ``str``

        :param     name: A key pair name to look for
        :type      name: ``str``

        :param     domainid: List only resources belonging to
                                     the domain specified
        :type      domainid: ``str``

        :return:   A list of keypair dictionaries
        :rtype:   ``list`` of ``dict``
        """
        warnings.warn("This method has been deprecated in favor of " "list_key_pairs method")

        key_pairs = self.list_key_pairs(**kwargs)

        result = []

        for key_pair in key_pairs:
            item = {
                "name": key_pair.name,
                "fingerprint": key_pair.fingerprint,
                "privateKey": key_pair.private_key,
            }
            result.append(item)

        return result

    def ex_create_keypair(self, name, **kwargs):
        """
        Creates a SSH KeyPair, returns fingerprint and private key

        :param     name: Name of the keypair (required)
        :type      name: ``str``

        :param     projectid: An optional project for the ssh key
        :type      projectid: ``str``

        :param     domainid: An optional domainId for the ssh key.
                             If the account parameter is used,
                             domainId must also be used.
        :type      domainid: ``str``

        :param     account: An optional account for the ssh key.
                            Must be used with domainId.
        :type      account: ``str``

        :return:   A keypair dictionary
        :rtype:    ``dict``
        """
        warnings.warn("This method has been deprecated in favor of " "create_key_pair method")

        key_pair = self.create_key_pair(name=name, **kwargs)

        result = {
            "name": key_pair.name,
            "fingerprint": key_pair.fingerprint,
            "privateKey": key_pair.private_key,
        }

        return result

    def ex_import_keypair_from_string(self, name, key_material):
        """
        Imports a new public key where the public key is passed in as a string

        :param     name: The name of the public key to import.
        :type      name: ``str``

        :param     key_material: The contents of a public key file.
        :type      key_material: ``str``

        :rtype: ``dict``
        """
        warnings.warn(
            "This method has been deprecated in favor of " "import_key_pair_from_string method"
        )

        key_pair = self.import_key_pair_from_string(name=name, key_material=key_material)
        result = {"keyName": key_pair.name, "keyFingerprint": key_pair.fingerprint}

        return result

    def ex_import_keypair(self, name, keyfile):
        """
        Imports a new public key where the public key is passed via a filename

        :param     name: The name of the public key to import.
        :type      name: ``str``

        :param     keyfile: The filename with path of the public key to import.
        :type      keyfile: ``str``

        :rtype: ``dict``
        """
        warnings.warn(
            "This method has been deprecated in favor of " "import_key_pair_from_file method"
        )

        key_pair = self.import_key_pair_from_file(name=name, key_file_path=keyfile)
        result = {"keyName": key_pair.name, "keyFingerprint": key_pair.fingerprint}

        return result

    def ex_delete_keypair(self, keypair, **kwargs):
        """
        Deletes an existing SSH KeyPair

        :param     keypair: Name of the keypair (required)
        :type      keypair: ``str``

        :param     projectid: The project associated with keypair
        :type      projectid: ``str``

        :param     domainid: The domain ID associated with the keypair
        :type      domainid: ``str``

        :param     account: The account associated with the keypair.
                             Must be used with the domainId parameter.
        :type      account: ``str``

        :return:   True of False based on success of Keypair deletion
        :rtype:    ``bool``
        """
        warnings.warn("This method has been deprecated in favor of " "delete_key_pair method")

        key_pair = KeyPair(name=keypair, public_key=None, fingerprint=None, driver=self)

        return self.delete_key_pair(key_pair=key_pair)

    def ex_list_security_groups(self, **kwargs):
        """
        Lists Security Groups

        :param domainid: List only resources belonging to the domain specified
        :type  domainid: ``str``

        :param account: List resources by account. Must be used with
                                                   the domainId parameter.
        :type  account: ``str``

        :param listall: If set to false, list only resources belonging to
                                         the command's caller; if set to true
                                         list resources that the caller is
                                         authorized to see.
                                         Default value is false
        :type  listall: ``bool``

        :param pagesize: Number of entries per page
        :type  pagesize: ``int``

        :param keyword: List by keyword
        :type  keyword: ``str``

        :param tags: List resources by tags (key/value pairs)
        :type  tags: ``dict``

        :param id: list the security group by the id provided
        :type  id: ``str``

        :param securitygroupname: lists security groups by name
        :type  securitygroupname: ``str``

        :param virtualmachineid: lists security groups by virtual machine id
        :type  virtualmachineid: ``str``

        :param projectid: list objects by project
        :type  projectid: ``str``

        :param isrecursive: (boolean) defaults to false, but if true,
                                      lists all resources from the parent
                                      specified by the domainId till leaves.
        :type  isrecursive: ``bool``

        :param page: (integer)
        :type  page: ``int``

        :rtype ``list``
        """
        extra_args = kwargs.copy()
        res = self._sync_request(command="listSecurityGroups", params=extra_args, method="GET")

        security_groups = res.get("securitygroup", [])
        return security_groups

    def ex_create_security_group(self, name, **kwargs):
        """
        Creates a new Security Group

        :param name: name of the security group (required)
        :type  name: ``str``

        :param account: An optional account for the security group.
                        Must be used with domainId.
        :type  account: ``str``

        :param domainid: An optional domainId for the security group.
                         If the account parameter is used,
                         domainId must also be used.
        :type  domainid: ``str``

        :param description: The description of the security group
        :type  description: ``str``

        :param projectid: Deploy vm for the project
        :type  projectid: ``str``

        :rtype: ``dict``
        """

        extra_args = kwargs.copy()

        for sg in self.ex_list_security_groups():
            if name in sg["name"]:
                raise LibcloudError("This Security Group name already exists")

        params = {"name": name}
        params.update(extra_args)

        return self._sync_request(command="createSecurityGroup", params=params, method="GET")[
            "securitygroup"
        ]

    def ex_delete_security_group(self, name):
        """
        Deletes a given Security Group

        :param domainid: The domain ID of account owning
                         the security group
        :type  domainid: ``str``

        :param id: The ID of the security group.
                   Mutually exclusive with name parameter
        :type  id: ``str``

        :param name: The ID of the security group.
                     Mutually exclusive with id parameter
        :type name: ``str``

        :param account: The account of the security group.
                        Must be specified with domain ID
        :type  account: ``str``

        :param projectid:  The project of the security group
        :type  projectid:  ``str``

        :rtype: ``bool``
        """

        return self._sync_request(
            command="deleteSecurityGroup", params={"name": name}, method="GET"
        )["success"]

    def ex_authorize_security_group_ingress(
        self,
        securitygroupname,
        protocol,
        cidrlist,
        startport=None,
        endport=None,
        icmptype=None,
        icmpcode=None,
        **kwargs,
    ):
        """
        Creates a new Security Group Ingress rule

        :param   securitygroupname: The name of the security group.
                                    Mutually exclusive with securitygroupid.
        :type    securitygroupname: ``str``

        :param   protocol: Can be TCP, UDP or ICMP.
                           Sometime other protocols can be used like AH, ESP
                           or GRE.
        :type    protocol: ``str``

        :param   cidrlist: Source address CIDR for which this rule applies.
        :type    cidrlist: ``str``

        :param   startport: Start port of the range for this ingress rule.
                            Applies to protocols TCP and UDP.
        :type    startport: ``int``

        :param   endport: End port of the range for this ingress rule.
                          It can be None to set only one port.
                          Applies to protocols TCP and UDP.
        :type    endport: ``int``

        :param   icmptype: Type of the ICMP packet (eg: 8 for Echo Request).
                           -1 or None means "all types".
                           Applies to protocol ICMP.
        :type    icmptype: ``int``

        :param   icmpcode: Code of the ICMP packet for the specified type.
                           If the specified type doesn't require a code set
                           this value to 0.
                           -1 or None means "all codes".
                           Applies to protocol ICMP.
        :type    icmpcode: ``int``

        :keyword   account: An optional account for the security group.
                            Must be used with domainId.
        :type      account: ``str``

        :keyword domainid: An optional domainId for the security group.
                           If the account parameter is used, domainId must also
                           be used.

        :keyword projectid: An optional project of the security group
        :type    projectid: ``str``

        :keyword securitygroupid: The ID of the security group.
                                  Mutually exclusive with securitygroupname
        :type    securitygroupid: ``str``

        :keyword usersecuritygrouplist: User to security group mapping
        :type    usersecuritygrouplist: ``dict``

        :rtype: ``dict``
        """

        args = kwargs.copy()
        protocol = protocol.upper()

        args.update(
            {
                "securitygroupname": securitygroupname,
                "protocol": protocol,
                "cidrlist": cidrlist,
            }
        )

        if protocol not in ("TCP", "UDP") and (startport is not None or endport is not None):
            raise LibcloudError(
                '"startport" and "endport" are only valid ' "with protocol TCP or UDP."
            )

        if protocol != "ICMP" and (icmptype is not None or icmpcode is not None):
            raise LibcloudError('"icmptype" and "icmpcode" are only valid ' "with protocol ICMP.")

        if protocol in ("TCP", "UDP"):
            if startport is None:
                raise LibcloudError(
                    "Protocols TCP and UDP require at least " '"startport" to be set.'
                )
            if startport is not None and endport is None:
                endport = startport

            args.update({"startport": startport, "endport": endport})

        if protocol == "ICMP":
            if icmptype is None:
                icmptype = -1
            if icmpcode is None:
                icmpcode = -1

            args.update({"icmptype": icmptype, "icmpcode": icmpcode})

        return self._async_request(
            command="authorizeSecurityGroupIngress", params=args, method="GET"
        )["securitygroup"]

    def ex_revoke_security_group_ingress(self, rule_id):
        """
        Revoke/delete an ingress security rule

        :param id: The ID of the ingress security rule
        :type  id: ``str``

        :rtype: ``bool``
        """

        self._async_request(
            command="revokeSecurityGroupIngress", params={"id": rule_id}, method="GET"
        )
        return True

    def ex_create_affinity_group(self, name, group_type):
        """
        Creates a new Affinity Group

        :param name: Name of the affinity group
        :type  name: ``str``

        :param group_type: Type of the affinity group from the available
                           affinity/anti-affinity group types
        :type  group_type: :class:`CloudStackAffinityGroupType`

        :param description: Optional description of the affinity group
        :type  description: ``str``

        :param domainid: domain ID of the account owning the affinity group
        :type  domainid: ``str``

        :rtype: :class:`CloudStackAffinityGroup`
        """

        for ag in self.ex_list_affinity_groups():
            if name == ag.name:
                raise LibcloudError("This Affinity Group name already exists")

        params = {"name": name, "type": group_type.type}

        result = self._async_request(command="createAffinityGroup", params=params, method="GET")

        return self._to_affinity_group(result["affinitygroup"])

    def ex_delete_affinity_group(self, affinity_group):
        """
        Delete an Affinity Group

        :param affinity_group: Instance of affinity group
        :type  affinity_group: :class:`CloudStackAffinityGroup`

        :rtype ``bool``
        """
        return self._async_request(
            command="deleteAffinityGroup",
            params={"id": affinity_group.id},
            method="GET",
        )["success"]

    def ex_update_node_affinity_group(self, node, affinity_group_list):
        """
        Updates the affinity/anti-affinity group associations of a virtual
        machine. The VM has to be stopped and restarted for the new properties
        to take effect.

        :param node: Node to update.
        :type node: :class:`CloudStackNode`

        :param affinity_group_list: List of CloudStackAffinityGroup to
                                    associate
        :type affinity_group_list: ``list`` of :class:`CloudStackAffinityGroup`

        :rtype :class:`CloudStackNode`
        """
        affinity_groups = ",".join(ag.id for ag in affinity_group_list)

        result = self._async_request(
            command="updateVMAffinityGroup",
            params={"id": node.id, "affinitygroupids": affinity_groups},
            method="GET",
        )
        return self._to_node(data=result["virtualmachine"])

    def ex_list_affinity_groups(self):
        """
        List Affinity Groups

        :rtype ``list`` of :class:`CloudStackAffinityGroup`
        """
        result = self._sync_request(command="listAffinityGroups", method="GET")

        if not result.get("count"):
            return []

        affinity_groups = []
        for ag in result["affinitygroup"]:
            affinity_groups.append(self._to_affinity_group(ag))

        return affinity_groups

    def ex_list_affinity_group_types(self):
        """
        List Affinity Group Types

        :rtype ``list`` of :class:`CloudStackAffinityGroupTypes`
        """
        result = self._sync_request(command="listAffinityGroupTypes", method="GET")

        if not result.get("count"):
            return []

        affinity_group_types = []
        for agt in result["affinityGroupType"]:
            affinity_group_types.append(CloudStackAffinityGroupType(agt["type"]))

        return affinity_group_types

    def ex_register_iso(self, name, url, location=None, **kwargs):
        """
        Registers an existing ISO by URL.

        :param      name: Name which should be used
        :type       name: ``str``

        :param      url: Url should be used
        :type       url: ``str``

        :param      location: Location which should be used
        :type       location: :class:`NodeLocation`

        :rtype: ``str``
        """
        if location is None:
            location = self.list_locations()[0]

        params = {"name": name, "displaytext": name, "url": url, "zoneid": location.id}
        params["bootable"] = kwargs.pop("bootable", False)
        if params["bootable"]:
            os_type_id = kwargs.pop("ostypeid", None)

            if not os_type_id:
                raise LibcloudError("If bootable=True, ostypeid is required!")

            params["ostypeid"] = os_type_id

        return self._sync_request(command="registerIso", params=params)

    def ex_limits(self):
        """
        Extra call to get account's resource limits, such as
        the amount of instances, volumes, snapshots and networks.

        CloudStack uses integers as the resource type so we will convert
        them to a more human readable string using the resource map

        A list of the resource type mappings can be found at
        http://goo.gl/17C6Gk

        :return: dict
        :rtype: ``dict``
        """

        result = self._sync_request(command="listResourceLimits", method="GET")

        limits = {}
        resource_map = {
            0: "max_instances",
            1: "max_public_ips",
            2: "max_volumes",
            3: "max_snapshots",
            4: "max_images",
            5: "max_projects",
            6: "max_networks",
            7: "max_vpc",
            8: "max_cpu",
            9: "max_memory",
            10: "max_primary_storage",
            11: "max_secondary_storage",
        }

        for limit in result.get("resourcelimit", []):
            # We will ignore unknown types
            resource = resource_map.get(int(limit["resourcetype"]), None)
            if not resource:
                continue
            limits[resource] = int(limit["max"])

        return limits

    def ex_create_tags(self, resource_ids, resource_type, tags):
        """
        Create tags for a resource (Node/StorageVolume/etc).
        A list of resource types can be found at http://goo.gl/6OKphH

        :param resource_ids: Resource IDs to be tagged. The resource IDs must
                             all be associated with the resource_type.
                             For example, for virtual machines (UserVm) you
                             can only specify a list of virtual machine IDs.
        :type  resource_ids: ``list`` of resource IDs

        :param resource_type: Resource type (eg: UserVm)
        :type  resource_type: ``str``

        :param tags: A dictionary or other mapping of strings to strings,
                     associating tag names with tag values.
        :type  tags: ``dict``

        :rtype: ``bool``
        """
        params = {"resourcetype": resource_type, "resourceids": ",".join(resource_ids)}

        for i, key in enumerate(tags):
            params["tags[%d].key" % i] = key
            params["tags[%d].value" % i] = tags[key]

        self._async_request(command="createTags", params=params, method="GET")
        return True

    def ex_delete_tags(self, resource_ids, resource_type, tag_keys):
        """
        Delete tags from a resource.

        :param resource_ids: Resource IDs to be tagged. The resource IDs must
                             all be associated with the resource_type.
                             For example, for virtual machines (UserVm) you
                             can only specify a list of virtual machine IDs.
        :type  resource_ids: ``list`` of resource IDs

        :param resource_type: Resource type (eg: UserVm)
        :type  resource_type: ``str``

        :param tag_keys: A list of keys to delete. CloudStack only requires
                         the keys from the key/value pair.
        :type  tag_keys: ``list``

        :rtype: ``bool``
        """
        params = {"resourcetype": resource_type, "resourceids": ",".join(resource_ids)}

        for i, key in enumerate(tag_keys):
            params["tags[%s].key" % i] = key

        self._async_request(command="deleteTags", params=params, method="GET")

        return True

    def list_snapshots(self):
        """
        Describe all snapshots.

        :rtype: ``list`` of :class:`VolumeSnapshot`
        """
        snapshots = self._sync_request("listSnapshots", method="GET")
        list_snapshots = []

        for snap in snapshots["snapshot"]:
            list_snapshots.append(self._to_snapshot(snap))
        return list_snapshots

    def create_volume_snapshot(self, volume, name=None):
        """
        Create snapshot from volume

        :param      volume: Instance of ``StorageVolume``
        :type       volume: ``StorageVolume``

        :param      name: The name of the snapshot is disregarded
                          by CloudStack drivers
        :type       name: `str`

        :rtype: :class:`VolumeSnapshot`
        """
        snapshot = self._async_request(
            command="createSnapshot", params={"volumeid": volume.id}, method="GET"
        )
        return self._to_snapshot(snapshot["snapshot"])

    def destroy_volume_snapshot(self, snapshot):
        self._async_request(command="deleteSnapshot", params={"id": snapshot.id}, method="GET")
        return True

    def ex_create_snapshot_template(self, snapshot, name, ostypeid, displaytext=None):
        """
        Create a template from a snapshot

        :param      snapshot: Instance of ``VolumeSnapshot``
        :type       volume: ``VolumeSnapshot``

        :param  name: the name of the template
        :type   name: ``str``

        :param  name: the os type id
        :type   name: ``str``

        :param  name: the display name of the template
        :type   name: ``str``

        :rtype: :class:`NodeImage`
        """
        if not displaytext:
            displaytext = name
        resp = self._async_request(
            command="createTemplate",
            params={
                "displaytext": displaytext,
                "name": name,
                "ostypeid": ostypeid,
                "snapshotid": snapshot.id,
            },
        )
        img = resp.get("template")
        extra = {
            "hypervisor": img["hypervisor"],
            "format": img["format"],
            "os": img["ostypename"],
            "displaytext": img["displaytext"],
        }
        return NodeImage(id=img["id"], name=img["name"], driver=self.connection.driver, extra=extra)

    def ex_list_os_types(self):
        """
        List all registered os types (needed for snapshot creation)

        :rtype: ``list``
        """
        ostypes = self._sync_request("listOsTypes")
        return ostypes["ostype"]

    def ex_list_nics(self, node):
        """
        List the available networks

        :param      vm: Node Object
        :type       vm: :class:`CloudStackNode

        :rtype ``list`` of :class:`CloudStackNic`
        """

        res = self._sync_request(
            command="listNics", params={"virtualmachineid": node.id}, method="GET"
        )
        items = res.get("nic", [])

        nics = []
        extra_map = RESOURCE_EXTRA_ATTRIBUTES_MAP["nic"]
        for item in items:
            extra = self._get_extra_dict(item, extra_map)

            nics.append(
                CloudStackNic(
                    id=item["id"],
                    network_id=item["networkid"],
                    net_mask=item["netmask"],
                    gateway=item["gateway"],
                    ip_address=item["ipaddress"],
                    is_default=item["isdefault"],
                    mac_address=item["macaddress"],
                    driver=self,
                    extra=extra,
                )
            )

        return nics

    def ex_attach_nic_to_node(self, node, network, ip_address=None):
        """
        Add an extra Nic to a VM

        :param  network: NetworkOffering object
        :type   network: :class:'CloudStackNetwork`

        :param  node: Node Object
        :type   node: :class:'CloudStackNode`

        :param  ip_address: Optional, specific IP for this Nic
        :type   ip_address: ``str``


        :rtype: ``bool``
        """

        args = {"virtualmachineid": node.id, "networkid": network.id}

        if ip_address is not None:
            args["ipaddress"] = ip_address

        self._async_request(command="addNicToVirtualMachine", params=args)
        return True

    def ex_detach_nic_from_node(self, nic, node):
        """
        Remove Nic from a VM

        :param  nic: Nic object
        :type   nic: :class:'CloudStackNetwork`

        :param  node: Node Object
        :type   node: :class:'CloudStackNode`

        :rtype: ``bool``
        """

        self._async_request(
            command="removeNicFromVirtualMachine",
            params={"nicid": nic.id, "virtualmachineid": node.id},
        )

        return True

    def ex_list_vpn_gateways(
        self,
        account=None,
        domain_id=None,
        for_display=None,
        id=None,
        is_recursive=None,
        keyword=None,
        list_all=None,
        page=None,
        page_size=None,
        project_id=None,
        vpc_id=None,
    ):
        """
        List VPN Gateways.

        :param   account: List resources by account (must be
                          used with the domain_id parameter).
        :type    account: ``str``

        :param   domain_id: List only resources belonging
                            to the domain specified.
        :type    domain_id: ``str``

        :param   for_display: List resources by display flag (only root
                              admin is eligible to pass this parameter).
        :type    for_display: ``bool``

        :param   id: ID of the VPN Gateway.
        :type    id: ``str``

        :param   is_recursive: Defaults to False, but if true, lists all
                               resources from the parent specified by the
                               domain ID till leaves.
        :type    is_recursive: ``bool``

        :param   keyword: List by keyword.
        :type    keyword: ``str``

        :param   list_all: If set to False, list only resources belonging to
                           the command's caller; if set to True - list
                           resources that the caller is authorized to see.
                           Default value is False.
        :type    list_all: ``str``

        :param   page: Start from page.
        :type    page: ``int``

        :param   page_size: Items per page.
        :type    page_size: ``int``

        :param   project_id: List objects by project.
        :type    project_id: ``str``

        :param   vpc_id: List objects by VPC.
        :type    vpc_id: ``str``

        :rtype: ``list`` of :class:`CloudStackVpnGateway`
        """
        args = {}

        if account is not None:
            args["account"] = account

        if domain_id is not None:
            args["domainid"] = domain_id

        if for_display is not None:
            args["fordisplay"] = for_display

        if id is not None:
            args["id"] = id

        if is_recursive is not None:
            args["isrecursive"] = is_recursive

        if keyword is not None:
            args["keyword"] = keyword

        if list_all is not None:
            args["listall"] = list_all

        if page is not None:
            args["page"] = page

        if page_size is not None:
            args["pagesize"] = page_size

        if project_id is not None:
            args["projectid"] = project_id

        if vpc_id is not None:
            args["vpcid"] = vpc_id

        res = self._sync_request(command="listVpnGateways", params=args, method="GET")

        items = res.get("vpngateway", [])
        vpn_gateways = []
        extra_map = RESOURCE_EXTRA_ATTRIBUTES_MAP["vpngateway"]

        for item in items:
            extra = self._get_extra_dict(item, extra_map)

            vpn_gateways.append(
                CloudStackVpnGateway(
                    id=item["id"],
                    account=item["account"],
                    domain=item["domain"],
                    domain_id=item["domainid"],
                    public_ip=item["publicip"],
                    vpc_id=item["vpcid"],
                    driver=self,
                    extra=extra,
                )
            )

        return vpn_gateways

    def ex_create_vpn_gateway(self, vpc, for_display=None):
        """
        Creates a VPN Gateway.

        :param vpc: VPC to create the Gateway for (required).
        :type  vpc: :class: `CloudStackVPC`

        :param for_display: Display the VPC to the end user or not.
        :type  for_display: ``bool``

        :rtype: :class: `CloudStackVpnGateway`
        """
        args = {
            "vpcid": vpc.id,
        }

        if for_display is not None:
            args["fordisplay"] = for_display

        res = self._async_request(command="createVpnGateway", params=args, method="GET")

        item = res["vpngateway"]
        extra_map = RESOURCE_EXTRA_ATTRIBUTES_MAP["vpngateway"]

        return CloudStackVpnGateway(
            id=item["id"],
            account=item["account"],
            domain=item["domain"],
            domain_id=item["domainid"],
            public_ip=item["publicip"],
            vpc_id=vpc.id,
            driver=self,
            extra=self._get_extra_dict(item, extra_map),
        )

    def ex_delete_vpn_gateway(self, vpn_gateway):
        """
        Deletes a VPN Gateway.

        :param vpn_gateway: The VPN Gateway (required).
        :type  vpn_gateway: :class:`CloudStackVpnGateway`

        :rtype: ``bool``
        """
        res = self._async_request(
            command="deleteVpnGateway", params={"id": vpn_gateway.id}, method="GET"
        )

        return res["success"]

    def ex_list_vpn_customer_gateways(
        self,
        account=None,
        domain_id=None,
        id=None,
        is_recursive=None,
        keyword=None,
        list_all=None,
        page=None,
        page_size=None,
        project_id=None,
    ):
        """
        List VPN Customer Gateways.

        :param   account: List resources by account (must be
                          used with the domain_id parameter).
        :type    account: ``str``

        :param   domain_id: List only resources belonging
                            to the domain specified.
        :type    domain_id: ``str``

        :param   id: ID of the VPN Customer Gateway.
        :type    id: ``str``

        :param   is_recursive: Defaults to False, but if true, lists all
                               resources from the parent specified by the
                               domain_id till leaves.
        :type    is_recursive: ``bool``

        :param   keyword: List by keyword.
        :type    keyword: ``str``

        :param   list_all: If set to False, list only resources belonging to
                           the command's caller; if set to True - list
                           resources that the caller is authorized to see.
                           Default value is False.
        :type    list_all: ``str``

        :param   page: Start from page.
        :type    page: ``int``

        :param   page_size: Items per page.
        :type    page_size: ``int``

        :param   project_id: List objects by project.
        :type    project_id: ``str``

        :rtype: ``list`` of :class:`CloudStackVpnCustomerGateway`
        """
        args = {}

        if account is not None:
            args["account"] = account

        if domain_id is not None:
            args["domainid"] = domain_id

        if id is not None:
            args["id"] = id

        if is_recursive is not None:
            args["isrecursive"] = is_recursive

        if keyword is not None:
            args["keyword"] = keyword

        if list_all is not None:
            args["listall"] = list_all

        if page is not None:
            args["page"] = page

        if page_size is not None:
            args["pagesize"] = page_size

        if project_id is not None:
            args["projectid"] = project_id

        res = self._sync_request(command="listVpnCustomerGateways", params=args, method="GET")

        items = res.get("vpncustomergateway", [])
        vpn_customer_gateways = []
        extra_map = RESOURCE_EXTRA_ATTRIBUTES_MAP["vpncustomergateway"]

        for item in items:
            extra = self._get_extra_dict(item, extra_map)

            vpn_customer_gateways.append(
                CloudStackVpnCustomerGateway(
                    id=item["id"],
                    cidr_list=item["cidrlist"],
                    esp_policy=item["esppolicy"],
                    gateway=item["gateway"],
                    ike_policy=item["ikepolicy"],
                    ipsec_psk=item["ipsecpsk"],
                    driver=self,
                    extra=extra,
                )
            )

        return vpn_customer_gateways

    def ex_create_vpn_customer_gateway(
        self,
        cidr_list,
        esp_policy,
        gateway,
        ike_policy,
        ipsec_psk,
        account=None,
        domain_id=None,
        dpd=None,
        esp_lifetime=None,
        ike_lifetime=None,
        name=None,
    ):
        """
        Creates a VPN Customer Gateway.

        :param cidr_list: Guest CIDR list of the Customer Gateway (required).
        :type  cidr_list: ``str``

        :param esp_policy: ESP policy of the Customer Gateway (required).
        :type  esp_policy: ``str``

        :param gateway: Public IP address of the Customer Gateway (required).
        :type  gateway: ``str``

        :param ike_policy: IKE policy of the Customer Gateway (required).
        :type  ike_policy: ``str``

        :param ipsec_psk: IPsec preshared-key of the Customer Gateway
                          (required).
        :type  ipsec_psk: ``str``

        :param account: The associated account with the Customer Gateway
                        (must be used with the domain_id param).
        :type  account: ``str``

        :param domain_id: The domain ID associated with the Customer Gateway.
                          If used with the account parameter returns the
                          gateway associated with the account for the
                          specified domain.
        :type  domain_id: ``str``

        :param dpd: If DPD is enabled for the VPN connection.
        :type  dpd: ``bool``

        :param esp_lifetime: Lifetime of phase 2 VPN connection to the
                             Customer Gateway, in seconds.
        :type  esp_lifetime: ``int``

        :param ike_lifetime: Lifetime of phase 1 VPN connection to the
                             Customer Gateway, in seconds.
        :type  ike_lifetime: ``int``

        :param name: Name of the Customer Gateway.
        :type  name: ``str``

        :rtype: :class: `CloudStackVpnCustomerGateway`
        """
        args = {
            "cidrlist": cidr_list,
            "esppolicy": esp_policy,
            "gateway": gateway,
            "ikepolicy": ike_policy,
            "ipsecpsk": ipsec_psk,
        }

        if account is not None:
            args["account"] = account

        if domain_id is not None:
            args["domainid"] = domain_id

        if dpd is not None:
            args["dpd"] = dpd

        if esp_lifetime is not None:
            args["esplifetime"] = esp_lifetime

        if ike_lifetime is not None:
            args["ikelifetime"] = ike_lifetime

        if name is not None:
            args["name"] = name

        res = self._async_request(command="createVpnCustomerGateway", params=args, method="GET")

        item = res["vpncustomergateway"]
        extra_map = RESOURCE_EXTRA_ATTRIBUTES_MAP["vpncustomergateway"]

        return CloudStackVpnCustomerGateway(
            id=item["id"],
            cidr_list=cidr_list,
            esp_policy=esp_policy,
            gateway=gateway,
            ike_policy=ike_policy,
            ipsec_psk=ipsec_psk,
            driver=self,
            extra=self._get_extra_dict(item, extra_map),
        )

    def ex_delete_vpn_customer_gateway(self, vpn_customer_gateway):
        """
        Deletes a VPN Customer Gateway.

        :param vpn_customer_gateway: The VPN Customer Gateway (required).
        :type  vpn_customer_gateway: :class:`CloudStackVpnCustomerGateway`

        :rtype: ``bool``
        """
        res = self._async_request(
            command="deleteVpnCustomerGateway",
            params={"id": vpn_customer_gateway.id},
            method="GET",
        )

        return res["success"]

    def ex_list_vpn_connections(
        self,
        account=None,
        domain_id=None,
        for_display=None,
        id=None,
        is_recursive=None,
        keyword=None,
        list_all=None,
        page=None,
        page_size=None,
        project_id=None,
        vpc_id=None,
    ):
        """
        List VPN Connections.

        :param   account: List resources by account (must be
                          used with the domain_id parameter).
        :type    account: ``str``

        :param   domain_id: List only resources belonging
                            to the domain specified.
        :type    domain_id: ``str``

        :param   for_display: List resources by display flag (only root
                              admin is eligible to pass this parameter).
        :type    for_display: ``bool``

        :param   id: ID of the VPN Connection.
        :type    id: ``str``

        :param   is_recursive: Defaults to False, but if true, lists all
                               resources from the parent specified by the
                               domain_id till leaves.
        :type    is_recursive: ``bool``

        :param   keyword: List by keyword.
        :type    keyword: ``str``

        :param   list_all: If set to False, list only resources belonging to
                           the command's caller; if set to True - list
                           resources that the caller is authorized to see.
                           Default value is False.
        :type    list_all: ``str``

        :param   page: Start from page.
        :type    page: ``int``

        :param   page_size: Items per page.
        :type    page_size: ``int``

        :param   project_id: List objects by project.
        :type    project_id: ``str``

        :param   vpc_id: List objects by VPC.
        :type    vpc_id: ``str``

        :rtype: ``list`` of :class:`CloudStackVpnConnection`
        """
        args = {}

        if account is not None:
            args["account"] = account

        if domain_id is not None:
            args["domainid"] = domain_id

        if for_display is not None:
            args["fordisplay"] = for_display

        if id is not None:
            args["id"] = id

        if is_recursive is not None:
            args["isrecursive"] = is_recursive

        if keyword is not None:
            args["keyword"] = keyword

        if list_all is not None:
            args["listall"] = list_all

        if page is not None:
            args["page"] = page

        if page_size is not None:
            args["pagesize"] = page_size

        if project_id is not None:
            args["projectid"] = project_id

        if vpc_id is not None:
            args["vpcid"] = vpc_id

        res = self._sync_request(command="listVpnConnections", params=args, method="GET")

        items = res.get("vpnconnection", [])
        vpn_connections = []
        extra_map = RESOURCE_EXTRA_ATTRIBUTES_MAP["vpnconnection"]

        for item in items:
            extra = self._get_extra_dict(item, extra_map)

            vpn_connections.append(
                CloudStackVpnConnection(
                    id=item["id"],
                    passive=item["passive"],
                    vpn_customer_gateway_id=item["s2scustomergatewayid"],
                    vpn_gateway_id=item["s2svpngatewayid"],
                    state=item["state"],
                    driver=self,
                    extra=extra,
                )
            )

        return vpn_connections

    def ex_create_vpn_connection(
        self, vpn_customer_gateway, vpn_gateway, for_display=None, passive=None
    ):
        """
        Creates a VPN Connection.

        :param   vpn_customer_gateway: The VPN Customer Gateway (required).
        :type    vpn_customer_gateway: :class:`CloudStackVpnCustomerGateway`

        :param   vpn_gateway: The VPN Gateway (required).
        :type    vpn_gateway: :class:`CloudStackVpnGateway`

        :param   for_display: Display the Connection to the end user or not.
        :type    for_display: ``str``

        :param   passive: If True, sets the connection to be passive.
        :type    passive: ``bool``

        :rtype: :class: `CloudStackVpnConnection`
        """
        args = {
            "s2scustomergatewayid": vpn_customer_gateway.id,
            "s2svpngatewayid": vpn_gateway.id,
        }

        if for_display is not None:
            args["fordisplay"] = for_display

        if passive is not None:
            args["passive"] = passive

        res = self._async_request(command="createVpnConnection", params=args, method="GET")

        item = res["vpnconnection"]
        extra_map = RESOURCE_EXTRA_ATTRIBUTES_MAP["vpnconnection"]

        return CloudStackVpnConnection(
            id=item["id"],
            passive=item["passive"],
            vpn_customer_gateway_id=vpn_customer_gateway.id,
            vpn_gateway_id=vpn_gateway.id,
            state=item["state"],
            driver=self,
            extra=self._get_extra_dict(item, extra_map),
        )

    def ex_delete_vpn_connection(self, vpn_connection):
        """
        Deletes a VPN Connection.

        :param vpn_connection: The VPN Connection (required).
        :type  vpn_connection: :class:`CloudStackVpnConnection`

        :rtype: ``bool``
        """
        res = self._async_request(
            command="deleteVpnConnection",
            params={"id": vpn_connection.id},
            method="GET",
        )

        return res["success"]

    def _to_snapshot(self, data):
        """
        Create snapshot object from data

        :param data: Node data object.
        :type data: ``dict``

        :rtype: :class:`VolumeSnapshot`
        """
        extra = {
            "tags": data.get("tags", None),
            "name": data.get("name", None),
            "volume_id": data.get("volumeid", None),
        }
        return VolumeSnapshot(data["id"], driver=self, extra=extra)

    def _to_node(self, data, public_ips=None):
        """
        :param data: Node data object.
        :type data: ``dict``

        :param public_ips: A list of additional IP addresses belonging to
                           this node. (optional)
        :type public_ips: ``list`` or ``None``
        """
        id = data["id"]

        if "name" in data:
            name = data["name"]
        elif "displayname" in data:
            name = data["displayname"]
        else:
            name = None

        state = self.NODE_STATE_MAP[data["state"]]

        public_ips = public_ips if public_ips else []
        private_ips = []

        for nic in data["nic"]:
            if "ipaddress" not in nic:
                continue
            if is_private_subnet(nic["ipaddress"]):
                private_ips.append(nic["ipaddress"])
            else:
                public_ips.append(nic["ipaddress"])

        security_groups = data.get("securitygroup", [])

        if security_groups:
            security_groups = [sg["name"] for sg in security_groups]

        affinity_groups = data.get("affinitygroup", [])

        if affinity_groups:
            affinity_groups = [ag["id"] for ag in affinity_groups]

        created = data.get("created", False)

        extra = self._get_extra_dict(data, RESOURCE_EXTRA_ATTRIBUTES_MAP["node"])

        # Add additional parameters to extra
        extra["security_group"] = security_groups
        extra["affinity_group"] = affinity_groups
        extra["ip_addresses"] = []
        extra["ip_forwarding_rules"] = []
        extra["port_forwarding_rules"] = []
        extra["created"] = created

        if "tags" in data:
            extra["tags"] = self._get_resource_tags(data["tags"])

        node = CloudStackNode(
            id=id,
            name=name,
            state=state,
            public_ips=list(set(public_ips)),
            private_ips=private_ips,
            driver=self,
            extra=extra,
        )
        return node

    def _to_key_pairs(self, data):
        key_pairs = [self._to_key_pair(data=item) for item in data]
        return key_pairs

    def _to_key_pair(self, data):
        key_pair = KeyPair(
            name=data["name"],
            fingerprint=data["fingerprint"],
            public_key=data.get("publickey", None),
            private_key=data.get("privatekey", None),
            driver=self,
        )
        return key_pair

    def _to_affinity_group(self, data):
        affinity_group = CloudStackAffinityGroup(
            id=data["id"],
            name=data["name"],
            group_type=CloudStackAffinityGroupType(data["type"]),
            account=data.get("account", ""),
            domain=data.get("domain", ""),
            domainid=data.get("domainid", ""),
            description=data.get("description", ""),
            virtualmachine_ids=data.get("virtualmachineIds", ""),
        )

        return affinity_group

    def _get_resource_tags(self, tag_set):
        """
        Parse tags from the provided element and return a dictionary with
        key/value pairs.

        :param      tag_set: A list of key/value tag pairs
        :type       tag_set: ``list```

        :rtype: ``dict``
        """
        tags = {}

        for tag in tag_set:
            key = tag["key"]
            value = tag["value"]
            tags[key] = value

        return tags

    def _get_extra_dict(self, response, mapping):
        """
        Extract attributes from the element based on rules provided in the
        mapping dictionary.

        :param      response: The JSON response to parse the values from.
        :type       response: ``dict``

        :param      mapping: Dictionary with the extra layout
        :type       mapping: ``dict``

        :rtype: ``dict``
        """
        extra = {}
        for attribute, values in mapping.items():
            transform_func = values["transform_func"]
            value = response.get(values["key_name"], None)

            if value is not None:
                extra[attribute] = transform_func(value)
            else:
                extra[attribute] = None

        return extra

    def _to_volume_state(self, vol):
        state = self.VOLUME_STATE_MAP.get(vol["state"], StorageVolumeState.UNKNOWN)

        # If a volume is 'Ready' and is attached to a virtualmachine, set
        # the status to INUSE
        if state == StorageVolumeState.AVAILABLE and "virtualmachineid" in vol:
            state = StorageVolumeState.INUSE

        return state
