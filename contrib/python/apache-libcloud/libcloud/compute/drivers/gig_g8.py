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
GiG G8 Driver

"""
import json

from libcloud.compute.base import (
    Node,
    NodeSize,
    NodeImage,
    UuidMixin,
    NodeDriver,
    StorageVolume,
    NodeAuthSSHKey,
)
from libcloud.common.gig_g8 import G8Connection
from libcloud.compute.types import Provider, NodeState
from libcloud.common.exceptions import BaseHTTPError


class G8ProvisionError(Exception):
    pass


class G8PortForward(UuidMixin):
    def __init__(self, network, node_id, publicport, privateport, protocol, driver):
        self.node_id = node_id
        self.network = network
        self.publicport = int(publicport)
        self.privateport = int(privateport)
        self.protocol = protocol
        self.driver = driver
        UuidMixin.__init__(self)

    def destroy(self):
        self.driver.ex_delete_portforward(self)


class G8Network(UuidMixin):
    """
    G8 Network object class.

    This class maps to a cloudspace

    """

    def __init__(self, id, name, cidr, publicipaddress, driver, extra=None):
        self.id = id
        self.name = name
        self._cidr = cidr
        self.driver = driver
        self.publicipaddress = publicipaddress
        self.extra = extra
        UuidMixin.__init__(self)

    @property
    def cidr(self):
        """
        Cidr is not part of the list result
        we will lazily fetch it with a get request
        """
        if self._cidr is None:
            networkdata = self.driver._api_request("/cloudspaces/get", {"cloudspaceId": self.id})
            self._cidr = networkdata["privatenetwork"]
        return self._cidr

    def list_nodes(self):
        return self.driver.list_nodes(self)

    def destroy(self):
        return self.driver.ex_destroy_network(self)

    def list_portforwards(self):
        return self.driver.ex_list_portforwards(self)

    def create_portforward(self, node, publicport, privateport, protocol="tcp"):
        return self.driver.ex_create_portforward(self, node, publicport, privateport, protocol)


class G8NodeDriver(NodeDriver):
    """
    GiG G8 node driver

    """

    NODE_STATE_MAP = {
        "VIRTUAL": NodeState.PENDING,
        "HALTED": NodeState.STOPPED,
        "RUNNING": NodeState.RUNNING,
        "DESTROYED": NodeState.TERMINATED,
        "DELETED": NodeState.TERMINATED,
        "PAUSED": NodeState.PAUSED,
        "ERROR": NodeState.ERROR,
        # transition states
        "DEPLOYING": NodeState.PENDING,
        "STOPPING": NodeState.STOPPING,
        "MOVING": NodeState.MIGRATING,
        "RESTORING": NodeState.PENDING,
        "STARTING": NodeState.STARTING,
        "PAUSING": NodeState.PENDING,
        "RESUMING": NodeState.PENDING,
        "RESETTING": NodeState.REBOOTING,
        "DELETING": NodeState.TERMINATED,
        "DESTROYING": NodeState.TERMINATED,
        "ADDING_DISK": NodeState.RECONFIGURING,
        "ATTACHING_DISK": NodeState.RECONFIGURING,
        "DETACHING_DISK": NodeState.RECONFIGURING,
        "ATTACHING_NIC": NodeState.RECONFIGURING,
        "DETTACHING_NIC": NodeState.RECONFIGURING,
        "DELETING_DISK": NodeState.RECONFIGURING,
        "CHANGING_DISK_LIMITS": NodeState.RECONFIGURING,
        "CLONING": NodeState.PENDING,
        "RESIZING": NodeState.RECONFIGURING,
        "CREATING_TEMPLATE": NodeState.PENDING,
    }

    name = "GiG G8 Node Provider"
    website = "https://gig.tech"
    type = Provider.GIG_G8
    connectionCls = G8Connection

    def __init__(self, user_id, key, api_url):
        # type (int, str, str) -> None
        """
        :param  key: Token to use for api (jwt)
        :type   key: ``str``
        :param  user_id: Id of the account to connect to (accountId)
        :type   user_id: ``int``
        :param  api_url: G8 api url
        :type   api_url: ``str``

        :rtype: ``None``
        """
        self._apiurl = api_url.rstrip("/")
        super().__init__(key=key)
        self._account_id = user_id
        self._location_data = None

    def _ex_connection_class_kwargs(self):
        return {"url": self._apiurl}

    def _api_request(self, endpoint, params=None):
        return self.connection.request(
            endpoint.lstrip("/"), data=json.dumps(params), method="POST"
        ).object

    @property
    def _location(self):
        if self._location_data is None:
            self._location_data = self._api_request("/locations/list")[0]
        return self._location_data

    def create_node(
        self,
        name,
        image,
        ex_network,
        ex_description,
        size=None,
        auth=None,
        ex_create_attr=None,
        ex_expose_ssh=False,
    ):
        # type (str, Image, G8Network, str, Size,
        #       Optional[NodeAuthSSHKey], Optional[Dict], bool) -> Node
        """
        Create a node.

        The `ex_create_attr` parameter can include the following dictionary
        key and value pairs:

        * `memory`: ``int`` Memory in MiB
                    (only used if size is None and vcpus is passed
        * `vcpus`: ``int`` Amount of vcpus
                   (only used if size is None and memory is passed)
        * `disk_size`: ``int`` Size of bootdisk
                       defaults to minimumsize of the image
        * `user_data`: ``str`` for cloud-config data
        * `private_ip`: ``str`` Private Ip inside network
        * `data_disks`: ``list(int)`` Extra data disks to assign
                        to vm list of disk sizes in GiB

        :param name: the name to assign the vm
        :type  name: ``str``

        :param size: the plan size to create
                       mutual exclusive with `memory` `vcpus`
        :type  size: :class:`NodeSize`

        :param image: which distribution to deploy on the vm
        :type  image: :class:`NodeImage`

        :param network: G8 Network to place vm in
        :type  size: :class:`G8Network`

        :param ex_description: Description of vm
        :type  size: : ``str``

        :param auth: an SSH key
        :type  auth: :class:`NodeAuthSSHKey`

        :param ex_create_attr: A dictionary of optional attributes for
                                 vm creation
        :type  ex_create_attr: ``dict``

        :param ex_expose_ssh: Create portforward for ssh port
        :type  ex_expose_ssh: int

        :return: The newly created node.
        :rtype: :class:`Node`
        """
        params = {
            "name": name,
            "imageId": int(image.id),
            "cloudspaceId": int(ex_network.id),
            "description": ex_description,
        }

        ex_create_attr = ex_create_attr or {}
        if size:
            params["sizeId"] = int(size.id)
        else:
            params["memory"] = ex_create_attr["memory"]
            params["vcpus"] = ex_create_attr["vcpus"]
        if "user_data" in ex_create_attr:
            params["userdata"] = ex_create_attr["user_data"]
        if "data_disks" in ex_create_attr:
            params["datadisks"] = ex_create_attr["data_disks"]
        if "private_ip" in ex_create_attr:
            params["privateIp"] = ex_create_attr["private_ip"]
        if "disk_size" in ex_create_attr:
            params["disksize"] = ex_create_attr["disk_size"]
        else:
            params["disksize"] = image.extra["min_disk_size"]
        if auth and isinstance(auth, NodeAuthSSHKey):
            userdata = params.setdefault("userdata", {})
            users = userdata.setdefault("users", [])
            root = None
            for user in users:
                if user["name"] == "root":
                    root = user
                    break
            else:
                root = {"name": "root", "shell": "/bin/bash"}
                users.append(root)
            keys = root.setdefault("ssh-authorized-keys", [])
            keys.append(auth.pubkey)
        elif auth:
            error = "Auth type {} is not implemented".format(type(auth))
            raise NotImplementedError(error)

        machineId = self._api_request("/machines/create", params)
        machine = self._api_request("/machines/get", params={"machineId": machineId})
        node = self._to_node(machine, ex_network)
        if ex_expose_ssh:
            port = self.ex_expose_ssh_node(node)
            node.extra["ssh_port"] = port
            node.extra["ssh_ip"] = ex_network.publicipaddress
        return node

    def _find_ssh_ports(self, ex_network, node):
        forwards = ex_network.list_portforwards()
        usedports = []
        result = {"node": None, "network": usedports}
        for forward in forwards:
            usedports.append(forward.publicport)
            if forward.node_id == node.id and forward.privateport == 22:
                result["node"] = forward.privateport
        return result

    def ex_expose_ssh_node(self, node):
        """
        Create portforward for ssh purposed

        :param node: Node to expose ssh for
        :type  node: ``Node``

        :rtype: ``int``
        """

        network = node.extra["network"]
        ports = self._find_ssh_ports(network, node)
        if ports["node"]:
            return ports["node"]
        usedports = ports["network"]
        sshport = 2200
        endport = 3000
        while sshport < endport:
            while sshport in usedports:
                sshport += 1
            try:
                network.create_portforward(node, sshport, 22)
                node.extra["ssh_port"] = sshport
                node.extra["ssh_ip"] = network.publicipaddress
                break
            except BaseHTTPError as e:
                if e.code == 409:
                    # port already used maybe raise let's try next
                    usedports.append(sshport)
                raise
        else:
            raise G8ProvisionError("Failed to create portforward")
        return sshport

    def ex_create_network(self, name, private_network="192.168.103.0/24", type="vgw"):
        # type (str, str, str) -> G8Network
        """
        Create network also known as cloudspace

        :param name: the name to assign to the network
        :type  name: ``str``

        :param private_network: subnet used as private network
        :type  private_network: ``str``

        :param type: type of the gateway vgw or routeros
        :type  type: ``str``
        """
        userinfo = self._api_request("../system/usermanager/whoami")
        params = {
            "accountId": self._account_id,
            "privatenetwork": private_network,
            "access": userinfo["name"],
            "name": name,
            "location": self._location["locationCode"],
            "type": type,
        }
        networkid = self._api_request("/cloudspaces/create", params)
        network = self._api_request("/cloudspaces/get", {"cloudspaceId": networkid})
        return self._to_network(network)

    def ex_destroy_network(self, network):
        # type (G8Network) -> bool
        self._api_request("/cloudspaces/delete", {"cloudspaceId": int(network.id)})
        return True

    def stop_node(self, node):
        # type (Node) -> bool
        """
        Stop virtual machine
        """
        node.state = NodeState.STOPPING
        self._api_request("/machines/stop", {"machineId": int(node.id)})
        node.state = NodeState.STOPPED
        return True

    def ex_list_portforwards(self, network):
        # type (G8Network) -> List[G8PortForward]
        data = self._api_request("/portforwarding/list", {"cloudspaceId": int(network.id)})
        forwards = []
        for forward in data:
            forwards.append(self._to_port_forward(forward, network))
        return forwards

    def ex_create_portforward(self, network, node, publicport, privateport, protocol="tcp"):
        # type (G8Network, Node, int, int, str) -> G8PortForward
        params = {
            "cloudspaceId": int(network.id),
            "machineId": int(node.id),
            "localPort": privateport,
            "publicPort": publicport,
            "publicIp": network.publicipaddress,
            "protocol": protocol,
        }
        self._api_request("/portforwarding/create", params)
        return self._to_port_forward(params, network)

    def ex_delete_portforward(self, portforward):
        # type (G8PortForward) -> bool
        params = {
            "cloudspaceId": int(portforward.network.id),
            "publicIp": portforward.network.publicipaddress,
            "publicPort": portforward.publicport,
            "proto": portforward.protocol,
        }
        self._api_request("/portforwarding/deleteByPort", params)
        return True

    def start_node(self, node):
        # type (Node) -> bool
        """
        Start virtual machine
        """
        node.state = NodeState.STARTING
        self._api_request("/machines/start", {"machineId": int(node.id)})
        node.state = NodeState.RUNNING
        return True

    def ex_list_networks(self):
        # type () -> List[G8Network]
        """
        Return the list of networks.

        :return: A list of network objects.
        :rtype: ``list`` of :class:`G8Network`
        """
        networks = []
        for network in self._api_request("/cloudspaces/list"):
            if network["accountId"] == self._account_id:
                networks.append(self._to_network(network))
        return networks

    def list_sizes(self):
        # type () -> List[Size]
        """
        Returns a list of node sizes as a cloud provider might have

        """
        location = self._location["locationCode"]

        sizes = []
        for size in self._api_request("/sizes/list", {"location": location}):
            sizes.extend(self._to_size(size))
        return sizes

    def list_nodes(self, ex_network=None):
        # type (Optional[G8Network]) -> List[Node]
        """
        List the nodes known to a particular driver;
        There are two default nodes created at the beginning
        """

        def _get_ssh_port(forwards, node):
            for forward in forwards:
                if forward.node_id == node.id and forward.privateport == 22:
                    return forward

        if ex_network:
            networks = [ex_network]
        else:
            networks = self.ex_list_networks()
        nodes = []
        for network in networks:
            nodes_list = self._api_request("/machines/list", params={"cloudspaceId": network.id})
            forwards = network.list_portforwards()
            for nodedata in nodes_list:
                node = self._to_node(nodedata, network)
                sshforward = _get_ssh_port(forwards, node)
                if sshforward:
                    node.extra["ssh_port"] = sshforward.publicport
                    node.extra["ssh_ip"] = network.publicipaddress
                nodes.append(node)
        return nodes

    def reboot_node(self, node):
        # type (Node) -> bool
        """
        Reboot node
        returns True as if the reboot had been successful.
        """
        node.state = NodeState.REBOOTING
        self._api_request("/machines/reboot", {"machineId": int(node.id)})
        node.state = NodeState.RUNNING
        return True

    def destroy_node(self, node):
        # type (Node) -> bool
        """
        Destroy node
        """
        self._api_request("/machines/delete", {"machineId": int(node.id)})
        return True

    def list_images(self):
        # type () -> List[Image]
        """
        Returns a list of images as a cloud provider might have

        @inherits: :class:`NodeDriver.list_images`
        """
        images = []
        for image in self._api_request("/images/list", {"accountId": self._account_id}):
            images.append(self._to_image(image))
        return images

    def list_volumes(self):
        # type () -> List[StorageVolume]
        volumes = []
        for disk in self._api_request("/disks/list", {"accountId": self._account_id}):
            if disk["status"] not in ["ASSIGNED", "CREATED"]:
                continue
            volumes.append(self._to_volume(disk))
        return volumes

    def create_volume(self, size, name, ex_description, ex_disk_type="D"):
        # type (int, str, str, Optional[str]) -> StorageVolume
        """
        Create volume

        :param size: Size of the volume to create in GiB
        :type  size: ``int``

        :param name: Name of the volume
        :type  name: ``str``

        :param description: Description of the volume
        :type  description: ``str``

        :param disk_type: Type of the disk depending on the G8
                            D for datadisk is always available
        :type  disk_type: ``str``

        :rtype: class:`StorageVolume`
        """
        params = {
            "size": size,
            "name": name,
            "type": ex_disk_type,
            "description": ex_description,
            "gid": self._location["gid"],
            "accountId": self._account_id,
        }
        diskId = self._api_request("/disks/create", params)
        disk = self._api_request("/disks/get", {"diskId": diskId})
        return self._to_volume(disk)

    def destroy_volume(self, volume):
        # type (StorageVolume) -> bool
        self._api_request("/disks/delete", {"diskId": int(volume.id)})
        return True

    def attach_volume(self, node, volume):
        # type (Node, StorageVolume) -> bool
        params = {"machineId": int(node.id), "diskId": int(volume.id)}
        self._api_request("/machines/attachDisk", params)
        return True

    def detach_volume(self, node, volume):
        # type (Node, StorageVolume) -> bool
        params = {"machineId": int(node.id), "diskId": int(volume.id)}
        self._api_request("/machines/detachDisk", params)
        return True

    def _to_volume(self, data):
        # type (dict) -> StorageVolume
        extra = {"type": data["type"], "node_id": data.get("machineId")}
        return StorageVolume(
            id=str(data["id"]),
            size=data["sizeMax"],
            name=data["name"],
            driver=self,
            extra=extra,
        )

    def _to_node(self, nodedata, ex_network):
        # type (dict) -> Node
        state = self.NODE_STATE_MAP.get(nodedata["status"], NodeState.UNKNOWN)
        public_ips = []
        private_ips = []
        nics = nodedata.get("nics", [])
        if not nics:
            nics = nodedata.get("interfaces", [])
        for nic in nics:
            if nic["type"] == "PUBLIC":
                public_ips.append(nic["ipAddress"].split("/")[0])
            else:
                private_ips.append(nic["ipAddress"])
        extra = {"network": ex_network}
        for account in nodedata.get("accounts", []):
            extra["password"] = account["password"]
            extra["username"] = account["login"]

        return Node(
            id=str(nodedata["id"]),
            name=nodedata["name"],
            driver=self,
            public_ips=public_ips,
            private_ips=private_ips,
            state=state,
            extra=extra,
        )

    def _to_network(self, network):
        # type (dict) -> G8Network
        return G8Network(
            str(network["id"]),
            network["name"],
            None,
            network["externalnetworkip"],
            self,
        )

    def _to_image(self, image):
        # type (dict) -> Image
        extra = {
            "min_disk_size": image["bootDiskSize"],
            "min_memory": image["memory"],
        }
        return NodeImage(id=str(image["id"]), name=image["name"], driver=self, extra=extra)

    def _to_size(self, size):
        # type (dict) -> Size
        sizes = []
        for disk in size["disks"]:
            sizes.append(
                NodeSize(
                    id=str(size["id"]),
                    name=size["name"],
                    ram=size["memory"],
                    disk=disk,
                    driver=self,
                    extra={"vcpus": size["vcpus"]},
                    bandwidth=0,
                    price=0,
                )
            )
        return sizes

    def _to_port_forward(self, data, ex_network):
        # type (dict, G8Network) -> G8PortForward
        return G8PortForward(
            ex_network,
            str(data["machineId"]),
            data["publicPort"],
            data["localPort"],
            data["protocol"],
            self,
        )


if __name__ == "__main__":
    import doctest

    doctest.testmod()
