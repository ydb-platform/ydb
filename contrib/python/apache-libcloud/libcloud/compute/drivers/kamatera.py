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
Kamatera node driver
"""
import json
import time
import datetime

from libcloud.utils.py3 import basestring
from libcloud.common.base import JsonResponse, ConnectionUserAndKey
from libcloud.compute.base import Node, NodeSize, NodeImage, NodeState, NodeDriver, NodeLocation
from libcloud.compute.types import Provider


class KamateraResponse(JsonResponse):
    """
    Response class for KamateraDriver
    """

    def parse_error(self):
        data = self.parse_body()
        if "message" in data:
            return data["message"]
        else:
            return json.dumps(data)


class KamateraConnection(ConnectionUserAndKey):
    """
    Connection class for KamateraDriver
    """

    host = "cloudcli.cloudwm.com"
    responseCls = KamateraResponse

    def add_default_headers(self, headers):
        """Adds headers that are needed for all requests"""
        headers["AuthClientId"] = self.user_id
        headers["AuthSecret"] = self.key
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"
        return headers


class KamateraNodeDriver(NodeDriver):
    """
    Kamatera node driver

    :keyword    key: API Client ID, required for authentication
    :type       key: ``str``

    :keyword    secret: API Secret, required for authentcaiont
    :type       secret: ``str``
    """

    type = Provider.KAMATERA
    name = "Kamatera"
    website = "https://www.kamatera.com/"
    connectionCls = KamateraConnection
    features = {"create_node": ["password", "generates_password", "ssh_key"]}

    EX_BILLINGCYCLE_HOURLY = "hourly"
    EX_BILLINGCYCLE_MONTHLY = "monthly"

    def list_locations(self):
        """
        List available locations for deployment

        :rtype: ``list`` of :class:`NodeLocation`
        """
        response = self.connection.request("service/server?datacenter=1")
        return [
            self.ex_get_location(datacenter["id"], datacenter["subCategory"], datacenter["name"])
            for datacenter in response.object
        ]

    def list_sizes(self, location):
        """
        List predefined sizes for the given location.

        :param location: Location of the deployment.
        :type location: :class:`.NodeLocation`

        @inherits: :class:`NodeDriver.list_sizes`
        """
        response = self.connection.request("service/server?sizes=1&datacenter=%s" % location.id)
        return [
            self.ex_get_size(
                size["ramMB"],
                size["diskSizeGB"],
                size["cpuType"],
                size["cpuCores"],
                extraDiskSizesGB=[],
                monthlyTrafficPackage=size["monthlyTrafficPackage"],
                id=size["id"],
            )
            for size in response.object
        ]

    def list_images(self, location):
        """
        List available disk images.

        :param location: Location of the deployment.
                         Available disk images depend on location.
        :type location: :class:`.NodeLocation`

        :rtype: ``list`` of :class:`NodeImage`
        """
        response = self.connection.request("service/server?images=1&datacenter=%s" % location.id)
        images = []
        for image in response.object:
            extra = self._copy_dict(("datacenter", "os", "code", "osDiskSizeGB", "ramMBMin"), image)
            images.append(self.ex_get_image(image["name"], image["id"], extra))
        return images

    def create_node(
        self,
        name,
        size,
        image,
        location,
        auth=None,
        ex_networks=None,
        ex_dailybackup=False,
        ex_managed=False,
        ex_billingcycle=EX_BILLINGCYCLE_HOURLY,
        ex_poweronaftercreate=True,
        ex_wait=True,
    ):
        """
        Creates a Kamatera node.

        If auth is not given then password will be generated.

        :param name:   String with a name for this new node (required)
        :type name:   ``str``

        :param size:   The size of resources allocated to this node (required)
        :type size:   :class:`.NodeSize`

        :param image:  OS Image to boot on node. (required)
        :type image:  :class:`.NodeImage`

        :param location: Which data center to create a node in. (required)
        :type location: :class:`.NodeLocation`

        :param auth:   Authentication information for the node (optional)
        :type auth:   :class:`.NodeAuthSSHKey` or :class:`.NodeAuthPassword`

        :param ex_networks:   Network configurations (optional)
        :type ex_networks:   ``list`` of ``dict``

        :param ex_dailybackup:   Whether to create daily backups (optional)
        :type ex_dailybackup:    ``bool``

        :param ex_managed:   Whether to provide managed support (optional)
        :type ex_managed:    ``bool``

        :param ex_billingcycle:   billing cycle (hourly / monthly) (optional)
        :type ex_billingcycle:    ``str``

        :param ex_poweronaftercreate:   power on after creation (optional)
        :type ex_poweronaftercreate:    ``bool``

        :param ex_wait:   wait for server to be running (optional)
        :type ex_wait:    ``bool``

        :return: The newly created node.
        :rtype: :class:`.Node`
        """
        password = None
        pubkey = None
        generate_password = False
        if isinstance(auth, basestring):
            password = auth
        else:
            auth_obj = self._get_and_check_auth(auth)
            if getattr(auth_obj, "generated", False):
                password = "__generate__"
                generate_password = True
            elif hasattr(auth_obj, "password"):
                password = auth_obj.password
                generate_password = False
            elif hasattr(auth_obj, "pubkey"):
                pubkey = auth_obj.pubkey
        if not ex_networks:
            ex_networks = [{"name": "wan", "ip": "auto"}]
        request_data = {
            "name": name,
            "password": password or "",
            "passwordValidate": password or "",
            "ssh-key": pubkey or "",
            "datacenter": location.id,
            "image": image.id,
            "cpu": "{}{}".format(size.extra["cpuCores"], size.extra["cpuType"]),
            "ram": size.ram,
            "disk": " ".join(
                ["size=%d" % disksize for disksize in [size.disk] + size.extra["extraDiskSizesGB"]]
            ),
            "dailybackup": "yes" if ex_dailybackup else "no",
            "managed": "yes" if ex_managed else "no",
            "network": " ".join(
                [
                    ",".join(["{}={}".format(k, v) for k, v in network.items()])
                    for network in ex_networks
                ]
            ),
            "quantity": 1,
            "billingcycle": ex_billingcycle,
            "monthlypackage": size.extra["monthlyTrafficPackage"] or "",
            "poweronaftercreate": "yes" if ex_poweronaftercreate else "no",
        }
        response = self.connection.request(
            "service/server", method="POST", data=json.dumps(request_data)
        )
        if generate_password:
            command_ids = response.object["commandIds"]
            generated_password = response.object["password"]
        else:
            command_ids = response.object
            generated_password = None
        if len(command_ids) != 1:
            raise RuntimeError("invalid response")
        node = self.ex_get_node(
            name=name,
            size=size,
            image=image,
            location=location,
            dailybackup=ex_dailybackup,
            managed=ex_managed,
            billingcycle=ex_billingcycle,
            generated_password=generated_password,
            create_command_id=command_ids[0],
            poweronaftercreate=ex_poweronaftercreate,
        )
        if ex_wait:
            if "create_command_id" not in node.extra or node.state != NodeState.UNKNOWN:
                raise ValueError("invalid node for updating create status")
            command = self.ex_wait_command(node.extra["create_command_id"])
            node.extra["create_log"] = command.get("log")
            if node.extra.get("poweronaftercreate"):
                node.state = NodeState.RUNNING
            else:
                node.state = NodeState.STOPPED
            if command.get("completed"):
                node.created_at = datetime.datetime.strptime(
                    command["completed"], "%Y-%m-%d %H:%M:%S"
                )
            name_lines = [
                line for line in node.extra["create_log"].split("\n") if line.startswith("Name: ")
            ]
            if len(name_lines) != 1:
                raise RuntimeError("Invalid node create log response")
            node.name = name_lines[0].replace("Name: ", "")
            response = self.connection.request(
                "/service/server/info",
                method="POST",
                data=json.dumps({"name": node.name}),
            )
            self._update_node_from_server_info(node, response.object[0])
        return node

    def list_nodes(self, ex_name_regex=None, ex_full_details=False, ex_id=None):
        """
        List nodes

        :param ex_name_regex:   Regular expression to match node names
                                if set returns full node details (optional)
        :type ex_name_regex:    ``str``

        :param ex_full_details:   Whether to return full node details
                                  takes longer to complete (optional)
        :type ex_full_details:    ``bool``

        :return: List of node objects
        :rtype: ``list`` of :class:`Node`
        """
        if ex_name_regex or ex_full_details or ex_id:
            request_data = {}
            if ex_id:
                request_data["id"] = ex_id
            else:
                if not ex_name_regex:
                    ex_name_regex = ".*"
                request_data["name"] = ex_name_regex
            response = self.connection.request(
                "/service/server/info", method="POST", data=json.dumps(request_data)
            )
            return [
                self._update_node_from_server_info(self.ex_get_node(), server)
                for server in response.object
            ]
        else:
            response = self.connection.request("/service/servers")
            return [
                self.ex_get_node(
                    id=server["id"],
                    name=server["name"],
                    state=(NodeState.RUNNING if server["power"] == "on" else NodeState.STOPPED),
                    location=self.ex_get_location(server["datacenter"]),
                )
                for server in response.object
            ]

    def reboot_node(self, node, ex_wait=True):
        """
        Reboot the given node

        :param node:     the node to reboot
        :type node: :class:`Node`

        :param ex_wait:     wait for reboot to complete (optional)
        :type ex_wait:  ``bool``

        :rtype: ``bool``
        """
        return self.ex_node_operation(node, "reboot", ex_wait)

    def destroy_node(self, node, ex_wait=True):
        """
        Destroy the given node

        :param node:     the node to destroy
        :type node: :class:`Node`

        :param ex_wait:     wait for destroy to complete (optional)
        :type ex_wait:      ``bool``

        :rtype: ``bool``
        """
        return self.ex_node_operation(node, "terminate", ex_wait)

    def stop_node(self, node, ex_wait=True):
        """
        Stop the given node

        :param node:     the node to stop
        :type node: :class:`Node`

        :param ex_wait:     wait for stop to complete (optional)
        :type ex_wait:      ``bool``

        :rtype: ``bool``
        """
        return self.ex_node_operation(node, "poweroff", ex_wait)

    def start_node(self, node, ex_wait=True):
        """
        Start the given node

        :param node:     the node to start
        :type node: :class:`Node`

        :param ex_wait:     wait for start to complete (optional)
        :type ex_wait:      ``bool``

        :rtype: ``bool``
        """
        return self.ex_node_operation(node, "poweron", ex_wait)

    def ex_node_operation(self, node, operation, wait=True):
        """
        Run custom operations on the node

        :param node:     the node to run operation on
        :type node: :class:`Node`

        :param operation:   the operation to run
        :type operation:   ``str``

        :param ex_wait:     wait for destroy to complete (optional)
        :type ex_wait:      ``bool``

        :rtype: ``bool``
        """
        if node.id:
            request_data = {"id": node.id}
        elif node.name:
            request_data = {"name": node.name}
        else:
            raise ValueError("Invalid node for %s node operation: " "missing id / name" % operation)
        if operation == "terminate":
            request_data["force"] = True
        command_id = self.connection.request(
            "/service/server/%s" % operation,
            method="POST",
            data=json.dumps(request_data),
        ).object[0]
        if wait:
            self.ex_wait_command(command_id)
        else:
            node.extra["%s_command_id" % operation] = command_id
        return True

    def ex_get_location(self, id, name=None, country=None):
        """
        Get a NodeLocation object to use for other methods

        :param id:     Location ID - uppercase letters code (required)
        :type id:      ``str``

        :param name:     Location Name (optional)
        :type name:      ``str``

        :param name:     Location country (optional)
        :type name:      ``str``

        :rtype: :class:`.NodeLocation`
        """
        return NodeLocation(id=id, name=name, country=country, driver=self)

    def ex_get_size(
        self,
        ramMB,
        diskSizeGB,
        cpuType,
        cpuCores,
        extraDiskSizesGB=None,
        monthlyTrafficPackage=None,
        id=None,
        name=None,
    ):
        """
        Get a NodeSize object to use for other methods

        :param ramMB:     Amount of RAM to allocate in MB (required)
        :type ramMB:      ``int``

        :param diskSizeGB:     disk size GB for primary hard disk (required)
        :type diskSizeGB:      ``int``

        :param cpuType:     CPU type ID (single uppercase letter),
                            see ex_list_capabilities (required)
        :type cpuType:      ``str``

        :param cpuCores:     Number of CPU cores to allocate (required)
        :type cpuCores:      ``int``

        :param extraDiskSizesGB:     additional disk sizes in GB (optional)
        :type extraDiskSizesGB:      ``list`` of :int:

        :param monthlyTrafficPackage:     ID of monthly traffic package
                                          see ex_list_capabilities (optional)
        :type monthlyTrafficPackage:      ``str``

        :param id:     Size ID (optional)
        :type id:      ``str``

        :param name:     Size Name (optional)
        :type name:      ``str``

        :rtype: :class:`.NodeLocation`
        """
        if not id:
            id = str(cpuCores) + cpuType
            id += "-" + str(ramMB) + "MB-" + str(diskSizeGB) + "GB"
            if monthlyTrafficPackage:
                id += "-" + monthlyTrafficPackage
        if not name:
            name = id
        return NodeSize(
            id=id,
            name=name,
            ram=ramMB,
            disk=diskSizeGB,
            bandwidth=0,
            price=0,
            driver=self.connection.driver,
            extra={
                "cpuType": cpuType,
                "cpuCores": cpuCores,
                "monthlyTrafficPackage": monthlyTrafficPackage,
                "extraDiskSizesGB": extraDiskSizesGB or [],
            },
        )

    def ex_get_image(self, name=None, id=None, extra=None):
        if not id and not name:
            raise ValueError("either id or name are required for NodeImage")
        return NodeImage(id=id or name, name=name or "", driver=self, extra=extra or {})

    def ex_list_capabilities(self, location):
        """
        List capabilities for given location.

        :param location: Location of the deployment.
        :type location: :class:`.NodeLocation`

        :return: ``dict``
        """
        return self.connection.request(
            "service/server?capabilities=1&" "datacenter=%s" % location.id
        ).object

    def ex_wait_command(self, command_id, timeout_seconds=600, poll_interval_seconds=2):
        """
        Wait for command to complete and return the command status details

        :param command_id: Command ID to wait for. (required)
        :type command_id: ``int``

        :param timeout_seconds: Max seconds to wait for command. (optional)
        :type timeout_seconds: ``int``

        :param poll_interval_seconds: Poll interval in seconds (optional)
        :type poll_interval_seconds: ``int``

        :return: ``dict``
        """
        start_time = datetime.datetime.now()
        time.sleep(poll_interval_seconds)
        while True:
            max_time = start_time + datetime.timedelta(seconds=timeout_seconds)
            if max_time < datetime.datetime.now():
                raise TimeoutError(
                    "Timeout waiting for command "
                    "(timeout_seconds=%s, command_id=%s)" % (str(timeout_seconds), str(command_id))
                )
            time.sleep(poll_interval_seconds)
            command = self.ex_get_command_status(command_id)
            status = command.get("status")
            if status == "complete":
                return command
            elif status == "error":
                raise RuntimeError("Command failed: " + command.get("log"))

    def ex_get_command_status(self, command_id):
        """
        Get Kamatera command status details

        :param command_id: Command ID to get details for. (required)
        :type command_id: ``int``

        :return: ``dict``
        """
        response = self.connection.request("/service/queue?id=" + str(command_id))
        if len(response.object) != 1:
            raise RuntimeError("invalid response")
        return response.object[0]

    def ex_get_node(
        self,
        id=None,
        name=None,
        state=NodeState.UNKNOWN,
        public_ips=None,
        private_ips=None,
        size=None,
        image=None,
        created_at=None,
        location=None,
        dailybackup=None,
        managed=None,
        billingcycle=None,
        generated_password=None,
        create_command_id=None,
        poweronaftercreate=None,
    ):
        """
        Get a Kamatera node object.

        :param id:   Node ID (optional)
        :type id:   ``str``

        :param name:   Node name (optional)
        :type name:   ``str``

        :param state:   Node state (optional)
        :type state:   :class:`libcloud.compute.types.NodeState`

        :param public_ips:   Node public IPS. (optional)
        :type public_ips:   ``list`` of :str:

        :param private_ips:   Node private IPS. (optional)
        :type private_ips:   ``list`` of :str:

        :param size:  node size. (optional)
        :type size:  :class:`.NodeSize`

        :param image:  Node OS Image. (optional)
        :type image:  :class:`.NodeImage`

        :param created_at:  Node creation time. (optional)
        :type created_at:  ``datetime.datetime``

        :param location: Node datacenter. (optional)
        :type location: :class:`.NodeLocation`

        :param dailybackup:   create daily backups for the node (optional)
        :type dailybackup:    ``bool``

        :param managed:   provide managed support for the node (optional)
        :type managed:    ``bool``

        :param billingcycle:   billing cycle (hourly / monthly) (optional)
        :type billingcycle:    ``str``

        :param generated_password:   server generated password (optional)
        :type generated_password:    ``str``

        :param create_command_id:   creation task command ID (optional)
        :type create_command_id:    ``int``

        :param poweronaftercreate:   power on the node after create (optional)
        :type poweronaftercreate:    ``bool``

        :return: The node.
        :rtype: :class:`.Node`
        """
        extra = {}
        if location:
            extra["location"] = location
        if dailybackup is not None:
            extra["dailybackup"] = dailybackup
        if managed is not None:
            extra["managed"] = managed
        if billingcycle is not None:
            extra["billingcycle"] = billingcycle
        if generated_password is not None:
            extra["generated_password"] = generated_password
        if create_command_id is not None:
            extra["create_command_id"] = create_command_id
        if poweronaftercreate is not None:
            extra["poweronaftercreate"] = poweronaftercreate
        return Node(
            id=id,
            name=name,
            state=state,
            public_ips=public_ips,
            private_ips=private_ips,
            driver=self,
            size=size,
            image=image,
            created_at=created_at,
            extra=extra,
        )

    def _copy_dict(self, keys, d):
        extra = {}
        for key in keys:
            extra[key] = d[key]
        return extra

    def _update_node_from_server_info(self, node, server):
        node.id = server["id"]
        node.name = server["name"]
        if server["power"] == "on":
            node.state = NodeState.RUNNING
        else:
            node.state = NodeState.STOPPED
        for network in server.get("networks", []):
            if network.get("network").startswith("wan-"):
                node.public_ips += network.get("ips", [])
            else:
                node.private_ips += network.get("ips", [])
        billing = server.get("billing", node.extra.get("billingcycle")).lower()
        if billing == self.EX_BILLINGCYCLE_HOURLY:
            node.extra["billingcycle"] = self.EX_BILLINGCYCLE_HOURLY
            node.extra["priceOn"] = server.get("priceHourlyOn")
            node.extra["priceOff"] = server.get("priceHourlyOff")
        else:
            node.extra["billingcycle"] = self.EX_BILLINGCYCLE_MONTHLY
            node.extra["priceOn"] = server.get("priceMonthlyOn")
            node.extra["priceOff"] = server.get("priceMonthlyOn")
        node.extra["location"] = self.ex_get_location(server["datacenter"])
        node.extra["dailybackup"] = server.get("backup") == "1"
        node.extra["managed"] = server.get("managed") == "1"
        return node
