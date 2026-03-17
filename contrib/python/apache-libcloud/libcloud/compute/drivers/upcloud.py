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
Upcloud node driver
"""
import json
import base64

from libcloud.utils.py3 import b, httplib
from libcloud.common.base import JsonResponse, ConnectionUserAndKey
from libcloud.common.types import InvalidCredsError
from libcloud.compute.base import Node, NodeSize, NodeImage, NodeState, NodeDriver, NodeLocation
from libcloud.compute.types import Provider
from libcloud.common.upcloud import (
    PlanPrice,
    UpcloudNodeDestroyer,
    UpcloudNodeOperations,
    UpcloudCreateNodeRequestBody,
)


class UpcloudResponse(JsonResponse):
    """
    Response class for UpcloudDriver
    """

    def success(self):
        if self.status == httplib.NO_CONTENT:
            return True
        return super().success()

    def parse_error(self):
        data = self.parse_body()
        if self.status == httplib.UNAUTHORIZED:
            raise InvalidCredsError(value=data["error"]["error_message"])
        return data


class UpcloudConnection(ConnectionUserAndKey):
    """
    Connection class for UpcloudDriver
    """

    host = "api.upcloud.com"
    responseCls = UpcloudResponse

    def add_default_headers(self, headers):
        """Adds headers that are needed for all requests"""
        headers["Authorization"] = self._basic_auth()
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"
        return headers

    def _basic_auth(self):
        """Constructs basic auth header content string"""
        credentials = b("{}:{}".format(self.user_id, self.key))
        credentials = base64.b64encode(credentials)
        return "Basic {}".format(credentials.decode("ascii"))


class UpcloudDriver(NodeDriver):
    """
    Upcloud node driver

    :keyword    username: Username required for authentication
    :type       username: ``str``

    :keyword    password: Password required for authentication
    :type       password: ``str``
    """

    type = Provider.UPCLOUD
    name = "Upcloud"
    website = "https://www.upcloud.com"
    connectionCls = UpcloudConnection
    features = {"create_node": ["ssh_key", "generates_password"]}

    NODE_STATE_MAP = {
        "started": NodeState.RUNNING,
        "stopped": NodeState.STOPPED,
        "maintenance": NodeState.RECONFIGURING,
        "error": NodeState.ERROR,
    }

    def __init__(self, username, password, **kwargs):
        super().__init__(key=username, secret=password, **kwargs)

    def list_locations(self):
        """
        List available locations for deployment

        :rtype: ``list`` of :class:`NodeLocation`
        """
        response = self.connection.request("1.2/zone")
        return self._to_node_locations(response.object["zones"]["zone"])

    def list_sizes(self, location=None):
        """
        List available plans

        :param location: Location of the deployment. Price depends on
        location. lf location is not given or price not found for
        location, price will be None (optional)
        :type location: :class:`.NodeLocation`

        :rtype: ``list`` of :class:`NodeSize`
        """
        prices_response = self.connection.request("1.2/price")
        response = self.connection.request("1.2/plan")
        return self._to_node_sizes(
            response.object["plans"]["plan"],
            prices_response.object["prices"]["zone"],
            location,
        )

    def list_images(self):
        """
        List available distributions.

        :rtype: ``list`` of :class:`NodeImage`
        """
        response = self.connection.request("1.2/storage/template")
        obj = response.object
        response = self.connection.request("1.2/storage/cdrom")
        storage = response.object["storages"]["storage"]
        obj["storages"]["storage"].extend(storage)
        return self._to_node_images(obj["storages"]["storage"])

    def create_node(
        self,
        name,
        size,
        image,
        location,
        auth=None,
        ex_hostname="localhost",
        ex_username="root",
    ):
        """
        Creates instance to upcloud.

        If auth is not given then password will be generated.

        :param name:   String with a name for this new node (required)
        :type name:   ``str``

        :param size:   The size of resources allocated to this node.
                            (required)
        :type size:   :class:`.NodeSize`

        :param image:  OS Image to boot on node. (required)
        :type image:  :class:`.NodeImage`

        :param location: Which data center to create a node in. If empty,
                              undefined behavior will be selected. (optional)
        :type location: :class:`.NodeLocation`

        :param auth:   Initial authentication information for the node
                            (optional)
        :type auth:   :class:`.NodeAuthSSHKey`

        :param ex_hostname: Hostname. Default is 'localhost'. (optional)
        :type ex_hostname: ``str``

        :param ex_username: User's username, which is created.
                            Default is 'root'. (optional)
        :type ex_username: ``str``

        :return: The newly created node.
        :rtype: :class:`.Node`
        """
        body = UpcloudCreateNodeRequestBody(
            name=name,
            size=size,
            image=image,
            location=location,
            auth=auth,
            ex_hostname=ex_hostname,
            ex_username=ex_username,
        )
        response = self.connection.request("1.2/server", method="POST", data=body.to_json())
        server = response.object["server"]
        # Upcloud server's are in maintenance state when going
        # from state to other, it is safe to assume STARTING state
        return self._to_node(server, state=NodeState.STARTING)

    def list_nodes(self):
        """
        List nodes

        :return: List of node objects
        :rtype: ``list`` of :class:`Node`
        """
        servers = []
        for nid in self._node_ids():
            response = self.connection.request("1.2/server/{}".format(nid))
            servers.append(response.object["server"])
        return self._to_nodes(servers)

    def reboot_node(self, node):
        """
        Reboot the given node

        :param      node: the node to reboot
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        body = {"restart_server": {"stop_type": "hard"}}
        self.connection.request(
            "1.2/server/{}/restart".format(node.id),
            method="POST",
            data=json.dumps(body),
        )
        return True

    def destroy_node(self, node):
        """
        Destroy the given node

        The disk resources, attached to node,  will not be removed.

        :param       node: the node to destroy
        :type        node: :class:`Node`

        :rtype: ``bool``
        """

        operations = UpcloudNodeOperations(self.connection)
        destroyer = UpcloudNodeDestroyer(operations)
        return destroyer.destroy_node(node.id)

    def _node_ids(self):
        """
        Returns list of server uids currently on upcloud
        """
        response = self.connection.request("1.2/server")
        servers = response.object["servers"]["server"]
        return [server["uuid"] for server in servers]

    def _to_nodes(self, servers):
        return [self._to_node(server) for server in servers]

    def _to_node(self, server, state=None):
        ip_addresses = server["ip_addresses"]["ip_address"]
        public_ips = [ip["address"] for ip in ip_addresses if ip["access"] == "public"]
        private_ips = [ip["address"] for ip in ip_addresses if ip["access"] == "private"]

        extra = {"vnc_password": server["vnc_password"]}
        if "password" in server:
            extra["password"] = server["password"]
        return Node(
            id=server["uuid"],
            name=server["title"],
            state=state or self.NODE_STATE_MAP[server["state"]],
            public_ips=public_ips,
            private_ips=private_ips,
            driver=self,
            extra=extra,
        )

    def _to_node_locations(self, zones):
        return [self._construct_node_location(zone) for zone in zones]

    def _construct_node_location(self, zone):
        return NodeLocation(
            id=zone["id"],
            name=zone["description"],
            country=self._parse_country(zone["id"]),
            driver=self,
        )

    def _parse_country(self, zone_id):
        """Parses the country information out of zone_id.
        Zone_id format [country]_[city][number], like fi_hel1"""
        return zone_id.split("-")[0].upper()

    def _to_node_sizes(self, plans, prices, location):
        plan_price = PlanPrice(prices)
        return [self._to_node_size(plan, plan_price, location) for plan in plans]

    def _to_node_size(self, plan, plan_price, location):
        extra = self._copy_dict(("core_number", "storage_tier"), plan)
        return NodeSize(
            id=plan["name"],
            name=plan["name"],
            ram=plan["memory_amount"],
            disk=plan["storage_size"],
            bandwidth=plan["public_traffic_out"],
            price=plan_price.get_price(plan["name"], location),
            driver=self,
            extra=extra,
        )

    def _to_node_images(self, images):
        return [self._construct_node_image(image) for image in images]

    def _construct_node_image(self, image):
        extra = self._copy_dict(("access", "license", "size", "state", "type"), image)
        return NodeImage(id=image["uuid"], name=image["title"], driver=self, extra=extra)

    def _copy_dict(self, keys, d):
        extra = {}
        for key in keys:
            extra[key] = d[key]
        return extra
