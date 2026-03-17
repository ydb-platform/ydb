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
A driver for cloudscale.ch.
"""

import json

from libcloud.utils.py3 import httplib
from libcloud.common.base import JsonResponse, ConnectionKey
from libcloud.common.types import InvalidCredsError
from libcloud.compute.base import Node, NodeSize, NodeImage, NodeDriver
from libcloud.compute.types import Provider, NodeState


class CloudscaleResponse(JsonResponse):
    valid_response_codes = [
        httplib.OK,
        httplib.ACCEPTED,
        httplib.CREATED,
        httplib.NO_CONTENT,
    ]

    def parse_error(self):
        body = self.parse_body()
        if self.status == httplib.UNAUTHORIZED:
            raise InvalidCredsError(body["detail"])
        else:
            # We are taking the first issue here. There might be multiple ones,
            # but that doesn't really matter. It's nicer if the error is just
            # one error (because it's a Python API and there's only one
            # exception.
            return next(iter(body.values()))

    def success(self):
        return self.status in self.valid_response_codes


class CloudscaleConnection(ConnectionKey):
    """
    Connection class for the cloudscale.ch driver.
    """

    host = "api.cloudscale.ch"
    responseCls = CloudscaleResponse

    def add_default_headers(self, headers):
        """
        Add headers that are necessary for every request

        This method adds ``token`` to the request.
        """
        headers["Authorization"] = "Bearer %s" % (self.key)
        headers["Content-Type"] = "application/json"
        return headers


class CloudscaleNodeDriver(NodeDriver):
    """
    Cloudscale's node driver.
    """

    connectionCls = CloudscaleConnection

    type = Provider.CLOUDSCALE
    name = "Cloudscale"
    website = "https://www.cloudscale.ch"

    NODE_STATE_MAP = dict(
        changing=NodeState.PENDING,
        running=NodeState.RUNNING,
        stopped=NodeState.STOPPED,
        paused=NodeState.PAUSED,
    )

    def __init__(self, key, **kwargs):
        super().__init__(key, **kwargs)

    def list_nodes(self):
        """
        List all your existing compute nodes.
        """
        return self._list_resources("/v1/servers", self._to_node)

    def list_sizes(self):
        """
        Lists all available sizes. On cloudscale these are known as flavors.
        """
        return self._list_resources("/v1/flavors", self._to_size)

    def list_images(self):
        """
        List all images.

        Images are identified by slugs on cloudscale.ch. This means that minor
        version upgrades (e.g. Ubuntu 16.04.1 to Ubuntu 16.04.2) will be
        possible within the same id ``ubuntu-16.04``.
        """
        return self._list_resources("/v1/images", self._to_image)

    def create_node(self, name, size, image, location=None, ex_create_attr=None):
        """
        Create a node.

        The `ex_create_attr` parameter can include the following dictionary
        key and value pairs:

        * `ssh_keys`: ``list`` of ``str`` ssh public keys
        * `volume_size_gb`: ``int`` defaults to 10.
        * `bulk_volume_size_gb`: defaults to None.
        * `use_public_network`: ``bool`` defaults to True
        * `use_private_network`: ``bool`` defaults to False
        * `use_ipv6`: ``bool`` defaults to True
        * `anti_affinity_with`: ``uuid`` of a server to create an anti-affinity
          group with that server or add it to the same group as that server.
        * `user_data`: ``str`` for optional cloud-config data

        :keyword ex_create_attr: A dictionary of optional attributes for
                                 droplet creation
        :type ex_create_attr: ``dict``

        :return: The newly created node.
        :rtype: :class:`Node`
        """
        ex_create_attr = ex_create_attr or {}
        attr = dict(ex_create_attr)
        attr.update(
            name=name,
            image=image.id,
            flavor=size.id,
        )
        result = self.connection.request("/v1/servers", data=json.dumps(attr), method="POST")
        return self._to_node(result.object)

    def reboot_node(self, node):
        """
        Reboot a node. It's also possible to use ``node.reboot()``.
        """
        return self._action(node, "reboot")

    def start_node(self, node):
        """
        Start a node. This is only possible if the node is stopped.
        """
        return self._action(node, "start")

    def stop_node(self, node):
        """
        Stop a specific node. Similar to ``shutdown -h now``. This is only
        possible if the node is running.
        """
        return self._action(node, "stop")

    def ex_start_node(self, node):
        # NOTE: This method is here for backward compatibility reasons after
        # this method was promoted to be part of the standard compute API in
        # Libcloud v2.7.0
        return self.start_node(node=node)

    def ex_stop_node(self, node):
        # NOTE: This method is here for backward compatibility reasons after
        # this method was promoted to be part of the standard compute API in
        # Libcloud v2.7.0
        return self.stop_node(node=node)

    def ex_node_by_uuid(self, uuid):
        """
        :param str ex_user_data: A valid uuid that references your existing
            cloudscale.ch server.
        :type       ex_user_data:  ``str``

        :return: The server node you asked for.
        :rtype: :class:`Node`
        """
        res = self.connection.request(self._get_server_url(uuid))
        return self._to_node(res.object)

    def destroy_node(self, node):
        """
        Delete a node. It's also possible to use ``node.destroy()``.
        This will irreversibly delete the cloudscale.ch server and all its
        volumes. So please be cautious.
        """
        res = self.connection.request(self._get_server_url(node.id), method="DELETE")
        return res.status == httplib.NO_CONTENT

    def _get_server_url(self, uuid):
        return "/v1/servers/%s" % uuid

    def _action(self, node, action_name):
        response = self.connection.request(
            self._get_server_url(node.id) + "/" + action_name, method="POST"
        )
        return response.status == httplib.OK

    def _list_resources(self, url, tranform_func):
        data = self.connection.request(url, method="GET").object
        return [tranform_func(obj) for obj in data]

    def _to_node(self, data):
        state = self.NODE_STATE_MAP.get(data["status"], NodeState.UNKNOWN)
        extra_keys_exclude = ["uuid", "name", "status", "flavor", "image"]
        extra = {}
        for k, v in data.items():
            if k not in extra_keys_exclude:
                extra[k] = v

        public_ips = []
        private_ips = []
        for interface in data["interfaces"]:
            if interface["type"] == "public":
                ips = public_ips
            else:
                ips = private_ips
            for address_obj in interface["addresses"]:
                ips.append(address_obj["address"])

        return Node(
            id=data["uuid"],
            name=data["name"],
            state=state,
            public_ips=public_ips,
            private_ips=private_ips,
            extra=extra,
            driver=self,
            image=self._to_image(data["image"]),
            size=self._to_size(data["flavor"]),
        )

    def _to_size(self, data):
        extra = {"vcpu_count": data["vcpu_count"]}
        ram = data["memory_gb"] * 1024

        return NodeSize(
            id=data["slug"],
            name=data["name"],
            ram=ram,
            disk=10,
            bandwidth=0,
            price=0,
            extra=extra,
            driver=self,
        )

    def _to_image(self, data):
        extra = {"operating_system": data["operating_system"]}
        return NodeImage(id=data["slug"], name=data["name"], extra=extra, driver=self)
