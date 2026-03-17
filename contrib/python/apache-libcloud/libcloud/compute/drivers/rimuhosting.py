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
RimuHosting Driver
"""
try:
    import simplejson as json
except ImportError:
    import json

from libcloud.common.base import JsonResponse, ConnectionKey
from libcloud.common.types import InvalidCredsError
from libcloud.compute.base import Node, NodeSize, NodeImage, NodeDriver, NodeLocation
from libcloud.compute.types import Provider, NodeState

API_CONTEXT = "/r"
API_HOST = "rimuhosting.com"


class RimuHostingException(Exception):
    """
    Exception class for RimuHosting driver
    """

    def __str__(self):
        # pylint: disable=unsubscriptable-object
        return self.args[0]

    def __repr__(self):
        # pylint: disable=unsubscriptable-object
        return "<RimuHostingException '%s'>" % (self.args[0])


class RimuHostingResponse(JsonResponse):
    """
    Response Class for RimuHosting driver
    """

    def success(self):
        if self.status == 403:
            raise InvalidCredsError()
        return True

    def parse_body(self):
        try:
            js = super().parse_body()
            keys = list(js.keys())
            if js[keys[0]]["response_type"] == "ERROR":
                raise RimuHostingException(js[keys[0]]["human_readable_message"])
            return js[keys[0]]
        except KeyError:
            raise RimuHostingException("Could not parse body: %s" % (self.body))


class RimuHostingConnection(ConnectionKey):
    """
    Connection class for the RimuHosting driver
    """

    api_context = API_CONTEXT
    host = API_HOST
    port = 443
    responseCls = RimuHostingResponse

    def __init__(self, key, secure=True, retry_delay=None, backoff=None, timeout=None):
        # override __init__ so that we can set secure of False for testing
        ConnectionKey.__init__(
            self, key, secure, timeout=timeout, retry_delay=retry_delay, backoff=backoff
        )

    def add_default_headers(self, headers):
        # We want JSON back from the server. Could be application/xml
        # (but JSON is better).
        headers["Accept"] = "application/json"
        # Must encode all data as json, or override this header.
        headers["Content-Type"] = "application/json"

        headers["Authorization"] = "rimuhosting apikey=%s" % (self.key)
        return headers

    def request(self, action, params=None, data="", headers=None, method="GET"):
        if not headers:
            headers = {}
        if not params:
            params = {}
        # Override this method to prepend the api_context
        return ConnectionKey.request(self, self.api_context + action, params, data, headers, method)


class RimuHostingNodeDriver(NodeDriver):
    """
    RimuHosting node driver
    """

    type = Provider.RIMUHOSTING
    name = "RimuHosting"
    website = "http://rimuhosting.com/"
    connectionCls = RimuHostingConnection
    features = {"create_node": ["password"]}

    def __init__(self, key, host=API_HOST, port=443, api_context=API_CONTEXT, secure=True):
        """
        :param    key: API key (required)
        :type     key: ``str``

        :param    host: hostname for connection
        :type     host: ``str``

        :param    port: Override port used for connections.
        :type     port: ``int``

        :param    api_context: Optional API context.
        :type     api_context: ``str``

        :param    secure: Whether to use HTTPS or HTTP.
        :type     secure: ``bool``

        :rtype: ``None``
        """
        # Pass in some extra vars so that
        self.key = key
        self.secure = secure
        self.connection = self.connectionCls(key, secure)
        self.connection.host = host
        self.connection.api_context = api_context
        self.connection.port = port
        self.connection.driver = self
        self.connection.connect()

    def _order_uri(self, node, resource):
        # Returns the order uri with its resource appended.
        return "/orders/{}/{}".format(node.id, resource)

    # TODO: Get the node state.
    def _to_node(self, order):
        n = Node(
            id=order["slug"],
            name=order["domain_name"],
            state=NodeState.RUNNING,
            public_ips=(
                [order["allocated_ips"]["primary_ip"]] + order["allocated_ips"]["secondary_ips"]
            ),
            private_ips=[],
            driver=self.connection.driver,
            extra={
                "order_oid": order["order_oid"],
                "monthly_recurring_fee": order.get("billing_info").get("monthly_recurring_fee"),
            },
        )
        return n

    def _to_size(self, plan):
        return NodeSize(
            id=plan["pricing_plan_code"],
            name=plan["pricing_plan_description"],
            ram=plan["minimum_memory_mb"],
            disk=plan["minimum_disk_gb"],
            bandwidth=plan["minimum_data_transfer_allowance_gb"],
            price=plan["monthly_recurring_amt"]["amt_usd"],
            driver=self.connection.driver,
        )

    def _to_image(self, image):
        return NodeImage(
            id=image["distro_code"],
            name=image["distro_description"],
            driver=self.connection.driver,
        )

    def list_sizes(self, location=None):
        # Returns a list of sizes (aka plans)
        # Get plans. Note this is really just for libcloud.
        # We are happy with any size.
        if location is None:
            location = ""
        else:
            location = ";dc_location=%s" % (location.id)

        res = self.connection.request("/pricing-plans;server-type=VPS%s" % (location)).object
        return list(map(lambda x: self._to_size(x), res["pricing_plan_infos"]))

    def list_nodes(self):
        # Returns a list of Nodes
        # Will only include active ones.
        res = self.connection.request("/orders;include_inactive=N").object
        return list(map(lambda x: self._to_node(x), res["about_orders"]))

    def list_images(self, location=None):
        # Get all base images.
        # TODO: add other image sources. (Such as a backup of a VPS)
        # All Images are available for use at all locations
        res = self.connection.request("/distributions").object
        return list(map(lambda x: self._to_image(x), res["distro_infos"]))

    def reboot_node(self, node):
        # Reboot
        # PUT the state of RESTARTING to restart a VPS.
        # All data is encoded as JSON
        data = {"reboot_request": {"running_state": "RESTARTING"}}
        uri = self._order_uri(node, "vps/running-state")
        self.connection.request(uri, data=json.dumps(data), method="PUT")
        # XXX check that the response was actually successful
        return True

    def destroy_node(self, node):
        # Shutdown a VPS.
        uri = self._order_uri(node, "vps")
        self.connection.request(uri, method="DELETE")
        # XXX check that the response was actually successful
        return True

    def create_node(
        self,
        name,
        size,
        image,
        auth=None,
        ex_billing_oid=None,
        ex_host_server_oid=None,
        ex_vps_order_oid_to_clone=None,
        ex_num_ips=1,
        ex_extra_ip_reason=None,
        ex_memory_mb=None,
        ex_disk_space_mb=None,
        ex_disk_space_2_mb=None,
        ex_control_panel=None,
    ):
        """Creates a RimuHosting instance

        @inherits: :class:`NodeDriver.create_node`

        :keyword    name: Must be a FQDN. e.g example.com.
        :type       name: ``str``

        :keyword    ex_billing_oid: If not set,
                                    a billing method is automatically picked.
        :type       ex_billing_oid: ``str``

        :keyword    ex_host_server_oid: The host server to set the VPS up on.
        :type       ex_host_server_oid: ``str``

        :keyword    ex_vps_order_oid_to_clone: Clone another VPS to use as
                                                the image for the new VPS.
        :type       ex_vps_order_oid_to_clone: ``str``

        :keyword    ex_num_ips: Number of IPs to allocate. Defaults to 1.
        :type       ex_num_ips: ``int``

        :keyword    ex_extra_ip_reason: Reason for needing the extra IPs.
        :type       ex_extra_ip_reason: ``str``

        :keyword    ex_memory_mb: Memory to allocate to the VPS.
        :type       ex_memory_mb: ``int``

        :keyword    ex_disk_space_mb: Diskspace to allocate to the VPS.
            Defaults to 4096 (4GB).
        :type       ex_disk_space_mb: ``int``

        :keyword    ex_disk_space_2_mb: Secondary disk size allocation.
                                        Disabled by default.
        :type       ex_disk_space_2_mb: ``int``

        :keyword    ex_control_panel: Control panel to install on the VPS.
        :type       ex_control_panel: ``str``
        """
        # Note we don't do much error checking in this because we
        # expect the API to error out if there is a problem.
        data = {
            "instantiation_options": {"domain_name": name, "distro": image.id},
            "pricing_plan_code": size.id,
            "vps_parameters": {},
        }

        if ex_control_panel:
            data["instantiation_options"]["control_panel"] = ex_control_panel

        auth = self._get_and_check_auth(auth)
        data["instantiation_options"]["password"] = auth.password

        if ex_billing_oid:
            # TODO check for valid oid.
            data["billing_oid"] = ex_billing_oid

        if ex_host_server_oid:
            data["host_server_oid"] = ex_host_server_oid

        if ex_vps_order_oid_to_clone:
            data["vps_order_oid_to_clone"] = ex_vps_order_oid_to_clone

        if ex_num_ips and int(ex_num_ips) > 1:
            if not ex_extra_ip_reason:
                raise RimuHostingException("Need an reason for having an extra IP")
            else:
                if "ip_request" not in data:
                    data["ip_request"] = {}
                data["ip_request"]["num_ips"] = int("ex_num_ips")
                data["ip_request"]["extra_ip_reason"] = ex_extra_ip_reason

        if ex_memory_mb:
            data["vps_parameters"]["memory_mb"] = ex_memory_mb

        if ex_disk_space_mb:
            data["vps_parameters"]["disk_space_mb"] = ex_disk_space_mb

        if ex_disk_space_2_mb:
            data["vps_parameters"]["disk_space_2_mb"] = ex_disk_space_2_mb

        # Don't send empty 'vps_parameters' attribute
        if not data["vps_parameters"]:
            del data["vps_parameters"]

        res = self.connection.request(
            "/orders/new-vps", method="POST", data=json.dumps({"new-vps": data})
        ).object
        node = self._to_node(res["about_order"])
        node.extra["password"] = res["new_order_request"]["instantiation_options"]["password"]
        return node

    def list_locations(self):
        return [
            NodeLocation("DCAUCKLAND", "RimuHosting Auckland", "NZ", self),
            NodeLocation("DCDALLAS", "RimuHosting Dallas", "US", self),
            NodeLocation("DCLONDON", "RimuHosting London", "GB", self),
            NodeLocation("DCSYDNEY", "RimuHosting Sydney", "AU", self),
        ]
