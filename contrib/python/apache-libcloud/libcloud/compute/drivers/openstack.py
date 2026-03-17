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
OpenStack driver
"""

import base64
import warnings

from libcloud.pricing import get_size_price
from libcloud.utils.py3 import ET, b, next, httplib, parse_qs, urlparse
from libcloud.utils.xml import findall
from libcloud.compute.base import (
    Node,
    KeyPair,
    NodeSize,
    NodeImage,
    UuidMixin,
    NodeDriver,
    NodeLocation,
    StorageVolume,
    VolumeSnapshot,
    NodeImageMember,
)
from libcloud.compute.types import (
    Type,
    Provider,
    NodeState,
    LibcloudError,
    StorageVolumeState,
    VolumeSnapshotState,
)
from libcloud.utils.iso8601 import parse_date
from libcloud.common.openstack import (
    OpenStackResponse,
    OpenStackException,
    OpenStackDriverMixin,
    OpenStackBaseConnection,
)
from libcloud.utils.networking import is_public_subnet
from libcloud.common.exceptions import BaseHTTPError

try:
    import simplejson as json
except ImportError:
    import json


__all__ = [
    "OpenStack_1_0_Response",
    "OpenStack_1_0_Connection",
    "OpenStack_1_0_NodeDriver",
    "OpenStack_1_0_SharedIpGroup",
    "OpenStack_1_0_NodeIpAddresses",
    "OpenStack_1_1_Response",
    "OpenStack_1_1_Connection",
    "OpenStack_1_1_NodeDriver",
    "OpenStack_1_1_FloatingIpPool",
    "OpenStack_2_FloatingIpPool",
    "OpenStack_1_1_FloatingIpAddress",
    "OpenStack_2_FloatingIpAddress",
    "OpenStack_2_PortInterfaceState",
    "OpenStack_2_PortInterface",
    "OpenStackNodeDriver",
]

ATOM_NAMESPACE = "http://www.w3.org/2005/Atom"

DEFAULT_API_VERSION = "1.1"

PAGINATION_LIMIT = 1000


class OpenStackComputeConnection(OpenStackBaseConnection):
    # default config for http://devstack.org/
    service_type = "compute"
    service_name = "nova"
    service_region = "RegionOne"


class OpenStackImageConnection(OpenStackBaseConnection):
    service_type = "image"
    service_name = "glance"
    service_region = "RegionOne"


class OpenStackNetworkConnection(OpenStackBaseConnection):
    service_type = "network"
    service_name = "neutron"
    service_region = "RegionOne"


class OpenStackVolumeV2Connection(OpenStackBaseConnection):
    service_type = "volumev2"
    service_name = "cinderv2"
    service_region = "RegionOne"


class OpenStackVolumeV3Connection(OpenStackBaseConnection):
    service_type = "volumev3"
    service_name = "cinderv3"
    service_region = "RegionOne"


class OpenStackNodeDriver(NodeDriver, OpenStackDriverMixin):
    """
    Base OpenStack node driver. Should not be used directly.
    """

    api_name = "openstack"
    name = "OpenStack"
    website = "http://openstack.org/"

    NODE_STATE_MAP = {
        "BUILD": NodeState.PENDING,
        "REBUILD": NodeState.PENDING,
        "ACTIVE": NodeState.RUNNING,
        "SUSPENDED": NodeState.SUSPENDED,
        "SHUTOFF": NodeState.STOPPED,
        "DELETED": NodeState.TERMINATED,
        "QUEUE_RESIZE": NodeState.PENDING,
        "PREP_RESIZE": NodeState.PENDING,
        "VERIFY_RESIZE": NodeState.RUNNING,
        "PASSWORD": NodeState.PENDING,
        "RESCUE": NodeState.PENDING,
        "REBOOT": NodeState.REBOOTING,
        "RESIZE": NodeState.RECONFIGURING,
        "HARD_REBOOT": NodeState.REBOOTING,
        "SHARE_IP": NodeState.PENDING,
        "SHARE_IP_NO_CONFIG": NodeState.PENDING,
        "DELETE_IP": NodeState.PENDING,
        "ERROR": NodeState.ERROR,
        "UNKNOWN": NodeState.UNKNOWN,
    }

    # http://developer.openstack.org/api-ref-blockstorage-v2.html#volumes-v2
    VOLUME_STATE_MAP = {
        "creating": StorageVolumeState.CREATING,
        "available": StorageVolumeState.AVAILABLE,
        "attaching": StorageVolumeState.ATTACHING,
        "in-use": StorageVolumeState.INUSE,
        "deleting": StorageVolumeState.DELETING,
        "error": StorageVolumeState.ERROR,
        "error_deleting": StorageVolumeState.ERROR,
        "backing-up": StorageVolumeState.BACKUP,
        "restoring-backup": StorageVolumeState.BACKUP,
        "error_restoring": StorageVolumeState.ERROR,
        "error_extending": StorageVolumeState.ERROR,
    }

    # http://developer.openstack.org/api-ref-blockstorage-v2.html#ext-backups-v2
    SNAPSHOT_STATE_MAP = {
        "creating": VolumeSnapshotState.CREATING,
        "available": VolumeSnapshotState.AVAILABLE,
        "deleting": VolumeSnapshotState.DELETING,
        "error": VolumeSnapshotState.ERROR,
        "restoring": VolumeSnapshotState.RESTORING,
        "error_restoring": VolumeSnapshotState.ERROR,
    }

    def __new__(
        cls,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        api_version=DEFAULT_API_VERSION,
        **kwargs,
    ):
        if cls is OpenStackNodeDriver:
            if api_version == "1.0":
                cls = OpenStack_1_0_NodeDriver
            elif api_version == "1.1":
                cls = OpenStack_1_1_NodeDriver
            elif api_version in ["2.0", "2.1", "2.2"]:
                cls = OpenStack_2_NodeDriver
            else:
                raise NotImplementedError(
                    "No OpenStackNodeDriver found for API version %s" % (api_version)
                )
        return super().__new__(cls)

    def __init__(self, *args, **kwargs):
        OpenStackDriverMixin.__init__(self, **kwargs)
        super().__init__(*args, **kwargs)

    @staticmethod
    def _paginated_request(url, obj, connection, params=None):
        """
        Perform multiple calls in order to have a full list of elements when
        the API responses are paginated.

        :param url: API endpoint
        :type url: ``str``

        :param obj: Result object key
        :type obj: ``str``

        :param connection: The API connection to use to perform the request
        :type connection: ``obj``

        :param params: Any request parameters
        :type params: ``dict``

        :return: ``list`` of API response objects
        :rtype: ``list``
        """
        params = params or {}
        objects = list()
        loop_count = 0
        while True:
            data = connection.request(url, params=params)
            values = data.object.get(obj, list())
            objects.extend(values)
            links = data.object.get("%s_links" % obj, list())
            next_links = [n for n in links if n["rel"] == "next"]
            if next_links:
                next_link = next_links[0]
                query = urlparse.urlparse(next_link["href"])
                # The query[4] references the query parameters from the url
                params.update(parse_qs(query[4]))
            else:
                break

            # Prevent the pagination from looping indefinitely in case
            # the API returns a loop for some reason.
            loop_count += 1
            if loop_count > PAGINATION_LIMIT:
                raise OpenStackException(
                    "Pagination limit reached for %s, the limit is %d. "
                    "This might indicate that your API is returning a "
                    "looping next target for pagination!" % (url, PAGINATION_LIMIT),
                    None,
                )
        return {obj: objects}

    def _paginated_request_next(self, path, request_method, response_key):
        """
        Perform multiple calls and retrieve all the elements for a paginated
        response.

        This method utilizes "next" attribute in the response object.

        It also includes an infinite loop protection (if the "next" value
        matches the current path, it will abort).

        :param request_method: Method to call which will send the request and
                               return a response. This method will get passed
                               in "path" as a first argument.

        :param response_key: Key in the response object dictionary which
                             contains actual objects we are interested in.
        """
        iteration_count = 0

        result = []
        while path:
            response = request_method(path)
            items = response.object.get(response_key, []) or []
            result.extend(items)

            # Retrieve next path
            next_path = response.object.get("next", None)

            if next_path == path:
                # Likely an infinite loop since the next path matches the
                # current one
                break

            if iteration_count > PAGINATION_LIMIT:
                # We have iterated over PAGINATION_LIMIT pages, likely an
                # API returned an invalid response
                raise OpenStackException(
                    "Pagination limit reached for %s, the limit is %d. "
                    "This might indicate that your API is returning a "
                    "looping next target for pagination!" % (path, PAGINATION_LIMIT),
                    None,
                )

            path = next_path
            iteration_count += 1

        return result

    def destroy_node(self, node):
        uri = "/servers/%s" % (node.id)
        resp = self.connection.request(uri, method="DELETE")
        # The OpenStack and Rackspace documentation both say this API will
        # return a 204, but in-fact, everyone everywhere agrees it actually
        # returns a 202, so we are going to accept either, and someday,
        # someone will fix either the implementation or the documentation to
        # agree.
        return resp.status in (httplib.NO_CONTENT, httplib.ACCEPTED)

    def reboot_node(self, node):
        # pylint: disable=no-member
        return self._reboot_node(node, reboot_type="HARD")

    def start_node(self, node):
        # pylint: disable=no-member
        return self._post_simple_node_action(node, "os-start")

    def stop_node(self, node):
        # pylint: disable=no-member
        return self._post_simple_node_action(node, "os-stop")

    def list_nodes(self, ex_all_tenants=False):
        """
        List the nodes in a tenant

        :param ex_all_tenants: List nodes for all the tenants. Note: Your user
                               must have admin privileges for this
                               functionality to work.
        :type ex_all_tenants: ``bool``
        """
        params = {}
        if ex_all_tenants:
            params = {"all_tenants": 1}

        # pylint: disable=no-member
        return self._to_nodes(self.connection.request("/servers/detail", params=params).object)

    def create_volume(self, size, name, location=None, snapshot=None, ex_volume_type=None):
        """
        Create a new volume.

        :param size: Size of volume in gigabytes (required)
        :type size: ``int``

        :param name: Name of the volume to be created
        :type name: ``str``

        :param location: Which data center to create a volume in. If
                               empty, undefined behavior will be selected.
                               (optional)
        :type location: :class:`.NodeLocation`

        :param snapshot:  Snapshot from which to create the new
                          volume.  (optional)
        :type snapshot:  :class:`.VolumeSnapshot`

        :param ex_volume_type: What kind of volume to create.
                            (optional)
        :type ex_volume_type: ``str``

        :return: The newly created volume.
        :rtype: :class:`StorageVolume`
        """
        volume = {
            "display_name": name,
            "display_description": name,
            "size": size,
            "metadata": {"contents": name},
        }

        if ex_volume_type:
            volume["volume_type"] = ex_volume_type

        if location:
            volume["availability_zone"] = location

        if snapshot:
            volume["snapshot_id"] = snapshot.id

        resp = self.connection.request("/os-volumes", method="POST", data={"volume": volume})

        # pylint: disable=no-member
        return self._to_volume(resp.object)

    def destroy_volume(self, volume):
        return self.connection.request("/os-volumes/%s" % volume.id, method="DELETE").success()

    def attach_volume(self, node, volume, device="auto"):
        # when "auto" or None is provided for device, openstack will let
        # the guest OS pick the next available device (fi. /dev/vdb)
        if device == "auto":
            device = None
        return self.connection.request(
            "/servers/%s/os-volume_attachments" % node.id,
            method="POST",
            data={"volumeAttachment": {"volumeId": volume.id, "device": device}},
        ).success()

    def detach_volume(self, volume, ex_node=None):
        # when ex_node is not provided, volume is detached from all nodes
        failed_nodes = []
        for attachment in volume.extra["attachments"]:
            if not ex_node or ex_node.id in filter(
                None, (attachment.get("serverId"), attachment.get("server_id"))
            ):
                response = self.connection.request(
                    "/servers/%s/os-volume_attachments/%s"
                    % (
                        attachment.get("serverId") or attachment["server_id"],
                        attachment["id"],
                    ),
                    method="DELETE",
                )

                if not response.success():
                    failed_nodes.append(attachment.get("serverId") or attachment["server_id"])
        if failed_nodes:
            raise OpenStackException(
                "detach_volume failed for nodes with id: %s" % ", ".join(failed_nodes),
                500,
                self,
            )
        return True

    def list_volumes(self):
        # pylint: disable=no-member
        return self._to_volumes(self.connection.request("/os-volumes").object)

    def ex_get_volume(self, volumeId):
        # pylint: disable=no-member
        return self._to_volume(self.connection.request("/os-volumes/%s" % volumeId).object)

    def list_images(self, location=None, ex_only_active=True):
        """
        Lists all active images

        @inherits: :class:`NodeDriver.list_images`

        :param ex_only_active: True if list only active (optional)
        :type ex_only_active: ``bool``

        """
        # pylint: disable=no-member
        return self._to_images(self.connection.request("/images/detail").object, ex_only_active)

    def get_image(self, image_id):
        """
        Get an image based on an image_id

        @inherits: :class:`NodeDriver.get_image`

        :param image_id: Image identifier
        :type image_id: ``str``

        :return: A NodeImage object
        :rtype: :class:`NodeImage`

        """
        # pylint: disable=no-member
        return self._to_image(
            self.connection.request("/images/{}".format(image_id)).object["image"]
        )

    def list_sizes(self, location=None):
        # pylint: disable=no-member
        return self._to_sizes(self.connection.request("/flavors/detail").object)

    def list_locations(self):
        return [NodeLocation(0, "", "", self)]

    def _ex_connection_class_kwargs(self):
        return self.openstack_connection_kwargs()

    def ex_get_node_details(self, node_id):
        """
        Lists details of the specified server.

        :param       node_id: ID of the node which should be used
        :type        node_id: ``str``

        :rtype: :class:`Node`
        """
        # @TODO: Remove this if in 0.6
        if isinstance(node_id, Node):
            node_id = node_id.id

        uri = "/servers/%s" % (node_id)
        try:
            resp = self.connection.request(uri, method="GET")
        except BaseHTTPError as e:
            if e.code == httplib.NOT_FOUND:
                return None
            raise

        # pylint: disable=no-member
        return self._to_node_from_obj(resp.object)

    def ex_soft_reboot_node(self, node):
        """
        Soft reboots the specified server

        :param      node:  node
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        # pylint: disable=no-member
        return self._reboot_node(node, reboot_type="SOFT")

    def ex_hard_reboot_node(self, node):
        """
        Hard reboots the specified server

        :param      node:  node
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        # pylint: disable=no-member
        return self._reboot_node(node, reboot_type="HARD")


class OpenStackNodeSize(NodeSize):
    """
    NodeSize class for the OpenStack.org driver.

    Following the example of OpenNebula.org driver
    and following guidelines:
    https://issues.apache.org/jira/browse/LIBCLOUD-119
    """

    def __init__(
        self,
        id,
        name,
        ram,
        disk,
        bandwidth,
        price,
        driver,
        vcpus=None,
        ephemeral_disk=None,
        swap=None,
        extra=None,
    ):
        super().__init__(
            id=id,
            name=name,
            ram=ram,
            disk=disk,
            bandwidth=bandwidth,
            price=price,
            driver=driver,
        )
        self.vcpus = vcpus
        self.ephemeral_disk = ephemeral_disk
        self.swap = swap
        self.extra = extra

    def __repr__(self):
        return (
            "<OpenStackNodeSize: id=%s, name=%s, ram=%s, disk=%s, "
            "bandwidth=%s, price=%s, driver=%s, vcpus=%s,  ...>"
        ) % (
            self.id,
            self.name,
            self.ram,
            self.disk,
            self.bandwidth,
            self.price,
            self.driver.name,
            self.vcpus,
        )


class OpenStack_1_0_Response(OpenStackResponse):
    def __init__(self, *args, **kwargs):
        # done because of a circular reference from
        # NodeDriver -> Connection -> Response
        self.node_driver = OpenStack_1_0_NodeDriver
        super().__init__(*args, **kwargs)


class OpenStack_1_0_Connection(OpenStackComputeConnection):
    responseCls = OpenStack_1_0_Response
    default_content_type = "application/xml; charset=UTF-8"
    accept_format = "application/xml"
    XML_NAMESPACE = "http://docs.rackspacecloud.com/servers/api/v1.0"


class OpenStack_1_0_NodeDriver(OpenStackNodeDriver):
    """
    OpenStack node driver.

    Extra node attributes:
        - password: root password, available after create.
        - hostId: represents the host your cloud server runs on
        - imageId: id of image
        - flavorId: id of flavor
    """

    connectionCls = OpenStack_1_0_Connection
    type = Provider.OPENSTACK

    features = {"create_node": ["generates_password"]}

    def __init__(self, *args, **kwargs):
        self._ex_force_api_version = str(kwargs.pop("ex_force_api_version", None))
        self.XML_NAMESPACE = self.connectionCls.XML_NAMESPACE
        super().__init__(*args, **kwargs)

    def _to_images(self, object, ex_only_active):
        images = []
        for image in findall(object, "image", self.XML_NAMESPACE):
            if ex_only_active and image.get("status") != "ACTIVE":
                continue
            images.append(self._to_image(image))

        return images

    def _to_image(self, element):
        return NodeImage(
            id=element.get("id"),
            name=element.get("name"),
            driver=self.connection.driver,
            extra={
                "updated": element.get("updated"),
                "created": element.get("created"),
                "status": element.get("status"),
                "serverId": element.get("serverId"),
                "progress": element.get("progress"),
                "minDisk": element.get("minDisk"),
                "minRam": element.get("minRam"),
            },
        )

    def _change_password_or_name(self, node, name=None, password=None):
        uri = "/servers/%s" % (node.id)

        if not name:
            name = node.name

        body = {"xmlns": self.XML_NAMESPACE, "name": name}

        if password is not None:
            body["adminPass"] = password

        server_elm = ET.Element("server", body)

        resp = self.connection.request(uri, method="PUT", data=ET.tostring(server_elm))

        if resp.status == httplib.NO_CONTENT and password is not None:
            node.extra["password"] = password

        return resp.status == httplib.NO_CONTENT

    def create_node(
        self,
        name,
        size,
        image,
        ex_metadata=None,
        ex_files=None,
        ex_shared_ip_group=None,
        ex_shared_ip_group_id=None,
    ):
        """
        Create a new node

        @inherits: :class:`NodeDriver.create_node`

        :keyword    ex_metadata: Key/Value metadata to associate with a node
        :type       ex_metadata: ``dict``

        :keyword    ex_files:   File Path => File contents to create on
                                the node
        :type       ex_files:   ``dict``

        :keyword    ex_shared_ip_group_id: The server is launched into
            that shared IP group
        :type       ex_shared_ip_group_id: ``str``
        """
        attributes = {
            "xmlns": self.XML_NAMESPACE,
            "name": name,
            "imageId": str(image.id),
            "flavorId": str(size.id),
        }

        if ex_shared_ip_group:
            # Deprecate this. Be explicit and call the variable
            # ex_shared_ip_group_id since user needs to pass in the id, not the
            # name.
            warnings.warn(
                "ex_shared_ip_group argument is deprecated." " Please use ex_shared_ip_group_id"
            )

        if ex_shared_ip_group_id:
            attributes["sharedIpGroupId"] = ex_shared_ip_group_id

        server_elm = ET.Element("server", attributes)

        metadata_elm = self._metadata_to_xml(ex_metadata or {})
        if metadata_elm:
            server_elm.append(metadata_elm)

        files_elm = self._files_to_xml(ex_files or {})
        if files_elm:
            server_elm.append(files_elm)

        resp = self.connection.request("/servers", method="POST", data=ET.tostring(server_elm))
        return self._to_node(resp.object)

    def ex_set_password(self, node, password):
        """
        Sets the Node's root password.

        This will reboot the instance to complete the operation.

        :class:`Node.extra['password']` will be set to the new value if the
        operation was successful.

        :param      node: node to set password
        :type       node: :class:`Node`

        :param      password: new password.
        :type       password: ``str``

        :rtype: ``bool``
        """
        return self._change_password_or_name(node, password=password)

    def ex_set_server_name(self, node, name):
        """
        Sets the Node's name.

        This will reboot the instance to complete the operation.

        :param      node: node to set name
        :type       node: :class:`Node`

        :param      name: new name
        :type       name: ``str``

        :rtype: ``bool``
        """
        return self._change_password_or_name(node, name=name)

    def ex_resize_node(self, node, size):
        """
        Change an existing server flavor / scale the server up or down.

        :param      node: node to resize.
        :type       node: :class:`Node`

        :param      size: new size.
        :type       size: :class:`NodeSize`

        :rtype: ``bool``
        """
        elm = ET.Element("resize", {"xmlns": self.XML_NAMESPACE, "flavorId": str(size.id)})

        resp = self.connection.request(
            "/servers/%s/action" % (node.id), method="POST", data=ET.tostring(elm)
        )
        return resp.status == httplib.ACCEPTED

    def ex_resize(self, node, size):
        """
        NOTE: This method is here for backward compatibility reasons.

        You should use ``ex_resize_node`` instead.
        """
        return self.ex_resize_node(node=node, size=size)

    def ex_confirm_resize(self, node):
        """
        Confirm a resize request which is currently in progress. If a resize
        request is not explicitly confirmed or reverted it's automatically
        confirmed after 24 hours.

        For more info refer to the API documentation: http://goo.gl/zjFI1

        :param      node: node for which the resize request will be confirmed.
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        elm = ET.Element(
            "confirmResize",
            {"xmlns": self.XML_NAMESPACE},
        )

        resp = self.connection.request(
            "/servers/%s/action" % (node.id), method="POST", data=ET.tostring(elm)
        )
        return resp.status == httplib.NO_CONTENT

    def ex_revert_resize(self, node):
        """
        Revert a resize request which is currently in progress.
        All resizes are automatically confirmed after 24 hours if they have
        not already been confirmed explicitly or reverted.

        For more info refer to the API documentation: http://goo.gl/AizBu

        :param      node: node for which the resize request will be reverted.
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        elm = ET.Element("revertResize", {"xmlns": self.XML_NAMESPACE})

        resp = self.connection.request(
            "/servers/%s/action" % (node.id), method="POST", data=ET.tostring(elm)
        )
        return resp.status == httplib.NO_CONTENT

    def ex_rebuild(self, node_id, image_id):
        """
        Rebuilds the specified server.

        :param       node_id: ID of the node which should be used
        :type        node_id: ``str``

        :param       image_id: ID of the image which should be used
        :type        image_id: ``str``

        :rtype: ``bool``
        """
        # @TODO: Remove those ifs in 0.6
        if isinstance(node_id, Node):
            node_id = node_id.id

        if isinstance(image_id, NodeImage):
            image_id = image_id.id

        elm = ET.Element("rebuild", {"xmlns": self.XML_NAMESPACE, "imageId": image_id})

        resp = self.connection.request(
            "/servers/%s/action" % node_id, method="POST", data=ET.tostring(elm)
        )
        return resp.status == httplib.ACCEPTED

    def ex_create_ip_group(self, group_name, node_id=None):
        """
        Creates a shared IP group.

        :param       group_name:  group name which should be used
        :type        group_name: ``str``

        :param       node_id: ID of the node which should be used
        :type        node_id: ``str``

        :rtype: ``bool``
        """
        # @TODO: Remove this if in 0.6
        if isinstance(node_id, Node):
            node_id = node_id.id

        group_elm = ET.Element("sharedIpGroup", {"xmlns": self.XML_NAMESPACE, "name": group_name})

        if node_id:
            ET.SubElement(group_elm, "server", {"id": node_id})

        resp = self.connection.request(
            "/shared_ip_groups", method="POST", data=ET.tostring(group_elm)
        )
        return self._to_shared_ip_group(resp.object)

    def ex_list_ip_groups(self, details=False):
        """
        Lists IDs and names for shared IP groups.
        If details lists all details for shared IP groups.

        :param       details: True if details is required
        :type        details: ``bool``

        :rtype: ``list`` of :class:`OpenStack_1_0_SharedIpGroup`
        """
        uri = "/shared_ip_groups/detail" if details else "/shared_ip_groups"
        resp = self.connection.request(uri, method="GET")
        groups = findall(resp.object, "sharedIpGroup", self.XML_NAMESPACE)
        return [self._to_shared_ip_group(el) for el in groups]

    def ex_delete_ip_group(self, group_id):
        """
        Deletes the specified shared IP group.

        :param       group_id:  group id which should be used
        :type        group_id: ``str``

        :rtype: ``bool``
        """
        uri = "/shared_ip_groups/%s" % group_id
        resp = self.connection.request(uri, method="DELETE")
        return resp.status == httplib.NO_CONTENT

    def ex_share_ip(self, group_id, node_id, ip, configure_node=True):
        """
        Shares an IP address to the specified server.

        :param       group_id:  group id which should be used
        :type        group_id: ``str``

        :param       node_id: ID of the node which should be used
        :type        node_id: ``str``

        :param       ip: ip which should be used
        :type        ip: ``str``

        :param       configure_node: configure node
        :type        configure_node: ``bool``

        :rtype: ``bool``
        """
        # @TODO: Remove this if in 0.6
        if isinstance(node_id, Node):
            node_id = node_id.id

        if configure_node:
            str_configure = "true"
        else:
            str_configure = "false"

        elm = ET.Element(
            "shareIp",
            {
                "xmlns": self.XML_NAMESPACE,
                "sharedIpGroupId": group_id,
                "configureServer": str_configure,
            },
        )

        uri = "/servers/{}/ips/public/{}".format(node_id, ip)

        resp = self.connection.request(uri, method="PUT", data=ET.tostring(elm))
        return resp.status == httplib.ACCEPTED

    def ex_unshare_ip(self, node_id, ip):
        """
        Removes a shared IP address from the specified server.

        :param       node_id: ID of the node which should be used
        :type        node_id: ``str``

        :param       ip: ip which should be used
        :type        ip: ``str``

        :rtype: ``bool``
        """
        # @TODO: Remove this if in 0.6
        if isinstance(node_id, Node):
            node_id = node_id.id

        uri = "/servers/{}/ips/public/{}".format(node_id, ip)

        resp = self.connection.request(uri, method="DELETE")
        return resp.status == httplib.ACCEPTED

    def ex_list_ip_addresses(self, node_id):
        """
        List all server addresses.

        :param       node_id: ID of the node which should be used
        :type        node_id: ``str``

        :rtype: :class:`OpenStack_1_0_NodeIpAddresses`
        """
        # @TODO: Remove this if in 0.6
        if isinstance(node_id, Node):
            node_id = node_id.id

        uri = "/servers/%s/ips" % node_id
        resp = self.connection.request(uri, method="GET")
        return self._to_ip_addresses(resp.object)

    def _metadata_to_xml(self, metadata):
        if not metadata:
            return None

        metadata_elm = ET.Element("metadata")
        for k, v in list(metadata.items()):
            meta_elm = ET.SubElement(metadata_elm, "meta", {"key": str(k)})
            meta_elm.text = str(v)

        return metadata_elm

    def _files_to_xml(self, files):
        if not files:
            return None

        personality_elm = ET.Element("personality")
        for k, v in list(files.items()):
            file_elm = ET.SubElement(personality_elm, "file", {"path": str(k)})
            file_elm.text = base64.b64encode(b(v)).decode("ascii")

        return personality_elm

    def _reboot_node(self, node, reboot_type="SOFT"):
        resp = self._node_action(node, ["reboot", ("type", reboot_type)])
        return resp.status == httplib.ACCEPTED

    def _node_action(self, node, body):
        if isinstance(body, list):
            attr = " ".join(['{}="{}"'.format(item[0], item[1]) for item in body[1:]])
            body = '<{} xmlns="{}" {}/>'.format(body[0], self.XML_NAMESPACE, attr)
        uri = "/servers/%s/action" % (node.id)
        resp = self.connection.request(uri, method="POST", data=body)
        return resp

    def _to_nodes(self, object):
        node_elements = findall(object, "server", self.XML_NAMESPACE)
        return [self._to_node(el) for el in node_elements]

    def _to_node_from_obj(self, obj):
        return self._to_node(findall(obj, "server", self.XML_NAMESPACE)[0])

    def _to_node(self, el):
        def get_ips(el):
            return [ip.get("addr") for ip in el]

        def get_meta_dict(el):
            d = {}
            for meta in el:
                d[meta.get("key")] = meta.text
            return d

        public_ip = get_ips(findall(el, "addresses/public/ip", self.XML_NAMESPACE))
        private_ip = get_ips(findall(el, "addresses/private/ip", self.XML_NAMESPACE))
        metadata = get_meta_dict(findall(el, "metadata/meta", self.XML_NAMESPACE))

        n = Node(
            id=el.get("id"),
            name=el.get("name"),
            state=self.NODE_STATE_MAP.get(el.get("status"), NodeState.UNKNOWN),
            public_ips=public_ip,
            private_ips=private_ip,
            driver=self.connection.driver,
            # pylint: disable=no-member
            extra={
                "password": el.get("adminPass"),
                "hostId": el.get("hostId"),
                "imageId": el.get("imageId"),
                "flavorId": el.get("flavorId"),
                "uri": "https://%s%s/servers/%s"
                % (self.connection.host, self.connection.request_path, el.get("id")),
                "service_name": self.connection.get_service_name(),
                "metadata": metadata,
            },
        )
        return n

    def _to_sizes(self, object):
        elements = findall(object, "flavor", self.XML_NAMESPACE)
        return [self._to_size(el) for el in elements]

    def _to_size(self, el):
        vcpus = int(el.get("vcpus")) if el.get("vcpus", None) else None
        return OpenStackNodeSize(
            id=el.get("id"),
            name=el.get("name"),
            ram=int(el.get("ram")),
            disk=int(el.get("disk")),
            # XXX: needs hardcode
            vcpus=vcpus,
            bandwidth=None,
            extra=el.get("extra_specs"),
            # Hardcoded
            price=self._get_size_price(el.get("id")),
            driver=self.connection.driver,
        )

    def ex_limits(self):
        """
        Extra call to get account's limits, such as
        rates (for example amount of POST requests per day)
        and absolute limits like total amount of available
        RAM to be used by servers.

        :return: dict with keys 'rate' and 'absolute'
        :rtype: ``dict``
        """

        def _to_rate(el):
            rate = {}
            for item in list(el.items()):
                rate[item[0]] = item[1]

            return rate

        def _to_absolute(el):
            return {el.get("name"): el.get("value")}

        limits = self.connection.request("/limits").object
        rate = [_to_rate(el) for el in findall(limits, "rate/limit", self.XML_NAMESPACE)]
        absolute = {}
        for item in findall(limits, "absolute/limit", self.XML_NAMESPACE):
            absolute.update(_to_absolute(item))

        return {"rate": rate, "absolute": absolute}

    def create_image(self, node, name, description=None, reboot=True):
        """Create an image for node.

        @inherits: :class:`NodeDriver.create_image`

        :param      node: node to use as a base for image
        :type       node: :class:`Node`

        :param      name: name for new image
        :type       name: ``str``

        :rtype: :class:`NodeImage`
        """

        image_elm = ET.Element(
            "image", {"xmlns": self.XML_NAMESPACE, "name": name, "serverId": node.id}
        )

        return self._to_image(
            self.connection.request("/images", method="POST", data=ET.tostring(image_elm)).object
        )

    def delete_image(self, image):
        """Delete an image for node.

        @inherits: :class:`NodeDriver.delete_image`

        :param      image: the image to be deleted
        :type       image: :class:`NodeImage`

        :rtype: ``bool``
        """
        uri = "/images/%s" % image.id
        resp = self.connection.request(uri, method="DELETE")
        return resp.status == httplib.NO_CONTENT

    def _to_shared_ip_group(self, el):
        servers_el = findall(el, "servers", self.XML_NAMESPACE)
        if servers_el:
            servers = [s.get("id") for s in findall(servers_el[0], "server", self.XML_NAMESPACE)]
        else:
            servers = None
        return OpenStack_1_0_SharedIpGroup(id=el.get("id"), name=el.get("name"), servers=servers)

    def _to_ip_addresses(self, el):
        public_ips = [
            ip.get("addr")
            for ip in findall(
                findall(el, "public", self.XML_NAMESPACE)[0], "ip", self.XML_NAMESPACE
            )
        ]
        private_ips = [
            ip.get("addr")
            for ip in findall(
                findall(el, "private", self.XML_NAMESPACE)[0], "ip", self.XML_NAMESPACE
            )
        ]

        return OpenStack_1_0_NodeIpAddresses(public_ips, private_ips)

    def _get_size_price(self, size_id):
        try:
            return get_size_price(driver_type="compute", driver_name=self.api_name, size_id=size_id)
        except KeyError:
            return 0.0


class OpenStack_1_0_SharedIpGroup:
    """
    Shared IP group info.
    """

    def __init__(self, id, name, servers=None):
        self.id = str(id)
        self.name = name
        self.servers = servers


class OpenStack_1_0_NodeIpAddresses:
    """
    List of public and private IP addresses of a Node.
    """

    def __init__(self, public_addresses, private_addresses):
        self.public_addresses = public_addresses
        self.private_addresses = private_addresses


class OpenStack_1_1_Response(OpenStackResponse):
    def __init__(self, *args, **kwargs):
        # done because of a circular reference from
        # NodeDriver -> Connection -> Response
        self.node_driver = OpenStack_1_1_NodeDriver
        super().__init__(*args, **kwargs)


class OpenStackNetwork:
    """
    A Virtual Network.
    """

    def __init__(self, id, name, cidr, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.cidr = cidr
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return '<OpenStackNetwork id="{}" name="{}" cidr="{}">'.format(
            self.id,
            self.name,
            self.cidr,
        )


class OpenStackSecurityGroup:
    """
    A Security Group.
    """

    def __init__(self, id, tenant_id, name, description, driver, rules=None, extra=None):
        """
        Constructor.

        :keyword    id: Group id.
        :type       id: ``str``

        :keyword    tenant_id: Owner of the security group.
        :type       tenant_id: ``str``

        :keyword    name: Human-readable name for the security group. Might
                          not be unique.
        :type       name: ``str``

        :keyword    description: Human-readable description of a security
                                 group.
        :type       description: ``str``

        :keyword    rules: Rules associated with this group.
        :type       rules: ``list`` of
                    :class:`OpenStackSecurityGroupRule`

        :keyword    extra: Extra attributes associated with this group.
        :type       extra: ``dict``
        """
        self.id = id
        self.tenant_id = tenant_id
        self.name = name
        self.description = description
        self.driver = driver
        self.rules = rules or []
        self.extra = extra or {}

    def __repr__(self):
        return (
            "<OpenStackSecurityGroup id=%s tenant_id=%s name=%s \
        description=%s>"
            % (self.id, self.tenant_id, self.name, self.description)
        )


class OpenStackSecurityGroupRule:
    """
    A Rule of a Security Group.
    """

    def __init__(
        self,
        id,
        parent_group_id,
        ip_protocol,
        from_port,
        to_port,
        driver,
        ip_range=None,
        group=None,
        tenant_id=None,
        direction=None,
        extra=None,
    ):
        """
        Constructor.

        :keyword    id: Rule id.
        :type       id: ``str``

        :keyword    parent_group_id: ID of the parent security group.
        :type       parent_group_id: ``str``

        :keyword    ip_protocol: IP Protocol (icmp, tcp, udp, etc).
        :type       ip_protocol: ``str``

        :keyword    from_port: Port at start of range.
        :type       from_port: ``int``

        :keyword    to_port: Port at end of range.
        :type       to_port: ``int``

        :keyword    ip_range: CIDR for address range.
        :type       ip_range: ``str``

        :keyword    group: Name of a source security group to apply to rule.
        :type       group: ``str``

        :keyword    tenant_id: Owner of the security group.
        :type       tenant_id: ``str``

        :keyword    direction: Security group Direction (ingress or egress).
        :type       direction: ``str``

        :keyword    extra: Extra attributes associated with this rule.
        :type       extra: ``dict``
        """
        self.id = id
        self.parent_group_id = parent_group_id
        self.ip_protocol = ip_protocol
        self.from_port = from_port
        self.to_port = to_port
        self.driver = driver
        self.ip_range = ""
        self.group = {}
        self.direction = "ingress"

        if group is None:
            self.ip_range = ip_range
        else:
            self.group = {"name": group, "tenant_id": tenant_id}

        # by default in old versions only ingress was used
        if direction is not None:
            if direction in ["ingress", "egress"]:
                self.direction = direction
            else:
                raise OpenStackException(
                    "Security group direction incorrect " "value: ingress or egress.",
                    500,
                    driver,
                )

        self.tenant_id = tenant_id
        self.extra = extra or {}

    def __repr__(self):
        return (
            "<OpenStackSecurityGroupRule id=%s parent_group_id=%s \
                ip_protocol=%s from_port=%s to_port=%s>"
            % (
                self.id,
                self.parent_group_id,
                self.ip_protocol,
                self.from_port,
                self.to_port,
            )
        )


class OpenStackKeyPair:
    """
    A KeyPair.
    """

    def __init__(self, name, fingerprint, public_key, driver, private_key=None, extra=None):
        """
        Constructor.

        :keyword    name: Name of the KeyPair.
        :type       name: ``str``

        :keyword    fingerprint: Fingerprint of the KeyPair
        :type       fingerprint: ``str``

        :keyword    public_key: Public key in OpenSSH format.
        :type       public_key: ``str``

        :keyword    private_key: Private key in PEM format.
        :type       private_key: ``str``

        :keyword    extra: Extra attributes associated with this KeyPair.
        :type       extra: ``dict``
        """
        self.name = name
        self.fingerprint = fingerprint
        self.public_key = public_key
        self.private_key = private_key
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return "<OpenStackKeyPair name={} fingerprint={} public_key={} ...>".format(
            self.name,
            self.fingerprint,
            self.public_key,
        )


class OpenStack_1_1_Connection(OpenStackComputeConnection):
    responseCls = OpenStack_1_1_Response
    accept_format = "application/json"
    default_content_type = "application/json; charset=UTF-8"

    def encode_data(self, data):
        return json.dumps(data)


class OpenStack_1_1_NodeDriver(OpenStackNodeDriver):
    """
    OpenStack node driver.
    """

    connectionCls = OpenStack_1_1_Connection
    type = Provider.OPENSTACK

    features = {"create_node": ["generates_password"]}
    _networks_url_prefix = "/os-networks"

    def __init__(self, *args, **kwargs):
        self._ex_force_api_version = str(kwargs.pop("ex_force_api_version", None))
        super().__init__(*args, **kwargs)

    def create_node(
        self,
        name,
        size,
        image=None,
        ex_keyname=None,
        ex_userdata=None,
        ex_config_drive=None,
        ex_security_groups=None,
        ex_metadata=None,
        ex_files=None,
        networks=None,
        ex_disk_config=None,
        ex_admin_pass=None,
        ex_availability_zone=None,
        ex_blockdevicemappings=None,
        ex_os_scheduler_hints=None,
    ):
        """Create a new node

        @inherits:  :class:`NodeDriver.create_node`

        :keyword    ex_keyname:  The name of the key pair
        :type       ex_keyname:  ``str``

        :keyword    ex_userdata: String containing user data
                                 see
                                 https://help.ubuntu.com/community/CloudInit
        :type       ex_userdata: ``str``

        :keyword    ex_config_drive: Enable config drive
                                     see
                                     http://docs.openstack.org/grizzly/openstack-compute/admin/content/config-drive.html
        :type       ex_config_drive: ``bool``

        :keyword    ex_security_groups: List of security groups to assign to
                                        the node
        :type       ex_security_groups: ``list`` of
                                       :class:`OpenStackSecurityGroup`

        :keyword    ex_metadata: Key/Value metadata to associate with a node
        :type       ex_metadata: ``dict``

        :keyword    ex_files:   File Path => File contents to create on
                                the node
        :type       ex_files:   ``dict``


        :keyword    networks: The server is launched into a set of Networks.
        :type       networks: ``list`` of :class:`OpenStackNetwork`

        :keyword    ex_disk_config: Name of the disk configuration.
                                    Can be either ``AUTO`` or ``MANUAL``.
        :type       ex_disk_config: ``str``

        :keyword    ex_config_drive: If True enables metadata injection in a
                                     server through a configuration drive.
        :type       ex_config_drive: ``bool``

        :keyword    ex_admin_pass: The root password for the node
        :type       ex_admin_pass: ``str``

        :keyword    ex_availability_zone: Nova availability zone for the node
        :type       ex_availability_zone: ``str``

        :keyword    ex_blockdevicemappings: Enables fine grained control of the
                                            block device mapping for an instance.
        :type       ex_blockdevicemappings: ``dict``

        :keyword    ex_os_scheduler_hints: The dictionary of data to send to
                                           the scheduler.
        :type       ex_os_scheduler_hints:   ``dict``
        """
        ex_metadata = ex_metadata or {}
        ex_files = ex_files or {}
        networks = networks or []
        ex_security_groups = ex_security_groups or []

        server_params = self._create_args_to_params(
            node=None,
            name=name,
            size=size,
            image=image,
            ex_keyname=ex_keyname,
            ex_userdata=ex_userdata,
            ex_config_drive=ex_config_drive,
            ex_security_groups=ex_security_groups,
            ex_metadata=ex_metadata,
            ex_files=ex_files,
            networks=networks,
            ex_disk_config=ex_disk_config,
            ex_availability_zone=ex_availability_zone,
            ex_blockdevicemappings=ex_blockdevicemappings,
        )

        data = {"server": server_params}
        if ex_os_scheduler_hints:
            data["os:scheduler_hints"] = ex_os_scheduler_hints

        resp = self.connection.request("/servers", method="POST", data=data)

        create_response = resp.object["server"]
        server_resp = self.connection.request("/servers/%s" % create_response["id"])
        server_object = server_resp.object["server"]

        # adminPass is not always present
        # http://docs.openstack.org/essex/openstack-compute/admin/
        # content/configuring-compute-API.html#d6e1833
        server_object["adminPass"] = create_response.get("adminPass", None)

        return self._to_node(server_object)

    def _to_images(self, obj, ex_only_active):
        images = []
        for image in obj["images"]:
            if ex_only_active and image.get("status") != "ACTIVE":
                continue
            images.append(self._to_image(image))

        return images

    def _to_image(self, api_image):
        server = api_image.get("server", {})
        updated = api_image.get("updated_at") or api_image["updated"]
        created = api_image.get("created_at") or api_image["created"]
        min_ram = api_image.get("min_ram")

        if min_ram is None:
            min_ram = api_image.get("minRam")

        min_disk = api_image.get("min_disk")

        if min_disk is None:
            min_disk = api_image.get("minDisk")

        return NodeImage(
            id=api_image["id"],
            name=api_image["name"],
            driver=self,
            extra=dict(
                visibility=api_image.get("visibility"),
                updated=updated,
                created=created,
                status=api_image["status"],
                progress=api_image.get("progress"),
                metadata=api_image.get("metadata"),
                os_type=api_image.get("os_type"),
                os_distro=api_image.get("os_distro"),
                os_version=api_image.get("os_version"),
                serverId=server.get("id"),
                minDisk=min_disk,
                minRam=min_ram,
            ),
        )

    def _to_image_member(self, api_image_member):
        created = api_image_member["created_at"]
        updated = api_image_member.get("updated_at")
        return NodeImageMember(
            id=api_image_member["member_id"],
            image_id=api_image_member["image_id"],
            state=api_image_member["status"],
            created=created,
            driver=self,
            extra=dict(
                schema=api_image_member.get("schema"),
                updated=updated,
            ),
        )

    def _to_nodes(self, obj):
        servers = obj["servers"]
        return [self._to_node(server) for server in servers]

    def _to_volumes(self, obj):
        volumes = obj["volumes"]
        return [self._to_volume(volume) for volume in volumes]

    def _to_snapshots(self, obj):
        snapshots = obj["snapshots"]
        return [self._to_snapshot(snapshot) for snapshot in snapshots]

    def _to_sizes(self, obj):
        flavors = obj["flavors"]
        return [self._to_size(flavor) for flavor in flavors]

    def _create_args_to_params(self, node, **kwargs):
        server_params = {
            "name": kwargs.get("name"),
            "metadata": kwargs.get("ex_metadata", {}) or {},
        }

        if kwargs.get("ex_files", None):
            server_params["personality"] = self._files_to_personality(kwargs.get("ex_files"))

        if kwargs.get("ex_availability_zone", None):
            server_params["availability_zone"] = kwargs["ex_availability_zone"]

        if kwargs.get("ex_keyname", None):
            server_params["key_name"] = kwargs["ex_keyname"]

        if kwargs.get("ex_userdata", None):
            server_params["user_data"] = base64.b64encode(b(kwargs["ex_userdata"])).decode("ascii")

        if kwargs.get("ex_disk_config", None):
            server_params["OS-DCF:diskConfig"] = kwargs["ex_disk_config"]

        if kwargs.get("ex_config_drive", None):
            server_params["config_drive"] = str(kwargs["ex_config_drive"])

        if kwargs.get("ex_admin_pass", None):
            server_params["adminPass"] = kwargs["ex_admin_pass"]

        if kwargs.get("networks", None):
            networks = kwargs["networks"] or []
            networks = [{"uuid": network.id} for network in networks]
            server_params["networks"] = networks

        if kwargs.get("ex_security_groups", None):
            server_params["security_groups"] = []
            for security_group in kwargs["ex_security_groups"] or []:
                name = security_group.name
                server_params["security_groups"].append({"name": name})

        if kwargs.get("ex_blockdevicemappings", None):
            server_params["block_device_mapping_v2"] = kwargs["ex_blockdevicemappings"]

        if kwargs.get("name", None):
            server_params["name"] = kwargs.get("name")
        else:
            server_params["name"] = node.name

        if kwargs.get("image", None):
            server_params["imageRef"] = kwargs.get("image").id
        else:
            server_params["imageRef"] = node.extra.get("imageId", "") if node else ""

        if kwargs.get("size", None):
            server_params["flavorRef"] = kwargs.get("size").id
        else:
            server_params["flavorRef"] = node.extra.get("flavorId")

        return server_params

    def _files_to_personality(self, files):
        rv = []

        for k, v in list(files.items()):
            rv.append({"path": k, "contents": base64.b64encode(b(v)).decode()})

        return rv

    def _reboot_node(self, node, reboot_type="SOFT"):
        resp = self._node_action(node, "reboot", type=reboot_type)
        return resp.status == httplib.ACCEPTED

    def ex_set_password(self, node, password):
        """
        Changes the administrator password for a specified server.

        :param      node: Node to rebuild.
        :type       node: :class:`Node`

        :param      password: The administrator password.
        :type       password: ``str``

        :rtype: ``bool``
        """
        resp = self._node_action(node, "changePassword", adminPass=password)
        node.extra["password"] = password
        return resp.status == httplib.ACCEPTED

    def ex_rebuild(self, node, image, **kwargs):
        """
        Rebuild a Node.

        :param      node: Node to rebuild.
        :type       node: :class:`Node`

        :param      image: New image to use.
        :type       image: :class:`NodeImage`

        :keyword    ex_metadata: Key/Value metadata to associate with a node
        :type       ex_metadata: ``dict``

        :keyword    ex_files:   File Path => File contents to create on
                                the node
        :type       ex_files:   ``dict``

        :keyword    ex_keyname:  Name of existing public key to inject into
                                 instance
        :type       ex_keyname:  ``str``

        :keyword    ex_userdata: String containing user data
                                 see
                                 https://help.ubuntu.com/community/CloudInit
        :type       ex_userdata: ``str``

        :keyword    ex_security_groups: List of security groups to assign to
                                        the node
        :type       ex_security_groups: ``list`` of
                                       :class:`OpenStackSecurityGroup`

        :keyword    ex_disk_config: Name of the disk configuration.
                                    Can be either ``AUTO`` or ``MANUAL``.
        :type       ex_disk_config: ``str``

        :keyword    ex_config_drive: If True enables metadata injection in a
                                     server through a configuration drive.
        :type       ex_config_drive: ``bool``

        :rtype: ``bool``
        """
        server_params = self._create_args_to_params(node, image=image, **kwargs)
        resp = self._node_action(node, "rebuild", **server_params)
        return resp.status == httplib.ACCEPTED

    def ex_resize(self, node, size):
        """
        Change a node size.

        :param      node: Node to resize.
        :type       node: :class:`Node`

        :type       size: :class:`NodeSize`
        :param      size: New size to use.

        :rtype: ``bool``
        """
        server_params = {"flavorRef": size.id}
        resp = self._node_action(node, "resize", **server_params)
        return resp.status == httplib.ACCEPTED

    def ex_confirm_resize(self, node):
        """
        Confirms a pending resize action.

        :param      node: Node to resize.
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        resp = self._node_action(node, "confirmResize")
        return resp.status == httplib.NO_CONTENT

    def ex_revert_resize(self, node):
        """
        Cancels and reverts a pending resize action.

        :param      node: Node to resize.
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        resp = self._node_action(node, "revertResize")
        return resp.status == httplib.ACCEPTED

    def create_image(self, node, name, metadata=None):
        """
        Creates a new image.

        :param      node: Node
        :type       node: :class:`Node`

        :param      name: The name for the new image.
        :type       name: ``str``

        :param      metadata: Key and value pairs for metadata.
        :type       metadata: ``dict``

        :rtype: :class:`NodeImage`
        """
        optional_params = {}
        if metadata:
            optional_params["metadata"] = metadata
        resp = self._node_action(node, "createImage", name=name, **optional_params)
        image_id = self._extract_image_id_from_url(resp.headers["location"])
        return self.get_image(image_id=image_id)

    def ex_set_server_name(self, node, name):
        """
        Sets the Node's name.

        :param      node: Node
        :type       node: :class:`Node`

        :param      name: The name of the server.
        :type       name: ``str``

        :rtype: :class:`Node`
        """
        return self._update_node(node, name=name)

    def ex_get_metadata(self, node):
        """
        Get a Node's metadata.

        :param      node: Node
        :type       node: :class:`Node`

        :return: Key/Value metadata associated with node.
        :rtype: ``dict``
        """
        return self.connection.request(
            "/servers/{}/metadata".format(node.id),
            method="GET",
        ).object["metadata"]

    def ex_set_metadata(self, node, metadata):
        """
        Sets the Node's metadata.

        :param      node: Node
        :type       node: :class:`Node`

        :param      metadata: Key/Value metadata to associate with a node
        :type       metadata: ``dict``

        :rtype: ``dict``
        """
        return self.connection.request(
            "/servers/{}/metadata".format(node.id),
            method="PUT",
            data={"metadata": metadata},
        ).object["metadata"]

    def ex_update_node(self, node, **node_updates):
        """
        Update the Node's editable attributes.  The OpenStack API currently
        supports editing name and IPv4/IPv6 access addresses.

        The driver currently only supports updating the node name.

        :param      node: Node
        :type       node: :class:`Node`

        :keyword    name:   New name for the server
        :type       name:   ``str``

        :rtype: :class:`Node`
        """
        potential_data = self._create_args_to_params(node, **node_updates)
        updates = {"name": potential_data["name"]}
        return self._update_node(node, **updates)

    def _to_networks(self, obj):
        networks = obj["networks"]
        return [self._to_network(network) for network in networks]

    def _to_network(self, obj):
        return OpenStackNetwork(
            id=obj["id"], name=obj["label"], cidr=obj.get("cidr", None), driver=self
        )

    def ex_list_networks(self):
        """
        Get a list of Networks that are available.

        :rtype: ``list`` of :class:`OpenStackNetwork`
        """
        response = self.connection.request(self._networks_url_prefix).object
        return self._to_networks(response)

    def ex_get_network(self, network_id):
        """
        Retrieve the Network with the given ID

        :param networkId: ID of the network
        :type networkId: ``str``

        :rtype :class:`OpenStackNetwork`
        """
        request_url = "{networks_url_prefix}/{network_id}".format(
            networks_url_prefix=self._networks_url_prefix, network_id=network_id
        )
        response = self.connection.request(request_url).object
        return self._to_network(response["network"])

    def ex_create_network(self, name, cidr):
        """
        Create a new Network

        :param name: Name of network which should be used
        :type name: ``str``

        :param cidr: cidr of network which should be used
        :type cidr: ``str``

        :rtype: :class:`OpenStackNetwork`
        """
        data = {"network": {"cidr": cidr, "label": name}}
        response = self.connection.request(
            self._networks_url_prefix, method="POST", data=data
        ).object
        return self._to_network(response["network"])

    def ex_delete_network(self, network):
        """
        Delete a Network

        :param network: Network which should be used
        :type network: :class:`OpenStackNetwork`

        :rtype: ``bool``
        """
        resp = self.connection.request(
            "{}/{}".format(self._networks_url_prefix, network.id), method="DELETE"
        )
        return resp.status in (httplib.NO_CONTENT, httplib.ACCEPTED)

    def ex_get_console_output(self, node, length=None):
        """
        Get console output

        :param      node: node
        :type       node: :class:`Node`

        :param      length: Optional number of lines to fetch from the
                            console log
        :type       length: ``int``

        :return: Dictionary with the output
        :rtype: ``dict``
        """

        data = {"os-getConsoleOutput": {"length": length}}

        resp = self.connection.request(
            "/servers/%s/action" % node.id, method="POST", data=data
        ).object
        return resp

    def ex_list_snapshots(self):
        return self._to_snapshots(self.connection.request("/os-snapshots").object)

    def ex_get_snapshot(self, snapshotId):
        return self._to_snapshot(self.connection.request("/os-snapshots/%s" % snapshotId).object)

    def list_volume_snapshots(self, volume):
        return [
            snapshot
            for snapshot in self.ex_list_snapshots()
            if snapshot.extra["volume_id"] == volume.id
        ]

    def create_volume_snapshot(self, volume, name=None, ex_description=None, ex_force=True):
        """
        Create snapshot from volume

        :param volume: Instance of `StorageVolume`
        :type  volume: `StorageVolume`

        :param name: Name of snapshot (optional)
        :type  name: `str` | `NoneType`

        :param ex_description: Description of the snapshot (optional)
        :type  ex_description: `str` | `NoneType`

        :param ex_force: Specifies if we create a snapshot that is not in
                         state `available`. For example `in-use`. Defaults
                         to True. (optional)
        :type  ex_force: `bool`

        :rtype: :class:`VolumeSnapshot`
        """
        data = {"snapshot": {"volume_id": volume.id, "force": ex_force}}

        if name is not None:
            data["snapshot"]["display_name"] = name

        if ex_description is not None:
            data["snapshot"]["display_description"] = ex_description

        return self._to_snapshot(
            self.connection.request("/os-snapshots", method="POST", data=data).object
        )

    def destroy_volume_snapshot(self, snapshot):
        resp = self.connection.request("/os-snapshots/%s" % snapshot.id, method="DELETE")
        return resp.status == httplib.NO_CONTENT

    def ex_create_snapshot(self, volume, name, description=None, force=False):
        """
        Create a snapshot based off of a volume.

        :param      volume: volume
        :type       volume: :class:`StorageVolume`

        :keyword    name: New name for the volume snapshot
        :type       name: ``str``

        :keyword    description: Description of the snapshot (optional)
        :type       description: ``str``

        :keyword    force: Whether to force creation (optional)
        :type       force: ``bool``

        :rtype:     :class:`VolumeSnapshot`
        """
        warnings.warn(
            "This method has been deprecated in favor of the " "create_volume_snapshot method"
        )
        return self.create_volume_snapshot(volume, name, ex_description=description, ex_force=force)

    def ex_delete_snapshot(self, snapshot):
        """
        Delete a VolumeSnapshot

        :param      snapshot: snapshot
        :type       snapshot: :class:`VolumeSnapshot`

        :rtype:     ``bool``
        """
        warnings.warn(
            "This method has been deprecated in favor of the " "destroy_volume_snapshot method"
        )
        return self.destroy_volume_snapshot(snapshot)

    def _to_security_group_rules(self, obj):
        return [self._to_security_group_rule(security_group_rule) for security_group_rule in obj]

    def _to_security_group_rule(self, obj):
        ip_range = group = tenant_id = None
        if obj["group"] == {}:
            ip_range = obj["ip_range"].get("cidr", None)
        else:
            group = obj["group"].get("name", None)
            tenant_id = obj["group"].get("tenant_id", None)

        return OpenStackSecurityGroupRule(
            id=obj["id"],
            parent_group_id=obj["parent_group_id"],
            ip_protocol=obj["ip_protocol"],
            from_port=obj["from_port"],
            to_port=obj["to_port"],
            driver=self,
            ip_range=ip_range,
            group=group,
            tenant_id=tenant_id,
        )

    def _to_security_groups(self, obj):
        security_groups = obj["security_groups"]
        return [self._to_security_group(security_group) for security_group in security_groups]

    def _to_security_group(self, obj):
        rules = self._to_security_group_rules(obj.get("security_group_rules", obj.get("rules", [])))
        return OpenStackSecurityGroup(
            id=obj["id"],
            tenant_id=obj["tenant_id"],
            name=obj["name"],
            description=obj.get("description", ""),
            rules=rules,
            driver=self,
        )

    def ex_list_security_groups(self):
        """
        Get a list of Security Groups that are available.

        :rtype: ``list`` of :class:`OpenStackSecurityGroup`
        """
        return self._to_security_groups(self.connection.request("/os-security-groups").object)

    def ex_get_node_security_groups(self, node):
        """
        Get Security Groups of the specified server.

        :rtype: ``list`` of :class:`OpenStackSecurityGroup`
        """
        return self._to_security_groups(
            self.connection.request("/servers/%s/os-security-groups" % (node.id)).object
        )

    def ex_create_security_group(self, name, description):
        """
        Create a new Security Group

        :param name: Name of the new Security Group
        :type  name: ``str``

        :param description: Description of the new Security Group
        :type  description: ``str``

        :rtype: :class:`OpenStackSecurityGroup`
        """
        return self._to_security_group(
            self.connection.request(
                "/os-security-groups",
                method="POST",
                data={"security_group": {"name": name, "description": description}},
            ).object["security_group"]
        )

    def ex_delete_security_group(self, security_group):
        """
        Delete a Security Group.

        :param security_group: Security Group should be deleted
        :type  security_group: :class:`OpenStackSecurityGroup`

        :rtype: ``bool``
        """
        resp = self.connection.request(
            "/os-security-groups/%s" % (security_group.id), method="DELETE"
        )
        return resp.status in (httplib.NO_CONTENT, httplib.ACCEPTED)

    def ex_create_security_group_rule(
        self,
        security_group,
        ip_protocol,
        from_port,
        to_port,
        cidr=None,
        source_security_group=None,
    ):
        """
        Create a new Rule in a Security Group

        :param security_group: Security Group in which to add the rule
        :type  security_group: :class:`OpenStackSecurityGroup`

        :param ip_protocol: Protocol to which this rule applies
                            Examples: tcp, udp, ...
        :type  ip_protocol: ``str``

        :param from_port: First port of the port range
        :type  from_port: ``int``

        :param to_port: Last port of the port range
        :type  to_port: ``int``

        :param cidr: CIDR notation of the source IP range for this rule
        :type  cidr: ``str``

        :param source_security_group: Existing Security Group to use as the
                                      source (instead of CIDR)
        :type  source_security_group: L{OpenStackSecurityGroup

        :rtype: :class:`OpenStackSecurityGroupRule`
        """
        source_security_group_id = None
        if type(source_security_group) == OpenStackSecurityGroup:
            source_security_group_id = source_security_group.id

        return self._to_security_group_rule(
            self.connection.request(
                "/os-security-group-rules",
                method="POST",
                data={
                    "security_group_rule": {
                        "ip_protocol": ip_protocol,
                        "from_port": from_port,
                        "to_port": to_port,
                        "cidr": cidr,
                        "group_id": source_security_group_id,
                        "parent_group_id": security_group.id,
                    }
                },
            ).object["security_group_rule"]
        )

    def ex_delete_security_group_rule(self, rule):
        """
        Delete a Rule from a Security Group.

        :param rule: Rule should be deleted
        :type  rule: :class:`OpenStackSecurityGroupRule`

        :rtype: ``bool``
        """
        resp = self.connection.request("/os-security-group-rules/%s" % (rule.id), method="DELETE")
        return resp.status == httplib.NO_CONTENT

    def _to_key_pairs(self, obj):
        key_pairs = obj["keypairs"]
        key_pairs = [self._to_key_pair(key_pair["keypair"]) for key_pair in key_pairs]
        return key_pairs

    def _to_key_pair(self, obj):
        key_pair = KeyPair(
            name=obj["name"],
            fingerprint=obj["fingerprint"],
            public_key=obj["public_key"],
            private_key=obj.get("private_key", None),
            driver=self,
        )
        return key_pair

    def list_key_pairs(self):
        response = self.connection.request("/os-keypairs")
        key_pairs = self._to_key_pairs(response.object)
        return key_pairs

    def get_key_pair(self, name):
        self.connection.set_context({"key_pair_name": name})

        response = self.connection.request("/os-keypairs/%s" % (name))
        key_pair = self._to_key_pair(response.object["keypair"])
        return key_pair

    def create_key_pair(self, name):
        data = {"keypair": {"name": name}}
        response = self.connection.request("/os-keypairs", method="POST", data=data)
        key_pair = self._to_key_pair(response.object["keypair"])
        return key_pair

    def import_key_pair_from_string(self, name, key_material):
        data = {"keypair": {"name": name, "public_key": key_material}}
        response = self.connection.request("/os-keypairs", method="POST", data=data)
        key_pair = self._to_key_pair(response.object["keypair"])
        return key_pair

    def delete_key_pair(self, key_pair):
        """
        Delete a KeyPair.

        :param keypair: KeyPair to delete
        :type  keypair: :class:`OpenStackKeyPair`

        :rtype: ``bool``
        """
        response = self.connection.request("/os-keypairs/%s" % (key_pair.name), method="DELETE")
        return response.status == httplib.ACCEPTED

    def ex_list_keypairs(self):
        """
        Get a list of KeyPairs that are available.

        :rtype: ``list`` of :class:`OpenStackKeyPair`
        """
        warnings.warn("This method has been deprecated in favor of " "list_key_pairs method")

        return self.list_key_pairs()

    def ex_create_keypair(self, name):
        """
        Create a new KeyPair

        :param name: Name of the new KeyPair
        :type  name: ``str``

        :rtype: :class:`OpenStackKeyPair`
        """
        warnings.warn("This method has been deprecated in favor of " "create_key_pair method")

        return self.create_key_pair(name=name)

    def ex_import_keypair(self, name, keyfile):
        """
        Import a KeyPair from a file

        :param name: Name of the new KeyPair
        :type  name: ``str``

        :param keyfile: Path to the public key file (in OpenSSH format)
        :type  keyfile: ``str``

        :rtype: :class:`OpenStackKeyPair`
        """
        warnings.warn(
            "This method has been deprecated in favor of " "import_key_pair_from_file method"
        )

        return self.import_key_pair_from_file(name=name, key_file_path=keyfile)

    def ex_import_keypair_from_string(self, name, key_material):
        """
        Import a KeyPair from a string

        :param name: Name of the new KeyPair
        :type  name: ``str``

        :param key_material: Public key (in OpenSSH format)
        :type  key_material: ``str``

        :rtype: :class:`OpenStackKeyPair`
        """
        warnings.warn(
            "This method has been deprecated in favor of " "import_key_pair_from_string method"
        )

        return self.import_key_pair_from_string(name=name, key_material=key_material)

    def ex_delete_keypair(self, keypair):
        """
        Delete a KeyPair.

        :param keypair: KeyPair to delete
        :type  keypair: :class:`OpenStackKeyPair`

        :rtype: ``bool``
        """
        warnings.warn("This method has been deprecated in favor of " "delete_key_pair method")

        return self.delete_key_pair(key_pair=keypair)

    def ex_get_size(self, size_id):
        """
        Get a NodeSize

        :param      size_id: ID of the size which should be used
        :type       size_id: ``str``

        :rtype: :class:`NodeSize`
        """
        return self._to_size(
            self.connection.request("/flavors/{}".format(size_id)).object["flavor"]
        )

    def ex_get_size_extra_specs(self, size_id):
        """
        Get the extra_specs field of a NodeSize

        :param      size_id: ID of the size which should be used
        :type       size_id: ``str``

        :rtype: `dict`
        """
        return self.connection.request("/flavors/{}/os-extra_specs".format(size_id)).object[
            "extra_specs"
        ]

    def get_image(self, image_id):
        """
        Get a NodeImage

        @inherits: :class:`NodeDriver.get_image`

        :param      image_id: ID of the image which should be used
        :type       image_id: ``str``

        :rtype: :class:`NodeImage`
        """
        return self._to_image(
            self.connection.request("/images/{}".format(image_id)).object["image"]
        )

    def delete_image(self, image):
        """
        Delete a NodeImage

        @inherits: :class:`NodeDriver.delete_image`

        :param      image: image witch should be used
        :type       image: :class:`NodeImage`

        :rtype: ``bool``
        """
        resp = self.connection.request("/images/{}".format(image.id), method="DELETE")
        return resp.status == httplib.NO_CONTENT

    def _node_action(self, node, action, **params):
        params = params or None
        return self.connection.request(
            "/servers/{}/action".format(node.id), method="POST", data={action: params}
        )

    def _update_node(self, node, **node_updates):
        """
        Updates the editable attributes of a server, which currently include
        its name and IPv4/IPv6 access addresses.
        """
        return self._to_node(
            self.connection.request(
                "/servers/{}".format(node.id), method="PUT", data={"server": node_updates}
            ).object["server"]
        )

    def _to_node_from_obj(self, obj):
        return self._to_node(obj["server"])

    def _to_node(self, api_node):
        public_networks_labels = ["public", "internet"]

        public_ips, private_ips = [], []

        for label, values in api_node["addresses"].items():
            for value in values:
                ip = value["addr"]
                is_public_ip = False

                try:
                    is_public_ip = is_public_subnet(ip)
                except Exception:
                    # IPv6

                    # Openstack Icehouse sets 'OS-EXT-IPS:type' to 'floating'
                    # for public and 'fixed' for private
                    explicit_ip_type = value.get("OS-EXT-IPS:type", None)

                    if label in public_networks_labels:
                        is_public_ip = True
                    elif explicit_ip_type == "floating":
                        is_public_ip = True
                    elif explicit_ip_type == "fixed":
                        is_public_ip = False

                if is_public_ip:
                    public_ips.append(ip)
                else:
                    private_ips.append(ip)

        # Sometimes 'image' attribute is not present if the node is in an error
        # state
        image = api_node.get("image", None)
        image_id = image.get("id", None) if image else None
        config_drive = api_node.get("config_drive", False)
        volumes_attached = api_node.get("os-extended-volumes:volumes_attached")
        created = parse_date(api_node["created"])

        return Node(
            id=api_node["id"],
            name=api_node["name"],
            state=self.NODE_STATE_MAP.get(api_node["status"], NodeState.UNKNOWN),
            public_ips=public_ips,
            private_ips=private_ips,
            created_at=created,
            driver=self,
            extra=dict(
                addresses=api_node["addresses"],
                hostId=api_node["hostId"],
                access_ip=api_node.get("accessIPv4"),
                access_ipv6=api_node.get("accessIPv6", None),
                # Docs says "tenantId", but actual is "tenant_id". *sigh*
                # Best handle both.
                tenantId=api_node.get("tenant_id") or api_node["tenantId"],
                userId=api_node.get("user_id", None),
                imageId=image_id,
                flavorId=api_node.get("flavor", {}).get("id", None),
                flavor_details=api_node.get("flavor", None),
                uri=next(link["href"] for link in api_node["links"] if link["rel"] == "self"),
                # pylint: disable=no-member
                service_name=self.connection.get_service_name(),
                metadata=api_node["metadata"],
                password=api_node.get("adminPass", None),
                created=api_node["created"],
                updated=api_node["updated"],
                key_name=api_node.get("key_name", None),
                disk_config=api_node.get("OS-DCF:diskConfig", None),
                config_drive=config_drive,
                availability_zone=api_node.get("OS-EXT-AZ:availability_zone"),
                volumes_attached=volumes_attached,
                task_state=api_node.get("OS-EXT-STS:task_state", None),
                vm_state=api_node.get("OS-EXT-STS:vm_state", None),
                power_state=api_node.get("OS-EXT-STS:power_state", None),
                progress=api_node.get("progress", None),
                fault=api_node.get("fault"),
            ),
        )

    def _to_volume(self, api_node):
        if "volume" in api_node:
            api_node = api_node["volume"]

        state = self.VOLUME_STATE_MAP.get(api_node["status"], StorageVolumeState.UNKNOWN)

        return StorageVolume(
            id=api_node["id"],
            name=api_node.get("displayName", api_node.get("name")),
            size=api_node["size"],
            state=state,
            driver=self,
            extra={
                "description": api_node.get("displayDescription", api_node.get("description")),
                "attachments": [att for att in api_node["attachments"] if att],
                # TODO: remove in 1.18.0
                "state": api_node.get("status", None),
                "snapshot_id": api_node.get("snapshot_id", api_node.get("snapshotId")),
                "location": api_node.get("availability_zone", api_node.get("availabilityZone")),
                "volume_type": api_node.get("volume_type", api_node.get("volumeType")),
                "metadata": api_node.get("metadata", None),
                "created_at": api_node.get("created_at", api_node.get("createdAt")),
            },
        )

    def _to_snapshot(self, data):
        if "snapshot" in data:
            data = data["snapshot"]

        volume_id = data.get("volume_id", data.get("volumeId", None))
        display_name = data.get("name", data.get("display_name", data.get("displayName", None)))
        created_at = data.get("created_at", data.get("createdAt", None))
        description = data.get(
            "description",
            data.get("display_description", data.get("displayDescription", None)),
        )
        status = data.get("status", None)

        extra = {
            "volume_id": volume_id,
            "name": display_name,
            "created": created_at,
            "description": description,
            "status": status,
        }

        state = self.SNAPSHOT_STATE_MAP.get(status, VolumeSnapshotState.UNKNOWN)

        try:
            created_dt = parse_date(created_at)
        except ValueError:
            created_dt = None

        snapshot = VolumeSnapshot(
            id=data["id"],
            driver=self,
            size=data["size"],
            extra=extra,
            created=created_dt,
            state=state,
            name=display_name,
        )
        return snapshot

    def _to_size(self, api_flavor, price=None, bandwidth=None):
        # if provider-specific subclasses can get better values for
        # price/bandwidth, then can pass them in when they super().
        if not price:
            price = self._get_size_price(str(api_flavor["id"]))

        extra = api_flavor.get("OS-FLV-WITH-EXT-SPECS:extra_specs", {})
        extra["disabled"] = api_flavor.get("OS-FLV-DISABLED:disabled", None)
        return OpenStackNodeSize(
            id=api_flavor["id"],
            name=api_flavor["name"],
            ram=api_flavor["ram"],
            disk=api_flavor["disk"],
            vcpus=api_flavor["vcpus"],
            ephemeral_disk=api_flavor.get("OS-FLV-EXT-DATA:ephemeral", None),
            swap=api_flavor["swap"],
            extra=extra,
            bandwidth=bandwidth,
            price=price,
            driver=self,
        )

    def _get_size_price(self, size_id):
        try:
            return get_size_price(
                driver_type="compute",
                driver_name=self.api_name,
                size_id=size_id,
            )
        except KeyError:
            return 0.0

    def _extract_image_id_from_url(self, location_header):
        path = urlparse.urlparse(location_header).path
        image_id = path.split("/")[-1]
        return image_id

    def ex_rescue(self, node, password=None):
        # Requires Rescue Mode extension
        """
        Rescue a node

        :param      node: node
        :type       node: :class:`Node`

        :param      password: password
        :type       password: ``str``

        :rtype: :class:`Node`
        """
        if password:
            resp = self._node_action(node, "rescue", adminPass=password)
        else:
            resp = self._node_action(node, "rescue")
            password = json.loads(resp.body)["adminPass"]
        node.extra["password"] = password
        return node

    def ex_unrescue(self, node):
        """
        Unrescue a node

        :param      node: node
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        resp = self._node_action(node, "unrescue")
        return resp.status == httplib.ACCEPTED

    def _to_floating_ip_pools(self, obj):
        pool_elements = obj["floating_ip_pools"]
        return [self._to_floating_ip_pool(pool) for pool in pool_elements]

    def _to_floating_ip_pool(self, obj):
        return OpenStack_1_1_FloatingIpPool(obj["name"], self.connection)

    def ex_list_floating_ip_pools(self):
        """
        List available floating IP pools

        :rtype: ``list`` of :class:`OpenStack_1_1_FloatingIpPool`
        """
        return self._to_floating_ip_pools(self.connection.request("/os-floating-ip-pools").object)

    def _to_floating_ips(self, obj):
        ip_elements = obj["floating_ips"]
        return [self._to_floating_ip(ip) for ip in ip_elements]

    def _to_floating_ip(self, obj):
        return OpenStack_1_1_FloatingIpAddress(
            id=obj["id"],
            ip_address=obj["ip"],
            pool=None,
            node_id=obj["instance_id"],
            driver=self,
        )

    def ex_list_floating_ips(self):
        """
        List floating IPs

        :rtype: ``list`` of :class:`OpenStack_1_1_FloatingIpAddress`
        """
        return self._to_floating_ips(self.connection.request("/os-floating-ips").object)

    def ex_get_floating_ip(self, ip):
        """
        Get specified floating IP

        :param      ip: floating IP to get
        :type       ip: ``str``

        :rtype: :class:`OpenStack_1_1_FloatingIpAddress`
        """
        for floating_ip in self.ex_list_floating_ips():
            if floating_ip.ip_address == ip:
                return floating_ip
        return None

    def ex_create_floating_ip(self, ip_pool=None):
        """
        Create new floating IP. The ip_pool attribute is optional only if your
        infrastructure has only one IP pool available.

        :param      ip_pool: name of the floating IP pool
        :type       ip_pool: ``str``

        :rtype: :class:`OpenStack_1_1_FloatingIpAddress`
        """
        data = {"pool": ip_pool} if ip_pool is not None else {}
        resp = self.connection.request("/os-floating-ips", method="POST", data=data)

        data = resp.object["floating_ip"]
        id = data["id"]
        ip_address = data["ip"]
        return OpenStack_1_1_FloatingIpAddress(
            id=id, ip_address=ip_address, pool=None, node_id=None, driver=self
        )

    def ex_delete_floating_ip(self, ip):
        """
        Delete specified floating IP

        :param      ip: floating IP to remove
        :type       ip: :class:`OpenStack_1_1_FloatingIpAddress`

        :rtype: ``bool``
        """
        resp = self.connection.request("/os-floating-ips/%s" % ip.id, method="DELETE")
        return resp.status in (httplib.NO_CONTENT, httplib.ACCEPTED)

    def ex_attach_floating_ip_to_node(self, node, ip):
        """
        Attach the floating IP to the node

        :param      node: node
        :type       node: :class:`Node`

        :param      ip: floating IP to attach
        :type       ip: ``str`` or :class:`OpenStack_1_1_FloatingIpAddress`

        :rtype: ``bool``
        """
        address = ip.ip_address if hasattr(ip, "ip_address") else ip
        data = {"addFloatingIp": {"address": address}}
        resp = self.connection.request("/servers/%s/action" % node.id, method="POST", data=data)
        return resp.status == httplib.ACCEPTED

    def ex_detach_floating_ip_from_node(self, node, ip):
        """
        Detach the floating IP from the node

        :param      node: node
        :type       node: :class:`Node`

        :param      ip: floating IP to remove
        :type       ip: ``str`` or :class:`OpenStack_1_1_FloatingIpAddress`

        :rtype: ``bool``
        """
        address = ip.ip_address if hasattr(ip, "ip_address") else ip
        data = {"removeFloatingIp": {"address": address}}
        resp = self.connection.request("/servers/%s/action" % node.id, method="POST", data=data)
        return resp.status == httplib.ACCEPTED

    def ex_get_metadata_for_node(self, node):
        """
        Return the metadata associated with the node.

        :param      node: Node instance
        :type       node: :class:`Node`

        :return: A dictionary or other mapping of strings to strings,
                 associating tag names with tag values.
        :type tags: ``dict``
        """
        return node.extra["metadata"]

    def ex_pause_node(self, node):
        return self._post_simple_node_action(node, "pause")

    def ex_unpause_node(self, node):
        return self._post_simple_node_action(node, "unpause")

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

    def ex_suspend_node(self, node):
        return self._post_simple_node_action(node, "suspend")

    def ex_resume_node(self, node):
        return self._post_simple_node_action(node, "resume")

    def _post_simple_node_action(self, node, action):
        """Post a simple, data-less action to the OS node action endpoint
        :param `Node` node:
        :param str action: the action to call
        :return `bool`: a boolean that indicates success
        """
        uri = "/servers/{node_id}/action".format(node_id=node.id)
        resp = self.connection.request(uri, method="POST", data={action: None})
        return resp.status == httplib.ACCEPTED


class OpenStack_2_Connection(OpenStackComputeConnection):
    responseCls = OpenStack_1_1_Response
    accept_format = "application/json"
    default_content_type = "application/json; charset=UTF-8"

    def encode_data(self, data):
        return json.dumps(data)


class OpenStack_2_ImageConnection(OpenStackImageConnection):
    responseCls = OpenStack_1_1_Response
    accept_format = "application/json"
    default_content_type = "application/json; charset=UTF-8"

    def encode_data(self, data):
        return json.dumps(data)


class OpenStack_2_NetworkConnection(OpenStackNetworkConnection):
    responseCls = OpenStack_1_1_Response
    accept_format = "application/json"
    default_content_type = "application/json; charset=UTF-8"

    def encode_data(self, data):
        return json.dumps(data)


class OpenStack_2_VolumeV2Connection(OpenStackVolumeV2Connection):
    responseCls = OpenStack_1_1_Response
    accept_format = "application/json"
    default_content_type = "application/json; charset=UTF-8"

    def encode_data(self, data):
        return json.dumps(data)


class OpenStack_2_VolumeV3Connection(OpenStackVolumeV3Connection):
    responseCls = OpenStack_1_1_Response
    accept_format = "application/json"
    default_content_type = "application/json; charset=UTF-8"

    def encode_data(self, data):
        return json.dumps(data)


class OpenStack_2_PortInterfaceState(Type):
    """
    Standard states of OpenStack_2_PortInterfaceState
    """

    BUILD = "build"
    ACTIVE = "active"
    DOWN = "down"
    UNKNOWN = "unknown"


class OpenStack_2_NodeDriver(OpenStack_1_1_NodeDriver):
    """
    OpenStack node driver.
    """

    connectionCls = OpenStack_2_Connection

    # Previously all image functionality was available through the
    # compute API. This deprecated proxied API does not offer all
    # functionality that the Glance Image service API offers.
    # See https://developer.openstack.org/api-ref/compute/
    #
    # > These APIs are proxy calls to the Image service. Nova has deprecated
    # > all the proxy APIs and users should use the native APIs instead. These
    # > will fail with a 404 starting from microversion 2.36. See: Relevant
    # > Image APIs.
    #
    # For example, managing image visibility and sharing machine
    # images across tenants can not be done using the proxied image API in the
    # compute endpoint, but it can be done with the Glance Image API.
    # See https://developer.openstack.org/api-ref/
    # image/v2/index.html#list-image-members
    image_connectionCls = OpenStack_2_ImageConnection
    image_connection = None

    # Similarly not all node-related operations are exposed through the
    # compute API
    # See https://developer.openstack.org/api-ref/compute/
    # For example, creating a new node in an OpenStack that is configured to
    # create a new port for every new instance will make it so that if that
    # port is detached it disappears. But if the port is manually created
    # beforehand using the neutron network API and node is booted with that
    # port pre-specified, then detaching that port later will result in that
    # becoming a re-attachable resource much like a floating ip. So because
    # even though this is the compute driver, we do connect to the networking
    # API here because some operations relevant for compute can only be
    # accessed from there.
    network_connectionCls = OpenStack_2_NetworkConnection
    network_connection = None

    # Similarly all image operations are not exposed through the block-storage
    # API of the cinder service:
    # https://developer.openstack.org/api-ref/block-storage/
    volumev2_connectionCls = OpenStack_2_VolumeV2Connection
    volumev3_connectionCls = OpenStack_2_VolumeV3Connection
    volumev2_connection = None
    volumev3_connection = None
    volume_connection = None

    type = Provider.OPENSTACK

    features = {"create_node": ["generates_password"]}
    _networks_url_prefix = "/v2.0/networks"
    _subnets_url_prefix = "/v2.0/subnets"

    PORT_INTERFACE_MAP = {
        "BUILD": OpenStack_2_PortInterfaceState.BUILD,
        "ACTIVE": OpenStack_2_PortInterfaceState.ACTIVE,
        "DOWN": OpenStack_2_PortInterfaceState.DOWN,
        "UNKNOWN": OpenStack_2_PortInterfaceState.UNKNOWN,
    }

    def __init__(self, *args, **kwargs):
        original_connectionCls = self.connectionCls
        self._ex_force_api_version = str(kwargs.pop("ex_force_api_version", None))
        if "ex_force_auth_version" not in kwargs:
            kwargs["ex_force_auth_version"] = "3.x_password"

        original_ex_force_base_url = kwargs.get("ex_force_base_url")

        # We run the init once to get the Glance V2 API connection
        # and put that on the object under self.image_connection.
        if original_ex_force_base_url or kwargs.get("ex_force_image_url"):
            kwargs["ex_force_base_url"] = str(
                kwargs.pop("ex_force_image_url", original_ex_force_base_url)
            )
        self.connectionCls = self.image_connectionCls
        super().__init__(*args, **kwargs)
        self.image_connection = self.connection

        # We run the init once to get the Cinder V2 API connection
        # and put that on the object under self.volumev2_connection.
        if original_ex_force_base_url or kwargs.get("ex_force_volume_url"):
            kwargs["ex_force_base_url"] = str(
                kwargs.pop("ex_force_volume_url", original_ex_force_base_url)
            )
        # the V3 API
        self.connectionCls = self.volumev3_connectionCls
        super().__init__(*args, **kwargs)
        self.volumev3_connection = self.connection
        # the V2 API
        self.connectionCls = self.volumev2_connectionCls
        super().__init__(*args, **kwargs)
        self.volumev2_connection = self.connection

        # We run the init once to get the Neutron V2 API connection
        # and put that on the object under self.network_connection.
        if original_ex_force_base_url or kwargs.get("ex_force_network_url"):
            kwargs["ex_force_base_url"] = str(
                kwargs.pop("ex_force_network_url", original_ex_force_base_url)
            )
        self.connectionCls = self.network_connectionCls
        super().__init__(*args, **kwargs)
        self.network_connection = self.connection

        # We run the init once again to get the compute API connection
        # and that's put under self.connection as normal.
        self._ex_force_base_url = original_ex_force_base_url
        if original_ex_force_base_url:
            kwargs["ex_force_base_url"] = self._ex_force_base_url
        # if ex_force_base_url is not set in original params delete it
        elif "ex_force_base_url" in kwargs:
            del kwargs["ex_force_base_url"]
        self.connectionCls = original_connectionCls
        super().__init__(*args, **kwargs)

    def _to_port(self, element):
        created = element.get("created_at")
        updated = element.get("updated_at")
        return OpenStack_2_PortInterface(
            id=element["id"],
            state=self.PORT_INTERFACE_MAP.get(
                element.get("status"), OpenStack_2_PortInterfaceState.UNKNOWN
            ),
            created=created,
            driver=self,
            extra=dict(
                admin_state_up=element.get("admin_state_up"),
                allowed_address_pairs=element.get("allowed_address_pairs"),
                binding_vnic_type=element.get("binding:vnic_type"),
                binding_host_id=element.get("binding:host_id", None),
                device_id=element.get("device_id"),
                description=element.get("description", None),
                device_owner=element.get("device_owner"),
                fixed_ips=element.get("fixed_ips"),
                mac_address=element.get("mac_address"),
                name=element.get("name"),
                network_id=element.get("network_id"),
                project_id=element.get("project_id", None),
                port_security_enabled=element.get("port_security_enabled", None),
                revision_number=element.get("revision_number", None),
                security_groups=element.get("security_groups"),
                tags=element.get("tags", None),
                tenant_id=element.get("tenant_id"),
                updated=updated,
            ),
        )

    def list_nodes(self, ex_all_tenants=False):
        """
        List the nodes in a tenant

        :param ex_all_tenants: List nodes for all the tenants. Note: Your user
                               must have admin privileges for this
                               functionality to work.
        :type ex_all_tenants: ``bool``
        """
        params = {}
        if ex_all_tenants:
            params = {"all_tenants": 1}
        return self._to_nodes(
            self._paginated_request("/servers/detail", "servers", self.connection, params=params)
        )

    def get_image(self, image_id):
        """
        Get a NodeImage using the V2 Glance API

        @inherits: :class:`OpenStack_1_1_NodeDriver.get_image`

        :param      image_id: ID of the image which should be used
        :type       image_id: ``str``

        :rtype: :class:`NodeImage`
        """
        return self._to_image(
            self.image_connection.request("/v2/images/{}".format(image_id)).object
        )

    def list_images(self, location=None, ex_only_active=True):
        """
        Lists all active images using the V2 Glance API

        @inherits: :class:`NodeDriver.list_images`

        :param location: Which data center to list the images in. If
                               empty, undefined behavior will be selected.
                               (optional)
        :type location: :class:`.NodeLocation`

        :param ex_only_active: True if list only active (optional)
        :type ex_only_active: ``bool``
        """
        if location is not None:
            raise NotImplementedError(
                "location in list_images is not implemented " "in the OpenStack_2_NodeDriver"
            )
        if not ex_only_active:
            raise NotImplementedError(
                "ex_only_active in list_images is not implemented " "in the OpenStack_2_NodeDriver"
            )

        result = self._paginated_request_next(
            path="/v2/images",
            request_method=self.image_connection.request,
            response_key="images",
        )

        images = []
        for item in result:
            images.append(self._to_image(item))

        return images

    def ex_update_image(self, image_id, data):
        """
        Patch a NodeImage. Can be used to set visibility

        :param      image_id: ID of the image which should be used
        :type       image_id: ``str``

        :param      data: The data to PATCH, either a dict or a list
        for example: [
          {'op': 'replace', 'path': '/visibility', 'value': 'shared'}
        ]
        :type       data: ``dict|list``

        :rtype: :class:`NodeImage`
        """
        response = self.image_connection.request(
            "/v2/images/{}".format(image_id),
            headers={"Content-type": "application/" "openstack-images-" "v2.1-json-patch"},
            method="PATCH",
            data=data,
        )
        return self._to_image(response.object)

    def ex_list_image_members(self, image_id):
        """
        List all members of an image. See
        https://developer.openstack.org/api-ref/image/v2/index.html#sharing

        :param      image_id: ID of the image of which the members should
        be listed
        :type       image_id: ``str``

        :rtype: ``list`` of :class:`NodeImageMember`
        """
        response = self.image_connection.request("/v2/images/{}/members".format(image_id))
        image_members = []
        for image_member in response.object["members"]:
            image_members.append(self._to_image_member(image_member))
        return image_members

    def ex_create_image_member(self, image_id, member_id):
        """
        Give a project access to an image.

        The image should have visibility status 'shared'.

        Note that this is not an idempotent operation. If this action is
        attempted using a tenant that is already in the image members
        group the API will throw a Conflict (409).
        See the 'create-image-member' section on
        https://developer.openstack.org/api-ref/image/v2/index.html

        :param str image_id: The ID of the image to share with the specified
        tenant
        :param str member_id: The ID of the project / tenant (the image member)
        Note that this is the Keystone project ID and not the project name,
        so something like e2151b1fe02d4a8a2d1f5fc331522c0a
        :return None:

        :param      image_id: ID of the image to share
        :type       image_id: ``str``

        :param      project: ID of the project to give access to the image
        :type       image_id: ``str``

        :rtype: ``list`` of :class:`NodeImageMember`
        """
        data = {"member": member_id}
        response = self.image_connection.request(
            "/v2/images/%s/members" % image_id, method="POST", data=data
        )
        return self._to_image_member(response.object)

    def ex_get_image_member(self, image_id, member_id):
        """
        Get a member of an image by id

        :param      image_id: ID of the image of which the member should
        be listed
        :type       image_id: ``str``

        :param      member_id: ID of the member to list
        :type       image_id: ``str``

        :rtype: ``list`` of :class:`NodeImageMember`
        """
        response = self.image_connection.request(
            "/v2/images/{}/members/{}".format(image_id, member_id)
        )
        return self._to_image_member(response.object)

    def ex_accept_image_member(self, image_id, member_id):
        """
        Accept a pending image as a member.

        This call is idempotent unlike ex_create_image_member,
        you can accept the same image many times.

        :param      image_id: ID of the image to accept
        :type       image_id: ``str``

        :param      project: ID of the project to accept the image as
        :type       image_id: ``str``

        :rtype: ``bool``
        """
        data = {"status": "accepted"}
        response = self.image_connection.request(
            "/v2/images/{}/members/{}".format(image_id, member_id), method="PUT", data=data
        )
        return self._to_image_member(response.object)

    def _to_networks(self, obj):
        networks = obj["networks"]
        return [self._to_network(network) for network in networks]

    def _to_network(self, obj):
        extra = {}
        if obj.get("router:external", None):
            extra["router:external"] = obj.get("router:external")
        if obj.get("subnets", None):
            extra["subnets"] = obj.get("subnets")
        if obj.get("tags", None):
            extra["tags"] = obj.get("tags")
        if obj.get("is_default", None) is not None:
            extra["is_default"] = obj.get("is_default")
        if obj.get("description", None) is not None:
            extra["description"] = obj.get("description")
        return OpenStackNetwork(id=obj["id"], name=obj["name"], cidr=None, driver=self, extra=extra)

    def ex_list_networks(self):
        """
        Get a list of Networks that are available.

        :rtype: ``list`` of :class:`OpenStackNetwork`
        """
        response = self.network_connection.request(self._networks_url_prefix).object
        return self._to_networks(response)

    def ex_get_network(self, network_id):
        """
        Retrieve the Network with the given ID

        :param networkId: ID of the network
        :type networkId: ``str``

        :rtype :class:`OpenStackNetwork`
        """
        request_url = "{networks_url_prefix}/{network_id}".format(
            networks_url_prefix=self._networks_url_prefix, network_id=network_id
        )
        response = self.network_connection.request(request_url).object
        return self._to_network(response["network"])

    def ex_create_network(self, name, **kwargs):
        """
        Create a new Network

        :param name: Name of network which should be used
        :type name: ``str``

        :rtype: :class:`OpenStackNetwork`
        """
        data = {"network": {"name": name}}
        # Add optional values
        for key, value in kwargs.items():
            data["network"][key] = value
        response = self.network_connection.request(
            self._networks_url_prefix, method="POST", data=data
        ).object
        return self._to_network(response["network"])

    def ex_delete_network(self, network):
        """
        Delete a Network

        :param network: Network which should be used
        :type network: :class:`OpenStackNetwork`

        :rtype: ``bool``
        """
        resp = self.network_connection.request(
            "{}/{}".format(self._networks_url_prefix, network.id), method="DELETE"
        )
        return resp.status in (httplib.NO_CONTENT, httplib.ACCEPTED)

    def _to_subnets(self, obj):
        subnets = obj["subnets"]
        return [self._to_subnet(subnet) for subnet in subnets]

    def _to_subnet(self, obj):
        extra = {}
        if obj.get("router:external", None):
            extra["router:external"] = obj.get("router:external")
        if obj.get("subnets", None):
            extra["subnets"] = obj.get("subnets")
        return OpenStack_2_SubNet(
            id=obj["id"],
            name=obj["name"],
            cidr=obj["cidr"],
            network_id=obj["network_id"],
            driver=self,
            extra=extra,
        )

    def ex_list_subnets(self):
        """
        Get a list of Subnet that are available.

        :rtype: ``list`` of :class:`OpenStack_2_SubNet`
        """
        response = self.network_connection.request(self._subnets_url_prefix).object
        return self._to_subnets(response)

    def ex_create_subnet(
        self,
        name,
        network,
        cidr,
        ip_version=4,
        description="",
        dns_nameservers=None,
        host_routes=None,
    ):
        """
        Create a new Subnet

        :param name: Name of subnet which should be used
        :type name: ``str``

        :param network: Parent network of the subnet
        :type network: ``OpenStackNetwork``

        :param cidr: cidr of network which should be used
        :type cidr: ``str``

        :param ip_version: ip_version of subnet which should be used
        :type ip_version: ``int``

        :param description: Description for the resource.
        :type description: ``str``

        :param dns_nameservers: List of dns name servers.
        :type dns_nameservers: ``list`` of ``str``

        :param host_routes: Additional routes for the subnet.
        :type host_routes: ``list`` of ``str``

        :rtype: :class:`OpenStack_2_SubNet`
        """
        data = {
            "subnet": {
                "cidr": cidr,
                "network_id": network.id,
                "ip_version": ip_version,
                "name": name or "",
                "description": description or "",
                "dns_nameservers": dns_nameservers or [],
                "host_routes": host_routes or [],
            }
        }
        response = self.network_connection.request(
            self._subnets_url_prefix, method="POST", data=data
        ).object
        return self._to_subnet(response["subnet"])

    def ex_delete_subnet(self, subnet):
        """
        Delete a Subnet

        :param subnet: Subnet which should be deleted
        :type subnet: :class:`OpenStack_2_SubNet`

        :rtype: ``bool``
        """
        resp = self.network_connection.request(
            "{}/{}".format(self._subnets_url_prefix, subnet.id), method="DELETE"
        )
        return resp.status in (httplib.NO_CONTENT, httplib.ACCEPTED)

    def ex_update_subnet(
        self,
        subnet,
        name=None,
        description=None,
        dns_nameservers=None,
        host_routes=None,
    ):
        """
        Update data of an existing SubNet

        :param subnet: Subnet which should be updated
        :type subnet: :class:`OpenStack_2_SubNet`

        :param name: Name of subnet which should be used
        :type name: ``str``

        :param description: Description for the resource.
        :type description: ``str``

        :param dns_nameservers: List of dns name servers.
        :type dns_nameservers: ``list`` of ``str``

        :param host_routes: Additional routes for the subnet.
        :type host_routes: ``list`` of ``str``

        :rtype: :class:`OpenStack_2_SubNet`
        """
        data = {"subnet": {}}
        if name is not None:
            data["subnet"]["name"] = name
        if description is not None:
            data["subnet"]["description"] = description
        if dns_nameservers is not None:
            data["subnet"]["dns_nameservers"] = dns_nameservers
        if host_routes is not None:
            data["subnet"]["host_routes"] = host_routes
        response = self.network_connection.request(
            "{}/{}".format(self._subnets_url_prefix, subnet.id), method="PUT", data=data
        ).object
        return self._to_subnet(response["subnet"])

    def ex_list_ports(self):
        """
        List all OpenStack_2_PortInterfaces

        https://developer.openstack.org/api-ref/network/v2/#list-ports

        :rtype: ``list`` of :class:`OpenStack_2_PortInterface`
        """
        response = self._paginated_request("/v2.0/ports", "ports", self.network_connection)
        return [self._to_port(port) for port in response["ports"]]

    def ex_delete_port(self, port):
        """
        Delete an OpenStack_2_PortInterface

        https://developer.openstack.org/api-ref/network/v2/#delete-port

        :param      port: port interface to remove
        :type       port: :class:`OpenStack_2_PortInterface`

        :rtype: ``bool``
        """
        response = self.network_connection.request("/v2.0/ports/%s" % port.id, method="DELETE")
        return response.success()

    def ex_detach_port_interface(self, node, port):
        """
        Detaches an OpenStack_2_PortInterface interface from a Node.
        :param      node: node
        :type       node: :class:`Node`

        :param      port: port interface to detach
        :type       port: :class:`OpenStack_2_PortInterface`

        :rtype: ``bool``
        """
        return self.connection.request(
            "/servers/{}/os-interface/{}".format(node.id, port.id), method="DELETE"
        ).success()

    def ex_attach_port_interface(self, node, port):
        """
        Attaches an OpenStack_2_PortInterface to a Node.

        :param      node: node
        :type       node: :class:`Node`

        :param      port: port interface to attach
        :type       port: :class:`OpenStack_2_PortInterface`

        :rtype: ``bool``
        """
        data = {"interfaceAttachment": {"port_id": port.id}}
        return self.connection.request(
            "/servers/{}/os-interface".format(node.id), method="POST", data=data
        ).success()

    def ex_create_port(self, network, description=None, admin_state_up=True, name=None):
        """
        Creates a new OpenStack_2_PortInterface

        :param      network: ID of the network where the newly created
                    port should be attached to
        :type       network: :class:`OpenStackNetwork`

        :param      description: Description of the port
        :type       description: str

        :param      admin_state_up: The administrative state of the
                    resource, which is up or down
        :type       admin_state_up: bool

        :param      name: Human-readable name of the resource
        :type       name: str

        :rtype: :class:`OpenStack_2_PortInterface`
        """
        data = {
            "port": {
                "description": description or "",
                "admin_state_up": admin_state_up,
                "name": name or "",
                "network_id": network.id,
            }
        }
        response = self.network_connection.request("/v2.0/ports", method="POST", data=data)
        return self._to_port(response.object["port"])

    def ex_get_port(self, port_interface_id):
        """
        Retrieve the OpenStack_2_PortInterface with the given ID

        :param      port_interface_id: ID of the requested port
        :type       port_interface_id: str

        :return: :class:`OpenStack_2_PortInterface`
        """
        response = self.network_connection.request(
            "/v2.0/ports/{}".format(port_interface_id), method="GET"
        )
        return self._to_port(response.object["port"])

    def ex_update_port(
        self,
        port,
        description=None,
        admin_state_up=None,
        name=None,
        port_security_enabled=None,
        qos_policy_id=None,
        security_groups=None,
        allowed_address_pairs=None,
    ):
        """
        Update a OpenStack_2_PortInterface

        :param      port: port interface to update
        :type       port: :class:`OpenStack_2_PortInterface`

        :param      description: Description of the port
        :type       description: ``str``

        :param      admin_state_up: The administrative state of the
                    resource, which is up or down
        :type       admin_state_up: ``bool``

        :param      name: Human-readable name of the resource
        :type       name: ``str``

        :param      port_security_enabled: 	The port security status
        :type       port_security_enabled: ``bool``

        :param      qos_policy_id: QoS policy associated with the port
        :type       qos_policy_id: ``str``

        :param      security_groups: The IDs of security groups applied
        :type       security_groups: ``list`` of ``str``

        :param      allowed_address_pairs: IP and MAC address that the port
                    can use when sending packets if port_security_enabled is
                    true
        :type       allowed_address_pairs: ``list`` of ``dict`` containing
                    ip_address and mac_address; mac_address is optional, taken
                    from the port if not specified

        :rtype: :class:`OpenStack_2_PortInterface`
        """
        data = {"port": {}}
        if description is not None:
            data["port"]["description"] = description
        if admin_state_up is not None:
            data["port"]["admin_state_up"] = admin_state_up
        if name is not None:
            data["port"]["name"] = name
        if port_security_enabled is not None:
            data["port"]["port_security_enabled"] = port_security_enabled
        if qos_policy_id is not None:
            data["port"]["qos_policy_id"] = qos_policy_id
        if security_groups is not None:
            data["port"]["security_groups"] = security_groups
        if allowed_address_pairs is not None:
            data["port"]["allowed_address_pairs"] = allowed_address_pairs
        response = self.network_connection.request(
            "/v2.0/ports/{}".format(port.id), method="PUT", data=data
        )
        return self._to_port(response.object["port"])

    def ex_get_node_ports(self, node):
        """
        Get the list of OpenStack_2_PortInterface interfaces from a Node.
        :param      node: node
        :type       node: :class:`Node`

        :rtype: ``list`` of :class:`OpenStack_2_PortInterface`
        """
        response = self.connection.request("/servers/%s/os-interface" % node.id, method="GET")
        ports = []
        for port in response.object["interfaceAttachments"]:
            port["id"] = port.pop("port_id")
            ports.append(self._to_port(port))
        return ports

    def _get_volume_connection(self):
        """
        Get the correct Volume connection (v3 or v2)
        """
        if not self.volume_connection:
            try:
                # Try to use v3 API first
                # if the endpoint is not found
                self.volumev3_connection.get_service_catalog()
                self.volume_connection = self.volumev3_connection
            except LibcloudError:
                # then return the v2 conn
                self.volume_connection = self.volumev2_connection
        return self.volume_connection

    def list_volumes(self):
        """
        Get a list of Volumes that are available.

        :rtype: ``list`` of :class:`StorageVolume`
        """
        return self._to_volumes(
            self._paginated_request("/volumes/detail", "volumes", self._get_volume_connection())
        )

    def ex_get_volume(self, volumeId):
        """
        Retrieve the StorageVolume with the given ID

        :param volumeId: ID of the volume
        :type volumeId: ``string``

        :return: :class:`StorageVolume`
        """
        return self._to_volume(
            self._get_volume_connection().request("/volumes/%s" % volumeId).object
        )

    def create_volume(
        self,
        size,
        name,
        location=None,
        snapshot=None,
        ex_volume_type=None,
        ex_image_ref=None,
    ):
        """
        Create a new volume.

        :param size: Size of volume in gigabytes (required)
        :type size: ``int``

        :param name: Name of the volume to be created
        :type name: ``str``

        :param location: Which data center to create a volume in. If
                               empty, undefined behavior will be selected.
                               (optional)
        :type location: :class:`.NodeLocation`

        :param snapshot:  Snapshot from which to create the new
                          volume.  (optional)
        :type snapshot:  :class:`.VolumeSnapshot`

        :param ex_volume_type: What kind of volume to create.
                            (optional)
        :type ex_volume_type: ``str``

        :param ex_image_ref: The image to create the volume from
                             when creating a bootable volume (optional)
        :type ex_image_ref: ``str``

        :return: The newly created volume.
        :rtype: :class:`StorageVolume`
        """
        volume = {
            "name": name,
            "description": name,
            "size": size,
            "metadata": {"contents": name},
        }

        if ex_volume_type:
            volume["volume_type"] = ex_volume_type

        if ex_image_ref:
            volume["imageRef"] = ex_image_ref

        if location:
            volume["availability_zone"] = location

        if snapshot:
            volume["snapshot_id"] = snapshot.id

        resp = self._get_volume_connection().request(
            "/volumes", method="POST", data={"volume": volume}
        )
        return self._to_volume(resp.object)

    def destroy_volume(self, volume):
        """
        Delete a Volume.

        :param volume: Volume to be deleted
        :type  volume: :class:`StorageVolume`

        :rtype: ``bool``
        """
        return (
            self._get_volume_connection()
            .request("/volumes/%s" % volume.id, method="DELETE")
            .success()
        )

    def ex_list_snapshots(self):
        """
        Get a list of Snapshot that are available.

        :rtype: ``list`` of :class:`VolumeSnapshot`
        """
        return self._to_snapshots(
            self._paginated_request("/snapshots/detail", "snapshots", self._get_volume_connection())
        )

    def create_volume_snapshot(self, volume, name=None, ex_description=None, ex_force=True):
        """
        Create snapshot from volume

        :param volume: Instance of `StorageVolume`
        :type  volume: `StorageVolume`

        :param name: Name of snapshot (optional)
        :type  name: `str` | `NoneType`

        :param ex_description: Description of the snapshot (optional)
        :type  ex_description: `str` | `NoneType`

        :param ex_force: Specifies if we create a snapshot that is not in
                         state `available`. For example `in-use`. Defaults
                         to True. (optional)
        :type  ex_force: `bool`

        :rtype: :class:`VolumeSnapshot`
        """
        data = {"snapshot": {"volume_id": volume.id, "force": ex_force}}

        if name is not None:
            data["snapshot"]["name"] = name

        if ex_description is not None:
            data["snapshot"]["description"] = ex_description

        return self._to_snapshot(
            self._get_volume_connection().request("/snapshots", method="POST", data=data).object
        )

    def destroy_volume_snapshot(self, snapshot):
        """
        Delete a Volume Snapshot.

        :param snapshot: Snapshot to be deleted
        :type  snapshot: :class:`VolumeSnapshot`

        :rtype: ``bool``
        """
        resp = self._get_volume_connection().request("/snapshots/%s" % snapshot.id, method="DELETE")
        return resp.status in (httplib.NO_CONTENT, httplib.ACCEPTED)

    def ex_list_security_groups(self):
        """
        Get a list of Security Groups that are available.

        :rtype: ``list`` of :class:`OpenStackSecurityGroup`
        """
        return self._to_security_groups(
            self.network_connection.request("/v2.0/security-groups").object
        )

    def ex_create_security_group(self, name, description):
        """
        Create a new Security Group

        :param name: Name of the new Security Group
        :type  name: ``str``

        :param description: Description of the new Security Group
        :type  description: ``str``

        :rtype: :class:`OpenStackSecurityGroup`
        """
        return self._to_security_group(
            self.network_connection.request(
                "/v2.0/security-groups",
                method="POST",
                data={"security_group": {"name": name, "description": description}},
            ).object["security_group"]
        )

    def ex_delete_security_group(self, security_group):
        """
        Delete a Security Group.

        :param security_group: Security Group should be deleted
        :type  security_group: :class:`OpenStackSecurityGroup`

        :rtype: ``bool``
        """
        resp = self.network_connection.request(
            "/v2.0/security-groups/%s" % (security_group.id), method="DELETE"
        )
        return resp.status == httplib.NO_CONTENT

    def _to_security_group_rule(self, obj):
        ip_range = group = tenant_id = parent_id = None
        protocol = from_port = to_port = direction = None

        if "parent_group_id" in obj:
            if obj["group"] == {}:
                ip_range = obj["ip_range"].get("cidr", None)
            else:
                group = obj["group"].get("name", None)
                tenant_id = obj["group"].get("tenant_id", None)

            parent_id = obj["parent_group_id"]
            from_port = obj["from_port"]
            to_port = obj["to_port"]
            protocol = obj["ip_protocol"]
        else:
            ip_range = obj.get("remote_ip_prefix", None)
            group = obj.get("remote_group_id", None)
            tenant_id = obj.get("tenant_id", None)

            parent_id = obj["security_group_id"]
            from_port = obj["port_range_min"]
            to_port = obj["port_range_max"]
            protocol = obj["protocol"]

        return OpenStackSecurityGroupRule(
            id=obj["id"],
            parent_group_id=parent_id,
            ip_protocol=protocol,
            from_port=from_port,
            to_port=to_port,
            driver=self,
            ip_range=ip_range,
            group=group,
            tenant_id=tenant_id,
            direction=direction,
        )

    def ex_create_security_group_rule(
        self,
        security_group,
        ip_protocol,
        from_port,
        to_port,
        cidr=None,
        source_security_group=None,
    ):
        """
        Create a new Rule in a Security Group

        :param security_group: Security Group in which to add the rule
        :type  security_group: :class:`OpenStackSecurityGroup`

        :param ip_protocol: Protocol to which this rule applies
                            Examples: tcp, udp, ...
        :type  ip_protocol: ``str``

        :param from_port: First port of the port range
        :type  from_port: ``int``

        :param to_port: Last port of the port range
        :type  to_port: ``int``

        :param cidr: CIDR notation of the source IP range for this rule
        :type  cidr: ``str``

        :param source_security_group: Existing Security Group to use as the
                                      source (instead of CIDR)
        :type  source_security_group: L{OpenStackSecurityGroup

        :rtype: :class:`OpenStackSecurityGroupRule`
        """
        source_security_group_id = None
        if type(source_security_group) == OpenStackSecurityGroup:
            source_security_group_id = source_security_group.id

        return self._to_security_group_rule(
            self.network_connection.request(
                "/v2.0/security-group-rules",
                method="POST",
                data={
                    "security_group_rule": {
                        "direction": "ingress",
                        "protocol": ip_protocol,
                        "port_range_min": from_port,
                        "port_range_max": to_port,
                        "remote_ip_prefix": cidr,
                        "remote_group_id": source_security_group_id,
                        "security_group_id": security_group.id,
                    }
                },
            ).object["security_group_rule"]
        )

    def ex_delete_security_group_rule(self, rule):
        """
        Delete a Rule from a Security Group.

        :param rule: Rule should be deleted
        :type  rule: :class:`OpenStackSecurityGroupRule`

        :rtype: ``bool``
        """
        resp = self.network_connection.request(
            "/v2.0/security-group-rules/%s" % (rule.id), method="DELETE"
        )
        return resp.status == httplib.NO_CONTENT

    def ex_remove_security_group_from_node(self, security_group, node):
        """
        Remove a Security Group from a node.

        :param security_group: Security Group to remove from node.
        :type  security_group: :class:`OpenStackSecurityGroup`

        :param      node: Node to remove the Security Group.
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        server_params = {"name": security_group.name}
        resp = self._node_action(node, "removeSecurityGroup", **server_params)
        return resp.status == httplib.ACCEPTED

    def _to_floating_ip_pool(self, obj):
        return OpenStack_2_FloatingIpPool(obj["id"], obj["name"], self.network_connection)

    def _to_floating_ip_pools(self, obj):
        pool_elements = obj["networks"]
        return [self._to_floating_ip_pool(pool) for pool in pool_elements]

    def ex_list_floating_ip_pools(self):
        """
        List available floating IP pools

        :rtype: ``list`` of :class:`OpenStack_2_FloatingIpPool`
        """
        return self._to_floating_ip_pools(
            self.network_connection.request(
                "/v2.0/networks?router:external" "=True&fields=id&fields=" "name"
            ).object
        )

    def _to_routers(self, obj):
        routers = obj["routers"]
        return [self._to_router(router) for router in routers]

    def _to_router(self, obj):
        extra = {}
        extra["external_gateway_info"] = obj["external_gateway_info"]
        extra["routes"] = obj["routes"]
        return OpenStack_2_Router(
            id=obj["id"],
            name=obj["name"],
            status=obj["status"],
            driver=self,
            extra=extra,
        )

    def ex_list_routers(self):
        """
        Get a list of Routers that are available.

        :rtype: ``list`` of :class:`OpenStack_2_Router`
        """
        response = self.network_connection.request("/v2.0/routers").object
        return self._to_routers(response)

    def ex_create_router(
        self, name, description="", admin_state_up=True, external_gateway_info=None
    ):
        """
        Create a new Router

        :param name: Name of router which should be used
        :type name: ``str``

        :param      description: Description of the port
        :type       description: ``str``

        :param      admin_state_up: The administrative state of the
                    resource, which is up or down
        :type       admin_state_up: ``bool``

        :param      external_gateway_info: The external gateway information
        :type       external_gateway_info: ``dict``

        :rtype: :class:`OpenStack_2_Router`
        """
        data = {
            "router": {
                "name": name or "",
                "description": description or "",
                "admin_state_up": admin_state_up,
            }
        }
        if external_gateway_info:
            data["router"]["external_gateway_info"] = external_gateway_info
        response = self.network_connection.request("/v2.0/routers", method="POST", data=data).object
        return self._to_router(response["router"])

    def ex_delete_router(self, router):
        """
        Delete a Router

        :param router: Router which should be deleted
        :type router: :class:`OpenStack_2_Router`

        :rtype: ``bool``
        """
        resp = self.network_connection.request(
            "{}/{}".format("/v2.0/routers", router.id), method="DELETE"
        )
        return resp.status in (httplib.NO_CONTENT, httplib.ACCEPTED)

    def _manage_router_interface(self, router, op, subnet=None, port=None):
        """
        Add/Remove interface to router

        :param router: Router to add/remove the interface
        :type router: :class:`OpenStack_2_Router`

        :param      op: Operation to perform: 'add' or 'remove'
        :type       op: ``str``

        :param subnet: Subnet object to be added to the router
        :type subnet: :class:`OpenStack_2_SubNet`

        :param port: Port object to be added to the router
        :type port: :class:`OpenStack_2_PortInterface`

        :rtype: ``bool``
        """
        data = {}
        if subnet:
            data["subnet_id"] = subnet.id
        elif port:
            data["port_id"] = port.id
        else:
            raise OpenStackException(
                "Error in router interface: " "port or subnet are None.", 500, self
            )

        resp = self.network_connection.request(
            "{}/{}/{}_router_interface".format("/v2.0/routers", router.id, op),
            method="PUT",
            data=data,
        )
        return resp.status == httplib.OK

    def ex_add_router_port(self, router, port):
        """
        Add port to a router

        :param router: Router to add the port
        :type router: :class:`OpenStack_2_Router`

        :param port: Port object to be added to the router
        :type port: :class:`OpenStack_2_PortInterface`

        :rtype: ``bool``
        """
        return self._manage_router_interface(router, "add", port=port)

    def ex_del_router_port(self, router, port):
        """
        Remove port from a router

        :param router: Router to remove the port
        :type router: :class:`OpenStack_2_Router`

        :param port: Port object to be added to the router
        :type port: :class:`OpenStack_2_PortInterface`

        :rtype: ``bool``
        """
        return self._manage_router_interface(router, "remove", port=port)

    def ex_add_router_subnet(self, router, subnet):
        """
        Add subnet to a router

        :param router: Router to add the subnet
        :type router: :class:`OpenStack_2_Router`

        :param subnet: Subnet object to be added to the router
        :type subnet: :class:`OpenStack_2_SubNet`

        :rtype: ``bool``
        """
        return self._manage_router_interface(router, "add", subnet=subnet)

    def ex_del_router_subnet(self, router, subnet):
        """
        Remove subnet to a router

        :param router: Router to remove the subnet
        :type router: :class:`OpenStack_2_Router`

        :param subnet: Subnet object to be added to the router
        :type subnet: :class:`OpenStack_2_SubNet`

        :rtype: ``bool``
        """
        return self._manage_router_interface(router, "remove", subnet=subnet)

    def _to_quota_set(self, obj):
        res = OpenStack_2_QuotaSet(
            id=obj["id"],
            cores=obj["cores"],
            instances=obj["instances"],
            key_pairs=obj["key_pairs"],
            metadata_items=obj["metadata_items"],
            ram=obj["ram"],
            server_groups=obj["server_groups"],
            server_group_members=obj["server_group_members"],
            fixed_ips=obj.get("fixed_ips", None),
            floating_ips=obj.get("floating_ips", None),
            networks=obj.get("networks", None),
            security_group_rules=obj.get("security_group_rules", None),
            security_groups=obj.get("security_groups", None),
            injected_file_content_bytes=obj.get("injected_file_content_bytes", None),
            injected_file_path_bytes=obj.get("injected_file_path_bytes", None),
            injected_files=obj.get("injected_files", None),
            driver=self.connection.driver,
        )

        return res

    def ex_get_quota_set(self, tenant_id, user_id=None):
        """
        Get the quota for a project or a project and a user.

        :param      tenant_id: The UUID of the tenant in a multi-tenancy cloud
        :type       tenant_id: ``str``

        :param      user_id: ID of user to list the quotas for.
        :type       user_id: ``str``

        :rtype: :class:`OpenStack_2_QuotaSet`
        """
        url = "/os-quota-sets/%s/detail" % tenant_id
        if user_id:
            url += "?user_id=%s" % user_id
        return self._to_quota_set(self.connection.request(url).object["quota_set"])

    def _to_network_quota(self, obj):
        res = OpenStack_2_NetworkQuota(
            floatingip=obj["floatingip"],
            network=obj["network"],
            port=obj["port"],
            rbac_policy=obj["rbac_policy"],
            router=obj.get("router", None),
            security_group=obj.get("security_group", None),
            security_group_rule=obj.get("security_group_rule", None),
            subnet=obj.get("subnet", None),
            subnetpool=obj.get("subnetpool", None),
            driver=self.connection.driver,
        )

        return res

    def ex_get_network_quotas(self, project_id):
        """
        Get the network quotas for a project

        :param      project_id: The ID of the project.
        :type       project_id: ``str``

        :rtype: :class:`OpenStack_2_NetworkQuota`
        """
        url = "/v2.0/quotas/%s/details.json" % project_id
        return self._to_network_quota(self.network_connection.request(url).object["quota"])

    def _to_volume_quota(self, obj):
        res = OpenStack_2_VolumeQuota(
            backup_gigabytes=obj.get("backup_gigabytes", None),
            gigabytes=obj.get("gigabytes", None),
            per_volume_gigabytes=obj.get("per_volume_gigabytes", None),
            backups=obj.get("backups", None),
            snapshots=obj.get("snapshots", None),
            volumes=obj.get("volumes", None),
            driver=self.connection.driver,
        )

        return res

    def ex_get_volume_quotas(self, project_id):
        """
        Get the volume quotas for a project

        :param      project_id: The ID of the project.
        :type       project_id: ``str``

        :rtype: :class:`OpenStack_2_VolumeQuota`
        """
        url = "/os-quota-sets/%s?usage=True" % project_id
        return self._to_volume_quota(self._get_volume_connection().request(url).object["quota_set"])

    def ex_list_server_groups(self):
        """
        List Server Groups

        :rtype: ``list`` of :class:`OpenStack_2_ServerGroup`
        """
        return self._to_server_groups(self.connection.request("/os-server-groups").object)

    def _to_server_groups(self, obj):
        sg_elements = obj["server_groups"]
        return [self._to_server_group(sg) for sg in sg_elements]

    def _to_server_group(self, obj):
        policy = None
        if "policy" in obj:
            policy = obj["policy"]
        elif "policies" in obj and obj["policies"]:
            policy = obj["policies"][0]
        return OpenStack_2_ServerGroup(
            id=obj["id"],
            name=obj["name"],
            policy=policy,
            members=obj.get("members"),
            rules=obj.get("rules"),
            driver=self.connection.driver,
        )

    def ex_get_server_group(self, server_group_id):
        """
        Get Server Group

        :rtype: :class:`OpenStack_2_ServerGroup`
        """
        return self._to_server_group(
            self.connection.request("/os-server-groups/%s" % server_group_id).object["server_group"]
        )

    def ex_add_server_group(self, name, policy, rules=[]):
        """
        Add a Server Group

        :param name: Server Group Name.
        :type name: ``str``
        :param policy: Server Group policy.
        :type policy: ``str``
        :param rules: Server Group rules.
        :type rules: ``list``

        :rtype: ``bool``
        """
        data = {"name": name}
        if rules:
            data["rules"] = rules
        try:
            # New in version 2.64
            data["policy"] = policy
            response = self.connection.request(
                "/os-server-groups", method="POST", data={"server_group": data}
            ).object
        except BaseHTTPError:
            # until version 2.63
            del data["policy"]
            data["policies"] = [policy]
            response = self.connection.request(
                "/os-server-groups", method="POST", data={"server_group": data}
            ).object
        return self._to_server_group(response["server_group"])

    def ex_del_server_group(self, server_group):
        """
        Delete a Server Group

        :param server_group: Server Group which should be deleted
        :type server_group: :class:`OpenStack_2_ServerGroup`

        :rtype: ``bool``
        """
        resp = self.connection.request("/os-server-groups/%s" % server_group.id, method="DELETE")
        return resp.status in (httplib.NO_CONTENT, httplib.ACCEPTED)

    def _to_floating_ips(self, obj):
        ip_elements = obj["floatingips"]
        return [self._to_floating_ip(ip) for ip in ip_elements]

    def _to_floating_ip(self, obj):
        extra = {}

        # In neutron version prior to 13.0.0 port_details does not exists
        extra["port_details"] = obj.get("port_details")
        extra["port_id"] = obj.get("port_id")
        extra["floating_network_id"] = obj.get("floating_network_id")

        return OpenStack_2_FloatingIpAddress(
            id=obj["id"],
            ip_address=obj["floating_ip_address"],
            pool=None,
            node_id=None,
            driver=self,
            extra=extra,
        )

    def ex_list_floating_ips(self):
        """
        List floating IPs
        :rtype: ``list`` of :class:`OpenStack_2_FloatingIpAddress`
        """
        return self._to_floating_ips(self.network_connection.request("/v2.0/floatingips").object)

    def ex_get_floating_ip(self, ip):
        """
        Get specified floating IP from the pool
        :param      ip: floating IP to get
        :type       ip: ``str``
        :rtype: :class:`OpenStack_2_FloatingIpAddress`
        """
        floating_ips = self._to_floating_ips(
            self.network_connection.request(
                "/v2.0/floatingips?floating_ip_address" "=%s" % ip
            ).object
        )
        return floating_ips[0] if floating_ips else None

    def ex_create_floating_ip(self, ip_pool):
        """
        Create new floating IP. The ip_pool attribute is optional only if your
        infrastructure has only one IP pool available.
        :param      ip_pool: name or id of the floating IP pool
        :type       ip_pool: ``str``
        :rtype: :class:`OpenStack_2_FloatingIpAddress`
        """
        for pool in self.ex_list_floating_ip_pools():
            if not ip_pool or ip_pool == pool.name or ip_pool == pool.id:
                return pool.create_floating_ip()

    def ex_delete_floating_ip(self, ip):
        """
        Delete specified floating IP
        :param      ip: floating IP to remove
        :type       ip: :class:`OpenStack_2_FloatingIpAddress`
        :rtype: ``bool``
        """
        resp = self.network_connection.request("/v2.0/floatingips/%s" % ip.id, method="DELETE")
        return resp.status in (httplib.NO_CONTENT, httplib.ACCEPTED)

    def ex_attach_floating_ip_to_node(self, node, ip, port_id=None):
        """
        Attach the floating IP to the node

        :param      node: node
        :type       node: :class:`Node`

        :param      ip: floating IP to attach
        :type       ip: ``str`` or :class:`OpenStack_1_1_FloatingIpAddress`

        :param      port_id: Optional node port ID to attach the floating IP
        :type       port_id: ``str``

        :rtype: ``bool``
        """
        ip_id = None
        if hasattr(ip, "id"):
            ip_id = ip.id
        else:
            for pool in self.ex_list_floating_ip_pools():
                fip = pool.get_floating_ip(ip)
                if fip:
                    ip_id = fip.id
        if not ip_id:
            return False
        if not port_id:
            ports = self.ex_get_node_ports(node)
            if ports:
                port_id = ports[0].id
        if port_id:
            # Set to the first node port
            resp = self.network_connection.request(
                "/v2.0/floatingips/%s" % ip_id,
                method="PUT",
                data={"floatingip": {"port_id": port_id}},
            )
            return resp.status == httplib.OK
        else:
            # if there are no ports
            return False

    def ex_detach_floating_ip_from_node(self, node, ip):
        """
        Detach the floating IP from the node

        :param      node: node
        :type       node: :class:`Node`

        :param      ip: floating IP to remove
        :type       ip: ``str`` or :class:`OpenStack_1_1_FloatingIpAddress`

        :rtype: ``bool``
        """
        ip_id = None
        if hasattr(ip, "id"):
            ip_id = ip.id
        else:
            for pool in self.ex_list_floating_ip_pools():
                fip = pool.get_floating_ip(ip)
                if fip:
                    ip_id = fip.id
        if not ip_id:
            return False
        resp = self.network_connection.request(
            "/v2.0/floatingips/%s" % ip_id,
            method="PUT",
            data={"floatingip": {"port_id": None}},
        )
        return resp.status == httplib.OK


class OpenStack_1_1_FloatingIpPool:
    """
    Floating IP Pool info.
    """

    def __init__(self, name, connection):
        self.name = name
        self.connection = connection

    def list_floating_ips(self):
        """
        List floating IPs in the pool

        :rtype: ``list`` of :class:`OpenStack_1_1_FloatingIpAddress`
        """
        return self._to_floating_ips(self.connection.request("/os-floating-ips").object)

    def _to_floating_ips(self, obj):
        ip_elements = obj["floating_ips"]
        return [self._to_floating_ip(ip) for ip in ip_elements]

    def _to_floating_ip(self, obj):
        return OpenStack_1_1_FloatingIpAddress(
            id=obj["id"],
            ip_address=obj["ip"],
            pool=self,
            node_id=obj["instance_id"],
            driver=self.connection.driver,
        )

    def get_floating_ip(self, ip):
        """
        Get specified floating IP from the pool

        :param      ip: floating IP to get
        :type       ip: ``str``

        :rtype: :class:`OpenStack_1_1_FloatingIpAddress`
        """
        for floating_ip in self.list_floating_ips():
            if floating_ip.ip_address == ip:
                return floating_ip
        return None

    def create_floating_ip(self):
        """
        Create new floating IP in the pool

        :rtype: :class:`OpenStack_1_1_FloatingIpAddress`
        """
        resp = self.connection.request("/os-floating-ips", method="POST", data={"pool": self.name})
        data = resp.object["floating_ip"]
        id = data["id"]
        ip_address = data["ip"]
        return OpenStack_1_1_FloatingIpAddress(
            id=id,
            ip_address=ip_address,
            pool=self,
            node_id=None,
            driver=self.connection.driver,
        )

    def delete_floating_ip(self, ip):
        """
        Delete specified floating IP from the pool

        :param      ip: floating IP to remove
        :type       ip: :class:`OpenStack_1_1_FloatingIpAddress`

        :rtype: ``bool``
        """
        resp = self.connection.request("/os-floating-ips/%s" % ip.id, method="DELETE")
        return resp.status in (httplib.NO_CONTENT, httplib.ACCEPTED)

    def __repr__(self):
        return "<OpenStack_1_1_FloatingIpPool: name=%s>" % self.name


class OpenStack_1_1_FloatingIpAddress:
    """
    Floating IP info.
    """

    def __init__(self, id, ip_address, pool, node_id=None, driver=None):
        self.id = str(id)
        self.ip_address = ip_address
        self.pool = pool
        self.node_id = node_id
        self.driver = driver

    def delete(self):
        """
        Delete this floating IP

        :rtype: ``bool``
        """
        if self.pool is not None:
            return self.pool.delete_floating_ip(self)
        elif self.driver is not None:
            return self.driver.ex_delete_floating_ip(self)

    def __repr__(self):
        return "<OpenStack_1_1_FloatingIpAddress: id=%s, ip_addr=%s," " pool=%s, driver=%s>" % (
            self.id,
            self.ip_address,
            self.pool,
            self.driver,
        )


class OpenStack_2_FloatingIpAddress(OpenStack_1_1_FloatingIpAddress):
    """
    Floating IP info 2.0.
    """

    def __init__(self, id, ip_address, pool, node_id=None, driver=None, extra=None):
        self.id = str(id)
        self.ip_address = ip_address
        self.pool = pool
        self.node_id = node_id
        self.driver = driver
        self.extra = extra if extra else {}

    def get_pool(self):
        if not self.pool:
            try:
                # If pool is not set, get the info from the floating_network_id
                net = self.driver.ex_get_network(self.extra["floating_network_id"])
            except Exception:
                net = None
            if net:
                self.pool = OpenStack_2_FloatingIpPool(
                    net.id, net.name, self.driver.network_connection
                )
        return self.pool

    def get_node_id(self):
        if not self.node_id:
            # if node id is not set, get it from port_details

            # In neutron version prior to 13.0.0 port_details does not exists
            if "port_details" not in self.extra or not self.extra["port_details"]:
                # if port_details is not available get if from port info using port_id
                try:
                    port = self.driver.ex_get_port(self.extra["port_id"])
                except Exception:
                    port = None
                if port:
                    self.extra["port_details"] = {
                        "device_id": port.extra["device_id"],
                        "device_owner": port.extra["device_owner"],
                        "mac_address": port.extra["mac_address"],
                    }

            if "port_details" in self.extra and self.extra["port_details"]:
                dev_owner = self.extra["port_details"]["device_owner"]
                if dev_owner and dev_owner.startswith("compute:"):
                    self.node_id = self.extra["port_details"]["device_id"]

        return self.node_id

    def __repr__(self):
        return "<OpenStack_2_FloatingIpAddress: id=%s, ip_addr=%s," " pool=%s, driver=%s>" % (
            self.id,
            self.ip_address,
            self.pool,
            self.driver,
        )


class OpenStack_2_FloatingIpPool:
    """
    Floating IP Pool info.
    """

    def __init__(self, id, name, connection):
        self.id = id
        self.name = name
        self.connection = connection

    def _to_floating_ips(self, obj):
        ip_elements = obj["floatingips"]
        return [self._to_floating_ip(ip) for ip in ip_elements]

    def _to_floating_ip(self, obj):
        extra = {}

        # In neutron version prior to 13.0.0 port_details does not exists
        extra["port_details"] = obj.get("port_details")
        extra["port_id"] = obj.get("port_id")

        return OpenStack_2_FloatingIpAddress(
            id=obj["id"],
            ip_address=obj["floating_ip_address"],
            pool=self,
            node_id=None,
            driver=self.connection.driver,
            extra=extra,
        )

    def list_floating_ips(self):
        """
        List floating IPs in the pool

        :rtype: ``list`` of :class:`OpenStack_2_FloatingIpAddress`
        """
        url = "/v2.0/floatingips?floating_network_id=%s" % self.id
        return self._to_floating_ips(self.connection.request(url).object)

    def get_floating_ip(self, ip):
        """
        Get specified floating IP from the pool

        :param      ip: floating IP to get
        :type       ip: ``str``

        :rtype: :class:`OpenStack_2_FloatingIpAddress`
        """
        url = "/v2.0/floatingips?floating_network_id=%s" % self.id
        url += "&floating_ip_address=%s" % ip
        floating_ips = self._to_floating_ips(self.connection.request(url).object)
        return floating_ips[0] if floating_ips else None

    def create_floating_ip(self):
        """
        Create new floating IP in the pool

        :rtype: :class:`OpenStack_2_FloatingIpAddress`
        """
        resp = self.connection.request(
            "/v2.0/floatingips",
            method="POST",
            data={"floatingip": {"floating_network_id": self.id}},
        )
        data = resp.object["floatingip"]
        return OpenStack_2_FloatingIpAddress(
            id=data["id"],
            ip_address=data["floating_ip_address"],
            pool=self,
            node_id=None,
            driver=self.connection.driver,
        )

    def delete_floating_ip(self, ip):
        """
        Delete specified floating IP from the pool

        :param      ip: floating IP to remove
        :type       ip: :class:`OpenStack_1_1_FloatingIpAddress`

        :rtype: ``bool``
        """
        resp = self.connection.request("/v2.0/floatingips/%s" % ip.id, method="DELETE")
        return resp.status in (httplib.NO_CONTENT, httplib.ACCEPTED)

    def __repr__(self):
        return "<OpenStack_2_FloatingIpPool: name=%s>" % self.name


class OpenStack_2_SubNet:
    """
    A Virtual SubNet.
    """

    def __init__(self, id, name, cidr, network_id, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.cidr = cidr
        self.network_id = network_id
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return '<OpenStack_2_SubNet id="{}" name="{}" cidr="{}">'.format(
            self.id,
            self.name,
            self.cidr,
        )


class OpenStack_2_Router:
    """
    A Virtual Router.
    """

    def __init__(self, id, name, status, driver, extra=None):
        self.id = str(id)
        self.name = name
        self.status = status
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return '<OpenStack_2_Router id="{}" name="{}">'.format(self.id, self.name)


class OpenStack_2_PortInterface(UuidMixin):
    """
    Port Interface info. Similar in functionality to a floating IP (can be
    attached / detached from a compute instance) but implementation-wise a
    bit different.

    > A port is a connection point for attaching a single device, such as the
    > NIC of a server, to a network. The port also describes the associated
    > network configuration, such as the MAC and IP addresses to be used on
    > that port.
    https://docs.openstack.org/python-openstackclient/pike/cli/command-objects/port.html

    Also see:
    https://developer.openstack.org/api-ref/compute/#port-interfaces-servers-os-interface
    """

    def __init__(self, id, state, driver, created=None, extra=None):
        """
        :param id: Port Interface ID.
        :type id: ``str``
        :param state: State of the OpenStack_2_PortInterface.
        :type state: :class:`.OpenStack_2_PortInterfaceState`
        :param      created: A datetime object that represents when the
                             port interface was created
        :type       created: ``datetime.datetime``
        :param extra: Optional provided specific attributes associated with
                      this image.
        :type extra: ``dict``
        """
        self.id = str(id)
        self.state = state
        self.driver = driver
        self.created = created
        self.extra = extra or {}
        UuidMixin.__init__(self)

    def delete(self):
        """
        Delete this Port Interface

        :rtype: ``bool``
        """
        return self.driver.ex_delete_port(self)

    def __repr__(self):
        return ("<OpenStack_2_PortInterface: id=%s, state=%s, " "driver=%s  ...>") % (
            self.id,
            self.state,
            self.driver.name,
        )


class OpenStack_2_QuotaSetItem:
    """
    Qouta Set Item info. Each item has three attributes: in_use,
    limit and reserved.

    See:
    https://docs.openstack.org/api-ref/compute/?expanded=show-the-detail-of-quota-detail#show-a-quota
    """

    def __init__(self, in_use, limit, reserved):
        """
        :param in_use: Number of currently used resources.
        :type in_use: ``int``
        :param limit: Max number of available resources.
        :type limit: ``int``
        :param reserved: Number of reserved resources.
        :type reserved: ``int``
        """
        self.in_use = in_use
        self.limit = limit
        self.reserved = reserved

    def __repr__(self):
        return '<OpenStack_2_QuotaSetItem in_use="%s", limit="%s",' 'reserved="%s">' % (
            self.in_use,
            self.limit,
            self.reserved,
        )


class OpenStack_2_QuotaSet:
    """
    Quota Set info. To get the information about quotas and used resources.

    See:
    https://docs.openstack.org/api-ref/compute/?expanded=show-the-detail-of-quota-detail#show-a-quota

    """

    def __init__(
        self,
        id,
        cores,
        instances,
        key_pairs,
        metadata_items,
        ram,
        server_groups,
        server_group_members,
        fixed_ips=None,
        floating_ips=None,
        networks=None,
        security_group_rules=None,
        security_groups=None,
        injected_file_content_bytes=None,
        injected_file_path_bytes=None,
        injected_files=None,
        driver=None,
    ):
        """
        :param id: Quota Set ID.
        :type id: ``str``
        :param cores: Quota Set of cores.
        :type cores: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param instances: Quota Set of instances.
        :type instances: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param key_pairs: Quota Set of key pairs.
        :type key_pairs: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param metadata_items: Quota Set of metadata items.
        :type metadata_items: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param ram: Quota Set of RAM.
        :type ram: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param server_groups: Quota Set of server groups.
        :type server_groups: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param fixed_ips: Quota Set of fixed ips. (optional)
        :type fixed_ips: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param floating_ips: Quota Set of floating ips. (optional)
        :type floating_ips: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param networks: Quota Set of networks. (optional)
        :type networks: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param security_group_rules: Quota Set of security group rules.
                                     (optional)
        :type security_group_rules: :class:`.OpenStack_2_QuotaSetItem`
                                    or ``dict``
        :param security_groups: Quota Set of security groups. (optional)
        :type security_groups: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param injected_file_content_bytes: Quota Set of injected file content
                                            bytes. (optional)
        :type injected_file_content_bytes: :class:`.OpenStack_2_QuotaSetItem`
                                           or ``dict``
        :param injected_file_path_bytes: Quota Set of injected file path bytes.
                                         (optional)
        :type injected_file_path_bytes: :class:`.OpenStack_2_QuotaSetItem`
                                        or ``dict``
        :param injected_files: Quota Set of injected files. (optional)
        :type injected_files: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        """
        self.id = str(id)
        self.cores = self._to_quota_set_item(cores)
        self.instances = self._to_quota_set_item(instances)
        self.key_pairs = self._to_quota_set_item(key_pairs)
        self.metadata_items = self._to_quota_set_item(metadata_items)
        self.ram = self._to_quota_set_item(ram)
        self.server_groups = self._to_quota_set_item(server_groups)
        self.server_group_members = self._to_quota_set_item(server_group_members)
        self.fixed_ips = self._to_quota_set_item(fixed_ips)
        self.floating_ips = self._to_quota_set_item(floating_ips)
        self.networks = self._to_quota_set_item(networks)
        self.security_group_rules = self._to_quota_set_item(security_group_rules)
        self.security_groups = self._to_quota_set_item(security_groups)
        self.injected_file_content_bytes = self._to_quota_set_item(injected_file_content_bytes)
        self.injected_file_path_bytes = self._to_quota_set_item(injected_file_path_bytes)
        self.injected_files = self._to_quota_set_item(injected_files)
        self.driver = driver

    def _to_quota_set_item(self, obj):
        if obj:
            if isinstance(obj, OpenStack_2_QuotaSetItem):
                return obj
            elif isinstance(obj, dict):
                return OpenStack_2_QuotaSetItem(obj["in_use"], obj["limit"], obj["reserved"])
        else:
            return None

    def __repr__(self):
        return '<OpenStack_2_QuotaSet id="%s", cores="%s", ram="%s",' ' instances="%s">' % (
            self.id,
            self.cores,
            self.ram,
            self.instances,
        )


class OpenStack_2_NetworkQuota:
    """
    Network Quota info. To get the information about quotas and used resources.

    See:
    https://docs.openstack.org/api-ref/network/v2/?expanded=show-quota-details-for-a-tenant-detail,list-quotas-for-a-project-detail#show-quota-details-for-a-tenant

    """

    def __init__(
        self,
        floatingip,
        network,
        port,
        rbac_policy,
        router,
        security_group,
        security_group_rule,
        subnet,
        subnetpool,
        driver=None,
    ):
        """
        :param floatingip: Quota of floating ips.
        :type floatingip: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param network: Quota of networks.
        :type network: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param port: Quota of ports.
        :type port: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param rbac_policy: Quota of rbac policies.
        :type rbac_policy: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param router: Quota of routers.
        :type router: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param security_group: Quota of security groups.
        :type security_group: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param security_group_rule: Quota of security group rules.
        :type security_group_rule: :class:`.OpenStack_2_QuotaSetItem`
                                   or ``dict``
        :param subnet: Quota of subnets.
        :type subnet: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        :param subnetpool: Quota of subnet pools.
        :type subnetpool: :class:`.OpenStack_2_QuotaSetItem` or ``dict``
        """
        self.floatingip = self._to_quota_set_item(floatingip)
        self.network = self._to_quota_set_item(network)
        self.port = self._to_quota_set_item(port)
        self.rbac_policy = self._to_quota_set_item(rbac_policy)
        self.router = self._to_quota_set_item(router)
        self.security_group = self._to_quota_set_item(security_group)
        self.security_group_rule = self._to_quota_set_item(security_group_rule)
        self.subnet = self._to_quota_set_item(subnet)
        self.subnetpool = self._to_quota_set_item(subnetpool)
        self.driver = driver

    def _to_quota_set_item(self, obj):
        if obj:
            if isinstance(obj, OpenStack_2_QuotaSetItem):
                return obj
            elif isinstance(obj, dict):
                return OpenStack_2_QuotaSetItem(obj["used"], obj["limit"], obj["reserved"])
        else:
            return None

    def __repr__(self):
        return (
            '<OpenStack_2_NetworkQuota Floating IPs="%s", networks="%s",'
            ' SGs="%s", SGRs="%s">'
            % (
                self.floatingip,
                self.network,
                self.security_group,
                self.security_group_rule,
            )
        )


class OpenStack_2_VolumeQuota:
    """
    Volume Quota info. To get the information about quotas and used resources.

    See:
    https://docs.openstack.org/api-ref/block-storage/v2/index.html?expanded=show-quotas-detail
    https://docs.openstack.org/api-ref/block-storage/v3/index.html?expanded=show-quota-usage-for-a-project-detail
    """

    def __init__(
        self,
        backup_gigabytes,
        gigabytes,
        per_volume_gigabytes,
        backups,
        snapshots,
        volumes,
        driver=None,
    ):
        """
        :param backup_gigabytes: Quota of backup size in gigabytes.
        :type backup_gigabytes: :class:`.OpenStack_2_QuotaSetItem` or ``int``
        :param gigabytes: Quota of volume size in gigabytes.
        :type gigabytes: :class:`.OpenStack_2_QuotaSetItem` or ``int``
        :param per_volume_gigabytes: Quota of per volume gigabytes.
        :type per_volume_gigabytes: :class:`.OpenStack_2_QuotaSetItem`
                                    or ``int``
        :param backups: Quota of backups.
        :type backups: :class:`.OpenStack_2_QuotaSetItem` or ``int``
        :param snapshots: Quota of snapshots.
        :type snapshots: :class:`.OpenStack_2_QuotaSetItem` or ``int``
        :param volumes: Quota of security volumes.
        :type volumes: :class:`.OpenStack_2_QuotaSetItem` or ``int``
        """
        self.backup_gigabytes = self._to_quota_set_item(backup_gigabytes)
        self.gigabytes = self._to_quota_set_item(gigabytes)
        self.per_volume_gigabytes = self._to_quota_set_item(per_volume_gigabytes)
        self.backups = self._to_quota_set_item(backups)
        self.snapshots = self._to_quota_set_item(snapshots)
        self.volumes = self._to_quota_set_item(volumes)
        self.driver = driver

    def _to_quota_set_item(self, obj):
        if obj:
            if isinstance(obj, OpenStack_2_QuotaSetItem):
                return obj
            elif isinstance(obj, dict):
                return OpenStack_2_QuotaSetItem(obj["in_use"], obj["limit"], obj["reserved"])
            elif isinstance(obj, int):
                return OpenStack_2_QuotaSetItem(0, obj, 0)
            else:
                return None
        else:
            return None

    def __repr__(self):
        return (
            '<OpenStack_2_VolumeQuota Volumes="%s", gigabytes="%s",'
            ' snapshots="%s", backups="%s">'
            % (self.volumes, self.gigabytes, self.snapshots, self.backups)
        )


class OpenStack_2_ServerGroup:
    """
    Server Group info.

    See:
    https://docs.openstack.org/api-ref/compute/?expanded=create-server-detail,list-server-groups-detail#server-groups-os-server-groups
    """

    def __init__(
        self,
        id,
        name,
        policy,
        members=None,
        rules=None,
        driver=None,
    ):
        """
        :param id: Server Group ID.
        :type id: ``str``
        :param name: Server Group Name.
        :type name: ``str``
        :param policy: Server Group policy.
        :type policy: ``str``
        :param members: Server Group members.
        :type members: ``list``
        :param rules: Server Group rules.
        :type rules: ``list``
        """
        self.id = id
        self.name = name
        self.policy = policy
        self.members = members or []
        self.rules = rules or []
        self.driver = driver

    def __repr__(self):
        return (
            '<OpenStack_2_ServerGroup id="%s", name="%s",'
            ' policy="%s", members="%s", rules="%s">'
            % (self.id, self.name, self.policy, self.members, self.rules)
        )
