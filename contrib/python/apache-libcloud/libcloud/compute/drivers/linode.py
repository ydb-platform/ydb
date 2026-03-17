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

"""libcloud driver for the Linode(R) API

This driver implements all libcloud functionality for the Linode API.
Since the API is a bit more fine-grained, create_node abstracts a significant
amount of work (and may take a while to run).

Linode home page                    http://www.linode.com/
Linode API documentation            http://www.linode.com/api/
Alternate bindings for reference    http://github.com/tjfontaine/linode-python

Linode(R) is a registered trademark of Linode, LLC.

"""

import os
import re
import binascii
import itertools
from copy import copy
from datetime import datetime

from libcloud.utils.py3 import httplib
from libcloud.compute.base import (
    Node,
    KeyPair,
    NodeSize,
    NodeImage,
    NodeDriver,
    NodeLocation,
    StorageVolume,
    NodeAuthSSHKey,
    NodeAuthPassword,
)
from libcloud.common.linode import (
    API_ROOT,
    LINODE_PLAN_IDS,
    DEFAULT_API_VERSION,
    LINODE_DISK_FILESYSTEMS,
    LINODE_DISK_FILESYSTEMS_V4,
    LinodeDisk,
    LinodeException,
    LinodeIPAddress,
    LinodeConnection,
    LinodeExceptionV4,
    LinodeConnectionV4,
)
from libcloud.compute.types import Provider, NodeState, StorageVolumeState
from libcloud.utils.networking import is_private_subnet

try:
    import simplejson as json
except ImportError:
    import json


class LinodeNodeDriver(NodeDriver):
    name = "Linode"
    website = "http://www.linode.com/"
    type = Provider.LINODE

    def __new__(
        cls,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        api_version=DEFAULT_API_VERSION,
        region=None,
        **kwargs,
    ):
        if cls is LinodeNodeDriver:
            if api_version == "3.0":
                cls = LinodeNodeDriverV3
            elif api_version == "4.0":
                cls = LinodeNodeDriverV4
            else:
                raise NotImplementedError(
                    "No Linode driver found for API version: %s" % (api_version)
                )
        return super().__new__(cls)


class LinodeNodeDriverV3(LinodeNodeDriver):
    """libcloud driver for the Linode API

    Rough mapping of which is which:

    - list_nodes              linode.list
    - reboot_node             linode.reboot
    - destroy_node            linode.delete
    - create_node             linode.create, linode.update,
                              linode.disk.createfromdistribution,
                              linode.disk.create, linode.config.create,
                              linode.ip.addprivate, linode.boot
    - list_sizes              avail.linodeplans
    - list_images             avail.distributions
    - list_locations          avail.datacenters
    - list_volumes            linode.disk.list
    - destroy_volume          linode.disk.delete

    For more information on the Linode API, be sure to read the reference:

        http://www.linode.com/api/
    """

    connectionCls = LinodeConnection
    _linode_plan_ids = LINODE_PLAN_IDS
    _linode_disk_filesystems = LINODE_DISK_FILESYSTEMS
    features = {"create_node": ["ssh_key", "password"]}

    def __init__(
        self,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        api_version=None,
        region=None,
        **kwargs,
    ):
        """Instantiate the driver with the given API key

        :param   key: the API key to use (required)
        :type    key: ``str``

        :rtype: ``None``
        """
        self.datacenter = None
        NodeDriver.__init__(self, key)

    # Converts Linode's state from DB to a NodeState constant.
    LINODE_STATES = {
        (-2): NodeState.UNKNOWN,  # Boot Failed
        (-1): NodeState.PENDING,  # Being Created
        0: NodeState.PENDING,  # Brand New
        1: NodeState.RUNNING,  # Running
        2: NodeState.STOPPED,  # Powered Off
        3: NodeState.REBOOTING,  # Shutting Down
        4: NodeState.UNKNOWN,  # Reserved
    }

    def list_nodes(self):
        """
        List all Linodes that the API key can access

        This call will return all Linodes that the API key in use has access
         to.
        If a node is in this list, rebooting will work; however, creation and
        destruction are a separate grant.

        :return: List of node objects that the API key can access
        :rtype: ``list`` of :class:`Node`
        """
        params = {"api_action": "linode.list"}
        data = self.connection.request(API_ROOT, params=params).objects[0]
        return self._to_nodes(data)

    def start_node(self, node):
        """
        Boot the given Linode

        """
        params = {"api_action": "linode.boot", "LinodeID": node.id}
        self.connection.request(API_ROOT, params=params)
        return True

    def stop_node(self, node):
        """
        Shutdown the given Linode

        """
        params = {"api_action": "linode.shutdown", "LinodeID": node.id}
        self.connection.request(API_ROOT, params=params)
        return True

    def reboot_node(self, node):
        """
        Reboot the given Linode

        Will issue a shutdown job followed by a boot job, using the last booted
        configuration.  In most cases, this will be the only configuration.

        :param      node: the Linode to reboot
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        params = {"api_action": "linode.reboot", "LinodeID": node.id}
        self.connection.request(API_ROOT, params=params)
        return True

    def destroy_node(self, node):
        """Destroy the given Linode

        Will remove the Linode from the account and issue a prorated credit. A
        grant for removing Linodes from the account is required, otherwise this
        method will fail.

        In most cases, all disk images must be removed from a Linode before the
        Linode can be removed; however, this call explicitly skips those
        safeguards. There is no going back from this method.

        :param       node: the Linode to destroy
        :type        node: :class:`Node`

        :rtype: ``bool``
        """
        params = {
            "api_action": "linode.delete",
            "LinodeID": node.id,
            "skipChecks": True,
        }
        self.connection.request(API_ROOT, params=params)
        return True

    def create_node(
        self,
        name,
        image,
        size,
        auth,
        location=None,
        ex_swap=None,
        ex_rsize=None,
        ex_kernel=None,
        ex_payment=None,
        ex_comment=None,
        ex_private=False,
        lconfig=None,
        lroot=None,
        lswap=None,
    ):
        """Create a new Linode, deploy a Linux distribution, and boot

        This call abstracts much of the functionality of provisioning a Linode
        and getting it booted.  A global grant to add Linodes to the account is
        required, as this call will result in a billing charge.

        Note that there is a safety valve of 5 Linodes per hour, in order to
        prevent a runaway script from ruining your day.

        :keyword name: the name to assign the Linode (mandatory)
        :type    name: ``str``

        :keyword image: which distribution to deploy on the Linode (mandatory)
        :type    image: :class:`NodeImage`

        :keyword size: the plan size to create (mandatory)
        :type    size: :class:`NodeSize`

        :keyword auth: an SSH key or root password (mandatory)
        :type    auth: :class:`NodeAuthSSHKey` or :class:`NodeAuthPassword`

        :keyword location: which datacenter to create the Linode in
        :type    location: :class:`NodeLocation`

        :keyword ex_swap: size of the swap partition in MB (128)
        :type    ex_swap: ``int``

        :keyword ex_rsize: size of the root partition in MB (plan size - swap).
        :type    ex_rsize: ``int``

        :keyword ex_kernel: a kernel ID from avail.kernels (Latest 2.6 Stable).
        :type    ex_kernel: ``str``

        :keyword ex_payment: one of 1, 12, or 24; subscription length (1)
        :type    ex_payment: ``int``

        :keyword ex_comment: a small comment for the configuration (libcloud)
        :type    ex_comment: ``str``

        :keyword ex_private: whether or not to request a private IP (False)
        :type    ex_private: ``bool``

        :keyword lconfig: what to call the configuration (generated)
        :type    lconfig: ``str``

        :keyword lroot: what to call the root image (generated)
        :type    lroot: ``str``

        :keyword lswap: what to call the swap space (generated)
        :type    lswap: ``str``

        :return: Node representing the newly-created Linode
        :rtype: :class:`Node`
        """
        auth = self._get_and_check_auth(auth)

        # Pick a location (resolves LIBCLOUD-41 in JIRA)
        if location:
            chosen = location.id
        elif self.datacenter:
            chosen = self.datacenter
        else:
            raise LinodeException(0xFB, "Need to select a datacenter first")

        # Step 0: Parameter validation before we purchase
        # We're especially careful here so we don't fail after purchase, rather
        # than getting halfway through the process and having the API fail.

        # Plan ID
        plans = self.list_sizes()
        if size.id not in [p.id for p in plans]:
            raise LinodeException(0xFB, "Invalid plan ID -- avail.plans")

        # Payment schedule
        payment = "1" if not ex_payment else str(ex_payment)
        if payment not in ["1", "12", "24"]:
            raise LinodeException(0xFB, "Invalid subscription (1, 12, 24)")

        ssh = None
        root = None
        # SSH key and/or root password
        if isinstance(auth, NodeAuthSSHKey):
            ssh = auth.pubkey  # pylint: disable=no-member
        elif isinstance(auth, NodeAuthPassword):
            root = auth.password

        if not ssh and not root:
            raise LinodeException(0xFB, "Need SSH key or root password")
        if root is not None and len(root) < 6:
            raise LinodeException(0xFB, "Root password is too short")

        # Swap size
        try:
            swap = 128 if not ex_swap else int(ex_swap)
        except Exception:
            raise LinodeException(0xFB, "Need an integer swap size")

        # Root partition size
        imagesize = (size.disk - swap) if not ex_rsize else int(ex_rsize)
        if (imagesize + swap) > size.disk:
            raise LinodeException(0xFB, "Total disk images are too big")

        # Distribution ID
        distros = self.list_images()
        if image.id not in [d.id for d in distros]:
            raise LinodeException(0xFB, "Invalid distro -- avail.distributions")

        # Kernel
        if ex_kernel:
            kernel = ex_kernel
        else:
            if image.extra["64bit"]:
                # For a list of available kernel ids, see
                # https://www.linode.com/kernels/
                kernel = 138
            else:
                kernel = 137
        params = {"api_action": "avail.kernels"}
        kernels = self.connection.request(API_ROOT, params=params).objects[0]
        if kernel not in [z["KERNELID"] for z in kernels]:
            raise LinodeException(0xFB, "Invalid kernel -- avail.kernels")

        # Comments
        comments = (
            "Created by Apache libcloud <https://www.libcloud.org>"
            if not ex_comment
            else ex_comment
        )

        # Step 1: linode.create
        params = {
            "api_action": "linode.create",
            "DatacenterID": chosen,
            "PlanID": size.id,
            "PaymentTerm": payment,
        }
        data = self.connection.request(API_ROOT, params=params).objects[0]
        linode = {"id": data["LinodeID"]}

        # Step 1b. linode.update to rename the Linode
        params = {
            "api_action": "linode.update",
            "LinodeID": linode["id"],
            "Label": name,
        }
        self.connection.request(API_ROOT, params=params)

        # Step 1c. linode.ip.addprivate if it was requested
        if ex_private:
            params = {"api_action": "linode.ip.addprivate", "LinodeID": linode["id"]}
            self.connection.request(API_ROOT, params=params)

        # Step 1d. Labels
        # use the linode id as the name can be up to 63 chars and the labels
        # are limited to 48 chars
        label = {
            "lconfig": "[%s] Configuration Profile" % linode["id"],
            "lroot": "[{}] {} Disk Image".format(linode["id"], image.name),
            "lswap": "[%s] Swap Space" % linode["id"],
        }

        if lconfig:
            label["lconfig"] = lconfig

        if lroot:
            label["lroot"] = lroot

        if lswap:
            label["lswap"] = lswap

        # Step 2: linode.disk.createfromdistribution
        if not root:
            root = binascii.b2a_base64(os.urandom(8)).decode("ascii").strip()

        params = {
            "api_action": "linode.disk.createfromdistribution",
            "LinodeID": linode["id"],
            "DistributionID": image.id,
            "Label": label["lroot"],
            "Size": imagesize,
            "rootPass": root,
        }
        if ssh:
            params["rootSSHKey"] = ssh
        data = self.connection.request(API_ROOT, params=params).objects[0]
        linode["rootimage"] = data["DiskID"]

        # Step 3: linode.disk.create for swap
        params = {
            "api_action": "linode.disk.create",
            "LinodeID": linode["id"],
            "Label": label["lswap"],
            "Type": "swap",
            "Size": swap,
        }
        data = self.connection.request(API_ROOT, params=params).objects[0]
        linode["swapimage"] = data["DiskID"]

        # Step 4: linode.config.create for main profile
        disks = "{},{},,,,,,,".format(linode["rootimage"], linode["swapimage"])
        params = {
            "api_action": "linode.config.create",
            "LinodeID": linode["id"],
            "KernelID": kernel,
            "Label": label["lconfig"],
            "Comments": comments,
            "DiskList": disks,
        }
        if ex_private:
            params["helper_network"] = True
            params["helper_distro"] = True

        data = self.connection.request(API_ROOT, params=params).objects[0]
        linode["config"] = data["ConfigID"]

        # Step 5: linode.boot
        params = {
            "api_action": "linode.boot",
            "LinodeID": linode["id"],
            "ConfigID": linode["config"],
        }
        self.connection.request(API_ROOT, params=params)

        # Make a node out of it and hand it back
        params = {"api_action": "linode.list", "LinodeID": linode["id"]}
        data = self.connection.request(API_ROOT, params=params).objects[0]
        nodes = self._to_nodes(data)

        if len(nodes) == 1:
            node = nodes[0]
            if getattr(auth, "generated", False):
                node.extra["password"] = auth.password
            return node

        return None

    def ex_resize_node(self, node, size):
        """Resizes a Linode from one plan to another

        Immediately shuts the Linode down, charges/credits the account,
        and issue a migration to another host server.
        Requires a size (numeric), which is the desired PlanID available from
        avail.LinodePlans()
        After resize is complete the node needs to be booted
        """

        params = {"api_action": "linode.resize", "LinodeID": node.id, "PlanID": size}
        self.connection.request(API_ROOT, params=params)
        return True

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

    def ex_rename_node(self, node, name):
        """Renames a node"""

        params = {"api_action": "linode.update", "LinodeID": node.id, "Label": name}
        self.connection.request(API_ROOT, params=params)
        return True

    def list_sizes(self, location=None):
        """
        List available Linode plans

        Gets the sizes that can be used for creating a Linode.  Since available
        Linode plans vary per-location, this method can also be passed a
        location to filter the availability.

        :keyword location: the facility to retrieve plans in
        :type    location: :class:`NodeLocation`

        :rtype: ``list`` of :class:`NodeSize`
        """
        params = {"api_action": "avail.linodeplans"}
        data = self.connection.request(API_ROOT, params=params).objects[0]
        sizes = []
        for obj in data:
            n = NodeSize(
                id=obj["PLANID"],
                name=obj["LABEL"],
                ram=obj["RAM"],
                disk=(obj["DISK"] * 1024),
                bandwidth=obj["XFER"],
                price=obj["PRICE"],
                driver=self.connection.driver,
            )
            sizes.append(n)
        return sizes

    def list_images(self):
        """
        List available Linux distributions

        Retrieve all Linux distributions that can be deployed to a Linode.

        :rtype: ``list`` of :class:`NodeImage`
        """
        params = {"api_action": "avail.distributions"}
        data = self.connection.request(API_ROOT, params=params).objects[0]
        distros = []
        for obj in data:
            i = NodeImage(
                id=obj["DISTRIBUTIONID"],
                name=obj["LABEL"],
                driver=self.connection.driver,
                extra={"pvops": obj["REQUIRESPVOPSKERNEL"], "64bit": obj["IS64BIT"]},
            )
            distros.append(i)
        return distros

    def list_locations(self):
        """
        List available facilities for deployment

        Retrieve all facilities that a Linode can be deployed in.

        :rtype: ``list`` of :class:`NodeLocation`
        """
        params = {"api_action": "avail.datacenters"}
        data = self.connection.request(API_ROOT, params=params).objects[0]
        nl = []
        for dc in data:
            country = None
            if "USA" in dc["LOCATION"]:
                country = "US"
            elif "UK" in dc["LOCATION"]:
                country = "GB"
            elif "JP" in dc["LOCATION"]:
                country = "JP"
            else:
                country = "??"
            nl.append(NodeLocation(dc["DATACENTERID"], dc["LOCATION"], country, self))
        return nl

    def linode_set_datacenter(self, dc):
        """
        Set the default datacenter for Linode creation

        Since Linodes must be created in a facility, this function sets the
        default that :class:`create_node` will use.  If a location keyword is
        not passed to :class:`create_node`, this method must have already been
        used.

        :keyword dc: the datacenter to create Linodes in unless specified
        :type    dc: :class:`NodeLocation`

        :rtype: ``bool``
        """
        did = dc.id
        params = {"api_action": "avail.datacenters"}
        data = self.connection.request(API_ROOT, params=params).objects[0]
        for datacenter in data:
            if did == dc["DATACENTERID"]:
                self.datacenter = did
                return

        dcs = ", ".join([d["DATACENTERID"] for d in data])
        self.datacenter = None
        raise LinodeException(0xFD, "Invalid datacenter (use one of %s)" % dcs)

    def destroy_volume(self, volume):
        """
        Destroys disk volume for the Linode. Linode id is to be provided as
        extra["LinodeId"] within :class:`StorageVolume`. It can be retrieved
        by :meth:`libcloud.compute.drivers.linode.LinodeNodeDriver\
                 .ex_list_volumes`.

        :param volume: Volume to be destroyed
        :type volume: :class:`StorageVolume`

        :rtype: ``bool``
        """
        if not isinstance(volume, StorageVolume):
            raise LinodeException(0xFD, "Invalid volume instance")

        if volume.extra["LINODEID"] is None:
            raise LinodeException(0xFD, "Missing LinodeID")

        params = {
            "api_action": "linode.disk.delete",
            "LinodeID": volume.extra["LINODEID"],
            "DiskID": volume.id,
        }
        self.connection.request(API_ROOT, params=params)

        return True

    def ex_create_volume(self, size, name, node, fs_type):
        """
        Create disk for the Linode.

        :keyword    size: Size of volume in megabytes (required)
        :type       size: ``int``

        :keyword    name: Name of the volume to be created
        :type       name: ``str``

        :keyword    node: Node to attach volume to.
        :type       node: :class:`Node`

        :keyword    fs_type: The formatted type of this disk. Valid types are:
                             ext3, ext4, swap, raw
        :type       fs_type: ``str``


        :return: StorageVolume representing the newly-created volume
        :rtype: :class:`StorageVolume`
        """
        # check node
        if not isinstance(node, Node):
            raise LinodeException(0xFD, "Invalid node instance")

        # check space available
        total_space = node.extra["TOTALHD"]
        existing_volumes = self.ex_list_volumes(node)
        used_space = 0
        for volume in existing_volumes:
            used_space = used_space + volume.size

        available_space = total_space - used_space
        if available_space < size:
            raise LinodeException(
                0xFD,
                "Volume size too big. Available space\
                    %d"
                % available_space,
            )

        # check filesystem type
        if fs_type not in self._linode_disk_filesystems:
            raise LinodeException(0xFD, "Not valid filesystem type")

        params = {
            "api_action": "linode.disk.create",
            "LinodeID": node.id,
            "Label": name,
            "Type": fs_type,
            "Size": size,
        }
        data = self.connection.request(API_ROOT, params=params).objects[0]
        volume = data["DiskID"]
        # Make a volume out of it and hand it back
        params = {
            "api_action": "linode.disk.list",
            "LinodeID": node.id,
            "DiskID": volume,
        }
        data = self.connection.request(API_ROOT, params=params).objects[0]
        return self._to_volumes(data)[0]

    def ex_list_volumes(self, node, disk_id=None):
        """
        List existing disk volumes for for given Linode.

        :keyword    node: Node to list disk volumes for. (required)
        :type       node: :class:`Node`

        :keyword    disk_id: Id for specific disk volume. (optional)
        :type       disk_id: ``int``

        :rtype: ``list`` of :class:`StorageVolume`
        """
        if not isinstance(node, Node):
            raise LinodeException(0xFD, "Invalid node instance")

        params = {"api_action": "linode.disk.list", "LinodeID": node.id}
        # Add param if disk_id was specified
        if disk_id is not None:
            params["DiskID"] = disk_id

        data = self.connection.request(API_ROOT, params=params).objects[0]
        return self._to_volumes(data)

    def _to_volumes(self, objs):
        """
        Convert returned JSON volumes into StorageVolume instances

        :keyword    objs: ``list`` of JSON dictionaries representing the
                         StorageVolumes
        :type       objs: ``list``

        :return: ``list`` of :class:`StorageVolume`s
        """
        volumes = {}
        for o in objs:
            vid = o["DISKID"]
            volumes[vid] = vol = StorageVolume(
                id=vid,
                name=o["LABEL"],
                size=int(o["SIZE"]),
                driver=self.connection.driver,
            )
            vol.extra = copy(o)
        return list(volumes.values())

    def _to_nodes(self, objs):
        """Convert returned JSON Linodes into Node instances

        :keyword objs: ``list`` of JSON dictionaries representing the Linodes
        :type objs: ``list``
        :return: ``list`` of :class:`Node`s"""

        # Get the IP addresses for the Linodes
        nodes = {}
        batch = []
        for o in objs:
            lid = o["LINODEID"]
            nodes[lid] = n = Node(
                id=lid,
                name=o["LABEL"],
                public_ips=[],
                private_ips=[],
                state=self.LINODE_STATES[o["STATUS"]],
                driver=self.connection.driver,
            )
            n.extra = copy(o)
            n.extra["PLANID"] = self._linode_plan_ids.get(o.get("TOTALRAM"))
            batch.append({"api_action": "linode.ip.list", "LinodeID": lid})

        # Avoid batch limitation
        ip_answers = []
        args = [iter(batch)] * 25

        for twenty_five in itertools.zip_longest(*args):
            twenty_five = [q for q in twenty_five if q]
            params = {
                "api_action": "batch",
                "api_requestArray": json.dumps(twenty_five),
            }
            req = self.connection.request(API_ROOT, params=params)
            if not req.success() or len(req.objects) == 0:
                return None
            ip_answers.extend(req.objects)

        # Add the returned IPs to the nodes and return them
        for ip_list in ip_answers:
            for ip in ip_list:
                lid = ip["LINODEID"]
                which = nodes[lid].public_ips if ip["ISPUBLIC"] == 1 else nodes[lid].private_ips
                which.append(ip["IPADDRESS"])
        return list(nodes.values())


class LinodeNodeDriverV4(LinodeNodeDriver):
    connectionCls = LinodeConnectionV4
    _linode_disk_filesystems = LINODE_DISK_FILESYSTEMS_V4

    LINODE_STATES = {
        "running": NodeState.RUNNING,
        "stopped": NodeState.STOPPED,
        "provisioning": NodeState.STARTING,
        "offline": NodeState.STOPPED,
        "booting": NodeState.STARTING,
        "rebooting": NodeState.REBOOTING,
        "shutting_down": NodeState.STOPPING,
        "deleting": NodeState.PENDING,
        "migrating": NodeState.MIGRATING,
        "rebuilding": NodeState.UPDATING,
        "cloning": NodeState.MIGRATING,
        "restoring": NodeState.PENDING,
        "resizing": NodeState.RECONFIGURING,
    }

    LINODE_DISK_STATES = {
        "ready": StorageVolumeState.AVAILABLE,
        "not ready": StorageVolumeState.CREATING,
        "deleting": StorageVolumeState.DELETING,
    }

    LINODE_VOLUME_STATES = {
        "creating": StorageVolumeState.CREATING,
        "active": StorageVolumeState.AVAILABLE,
        "resizing": StorageVolumeState.UPDATING,
        "contact_support": StorageVolumeState.UNKNOWN,
    }

    def list_nodes(self):
        """
        Returns a list of Linodes the API key in use has access
        to view.

        :return: List of node objects
        :rtype: ``list`` of :class:`Node`
        """

        data = self._paginated_request("/v4/linode/instances", "data")
        return [self._to_node(obj) for obj in data]

    def list_sizes(self):
        """
        Returns a list of Linode Types

        : rtype: ``list`` of :class: `NodeSize`
        """
        data = self._paginated_request("/v4/linode/types", "data")
        return [self._to_size(obj) for obj in data]

    def list_images(self):
        """
        Returns a list of images

        :rtype: ``list`` of :class:`NodeImage`
        """
        data = self._paginated_request("/v4/images", "data")
        return [self._to_image(obj) for obj in data]

    def create_key_pair(self, name, public_key=""):
        """
        Creates an SSH keypair

        :param name: The name to be given to the keypair (required).\
        :type name: `str`

        :keyword public_key: Contents of the public key the the SSH key pair
        :type public_key: `str`

        :rtype: :class: `KeyPair`
        """
        attr = {"label": name, "ssh_key": public_key}
        response = self.connection.request(
            "/v4/profile/sshkeys", data=json.dumps(attr), method="POST"
        ).object
        return self._to_key_pair(response)

    def list_key_pairs(self):
        """
        Provide a list of all the SSH keypairs in your account.

        :rtype: ``list`` of :class: `KeyPair`
        """
        data = self._paginated_request("/v4/profile/sshkeys", "data")
        return [self._to_key_pair(obj) for obj in data]

    def list_locations(self):
        """
        Lists the Regions available for Linode services

        :rtype: ``list`` of :class:`NodeLocation`
        """
        data = self._paginated_request("/v4/regions", "data")
        return [self._to_location(obj) for obj in data]

    def start_node(self, node):
        """Boots a node the API Key has permission to modify

        :param       node: the node to start
        :type        node: :class:`Node`

        :rtype: ``bool``
        """
        if not isinstance(node, Node):
            raise LinodeExceptionV4("Invalid node instance")

        response = self.connection.request("/v4/linode/instances/%s/boot" % node.id, method="POST")
        return response.status == httplib.OK

    def ex_start_node(self, node):
        # NOTE: This method is here for backward compatibility reasons after
        # this method was promoted to be part of the standard compute API in
        # Libcloud v2.7.0
        return self.start_node(node=node)

    def stop_node(self, node):
        """Shuts down a a node the API Key has permission to modify.

        :param       node: the Linode to destroy
        :type        node: :class:`Node`

        :rtype: ``bool``
        """
        if not isinstance(node, Node):
            raise LinodeExceptionV4("Invalid node instance")

        response = self.connection.request(
            "/v4/linode/instances/%s/shutdown" % node.id, method="POST"
        )
        return response.status == httplib.OK

    def ex_stop_node(self, node):
        # NOTE: This method is here for backward compatibility reasons after
        # this method was promoted to be part of the standard compute API in
        # Libcloud v2.7.0
        return self.stop_node(node=node)

    def destroy_node(self, node):
        """Deletes a node the API Key has permission to `read_write`

        :param       node: the Linode to destroy
        :type        node: :class:`Node`

        :rtype: ``bool``
        """
        if not isinstance(node, Node):
            raise LinodeExceptionV4("Invalid node instance")

        response = self.connection.request("/v4/linode/instances/%s" % node.id, method="DELETE")
        return response.status == httplib.OK

    def reboot_node(self, node):
        """Reboots a node the API Key has permission to modify.

        :param       node: the Linode to destroy
        :type        node: :class:`Node`

        :rtype: ``bool``
        """
        if not isinstance(node, Node):
            raise LinodeExceptionV4("Invalid node instance")

        response = self.connection.request(
            "/v4/linode/instances/%s/reboot" % node.id, method="POST"
        )
        return response.status == httplib.OK

    def create_node(
        self,
        location,
        # Previously, the following 3 parameters did not match the rest of the libcloud
        # codebase drivers. They should be in the same order as other compute drivers.
        # Previously, it looked like this:
        #         size,
        #         image=None,
        #         name=None,
        #
        # Comments welcome on how backwards compatibility (if any) should work here.
        # Since it was not compatible with other drivers, it is not clear to me if this
        # would break anyone's codebase if they were not using any other libcloud drivers
        # to other cloud providers in the first place. If they were not, that seems to
        # kind of defeat the purpose of using libcloud.
        name,  # Can be None
        size,  # Can be None
        image,  # Can be None
        root_pass=None,
        ex_authorized_keys=None,
        ex_authorized_users=None,
        ex_tags=None,
        ex_backups_enabled=False,
        ex_private_ip=False,
        ex_userdata=None,
    ):
        """Creates a Linode Instance.
        In order for this request to complete successfully,
        the user must have the `add_linodes` grant as this call
        will incur a charge.

        :param location: which region to create the node in
        :type    location: :class:`NodeLocation`

        :param size: the plan size to create
        :type    size: :class:`NodeSize`

        :keyword image: which distribution to deploy on the node
        :type    image: :class:`NodeImage`

        :keyword name: the name to assign to node.\
        Must start with an alpha character.\
        May only consist of alphanumeric characters,\
         dashes (-), underscores (_) or periods (.).\
        Cannot have two dashes (--), underscores (__) or periods (..) in a row.
        :type    name: ``str``

        :keyword root_pass: the root password (required if image is provided)
        :type    root_pass: ``str``

        :keyword ex_authorized_keys: a list of public SSH keys
        :type    ex_authorized_keys: ``list`` of ``str``

        :keyword ex_authorized_users:  a list of usernames.\
        If the usernames have associated SSH keys,\
        the keys will be appended to the root users `authorized_keys`
        :type    ex_authorized_users: ``list`` of ``str``

        :keyword ex_tags: list of tags for the node
        :type    ex_tags: ``list`` of ``str``

        :keyword ex_backups_enabled: whether to be enrolled \
        in the Linode Backup service (False)
        :type    ex_backups_enabled: ``bool``

        :keyword ex_private_ip: whether or not to request a private IP
        :type    ex_private_ip: ``bool``

        :keyword ex_userdata: add cloud-config compatible userdata to be
        processed by cloud-init inside the Linode instance. NOTE: the
        contents of this string must be base64 encoded before passing
        it to this function.
        :type    ex_userdata: ``str``

        :return: Node representing the newly-created node
        :rtype: :class:`Node`
        """

        if not isinstance(location, NodeLocation):
            raise LinodeExceptionV4("Invalid location instance")

        if not isinstance(size, NodeSize):
            raise LinodeExceptionV4("Invalid size instance")

        attr = {
            "region": location.id,
            "type": size.id,
            "private_ip": ex_private_ip,
            "backups_enabled": ex_backups_enabled,
        }

        if ex_userdata:
            attr["metadata"] = {
                "user_data": binascii.b2a_base64(bytes(ex_userdata.encode("utf-8")))
                .decode("ascii")
                .strip()
            }

        if image is not None:
            if root_pass is None:
                raise LinodeExceptionV4("root password required " "when providing an image")
            attr["image"] = image.id
            attr["root_pass"] = root_pass

        if name is not None:
            valid_name = r"^[a-zA-Z]((?!--|__|\.\.)[a-zA-Z0-9-_.])+$"
            if not re.match(valid_name, name):
                raise LinodeExceptionV4("Invalid name")
            attr["label"] = name
        if ex_authorized_keys is not None:
            attr["authorized_keys"] = list(ex_authorized_keys)
        if ex_authorized_users is not None:
            attr["authorized_users"] = list(ex_authorized_users)
        if ex_tags is not None:
            attr["tags"] = list(ex_tags)

        response = self.connection.request(
            "/v4/linode/instances", data=json.dumps(attr), method="POST"
        ).object
        return self._to_node(response)

    def ex_get_node(self, node_id):
        """
        Return a Node object based on a node ID.

        :keyword node_id: Node's ID
        :type    node_id: ``str``

        :return: Created node
        :rtype  : :class:`Node`
        """
        response = self.connection.request("/v4/linode/instances/%s" % node_id).object
        return self._to_node(response)

    def ex_list_disks(self, node):
        """
        List disks associated with the node.

        :param    node: Node to list disks. (required)
        :type       node: :class:`Node`

        :rtype: ``list`` of :class:`LinodeDisk`
        """
        if not isinstance(node, Node):
            raise LinodeExceptionV4("Invalid node instance")

        data = self._paginated_request("/v4/linode/instances/%s/disks" % node.id, "data")

        return [self._to_disk(obj) for obj in data]

    def ex_create_disk(
        self,
        size,
        name,
        node,
        fs_type,
        image=None,
        ex_root_pass=None,
        ex_authorized_keys=None,
        ex_authorized_users=None,
        ex_read_only=False,
    ):
        """
        Adds a new disk to node

        :param    size: Size of disk in megabytes (required)
        :type       size: ``int``

        :param    name: Name of the disk to be created (required)
        :type       name: ``str``

        :param    node: Node to attach disk to (required)
        :type       node: :class:`Node`

        :param    fs_type: The formatted type of this disk. Valid types are:
                             ext3, ext4, swap, raw, initrd
        :type       fs_type: ``str``

        :keyword    image: Image  to deploy the volume from
        :type       image: :class:`NodeImage`

        :keyword    ex_root_pass: root password,required \
                    if an image is provided
        :type       ex_root_pass: ``str``

        :keyword ex_authorized_keys:  a list of SSH keys
        :type    ex_authorized_keys: ``list`` of ``str``

        :keyword ex_authorized_users:  a list of usernames \
                 that will have their SSH keys,\
                 if any, automatically appended \
                 to the root user's ~/.ssh/authorized_keys file.
        :type    ex_authorized_users: ``list`` of ``str``

        :keyword ex_read_only: if true, this disk is read-only
        :type ex_read_only: ``bool``

        :return: LinodeDisk representing the newly-created disk
        :rtype: :class:`LinodeDisk`
        """

        attr = {
            "label": str(name),
            "size": int(size),
            "filesystem": fs_type,
            "read_only": ex_read_only,
        }

        if not isinstance(node, Node):
            raise LinodeExceptionV4("Invalid node instance")

        if fs_type not in self._linode_disk_filesystems:
            raise LinodeExceptionV4("Not valid filesystem type")

        if image is not None:
            if not isinstance(image, NodeImage):
                raise LinodeExceptionV4("Invalid image instance")
            # when an image is set, root pass must be set as well
            if ex_root_pass is None:
                raise LinodeExceptionV4("root_pass is required when " "deploying an image")
            attr["image"] = image.id
            attr["root_pass"] = ex_root_pass

        if ex_authorized_keys is not None:
            attr["authorized_keys"] = list(ex_authorized_keys)

        if ex_authorized_users is not None:
            attr["authorized_users"] = list(ex_authorized_users)

        response = self.connection.request(
            "/v4/linode/instances/%s/disks" % node.id,
            data=json.dumps(attr),
            method="POST",
        ).object
        return self._to_disk(response)

    def ex_destroy_disk(self, node, disk):
        """
        Destroys disk for the given node.

        :param node: The Node the disk is attached to. (required)
        :type    node: :class:`Node`

        :param disk: LinodeDisk to be destroyed (required)
        :type disk: :class:`LinodeDisk`

        :rtype: ``bool``
        """
        if not isinstance(node, Node):
            raise LinodeExceptionV4("Invalid node instance")

        if not isinstance(disk, LinodeDisk):
            raise LinodeExceptionV4("Invalid disk instance")

        if node.state != self.LINODE_STATES["stopped"]:
            raise LinodeExceptionV4("Node needs to be stopped" " before disk is destroyed")

        response = self.connection.request(
            "/v4/linode/instances/{}/disks/{}".format(node.id, disk.id), method="DELETE"
        )
        return response.status == httplib.OK

    def list_volumes(self):
        """Get all volumes of the account
        :rtype: `list` of :class: `StorageVolume`
        """
        data = self._paginated_request("/v4/volumes", "data")

        return [self._to_volume(obj) for obj in data]

    def create_volume(self, name, size, location=None, node=None, tags=None):
        """Creates a volume and optionally attaches it to a node.

        :param name: The name to be given to volume (required).\
        Must start with an alpha character. \
        May only consist of alphanumeric characters,\
         dashes (-), underscores (_)\
        Cannot have two dashes (--), underscores (__) in a row.

        :type name: `str`

        :param size: Size in gigabytes (required)
        :type size: `int`

        :keyword location: Location to create the node.\
        Required if node is not given.
        :type location: :class:`NodeLocation`

        :keyword volume: Node to attach the volume to
        :type volume: :class:`Node`

        :keyword tags: tags to apply to volume
        :type tags: `list` of `str`

        :rtype: :class: `StorageVolume`
        """

        valid_name = "^[a-zA-Z]((?!--|__)[a-zA-Z0-9-_])+$"
        if not re.match(valid_name, name):
            raise LinodeExceptionV4("Invalid name")

        attr = {
            "label": name,
            "size": int(size),
        }

        if node is not None:
            if not isinstance(node, Node):
                raise LinodeExceptionV4("Invalid node instance")
            attr["linode_id"] = int(node.id)
        else:
            # location is only required if a node is not given
            if location:
                if not isinstance(location, NodeLocation):
                    raise LinodeExceptionV4("Invalid location instance")
                attr["region"] = location.id
            else:
                raise LinodeExceptionV4("Region must be provided " "when node is not")
        if tags is not None:
            attr["tags"] = list(tags)

        response = self.connection.request(
            "/v4/volumes", data=json.dumps(attr), method="POST"
        ).object
        return self._to_volume(response)

    def attach_volume(self, node, volume, persist_across_boots=True):
        """Attaches a volume to a node.
        Volume and node must be located in the same region

        :param node: Node to attach the volume to(required)
        :type node: :class:`Node`

        :param volume: Volume to be attached (required)
        :type volume: :class:`StorageVolume`

        :keyword persist_across_boots: Whether volume should be \
        attached to node across boots
        :type persist_across_boots: `bool`

        :rtype: :class: `StorageVolume`
        """
        if not isinstance(volume, StorageVolume):
            raise LinodeExceptionV4("Invalid volume instance")

        if not isinstance(node, Node):
            raise LinodeExceptionV4("Invalid node instance")

        if volume.extra["linode_id"] is not None:
            raise LinodeExceptionV4("Volume is already attached to a node")

        if node.extra["location"] != volume.extra["location"]:
            raise LinodeExceptionV4("Volume and node " "must be on the same region")

        attr = {"linode_id": int(node.id), "persist_across_boots": persist_across_boots}

        response = self.connection.request(
            "/v4/volumes/%s/attach" % volume.id, data=json.dumps(attr), method="POST"
        ).object
        return self._to_volume(response)

    def detach_volume(self, volume):
        """Detaches a volume from a node.

        :param volume: Volume to be detached (required)
        :type volume: :class:`StorageVolume`

        :rtype: ``bool``
        """
        if not isinstance(volume, StorageVolume):
            raise LinodeExceptionV4("Invalid volume instance")

        if volume.extra["linode_id"] is None:
            raise LinodeExceptionV4("Volume is already detached")

        response = self.connection.request("/v4/volumes/%s/detach" % volume.id, method="POST")
        return response.status == httplib.OK

    def destroy_volume(self, volume):
        """Destroys the volume given.

        :param volume: Volume to be deleted (required)
        :type volume: :class:`StorageVolume`

        :rtype: ``bool``
        """
        if not isinstance(volume, StorageVolume):
            raise LinodeExceptionV4("Invalid volume instance")

        if volume.extra["linode_id"] is not None:
            raise LinodeExceptionV4("Volume must be detached" " before it can be deleted.")
        response = self.connection.request("/v4/volumes/%s" % volume.id, method="DELETE")
        return response.status == httplib.OK

    def ex_resize_volume(self, volume, size):
        """Resizes the volume given.

        :param volume: Volume to be resized
        :type  volume: :class:`StorageVolume`

        :param size: new volume size in gigabytes, must be\
        greater than current size
        :type  size: `int`

        :rtype: ``bool``
        """
        if not isinstance(volume, StorageVolume):
            raise LinodeExceptionV4("Invalid volume instance")

        if volume.size >= size:
            raise LinodeExceptionV4("Volumes can only be resized up")
        attr = {"size": size}

        response = self.connection.request(
            "/v4/volumes/%s/resize" % volume.id, data=json.dumps(attr), method="POST"
        )
        return response.status == httplib.OK

    def ex_clone_volume(self, volume, name):
        """Clones the volume given

        :param volume: Volume to be cloned
        :type  volume: :class:`StorageVolume`

        :param name: new cloned volume name
        :type  name: `str`

        :rtype: :class:`StorageVolume`
        """

        if not isinstance(volume, StorageVolume):
            raise LinodeExceptionV4("Invalid volume instance")

        attr = {"label": name}
        response = self.connection.request(
            "/v4/volumes/%s/clone" % volume.id, data=json.dumps(attr), method="POST"
        ).object

        return self._to_volume(response)

    def ex_get_volume(self, volume_id):
        """
        Return a Volume object based on a volume ID.

        :param  volume_id: Volume's id
        :type   volume_id: ``str``

        :return:  A StorageVolume object for the volume
        :rtype:   :class:`StorageVolume`
        """
        response = self.connection.request("/v4/volumes/%s" % volume_id).object
        return self._to_volume(response)

    def get_image(self, image):
        """
        Lookup a Linode image

        :param image: The name to image to be looked up (required).\
        :type name: `str`

        :rtype: :class: `NodeImage`
        """
        response = self.connection.request("/v4/images/%s" % image, method="GET")
        return self._to_image(response.object)

    def create_image(self, disk, name=None, description=None):
        """Creates a private image from a LinodeDisk.
         Images are limited to three per account.

        :param disk: LinodeDisk to create the image from (required)
        :type disk: :class:`LinodeDisk`

        :keyword name: A name for the image.\
        Defaults to the name of the disk \
        it is being created from if not provided
        :type name: `str`

        :keyword description: A description of the image
        :type description: `str`

        :return: The newly created NodeImage
        :rtype: :class:`NodeImage`
        """

        if not isinstance(disk, LinodeDisk):
            raise LinodeExceptionV4("Invalid disk instance")

        attr = {"disk_id": int(disk.id), "label": name, "description": description}

        response = self.connection.request(
            "/v4/images", data=json.dumps(attr), method="POST"
        ).object
        return self._to_image(response)

    def delete_image(self, image):
        """Deletes a private image

        :param image: NodeImage to delete (required)
        :type image: :class:`NodeImage`

        :rtype: ``bool``
        """
        if not isinstance(image, NodeImage):
            raise LinodeExceptionV4("Invalid image instance")

        response = self.connection.request("/v4/images/%s" % image.id, method="DELETE")
        return response.status == httplib.OK

    def ex_list_addresses(self):
        """List IP addresses

        :return: LinodeIPAddress list
        :rtype: `list` of :class:`LinodeIPAddress`
        """
        data = self._paginated_request("/v4/networking/ips", "data")

        return [self._to_address(obj) for obj in data]

    def ex_list_node_addresses(self, node):
        """List all IPv4 addresses attached to node

        :param node: Node to list IP addresses
        :type node: :class:`Node`

        :return: LinodeIPAddress list
        :rtype: `list` of :class:`LinodeIPAddress`
        """
        if not isinstance(node, Node):
            raise LinodeExceptionV4("Invalid node instance")

        response = self.connection.request("/v4/linode/instances/%s/ips" % node.id).object
        return self._to_addresses(response)

    def ex_allocate_private_address(self, node, address_type="ipv4"):
        """Allocates a private IPv4 address to node.Only ipv4 is currently supported

        :param node: Node to attach the IP address
        :type node: :class:`Node`

        :keyword address_type: Type of IP address
        :type address_type: `str`

        :return: The newly created LinodeIPAddress
        :rtype: :class:`LinodeIPAddress`
        """
        if not isinstance(node, Node):
            raise LinodeExceptionV4("Invalid node instance")

        # Only ipv4 is currently supported
        if address_type != "ipv4":
            raise LinodeExceptionV4("Address type not supported")
        # Only one private IP address can be allocated
        if len(node.private_ips) >= 1:
            raise LinodeExceptionV4("Nodes can have up to one private IP")

        attr = {"public": False, "type": address_type}

        response = self.connection.request(
            "/v4/linode/instances/%s/ips" % node.id,
            data=json.dumps(attr),
            method="POST",
        ).object
        return self._to_address(response)

    def ex_share_address(self, node, addresses):
        """Shares an IP with another node.This can be used to allow one Linode
         to begin serving requests should another become unresponsive.

        :param node: Node to share the IP addresses with
        :type node: :class:`Node`

        :keyword addresses: List of IP addresses to share
        :type address_type: `list` of :class: `LinodeIPAddress`

        :rtype: ``bool``
        """
        if not isinstance(node, Node):
            raise LinodeExceptionV4("Invalid node instance")

        if not all(isinstance(address, LinodeIPAddress) for address in addresses):
            raise LinodeExceptionV4("Invalid address instance")

        attr = {
            "ips": [address.inet for address in addresses],
            "linode_id": int(node.id),
        }
        response = self.connection.request(
            "/v4/networking/ipv4/share", data=json.dumps(attr), method="POST"
        )
        return response.status == httplib.OK

    def ex_resize_node(self, node, size, allow_auto_disk_resize=False):
        """
        Resizes a node the API Key has read_write permission
        to a different Type.
        The following requirements must be met:
        - The node must not have a pending migration
        - The account cannot have an outstanding balance
        - The node must not have more disk allocation than the new size allows

        :param node: the Linode to resize
        :type node: :class:`Node`

        :param size: the size of the new node
        :type size: :class:`NodeSize`

        :keyword allow_auto_disk_resize: Automatically resize disks \
        when resizing a node.
        :type allow_auto_disk_resize: ``bool``

        :rtype: ``bool``
        """
        if not isinstance(node, Node):
            raise LinodeExceptionV4("Invalid node instance")
        if not isinstance(size, NodeSize):
            raise LinodeExceptionV4("Invalid node size")

        attr = {"type": size.id, "allow_auto_disk_resize": allow_auto_disk_resize}

        response = self.connection.request(
            "/v4/linode/instances/%s/resize" % node.id,
            data=json.dumps(attr),
            method="POST",
        )

        return response.status == httplib.OK

    def ex_rename_node(self, node, name):
        """Renames a node

        :param node: the Linode to resize
        :type node: :class:`Node`

        :param name: the node's new name
        :type name: ``str``

        :return: Changed Node
        :rtype: :class:`Node`
        """
        if not isinstance(node, Node):
            raise LinodeExceptionV4("Invalid node instance")

        attr = {"label": name}

        response = self.connection.request(
            "/v4/linode/instances/%s" % node.id, data=json.dumps(attr), method="PUT"
        ).object

        return self._to_node(response)

    def _to_key_pair(self, data):
        extra = {"id": data["id"]}

        return KeyPair(
            name=data["label"],
            fingerprint=None,
            public_key=data["ssh_key"],
            private_key=None,
            driver=self,
            extra=extra,
        )

    def _to_node(self, data):
        extra = {
            "tags": data["tags"],
            "location": data["region"],
            "ipv6": data["ipv6"],
            "hypervisor": data["hypervisor"],
            "specs": data["specs"],
            "alerts": data["alerts"],
            "backups": data["backups"],
            "watchdog_enabled": data["watchdog_enabled"],
        }

        public_ips = [ip for ip in data["ipv4"] if not is_private_subnet(ip)]
        private_ips = [ip for ip in data["ipv4"] if is_private_subnet(ip)]
        return Node(
            id=data["id"],
            name=data["label"],
            state=self.LINODE_STATES[data["status"]],
            public_ips=public_ips,
            private_ips=private_ips,
            driver=self,
            size=data["type"],
            image=data["image"],
            created_at=self._to_datetime(data["created"]),
            extra=extra,
        )

    def _to_datetime(self, strtime):
        return datetime.strptime(strtime, "%Y-%m-%dT%H:%M:%S")

    def _to_size(self, data):
        extra = {
            "class": data["class"],
            "monthly_price": data["price"]["monthly"],
            "addons": data["addons"],
            "successor": data["successor"],
            "transfer": data["transfer"],
            "vcpus": data["vcpus"],
            "gpus": data["gpus"],
        }
        return NodeSize(
            id=data["id"],
            name=data["label"],
            ram=data["memory"],
            disk=data["disk"],
            bandwidth=data["network_out"],
            price=data["price"]["hourly"],
            driver=self,
            extra=extra,
        )

    def _to_image(self, data):
        extra = {
            "type": data["type"],
            "description": data["description"],
            "created": self._to_datetime(data["created"]),
            "created_by": data["created_by"],
            "is_public": data["is_public"],
            "size": data["size"],
            "eol": data["eol"],
            "vendor": data["vendor"],
        }
        return NodeImage(id=data["id"], name=data["label"], driver=self, extra=extra)

    def _to_location(self, data):
        extra = {
            "status": data["status"],
            "capabilities": data["capabilities"],
            "resolvers": data["resolvers"],
        }
        return NodeLocation(
            id=data["id"],
            name=data["id"],
            country=data["country"].upper(),
            driver=self,
            extra=extra,
        )

    def _to_volume(self, data):
        extra = {
            "created": self._to_datetime(data["created"]),
            "tags": data["tags"],
            "location": data["region"],
            "linode_id": data["linode_id"],
            "linode_label": data["linode_label"],
            "state": self.LINODE_VOLUME_STATES[data["status"]],
            "filesystem_path": data["filesystem_path"],
        }
        return StorageVolume(
            id=str(data["id"]),
            name=data["label"],
            size=data["size"],
            driver=self,
            extra=extra,
        )

    def _to_disk(self, data):
        return LinodeDisk(
            id=data["id"],
            state=self.LINODE_DISK_STATES[data["status"]],
            name=data["label"],
            filesystem=data["filesystem"],
            size=data["size"],
            driver=self,
        )

    def _to_address(self, data):
        extra = {
            "gateway": data["gateway"],
            "subnet_mask": data["subnet_mask"],
            "prefix": data["prefix"],
            "rdns": data["rdns"],
            "node_id": data["linode_id"],
            "region": data["region"],
        }
        return LinodeIPAddress(
            inet=data["address"],
            public=data["public"],
            version=data["type"],
            driver=self,
            extra=extra,
        )

    def _to_addresses(self, data):
        addresses = data["ipv4"]["public"] + data["ipv4"]["private"]
        return [self._to_address(address) for address in addresses]

    def _paginated_request(self, url, obj, params=None):
        """
        Perform multiple calls in order to have a full list of elements when
        the API responses are paginated.

        :param url: API endpoint
        :type url: ``str``

        :param obj: Result object key
        :type obj: ``str``

        :param params: Request parameters
        :type params: ``dict``

        :return: ``list`` of API response objects
        :rtype: ``list``
        """
        objects = []
        params = params if params is not None else {}

        ret = self.connection.request(url, params=params).object

        data = list(ret.get(obj, []))
        current_page = int(ret.get("page", 1))
        num_of_pages = int(ret.get("pages", 1))
        objects.extend(data)
        for page in range(current_page + 1, num_of_pages + 1):
            # add param to request next page
            params["page"] = page
            ret = self.connection.request(url, params=params).object
            data = list(ret.get(obj, []))
            objects.extend(data)
        return objects
