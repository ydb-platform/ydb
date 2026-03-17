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
Provides base classes for working with drivers
"""


import os
import re
import time
import atexit
import random
import socket
import hashlib
import binascii
import datetime
import traceback
from typing import TYPE_CHECKING, Any, Dict, List, Type, Tuple, Union, Callable, Optional

import libcloud.compute.ssh
from libcloud.pricing import get_size_price
from libcloud.utils.py3 import b
from libcloud.common.base import BaseDriver, Connection, ConnectionKey
from libcloud.compute.ssh import SSHClient, BaseSSHClient, SSHCommandTimeoutError, have_paramiko
from libcloud.common.types import LibcloudError
from libcloud.compute.types import (
    Provider,
    NodeState,
    DeploymentError,
    StorageVolumeState,
    NodeImageMemberState,
)
from libcloud.utils.networking import is_private_subnet, is_valid_ip_address

if TYPE_CHECKING:
    from libcloud.compute.deployment import Deployment


if have_paramiko:
    from paramiko.ssh_exception import SSHException, AuthenticationException

    SSH_TIMEOUT_EXCEPTION_CLASSES = (
        AuthenticationException,
        SSHException,
        IOError,
        socket.gaierror,
        socket.error,
    )
else:
    SSH_TIMEOUT_EXCEPTION_CLASSES = (
        IOError,  # type: ignore
        socket.gaierror,  # type: ignore
        socket.error,
    )  # type: ignore

T_Auth = Union["NodeAuthSSHKey", "NodeAuthPassword"]

T_Ssh_key = Union[List[str], str]

# How long to wait for the node to come online after creating it
NODE_ONLINE_WAIT_TIMEOUT = 10 * 60

# How long to try connecting to a remote SSH server when running a deployment
# script.
SSH_CONNECT_TIMEOUT = 5 * 60

# Error message which should be considered fatal for deploy_node() method and
# on which we should abort retrying and immediately propagate the error
SSH_FATAL_ERROR_MSGS = [
    # Propagate (key) file doesn't exist errors
    # NOTE: Paramiko only supports PEM private key format
    # See https://github.com/paramiko/paramiko/issues/1313
    # for details
    "no such file or directory",
    "invalid key",
    "not a valid ",
    "invalid or unsupported key type",
    "private file is encrypted",
    "private key file is encrypted",
    "private key file checkints do not match",
    "invalid password provided",
]

__all__ = [
    "Node",
    "NodeState",
    "NodeSize",
    "NodeImage",
    "NodeImageMember",
    "NodeLocation",
    "NodeAuthSSHKey",
    "NodeAuthPassword",
    "NodeDriver",
    "StorageVolume",
    "StorageVolumeState",
    "VolumeSnapshot",
    # Deprecated, moved to libcloud.utils.networking
    "is_private_subnet",
    "is_valid_ip_address",
]


class UuidMixin:
    """
    Mixin class for get_uuid function.
    """

    def __init__(self) -> None:
        self._uuid = None  # type: Optional[str]

    def get_uuid(self):
        """
        Unique hash for a node, node image, or node size

        The hash is a function of an SHA1 hash of the node, node image,
        or node size's ID and its driver which means that it should be
        unique between all objects of its type.
        In some subclasses there is no ID available so the public IP
        address is used.  This means that, unlike a properly done system
        UUID, the same UUID may mean a different system install at a
        different time

        >>> from libcloud.compute.drivers.dummy import DummyNodeDriver
        >>> driver = DummyNodeDriver(0)
        >>> node = driver.create_node()
        >>> node.get_uuid()
        'd3748461511d8b9b0e0bfa0d4d3383a619a2bb9f'

        Note, for example, that this example will always produce the
        same UUID!

        :rtype: ``str``
        """

        if not self._uuid:
            self._uuid = hashlib.sha1(
                b("{}:{}".format(self.id, self.driver.type))
            ).hexdigest()  # nosec

        return self._uuid

    @property
    def uuid(self):
        # type: () -> str

        return self.get_uuid()


class Node(UuidMixin):
    """
    Provide a common interface for handling nodes of all types.

    The Node object provides the interface in libcloud through which
    we can manipulate nodes in different cloud providers in the same
    way.  Node objects don't actually do much directly themselves,
    instead the node driver handles the connection to the node.

    You don't normally create a node object yourself; instead you use
    a driver and then have that create the node for you.

    >>> from libcloud.compute.drivers.dummy import DummyNodeDriver
    >>> driver = DummyNodeDriver(0)
    >>> node = driver.create_node()
    >>> node.public_ips[0]
    '127.0.0.3'
    >>> node.name
    'dummy-3'

    You can also get nodes from the driver's list_node function.

    >>> node = driver.list_nodes()[0]
    >>> node.name
    'dummy-1'

    The node keeps a reference to its own driver which means that we
    can work on nodes from different providers without having to know
    which is which.

    >>> driver = DummyNodeDriver(72)
    >>> node2 = driver.create_node()
    >>> node.driver.creds
    0
    >>> node2.driver.creds
    72

    Although Node objects can be subclassed, this isn't normally
    done.  Instead, any driver specific information is stored in the
    "extra" attribute of the node.

    >>> node.extra
    {'foo': 'bar'}
    """

    def __init__(
        self,
        id,  # type: str
        name,  # type: str
        state,  # type: NodeState
        public_ips,  # type: List[str]
        private_ips,  # type: List[str]
        driver,
        size=None,  # type: Optional[NodeSize]
        image=None,  # type: Optional[NodeImage]
        extra=None,  # type: Optional[dict]
        created_at=None,  # type: Optional[datetime.datetime]
    ):
        """
        :param id: Node ID.
        :type id: ``str``

        :param name: Node name.
        :type name: ``str``

        :param state: Node state.
        :type state: :class:`libcloud.compute.types.NodeState`


        :param public_ips: Public IP addresses associated with this node.
        :type public_ips: ``list``

        :param private_ips: Private IP addresses associated with this node.
        :type private_ips: ``list``

        :param driver: Driver this node belongs to.
        :type driver: :class:`.NodeDriver`

        :param size: Size of this node. (optional)
        :type size: :class:`.NodeSize`

        :param image: Image of this node. (optional)
        :type image: :class:`.NodeImage`

        :param created_at: The datetime this node was created (optional)
        :type created_at: :class: `datetime.datetime`

        :param extra: Optional provider specific attributes associated with
                      this node.
        :type extra: ``dict``

        """
        self.id = str(id) if id else None
        self.name = name
        self.state = state
        self.public_ips = public_ips if public_ips else []
        self.private_ips = private_ips if private_ips else []
        self.driver = driver
        self.size = size
        self.created_at = created_at
        self.image = image
        self.extra = extra or {}
        UuidMixin.__init__(self)

    def reboot(self):
        # type: () -> bool
        """
        Reboot this node

        :return: ``bool``

        This calls the node's driver and reboots the node

        >>> from libcloud.compute.drivers.dummy import DummyNodeDriver
        >>> driver = DummyNodeDriver(0)
        >>> node = driver.create_node()
        >>> node.state == NodeState.RUNNING
        True
        >>> node.state == NodeState.REBOOTING
        False
        >>> node.reboot()
        True
        >>> node.state == NodeState.REBOOTING
        True
        """

        return self.driver.reboot_node(self)

    def start(self):
        # type: () -> bool
        """
        Start this node.

        :return: ``bool``
        """

        return self.driver.start_node(self)

    def stop_node(self):
        # type: () -> bool
        """
        Stop (shutdown) this node.

        :return: ``bool``
        """

        return self.driver.stop_node(self)

    def destroy(self):
        # type: () -> bool
        """
        Destroy this node

        :return: ``bool``

        This calls the node's driver and destroys the node

        >>> from libcloud.compute.drivers.dummy import DummyNodeDriver
        >>> driver = DummyNodeDriver(0)
        >>> from libcloud.compute.types import NodeState
        >>> node = driver.create_node()
        >>> node.state == NodeState.RUNNING
        True
        >>> node.destroy()
        True
        >>> node.state == NodeState.RUNNING
        False

        """

        return self.driver.destroy_node(self)

    def __repr__(self):
        state = NodeState.tostring(self.state)

        return (
            "<Node: uuid=%s, name=%s, state=%s, public_ips=%s, " "private_ips=%s, provider=%s ...>"
        ) % (
            self.uuid,
            self.name,
            state,
            self.public_ips,
            self.private_ips,
            self.driver.name,
        )


class NodeSize(UuidMixin):
    """
    A Base NodeSize class to derive from.

    NodeSizes are objects which are typically returned a driver's
    list_sizes function.  They contain a number of different
    parameters which define how big an image is.

    The exact parameters available depends on the provider.

    N.B. Where a parameter is "unlimited" (for example bandwidth in
    Amazon) this will be given as 0.

    >>> from libcloud.compute.drivers.dummy import DummyNodeDriver
    >>> driver = DummyNodeDriver(0)
    >>> size = driver.list_sizes()[0]
    >>> size.ram
    128
    >>> size.bandwidth
    500
    >>> size.price
    4
    """

    def __init__(
        self,
        id,  # type: str
        name,  # type: str
        ram,  # type: int
        disk,  # type: int
        bandwidth,  # type: Optional[int]
        price,  # type: float
        driver,  # type: NodeDriver
        extra=None,  # type: Optional[dict]
    ):
        """
        :param id: Size ID.
        :type  id: ``str``

        :param name: Size name.
        :type  name: ``str``

        :param ram: Amount of memory (in MB) provided by this size.
        :type  ram: ``int``

        :param disk: Amount of disk storage (in GB) provided by this image.
        :type  disk: ``int``

        :param bandwidth: Amount of bandiwdth included with this size.
        :type  bandwidth: ``int``

        :param price: Price (in US dollars) of running this node for an hour.
        :type  price: ``float``

        :param driver: Driver this size belongs to.
        :type  driver: :class:`.NodeDriver`

        :param extra: Optional provider specific attributes associated with
                      this size.
        :type  extra: ``dict``
        """
        self.id = str(id)
        self.name = name
        self.ram = ram
        self.disk = disk
        self.bandwidth = bandwidth
        self.price = price
        self.driver = driver
        self.extra = extra or {}
        UuidMixin.__init__(self)

    def __repr__(self):
        return (
            "<NodeSize: id=%s, name=%s, ram=%s disk=%s bandwidth=%s " "price=%s driver=%s ...>"
        ) % (
            self.id,
            self.name,
            self.ram,
            self.disk,
            self.bandwidth,
            self.price,
            self.driver.name,
        )


class NodeImage(UuidMixin):
    """
    An operating system image.

    NodeImage objects are typically returned by the driver for the
    cloud provider in response to the list_images function

    >>> from libcloud.compute.drivers.dummy import DummyNodeDriver
    >>> driver = DummyNodeDriver(0)
    >>> image = driver.list_images()[0]
    >>> image.name
    'Ubuntu 9.10'

    Apart from name and id, there is no further standard information;
    other parameters are stored in a driver specific "extra" variable

    When creating a node, a node image should be given as an argument
    to the create_node function to decide which OS image to use.

    >>> node = driver.create_node(image=image)
    """

    def __init__(
        self,
        id,  # type: str
        name,  # type: str
        driver,  # type: NodeDriver
        extra=None,  # type: Optional[dict]
    ):
        """
        :param id: Image ID.
        :type id: ``str``

        :param name: Image name.
        :type name: ``str``

        :param driver: Driver this image belongs to.
        :type driver: :class:`.NodeDriver`

        :param extra: Optional provided specific attributes associated with
                      this image.
        :type extra: ``dict``
        """
        self.id = str(id)
        self.name = name
        self.driver = driver
        self.extra = extra or {}
        UuidMixin.__init__(self)

    def __repr__(self):
        return ("<NodeImage: id=%s, name=%s, driver=%s  ...>") % (
            self.id,
            self.name,
            self.driver.name,
        )


class NodeImageMember(UuidMixin):
    """
    A member of an image. At some cloud providers there is a mechanism
    to share images. Once an image is shared with another account that
    user will be a 'member' of the image.

    For example, see the image members schema in the OpenStack Image
    Service API v2 documentation. https://developer.openstack.org/
    api-ref/image/v2/index.html#image-members-schema

    NodeImageMember objects are typically returned by the driver for the
    cloud provider in response to the list_image_members method
    """

    def __init__(
        self,
        id,  # type: str
        image_id,  # type: str
        state,  # type: NodeImageMemberState
        driver,  # type: NodeDriver
        created=None,  # type: Optional[datetime.datetime]
        extra=None,  # type: Optional[dict]
    ):
        """
        :param id: Image member ID.
        :type id: ``str``

        :param id: The associated image ID.
        :type id: ``str``

        :param state: State of the NodeImageMember. If not
                      provided, will default to UNKNOWN.
        :type state: :class:`.NodeImageMemberState`

        :param driver: Driver this image belongs to.
        :type driver: :class:`.NodeDriver`

        :param      created: A datetime object that represents when the
                             image member was created
        :type       created: ``datetime.datetime``

        :param extra: Optional provided specific attributes associated with
                      this image.
        :type extra: ``dict``
        """
        self.id = str(id)
        self.image_id = str(image_id)
        self.state = state
        self.driver = driver
        self.created = created
        self.extra = extra or {}
        UuidMixin.__init__(self)

    def __repr__(self):
        return ("<NodeImageMember: id=%s, image_id=%s, " "state=%s, driver=%s  ...>") % (
            self.id,
            self.image_id,
            self.state,
            self.driver.name,
        )


class NodeLocation:
    """
    A physical location where nodes can be.

    >>> from libcloud.compute.drivers.dummy import DummyNodeDriver
    >>> driver = DummyNodeDriver(0)
    >>> location = driver.list_locations()[0]
    >>> location.country
    'US'
    """

    def __init__(
        self,
        id,  # type: str
        name,  # type: str
        country,  # type: str
        driver,  # type: NodeDriver
        extra=None,  # type: Optional[dict]
    ):
        """
        :param id: Location ID.
        :type id: ``str``

        :param name: Location name.
        :type name: ``str``

        :param country: Location country.
        :type country: ``str``

        :param driver: Driver this location belongs to.
        :type driver: :class:`.NodeDriver`

        :param extra: Optional provided specific attributes associated with
                      this location.
        :type extra: ``dict``
        """
        self.id = str(id)
        self.name = name
        self.country = country
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return ("<NodeLocation: id=%s, name=%s, country=%s, driver=%s>") % (
            self.id,
            self.name,
            self.country,
            self.driver.name,
        )


class NodeAuthSSHKey:
    """
    An SSH key to be installed for authentication to a node.

    This is the actual contents of the users ssh public key which will
    normally be installed as root's public key on the node.

    >>> pubkey = '...' # read from file
    >>> from libcloud.compute.base import NodeAuthSSHKey
    >>> k = NodeAuthSSHKey(pubkey)
    >>> k
    <NodeAuthSSHKey>
    """

    def __init__(self, pubkey):
        # type: (str) -> None
        """
        :param pubkey: Public key material.
        :type pubkey: ``str``
        """
        self.pubkey = pubkey

    def __repr__(self):
        return "<NodeAuthSSHKey>"


class NodeAuthPassword:
    """
    A password to be used for authentication to a node.
    """

    def __init__(self, password, generated=False):
        # type: (str, bool) -> None
        """
        :param password: Password.
        :type password: ``str``

        :type generated: ``True`` if this password was automatically generated,
                         ``False`` otherwise.
        """
        self.password = password
        self.generated = generated

    def __repr__(self):
        return "<NodeAuthPassword>"


class StorageVolume(UuidMixin):
    """
    A base StorageVolume class to derive from.
    """

    def __init__(
        self,
        id,  # type: str
        name,  # type: str
        size,  # type: int
        driver,  # type: NodeDriver
        state=None,  # type: Optional[StorageVolumeState]
        extra=None,  # type: Optional[Dict]
    ):
        # type: (...) -> None
        """
        :param id: Storage volume ID.
        :type id: ``str``

        :param name: Storage volume name.
        :type name: ``str``

        :param size: Size of this volume (in GB).
        :type size: ``int``

        :param driver: Driver this image belongs to.
        :type driver: :class:`.NodeDriver`

        :param state: Optional state of the StorageVolume. If not
                      provided, will default to UNKNOWN.
        :type state: :class:`.StorageVolumeState`

        :param extra: Optional provider specific attributes.
        :type extra: ``dict``
        """
        self.id = id
        self.name = name
        self.size = size
        self.driver = driver
        self.extra = extra
        self.state = state
        UuidMixin.__init__(self)

    def list_snapshots(self):
        # type: () -> List[VolumeSnapshot]
        """
        :rtype: ``list`` of ``VolumeSnapshot``
        """

        return self.driver.list_volume_snapshots(volume=self)

    def attach(self, node, device=None):
        # type: (Node, Optional[str]) -> bool
        """
        Attach this volume to a node.

        :param node: Node to attach volume to
        :type node: :class:`.Node`

        :param device: Where the device is exposed,
                            e.g. '/dev/sdb (optional)
        :type device: ``str``

        :return: ``True`` if attach was successful, ``False`` otherwise.
        :rtype: ``bool``
        """

        return self.driver.attach_volume(node=node, volume=self, device=device)

    def detach(self):
        # type: () -> bool
        """
        Detach this volume from its node

        :return: ``True`` if detach was successful, ``False`` otherwise.
        :rtype: ``bool``
        """

        return self.driver.detach_volume(volume=self)

    def snapshot(self, name):
        # type: (str) -> VolumeSnapshot
        """
        Creates a snapshot of this volume.

        :return: Created snapshot.
        :rtype: ``VolumeSnapshot``
        """

        return self.driver.create_volume_snapshot(volume=self, name=name)

    def destroy(self):
        # type: () -> bool
        """
        Destroy this storage volume.

        :return: ``True`` if destroy was successful, ``False`` otherwise.
        :rtype: ``bool``
        """

        return self.driver.destroy_volume(volume=self)

    def __repr__(self):
        return "<StorageVolume id={} size={} driver={}>".format(
            self.id,
            self.size,
            self.driver.name,
        )


class VolumeSnapshot:
    """
    A base VolumeSnapshot class to derive from.
    """

    def __init__(
        self,
        id,  # type: str
        driver,  # type: NodeDriver
        size=None,  # type: Optional[int]
        extra=None,  # type: Optional[Dict]
        created=None,  # type: Optional[datetime.datetime]
        state=None,  # type: Optional[StorageVolumeState]
        name=None,  # type: Optional[str]
    ):
        # type: (...) -> None
        """
        VolumeSnapshot constructor.

        :param      id: Snapshot ID.
        :type       id: ``str``

        :param      driver: The driver that represents a connection to the
                            provider
        :type       driver: `NodeDriver`

        :param      size: A snapshot size in GB.
        :type       size: ``int``

        :param      extra: Provider depends parameters for snapshot.
        :type       extra: ``dict``

        :param      created: A datetime object that represents when the
                             snapshot was created
        :type       created: ``datetime.datetime``

        :param      state: A string representing the state the snapshot is
                           in. See `libcloud.compute.types.StorageVolumeState`.
        :type       state: ``StorageVolumeState``

        :param      name: A string representing the name of the snapshot
        :type       name: ``str``
        """
        self.id = id
        self.driver = driver
        self.size = size
        self.extra = extra or {}
        self.created = created
        self.state = state
        self.name = name

    def destroy(self):
        # type: () -> bool
        """
        Destroys this snapshot.

        :rtype: ``bool``
        """

        return self.driver.destroy_volume_snapshot(snapshot=self)

    def __repr__(self):
        return '<VolumeSnapshot "{}" id={} size={} driver={} state={}>'.format(
            self.name,
            self.id,
            self.size,
            self.driver.name,
            self.state,
        )


class KeyPair:
    """
    Represents a SSH key pair.
    """

    def __init__(
        self,
        name,  # type: str
        public_key,  # type: str
        fingerprint,  # type: str
        driver,  # type: NodeDriver
        private_key=None,  # type: Optional[str]
        extra=None,  # type: Optional[Dict]
    ):
        # type: (...) -> None
        """
        Constructor.

        :keyword    name: Name of the key pair object.
        :type       name: ``str``

        :keyword    fingerprint: Key fingerprint.
        :type       fingerprint: ``str``

        :keyword    public_key: Public key in OpenSSH format.
        :type       public_key: ``str``

        :keyword    private_key: Private key in PEM format.
        :type       private_key: ``str``

        :keyword    extra: Provider specific attributes associated with this
                           key pair. (optional)
        :type       extra: ``dict``
        """
        self.name = name
        self.fingerprint = fingerprint
        self.public_key = public_key
        self.private_key = private_key
        self.driver = driver
        self.extra = extra or {}

    def __repr__(self):
        return "<KeyPair name={} fingerprint={} driver={}>".format(
            self.name,
            self.fingerprint,
            self.driver.name,
        )


class NodeDriver(BaseDriver):
    """
    A base NodeDriver class to derive from

    This class is always subclassed by a specific driver.  For
    examples of base behavior of most functions (except deploy node)
    see the dummy driver.
    """

    connectionCls = ConnectionKey  # type: Type[Connection]
    name = None  # type: str
    api_name = None  # type: str
    website = None  # type: str
    type = None  # type: Union[Provider,str]
    port = None  # type: int
    features = {"create_node": []}  # type: Dict[str, List[str]]

    """
    List of available features for a driver.
        - :meth:`libcloud.compute.base.NodeDriver.create_node`
            - ssh_key: Supports :class:`.NodeAuthSSHKey` as an authentication
              method for nodes.
            - password: Supports :class:`.NodeAuthPassword` as an
              authentication
              method for nodes.
            - generates_password: Returns a password attribute on the Node
              object returned from creation.
    """

    NODE_STATE_MAP = {}  # type: Dict[str, NodeState]

    def list_nodes(self, *args, **kwargs):
        # type: (Any, Any) -> List[Node]
        """
        List all nodes.

        :return:  list of node objects
        :rtype: ``list`` of :class:`.Node`
        """
        raise NotImplementedError("list_nodes not implemented for this driver")

    def list_sizes(self, location=None):
        # type: (Optional[NodeLocation]) -> List[NodeSize]
        """
        List sizes on a provider

        :param location: The location at which to list sizes
        :type location: :class:`.NodeLocation`

        :return: list of node size objects
        :rtype: ``list`` of :class:`.NodeSize`
        """
        raise NotImplementedError("list_sizes not implemented for this driver")

    def list_locations(self):
        # type: () -> List[NodeLocation]
        """
        List data centers for a provider

        :return: list of node location objects
        :rtype: ``list`` of :class:`.NodeLocation`
        """
        raise NotImplementedError("list_locations not implemented for this driver")

    def create_node(
        self,
        name,  # type: str
        size,  # type: NodeSize
        image,  # type: NodeImage
        location=None,  # type: Optional[NodeLocation]
        auth=None,  # type: Optional[T_Auth]
    ):
        # type: (...) -> Node
        """
        Create a new node instance. This instance will be started
        automatically.

        Not all hosting API's are created equal and to allow libcloud to
        support as many as possible there are some standard supported
        variations of ``create_node``. These are declared using a
        ``features`` API.
        You can inspect ``driver.features['create_node']`` to see what
        variation of the API you are dealing with:

        ``ssh_key``
            You can inject a public key into a new node allows key based SSH
            authentication.
        ``password``
            You can inject a password into a new node for SSH authentication.
            If no password is provided libcloud will generated a password.
            The password will be available as
            ``return_value.extra['password']``.
        ``generates_password``
            The hosting provider will generate a password. It will be returned
            to you via ``return_value.extra['password']``.

        Some drivers allow you to set how you will authenticate with the
        instance that is created. You can inject this initial authentication
        information via the ``auth`` parameter.

        If a driver supports the ``ssh_key`` feature flag for ``created_node``
        you can upload a public key into the new instance::

            >>> from libcloud.compute.drivers.dummy import DummyNodeDriver
            >>> driver = DummyNodeDriver(0)
            >>> auth = NodeAuthSSHKey('pubkey data here')
            >>> node = driver.create_node("test_node", auth=auth)

        If a driver supports the ``password`` feature flag for ``create_node``
        you can set a password::

            >>> driver = DummyNodeDriver(0)
            >>> auth = NodeAuthPassword('mysecretpassword')
            >>> node = driver.create_node("test_node", auth=auth)

        If a driver supports the ``password`` feature and you don't provide the
        ``auth`` argument libcloud will assign a password::

            >>> driver = DummyNodeDriver(0)
            >>> node = driver.create_node("test_node")
            >>> password = node.extra['password']

        A password will also be returned in this way for drivers that declare
        the ``generates_password`` feature, though in that case the password is
        actually provided to the driver API by the hosting provider rather than
        generated by libcloud.

        You can only pass a :class:`.NodeAuthPassword` or
        :class:`.NodeAuthSSHKey` to ``create_node`` via the auth parameter if
        has the corresponding feature flag.

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
        :type auth:   :class:`.NodeAuthSSHKey` or :class:`NodeAuthPassword`

        :return: The newly created node.
        :rtype: :class:`.Node`
        """
        raise NotImplementedError("create_node not implemented for this driver")

    def deploy_node(
        self,
        deploy,  # type: Deployment
        ssh_username="root",  # type: str
        ssh_alternate_usernames=None,  # type: Optional[List[str]]
        ssh_port=22,  # type: int
        ssh_timeout=10,  # type: int
        ssh_key=None,  # type: Optional[T_Ssh_key]
        ssh_key_password=None,  # type: Optional[str]
        auth=None,  # type: Optional[T_Auth]
        timeout=SSH_CONNECT_TIMEOUT,  # type: int
        max_tries=3,  # type: int
        ssh_interface="public_ips",  # type: str
        at_exit_func=None,  # type: Optional[Callable]
        wait_period=5,  # type: int
        **create_node_kwargs,
    ):
        # type: (...) -> Node
        """
        Create a new node, and start deployment.

        In order to be able to SSH into a created node access credentials are
        required.

        A user can pass either a :class:`.NodeAuthPassword` or
        :class:`.NodeAuthSSHKey` to the ``auth`` argument. If the
        ``create_node`` implementation supports that kind if credential (as
        declared in ``self.features['create_node']``) then it is passed on to
        ``create_node``. Otherwise it is not passed on to ``create_node`` and
        it is only used for authentication.

        If the ``auth`` parameter is not supplied but the driver declares it
        supports ``generates_password`` then the password returned by
        ``create_node`` will be used to SSH into the server.

        Finally, if the ``ssh_key_file`` is supplied that key will be used to
        SSH into the server.

        This function may raise a :class:`DeploymentException`, if a
        create_node call was successful, but there is a later error (like SSH
        failing or timing out).  This exception includes a Node object which
        you may want to destroy if incomplete deployments are not desirable.

        >>> from libcloud.compute.drivers.dummy import DummyNodeDriver
        >>> from libcloud.compute.deployment import ScriptDeployment
        >>> from libcloud.compute.deployment import MultiStepDeployment
        >>> from libcloud.compute.base import NodeAuthSSHKey
        >>> driver = DummyNodeDriver(0)
        >>> key = NodeAuthSSHKey('...') # read from file
        >>> script = ScriptDeployment("yum -y install emacs strace tcpdump")
        >>> msd = MultiStepDeployment([key, script])
        >>> def d():
        ...     try:
        ...         driver.deploy_node(deploy=msd)
        ...     except NotImplementedError:
        ...         print ("not implemented for dummy driver")
        >>> d()
        not implemented for dummy driver

        Deploy node is typically not overridden in subclasses.  The
        existing implementation should be able to handle most such.

        :param deploy: Deployment to run once machine is online and
                            available to SSH.
        :type deploy: :class:`Deployment`

        :param ssh_username: Optional name of the account which is used
                                  when connecting to
                                  SSH server (default is root)
        :type ssh_username: ``str``

        :param ssh_alternate_usernames: Optional list of ssh usernames to
                                             try to connect with if using the
                                             default one fails
        :type ssh_alternate_usernames: ``list``

        :param ssh_port: Optional SSH server port (default is 22)
        :type ssh_port: ``int``

        :param ssh_timeout: Optional SSH connection timeout in seconds
                                 (default is 10)
        :type ssh_timeout: ``float``

        :param auth:   Initial authentication information for the node
                            (optional)
        :type auth:   :class:`.NodeAuthSSHKey` or :class:`NodeAuthPassword`

        :param ssh_key: A path (or paths) to an SSH private key with which
                             to attempt to authenticate. (optional)
        :type ssh_key: ``str`` or ``list`` of ``str``

        :param ssh_key_password: Optional password used for encrypted keys.
        :type ssh_key_password: ``str``

        :param timeout: How many seconds to wait before timing out.
                             (default is 600)
        :type timeout: ``int``

        :param max_tries: How many times to retry if a deployment fails
                               before giving up (default is 3)
        :type max_tries: ``int``

        :param ssh_interface: The interface to wait for. Default is
                                   'public_ips', other option is 'private_ips'.
        :type ssh_interface: ``str``

        :param at_exit_func: Optional atexit handler function which will be
                             registered and called with created node if user
                             cancels the deploy process (e.g. CTRL+C), after
                             the node has been created, but before the deploy
                             process has finished.

                             This method gets passed in two keyword arguments:

                             - driver -> node driver in question
                             - node -> created Node object

                             Keep in mind that this function will only be
                             called in such scenario. In case the method
                             finishes (this includes throwing an exception),
                             at exit handler function won't be called.
        :type at_exit_func: ``func``

        :param wait_period: How many seconds to wait between each iteration
                            while waiting for node to transition into
                            running state and have IP assigned. (default is 5)
        :type wait_period: ``int``

        """

        if not libcloud.compute.ssh.have_paramiko:
            raise RuntimeError(
                "paramiko is not installed. You can install " + "it using pip: pip install paramiko"
            )

        if auth:
            if not isinstance(auth, (NodeAuthSSHKey, NodeAuthPassword)):
                raise NotImplementedError(
                    "If providing auth, only NodeAuthSSHKey or" "NodeAuthPassword is supported"
                )
        elif ssh_key:
            # If an ssh_key is provided we can try deploy_node
            pass
        elif "create_node" in self.features:
            f = self.features["create_node"]

            if "generates_password" not in f and "password" not in f:
                raise NotImplementedError("deploy_node not implemented for this driver")
        else:
            raise NotImplementedError("deploy_node not implemented for this driver")

        # NOTE 1: This is a workaround for legacy code. Sadly a lot of legacy
        # code uses **kwargs in "create_node()" method and simply ignores
        # "deploy_node()" arguments which are passed to it.
        # That's obviously far from idea that's why we first try to pass only
        # non-deploy node arguments to the "create_node()" methods and if it
        # that doesn't work, fall back to the old approach and simply pass in
        # all the arguments
        # NOTE 2: Some drivers which use password based SSH authentication
        # rely on password being stored on the "auth" argument and that's why
        # we also propagate that argument to "create_node()" method.
        try:
            # NOTE: We only pass auth to the method if auth argument is
            # provided

            if auth:
                node = self.create_node(auth=auth, **create_node_kwargs)
            else:
                node = self.create_node(**create_node_kwargs)
        except TypeError as e:
            msg_1_re = r"create_node\(\) missing \d+ required " "positional arguments.*"
            msg_2_re = r"create_node\(\) takes at least \d+ arguments.*"

            if re.match(msg_1_re, str(e)) or re.match(msg_2_re, str(e)):
                # pylint: disable=unexpected-keyword-arg
                node = self.create_node(  # type: ignore
                    deploy=deploy,
                    ssh_username=ssh_username,
                    ssh_alternate_usernames=ssh_alternate_usernames,
                    ssh_port=ssh_port,
                    ssh_timeout=ssh_timeout,
                    ssh_key=ssh_key,
                    auth=auth,
                    timeout=timeout,
                    max_tries=max_tries,
                    ssh_interface=ssh_interface,
                    **create_node_kwargs,
                )
                # pylint: enable=unexpected-keyword-arg
            else:
                raise e

        if at_exit_func:
            atexit.register(at_exit_func, driver=self, node=node)

        password = None

        if auth:
            if isinstance(auth, NodeAuthPassword):
                password = auth.password
        elif "password" in node.extra:
            password = node.extra["password"]

        wait_timeout = timeout or NODE_ONLINE_WAIT_TIMEOUT

        # Wait until node is up and running and has IP assigned
        try:
            node, ip_addresses = self.wait_until_running(
                nodes=[node],
                wait_period=wait_period,
                timeout=wait_timeout,
                ssh_interface=ssh_interface,
            )[0]
        except Exception as e:
            if at_exit_func:
                atexit.unregister(at_exit_func)

            raise DeploymentError(node=node, original_exception=e, driver=self)

        ssh_alternate_usernames = ssh_alternate_usernames or []
        deploy_timeout = timeout or SSH_CONNECT_TIMEOUT

        deploy_error = None

        for username in [ssh_username] + ssh_alternate_usernames:
            try:
                self._connect_and_run_deployment_script(
                    task=deploy,
                    node=node,
                    ssh_hostname=ip_addresses[0],
                    ssh_port=ssh_port,
                    ssh_username=username,
                    ssh_password=password,
                    ssh_key_file=ssh_key,
                    ssh_key_password=ssh_key_password,
                    ssh_timeout=ssh_timeout,
                    timeout=deploy_timeout,
                    max_tries=max_tries,
                )
            except Exception as e:
                # Try alternate username
                # Todo: Need to fix paramiko so we can catch a more specific
                # exception
                deploy_error = e
            else:
                # Script successfully executed, don't try alternate username
                deploy_error = None

                break

        if deploy_error is not None:
            if at_exit_func:
                atexit.unregister(at_exit_func)

            raise DeploymentError(node=node, original_exception=deploy_error, driver=self)

        if at_exit_func:
            atexit.unregister(at_exit_func)

        return node

    def reboot_node(self, node):
        # type: (Node) -> bool
        """
        Reboot a node.

        :param node: The node to be rebooted
        :type node: :class:`.Node`

        :return: True if the reboot was successful, otherwise False
        :rtype: ``bool``
        """
        raise NotImplementedError("reboot_node not implemented for this driver")

    def start_node(self, node):
        # type: (Node) -> bool
        """
        Start a node.

        :param node: The node to be started
        :type node: :class:`.Node`

        :return: True if the start was successful, otherwise False
        :rtype: ``bool``
        """
        raise NotImplementedError("start_node not implemented for this driver")

    def stop_node(self, node):
        # type: (Node) -> bool
        """
        Stop a node

        :param node: The node to be stopped.
        :type node: :class:`.Node`

        :return: True if the stop was successful, otherwise False
        :rtype: ``bool``
        """
        raise NotImplementedError("stop_node not implemented for this driver")

    def destroy_node(self, node):
        # type: (Node) -> bool
        """
        Destroy a node.

        Depending upon the provider, this may destroy all data associated with
        the node, including backups.

        :param node: The node to be destroyed
        :type node: :class:`.Node`

        :return: True if the destroy was successful, False otherwise.
        :rtype: ``bool``
        """
        raise NotImplementedError("destroy_node not implemented for this driver")

    ##
    # Volume and snapshot management methods
    ##

    def list_volumes(self):
        # type: () -> List[StorageVolume]
        """
        List storage volumes.

        :rtype: ``list`` of :class:`.StorageVolume`
        """
        raise NotImplementedError("list_volumes not implemented for this driver")

    def list_volume_snapshots(self, volume):
        # type: (StorageVolume) -> List[VolumeSnapshot]
        """
        List snapshots for a storage volume.

        :rtype: ``list`` of :class:`VolumeSnapshot`
        """
        raise NotImplementedError("list_volume_snapshots not implemented for this driver")

    def create_volume(
        self,
        size,  # type: int
        name,  # type: str
        location=None,  # Optional[NodeLocation]
        snapshot=None,  # Optional[VolumeSnapshot]
    ):
        # type: (...) -> StorageVolume
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
        :type snapshot: :class:`.VolumeSnapshot`

        :return: The newly created volume.
        :rtype: :class:`StorageVolume`
        """
        raise NotImplementedError("create_volume not implemented for this driver")

    def create_volume_snapshot(self, volume, name=None):
        # type: (StorageVolume, Optional[str]) -> VolumeSnapshot
        """
        Creates a snapshot of the storage volume.

        :param volume: The StorageVolume to create a VolumeSnapshot from
        :type volume: :class:`.StorageVolume`

        :param name: Name of created snapshot (optional)
        :type name: `str`

        :rtype: :class:`VolumeSnapshot`
        """
        raise NotImplementedError("create_volume_snapshot not implemented for this driver")

    def attach_volume(self, node, volume, device=None):
        # type: (Node, StorageVolume, Optional[str]) -> bool
        """
        Attaches volume to node.

        :param node: Node to attach volume to.
        :type node: :class:`.Node`

        :param volume: Volume to attach.
        :type volume: :class:`.StorageVolume`

        :param device: Where the device is exposed, e.g. '/dev/sdb'
        :type device: ``str``

        :rytpe: ``bool``
        """
        raise NotImplementedError("attach not implemented for this driver")

    def detach_volume(self, volume):
        # type: (StorageVolume) -> bool
        """
        Detaches a volume from a node.

        :param volume: Volume to be detached
        :type volume: :class:`.StorageVolume`

        :rtype: ``bool``
        """

        raise NotImplementedError("detach not implemented for this driver")

    def destroy_volume(self, volume):
        # type: (StorageVolume) -> bool
        """
        Destroys a storage volume.

        :param volume: Volume to be destroyed
        :type volume: :class:`StorageVolume`

        :rtype: ``bool``
        """

        raise NotImplementedError("destroy_volume not implemented for this driver")

    def destroy_volume_snapshot(self, snapshot):
        # type: (VolumeSnapshot) -> bool
        """
        Destroys a snapshot.

        :param snapshot: The snapshot to delete
        :type snapshot: :class:`VolumeSnapshot`

        :rtype: :class:`bool`
        """
        raise NotImplementedError("destroy_volume_snapshot not implemented for this driver")

    ##
    # Image management methods
    ##

    def list_images(self, location=None):
        # type: (Optional[NodeLocation]) -> List[NodeImage]
        """
        List images on a provider.

        :param location: The location at which to list images.
        :type location: :class:`.NodeLocation`

        :return: list of node image objects.
        :rtype: ``list`` of :class:`.NodeImage`
        """
        raise NotImplementedError("list_images not implemented for this driver")

    def create_image(self, node, name, description=None):
        # type: (Node, str, Optional[str]) -> List[NodeImage]
        """
        Creates an image from a node object.

        :param node: Node to run the task on.
        :type node: :class:`.Node`

        :param name: name for new image.
        :type name: ``str``

        :param description: description for new image.
        :type name: ``description``

        :rtype: :class:`.NodeImage`:
        :return: NodeImage instance on success.

        """
        raise NotImplementedError("create_image not implemented for this driver")

    def delete_image(self, node_image):
        # type: (NodeImage) -> bool
        """
        Deletes a node image from a provider.

        :param node_image: Node image object.
        :type node_image: :class:`.NodeImage`

        :return: ``True`` if delete_image was successful, ``False`` otherwise.
        :rtype: ``bool``
        """

        raise NotImplementedError("delete_image not implemented for this driver")

    def get_image(self, image_id):
        # type: (str) -> NodeImage
        """
        Returns a single node image from a provider.

        :param image_id: Node to run the task on.
        :type image_id: ``str``

        :rtype :class:`.NodeImage`:
        :return: NodeImage instance on success.
        """
        raise NotImplementedError("get_image not implemented for this driver")

    def copy_image(self, source_region, node_image, name, description=None):
        # type: (str, NodeImage, str, Optional[str]) -> NodeImage
        """
        Copies an image from a source region to the current region.

        :param source_region: Region to copy the node from.
        :type source_region: ``str``

        :param node_image: NodeImage to copy.
        :type node_image: :class:`.NodeImage`:

        :param name: name for new image.
        :type name: ``str``

        :param description: description for new image.
        :type name: ``str``

        :rtype: :class:`.NodeImage`:
        :return: NodeImage instance on success.
        """
        raise NotImplementedError("copy_image not implemented for this driver")

    ##
    # SSH key pair management methods
    ##

    def list_key_pairs(self):
        # type: () -> List[KeyPair]
        """
        List all the available key pair objects.

        :rtype: ``list`` of :class:`.KeyPair` objects
        """
        raise NotImplementedError("list_key_pairs not implemented for this driver")

    def get_key_pair(self, name):
        # type: (str) -> KeyPair
        """
        Retrieve a single key pair.

        :param name: Name of the key pair to retrieve.
        :type name: ``str``

        :rtype: :class:`.KeyPair`
        """
        raise NotImplementedError("get_key_pair not implemented for this driver")

    def create_key_pair(self, name):
        # type: (str) -> KeyPair
        """
        Create a new key pair object.

        :param name: Key pair name.
        :type name: ``str``

        :rtype: :class:`.KeyPair` object
        """
        raise NotImplementedError("create_key_pair not implemented for this driver")

    def import_key_pair_from_string(self, name, key_material):
        # type: (str, str) -> KeyPair
        """
        Import a new public key from string.

        :param name: Key pair name.
        :type name: ``str``

        :param key_material: Public key material.
        :type key_material: ``str``

        :rtype: :class:`.KeyPair` object
        """
        raise NotImplementedError("import_key_pair_from_string not implemented for this driver")

    def import_key_pair_from_file(self, name, key_file_path):
        # type: (str, str) -> KeyPair
        """
        Import a new public key from string.

        :param name: Key pair name.
        :type name: ``str``

        :param key_file_path: Path to the public key file.
        :type key_file_path: ``str``

        :rtype: :class:`.KeyPair` object
        """
        key_file_path = os.path.expanduser(key_file_path)

        with open(key_file_path) as fp:
            key_material = fp.read().strip()

        return self.import_key_pair_from_string(name=name, key_material=key_material)

    def delete_key_pair(self, key_pair):
        # type: (KeyPair) -> bool
        """
        Delete an existing key pair.

        :param key_pair: Key pair object.
        :type key_pair: :class:`.KeyPair`

        :rtype: ``bool``
        """
        raise NotImplementedError("delete_key_pair not implemented for this driver")

    def wait_until_running(
        self,
        nodes,  # type: List[Node]
        wait_period=5,  # type: float
        timeout=600,  # type: int
        ssh_interface="public_ips",  # type: str
        force_ipv4=True,  # type: bool
        ex_list_nodes_kwargs=None,  # type: Optional[Dict]
    ):
        # type: (...) -> List[Tuple[Node, List[str]]]
        """
        Block until the provided nodes are considered running.

        Node is considered running when it's state is "running" and when it has
        at least one IP address assigned.

        :param nodes: List of nodes to wait for.
        :type nodes: ``list`` of :class:`.Node`

        :param wait_period: How many seconds to wait between each loop
                            iteration. (default is 3)
        :type wait_period: ``int``

        :param timeout: How many seconds to wait before giving up.
                        (default is 600)
        :type timeout: ``int``

        :param ssh_interface: Which attribute on the node to use to obtain
                              an IP address. Valid options: public_ips,
                              private_ips. Default is public_ips.
        :type ssh_interface: ``str``

        :param force_ipv4: Ignore IPv6 addresses (default is True).
        :type force_ipv4: ``bool``

        :param ex_list_nodes_kwargs: Optional driver-specific keyword arguments
                                     which are passed to the ``list_nodes``
                                     method.
        :type ex_list_nodes_kwargs: ``dict``

        :return: ``[(Node, ip_addresses)]`` list of tuple of Node instance and
                 list of ip_address on success.
        :rtype: ``list`` of ``tuple``
        """
        ex_list_nodes_kwargs = ex_list_nodes_kwargs or {}

        def is_supported(address):
            # type: (str) -> bool
            """
            Return True for supported address.
            """

            if force_ipv4 and not is_valid_ip_address(address=address, family=socket.AF_INET):
                return False

            return True

        def filter_addresses(addresses):
            # type: (List[str]) -> List[str]
            """
            Return list of supported addresses.
            """

            return [address for address in addresses if is_supported(address)]

        if ssh_interface not in ["public_ips", "private_ips"]:
            raise ValueError("ssh_interface argument must either be " + "public_ips or private_ips")

        start = time.time()
        end = start + timeout

        uuids = {node.uuid for node in nodes}

        while time.time() < end:
            all_nodes = self.list_nodes(**ex_list_nodes_kwargs)
            matching_nodes = list(node for node in all_nodes if node.uuid in uuids)

            if len(matching_nodes) > len(uuids):
                found_uuids = [node.uuid for node in matching_nodes]
                msg = (
                    "Unable to match specified uuids "
                    + "(%s) with existing nodes. Found " % (uuids)
                    + "multiple nodes with same uuid: (%s)" % (found_uuids)
                )
                raise LibcloudError(value=msg, driver=self)

            running_nodes = [node for node in matching_nodes if node.state == NodeState.RUNNING]
            addresses = []

            for node in running_nodes:
                node_addresses = filter_addresses(getattr(node, ssh_interface))

                if len(node_addresses) >= 1:
                    addresses.append(node_addresses)

            if len(running_nodes) == len(uuids) == len(addresses):
                return list(zip(running_nodes, addresses))
            else:
                time.sleep(wait_period)

                continue

        raise LibcloudError(value="Timed out after %s seconds" % (timeout), driver=self)

    def _get_and_check_auth(self, auth):
        # type: (T_Auth) -> T_Auth
        """
        Helper function for providers supporting :class:`.NodeAuthPassword` or
        :class:`.NodeAuthSSHKey`

        Validates that only a supported object type is passed to the auth
        parameter and raises an exception if it is not.

        If no :class:`.NodeAuthPassword` object is provided but one is expected
        then a password is automatically generated.
        """

        if isinstance(auth, NodeAuthPassword):
            if "password" in self.features["create_node"]:
                return auth
            raise LibcloudError(
                "Password provided as authentication information, but password" "not supported",
                driver=self,
            )

        if isinstance(auth, NodeAuthSSHKey):
            if "ssh_key" in self.features["create_node"]:
                return auth
            raise LibcloudError(
                "SSH Key provided as authentication information, but SSH Key" "not supported",
                driver=self,
            )

        if "password" in self.features["create_node"]:
            value = os.urandom(16)
            value = binascii.hexlify(value).decode("ascii")

            # Some providers require password to also include uppercase
            # characters so convert some characters to uppercase
            password = ""

            for char in value:
                if not char.isdigit() and char.islower():
                    if random.randint(0, 1) == 1:
                        char = char.upper()

                password += char

            return NodeAuthPassword(password, generated=True)

        if auth:
            raise LibcloudError(
                '"auth" argument provided, but it was not a NodeAuthPassword'
                "or NodeAuthSSHKey object",
                driver=self,
            )

    def _wait_until_running(
        self,
        node,
        wait_period=3,
        timeout=600,
        ssh_interface="public_ips",
        force_ipv4=True,
    ):
        # type: (Node, float, int, str, bool) -> List[Tuple[Node, List[str]]]
        # This is here for backward compatibility and will be removed in the
        # next major release

        return self.wait_until_running(
            nodes=[node],
            wait_period=wait_period,
            timeout=timeout,
            ssh_interface=ssh_interface,
            force_ipv4=force_ipv4,
        )

    def _ssh_client_connect(self, ssh_client, wait_period=1.5, timeout=300):
        # type: (BaseSSHClient, float, int) -> BaseSSHClient
        """
        Try to connect to the remote SSH server. If a connection times out or
        is refused it is retried up to timeout number of seconds.

        :param ssh_client: A configured SSHClient instance
        :type ssh_client: ``SSHClient``

        :param wait_period: How many seconds to wait between each loop
                            iteration. (default is 1.5)
        :type wait_period: ``int``

        :param timeout: How many seconds to wait before giving up.
                        (default is 300)
        :type timeout: ``int``

        :return: ``SSHClient`` on success
        """
        start = time.time()
        end = start + timeout

        while time.time() < end:
            try:
                ssh_client.connect()
            except SSH_TIMEOUT_EXCEPTION_CLASSES as e:
                # Errors which represent fatal invalid key files which should
                # be propagated to the user without us retrying
                message = str(e).lower()

                for fatal_msg in SSH_FATAL_ERROR_MSGS:
                    if fatal_msg in message:
                        raise e

                # Retry if a connection is refused, timeout occurred,
                # or the connection fails due to failed authentication.
                try:
                    ssh_client.close()
                except Exception:
                    # Exception on close() should not be fatal since client
                    # socket might already be closed
                    pass

                time.sleep(wait_period)

                continue
            else:
                return ssh_client

        raise LibcloudError(
            value="Could not connect to the remote SSH " + "server. Giving up.",
            driver=self,
        )

    def _connect_and_run_deployment_script(
        self,
        task,  # type: Deployment
        node,  # type: Node
        ssh_hostname,  # type: str
        ssh_port,  # type: int
        ssh_username,  # type: str
        ssh_password,  # type: Optional[str]
        ssh_key_file,  # type: Optional[T_Ssh_key]
        ssh_key_password,  # type: Optional[str]
        ssh_timeout,  # type: int
        timeout,  # type: int
        max_tries,  # type: int
    ):
        """
        Establish an SSH connection to the node and run the provided deployment
        task.

        :rtype: :class:`.Node`:
        :return: Node instance on success.
        """
        ssh_client = SSHClient(
            hostname=ssh_hostname,
            port=ssh_port,
            username=ssh_username,
            password=ssh_key_password or ssh_password,
            key_files=ssh_key_file,
            timeout=ssh_timeout,
        )

        ssh_client = self._ssh_client_connect(ssh_client=ssh_client, timeout=timeout)

        # Execute the deployment task
        node = self._run_deployment_script(
            task=task, node=node, ssh_client=ssh_client, max_tries=max_tries
        )

        return node

    def _run_deployment_script(self, task, node, ssh_client, max_tries=3):
        # type: (Deployment, Node, BaseSSHClient, int) -> Node
        """
        Run the deployment script on the provided node. At this point it is
        assumed that SSH connection has already been established.

        :param task: Deployment task to run.
        :type task: :class:`Deployment`

        :param node: Node to run the task on.
        :type node: ``Node``

        :param ssh_client: A configured and connected SSHClient instance.
        :type ssh_client: :class:`SSHClient`

        :param max_tries: How many times to retry if a deployment fails
                          before giving up. (default is 3)
        :type max_tries: ``int``

        :rtype: :class:`.Node`
        :return: ``Node`` Node instance on success.
        """
        tries = 0

        while tries < max_tries:
            try:
                node = task.run(node, ssh_client)
            except SSHCommandTimeoutError as e:
                # Command timeout exception is fatal so we don't retry it.
                raise e
            except Exception as e:
                tries += 1

                if "ssh session not active" in str(e).lower():
                    # Sometimes connection gets closed or disconnected half
                    # way through for whatever reason.
                    # If this happens, we try to re-connect before
                    # re-attempting to run the step.
                    try:
                        ssh_client.close()
                    except Exception:
                        # Non fatal since connection is most likely already
                        # closed at this point
                        pass

                    timeout = int(ssh_client.timeout) if ssh_client.timeout else 10
                    ssh_client = self._ssh_client_connect(ssh_client=ssh_client, timeout=timeout)

                if tries >= max_tries:
                    tb = traceback.format_exc()
                    raise LibcloudError(
                        value="Failed after %d tries: %s.\n%s" % (max_tries, str(e), tb),
                        driver=self,
                    )
            else:
                # Deployment succeeded
                ssh_client.close()

                return node

        return node

    def _get_size_price(self, size_id):
        # type: (str) -> Optional[float]
        """
        Return pricing information for the provided size id.
        """

        return get_size_price(driver_type="compute", driver_name=self.api_name, size_id=size_id)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
