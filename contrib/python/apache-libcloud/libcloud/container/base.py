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


from typing import List, Optional

from libcloud.common.base import BaseDriver, ConnectionUserAndKey
from libcloud.container.types import ContainerState

__all__ = [
    "Container",
    "ContainerImage",
    "ContainerCluster",
    "ClusterLocation",
    "ContainerDriver",
]


class Container:
    """
    Container.
    """

    def __init__(
        self,
        id,  # type: str
        name,  # type: str
        image,  # type: ContainerImage
        state,  # type: ContainerState
        ip_addresses,  # type: List[str]
        driver,  # type: ContainerDriver
        extra=None,  # type: Optional[dict]
        created_at=None,  # type: Optional[str]
    ):
        """
        :param id: Container id.
        :type id: ``str``

        :param name: The name of the container.
        :type  name: ``str``

        :param image: The image this container was deployed using.
        :type  image: :class:`.ContainerImage`

        :param state: The state of the container, e.g. running
        :type  state: :class:`libcloud.container.types.ContainerState`

        :param ip_addresses: A list of IP addresses for this container
        :type  ip_addresses: ``list`` of ``str``

        :param driver: ContainerDriver instance.
        :type driver: :class:`.ContainerDriver`

        :param extra: (optional) Extra attributes (driver specific).
        :type extra: ``dict``
        """
        self.id = str(id) if id else None
        self.name = name
        self.image = image
        self.state = state
        self.ip_addresses = ip_addresses
        self.driver = driver
        self.extra = extra or {}
        self.created_at = created_at

    def start(self):
        # type: () -> Container
        return self.driver.start_container(container=self)

    def stop(self):
        # type: () -> Container
        return self.driver.stop_container(container=self)

    def restart(self):
        # type: () -> Container
        return self.driver.restart_container(container=self)

    def destroy(self):
        # type: () -> bool
        return self.driver.destroy_container(container=self)

    def __repr__(self):
        return "<Container: id=%s, name=%s," "state=%s, provider=%s ...>" % (
            self.id,
            self.name,
            self.state,
            self.driver.name,
        )


class ContainerImage:
    """
    Container Image.
    """

    def __init__(
        self,
        id,  # type: str
        name,  # type: str
        path,  # type: str
        version,  # type: str
        driver,  # type: ContainerDriver
        extra=None,  # type: Optional[dict]
    ):
        """
        :param id: Container Image id.
        :type id: ``str``

        :param name: The name of the image.
        :type  name: ``str``

        :param path: The path to the image
        :type  path: ``str``

        :param version: The version of the image
        :type  version: ``str``

        :param driver: ContainerDriver instance.
        :type driver: :class:`.ContainerDriver`

        :param extra: (optional) Extra attributes (driver specific).
        :type extra: ``dict``
        """
        self.id = str(id) if id else None
        self.name = name
        self.path = path
        self.version = version
        self.driver = driver
        self.extra = extra or {}

    def deploy(self, name, parameters, cluster=None, start=True):
        # type: (str, str, Optional[ContainerCluster], bool) -> Container
        return self.driver.deploy_container(
            name=name, image=self, parameters=parameters, cluster=cluster, start=start
        )

    def __repr__(self):
        return "<ContainerImage: id={}, name={}, path={} ...>".format(
            self.id,
            self.name,
            self.path,
        )


class ContainerCluster:
    """
    A cluster group for containers
    """

    def __init__(
        self,
        id,  # type: str
        name,  # type: str
        driver,  # type: ContainerDriver
        extra=None,  # type: Optional[dict]
    ):
        """
        :param id: Container Image id.
        :type id: ``str``

        :param name: The name of the image.
        :type  name: ``str``

        :param driver: ContainerDriver instance.
        :type driver: :class:`.ContainerDriver`

        :param extra: (optional) Extra attributes (driver specific).
        :type extra: ``dict``
        """
        self.id = str(id) if id else None
        self.name = name
        self.driver = driver
        self.extra = extra or {}

    def list_containers(self):
        # type: () -> List[Container]
        return self.driver.list_containers(cluster=self)

    def destroy(self):
        # type: () -> bool
        return self.driver.destroy_cluster(cluster=self)

    def __repr__(self):
        return "<ContainerCluster: id={}, name={}, provider={} ...>".format(
            self.id,
            self.name,
            self.driver.name,
        )


class ClusterLocation:
    """
    A physical location where clusters can be.

    >>> from libcloud.container.drivers.dummy import DummyContainerDriver
    >>> driver = DummyContainerDriver(0)
    >>> location = driver.list_locations()[0]
    >>> location.country
    'US'
    """

    def __init__(
        self,
        id,  # type: str
        name,  # type: str
        country,  # type: str
        driver,  # type: ContainerDriver
    ):
        """
        :param id: Location ID.
        :type id: ``str``

        :param name: Location name.
        :type name: ``str``

        :param country: Location country.
        :type country: ``str``

        :param driver: Driver this location belongs to.
        :type driver: :class:`.ContainerDriver`
        """
        self.id = str(id)
        self.name = name
        self.country = country
        self.driver = driver

    def __repr__(self):
        return ("<ClusterLocation: id=%s, name=%s, country=%s, driver=%s>") % (
            self.id,
            self.name,
            self.country,
            self.driver.name,
        )


class ContainerDriver(BaseDriver):
    """
    A base ContainerDriver class to derive from

    This class is always subclassed by a specific driver.
    """

    connectionCls = ConnectionUserAndKey
    name = None
    website = None
    supports_clusters = False
    """
    Whether the driver supports containers being deployed into clusters
    """

    def __init__(self, key, secret=None, secure=True, host=None, port=None, **kwargs):
        """
        :param    key: API key or username to used (required)
        :type     key: ``str``

        :param    secret: Secret password to be used (required)
        :type     secret: ``str``

        :param    secure: Whether to use HTTPS or HTTP. Note: Some providers
                only support HTTPS, and it is on by default.
        :type     secure: ``bool``

        :param    host: Override hostname used for connections.
        :type     host: ``str``

        :param    port: Override port used for connections.
        :type     port: ``int``

        :return: ``None``
        """
        super().__init__(key=key, secret=secret, secure=secure, host=host, port=port, **kwargs)

    def install_image(self, path):
        # type: (str) -> ContainerImage
        """
        Install a container image from a remote path.

        :param path: Path to the container image
        :type  path: ``str``

        :rtype: :class:`.ContainerImage`
        """
        raise NotImplementedError("install_image not implemented for this driver")

    def list_images(self):
        # type: () -> List[ContainerImage]
        """
        List the installed container images

        :rtype: ``list`` of :class:`.ContainerImage`
        """
        raise NotImplementedError("list_images not implemented for this driver")

    def list_containers(
        self,
        image=None,  # type: Optional[ContainerImage]
        cluster=None,  # type: Optional[ContainerCluster]
    ):
        # type: (...) -> List[Container]
        """
        List the deployed container images

        :param image: Filter to containers with a certain image
        :type  image: :class:`.ContainerImage`

        :param cluster: Filter to containers in a cluster
        :type  cluster: :class:`.ContainerCluster`

        :rtype: ``list`` of :class:`.Container`
        """
        raise NotImplementedError("list_containers not implemented for this driver")

    def deploy_container(
        self,
        name,  # type: str
        image,  # type: ContainerImage
        cluster=None,  # type: Optional[ContainerCluster]
        parameters=None,  # type: Optional[str]
        start=True,  # type: bool
    ):
        # type: (...) -> Container
        """
        Deploy an installed container image

        :param name: The name of the new container
        :type  name: ``str``

        :param image: The container image to deploy
        :type  image: :class:`.ContainerImage`

        :param cluster: The cluster to deploy to, None is default
        :type  cluster: :class:`.ContainerCluster`

        :param parameters: Container Image parameters
        :type  parameters: ``str``

        :param start: Start the container on deployment
        :type  start: ``bool``

        :rtype: :class:`.Container`
        """
        raise NotImplementedError("deploy_container not implemented for this driver")

    def get_container(self, id):
        # type: (str) -> Container
        """
        Get a container by ID

        :param id: The ID of the container to get
        :type  id: ``str``

        :rtype: :class:`.Container`
        """
        raise NotImplementedError("get_container not implemented for this driver")

    def start_container(self, container):
        # type: (Container) -> Container
        """
        Start a deployed container

        :param container: The container to start
        :type  container: :class:`.Container`

        :rtype: :class:`.Container`
        """
        raise NotImplementedError("start_container not implemented for this driver")

    def stop_container(self, container):
        # type: (Container) -> Container
        """
        Stop a deployed container

        :param container: The container to stop
        :type  container: :class:`.Container`

        :rtype: :class:`.Container`
        """
        raise NotImplementedError("stop_container not implemented for this driver")

    def restart_container(self, container):
        # type: (Container) -> Container
        """
        Restart a deployed container

        :param container: The container to restart
        :type  container: :class:`.Container`

        :rtype: :class:`.Container`
        """
        raise NotImplementedError("restart_container not implemented for this driver")

    def destroy_container(self, container):
        # type: (Container) -> bool
        """
        Destroy a deployed container

        :param container: The container to destroy
        :type  container: :class:`.Container`

        :rtype: ``bool``
        """
        raise NotImplementedError("destroy_container not implemented for this driver")

    def list_locations(self):
        # type: () -> List[ClusterLocation]
        """
        Get a list of potential locations to deploy clusters into

        :rtype: ``list`` of :class:`.ClusterLocation`
        """
        raise NotImplementedError("list_locations not implemented for this driver")

    def create_cluster(self, name, location=None):
        # type: (str, Optional[ClusterLocation]) -> ContainerCluster
        """
        Create a container cluster

        :param  name: The name of the cluster
        :type   name: ``str``

        :param  location: The location to create the cluster in
        :type   location: :class:`.ClusterLocation`

        :rtype: :class:`.ContainerCluster`
        """
        raise NotImplementedError("create_cluster not implemented for this driver")

    def destroy_cluster(self, cluster):
        # type: (ContainerCluster) -> bool
        """
        Delete a cluster

        :return: ``True`` if the destroy was successful, otherwise ``False``.
        :rtype: ``bool``
        """
        raise NotImplementedError("destroy_cluster not implemented for this driver")

    def list_clusters(self, location=None):
        # type: (Optional[ClusterLocation]) -> List[ContainerCluster]
        """
        Get a list of potential locations to deploy clusters into

        :param  location: The location to search in
        :type   location: :class:`.ClusterLocation`

        :rtype: ``list`` of :class:`.ContainerCluster`
        """
        raise NotImplementedError("list_clusters not implemented for this driver")

    def get_cluster(self, id):
        # type: (str) -> ContainerCluster
        """
        Get a cluster by ID

        :param id: The ID of the cluster to get
        :type  id: ``str``

        :rtype: :class:`.ContainerCluster`
        """
        raise NotImplementedError("list_clusters not implemented for this driver")
