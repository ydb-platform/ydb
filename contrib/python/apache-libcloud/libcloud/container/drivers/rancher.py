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

from libcloud.utils.py3 import b, httplib, urlparse
from libcloud.common.base import JsonResponse, ConnectionUserAndKey
from libcloud.container.base import Container, ContainerImage, ContainerDriver
from libcloud.container.types import ContainerState
from libcloud.container.providers import Provider

try:
    import simplejson as json
except Exception:
    import json


VALID_RESPONSE_CODES = [
    httplib.OK,
    httplib.ACCEPTED,
    httplib.CREATED,
    httplib.NO_CONTENT,
]


class RancherResponse(JsonResponse):
    def parse_error(self):
        parsed = super().parse_error()
        if "fieldName" in parsed:
            return "Field {} is {}: {} - {}".format(
                parsed["fieldName"],
                parsed["code"],
                parsed["message"],
                parsed["detail"],
            )
        else:
            return "{} - {}".format(parsed["message"], parsed["detail"])

    def success(self):
        return self.status in VALID_RESPONSE_CODES


class RancherException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        self.args = (code, message)

    def __str__(self):
        return "{} {}".format(self.code, self.message)

    def __repr__(self):
        return "RancherException {} {}".format(self.code, self.message)


class RancherConnection(ConnectionUserAndKey):
    responseCls = RancherResponse
    timeout = 30

    def add_default_headers(self, headers):
        """
        Add parameters that are necessary for every request
        If user and password are specified, include a base http auth
        header
        """
        headers["Content-Type"] = "application/json"
        headers["Accept"] = "application/json"
        if self.key and self.user_id:
            user_b64 = base64.b64encode(b("{}:{}".format(self.user_id, self.key)))
            headers["Authorization"] = "Basic %s" % (user_b64.decode("utf-8"))
        return headers


class RancherContainerDriver(ContainerDriver):
    """
    Driver for Rancher by Rancher Labs.

    This driver is capable of interacting with the Version 1 API of Rancher.
    It currently does NOT support the Version 2 API.

    Example:

        >>> from libcloud.container.providers import get_driver
        >>> from libcloud.container.types import Provider

        >>> driver = get_driver(Provider.RANCHER)
        >>> connection = driver(key="ACCESS_KEY_HERE",
        secret="SECRET_KEY_HERE", host="172.30.0.100", port=8080)

        >>> image = ContainerImage("hastebin", "hastebin", "rlister/hastebin",
        "latest", driver=None)
        >>> newcontainer = connection.deploy_container("myawesomepastebin",
        image, environment={"STORAGE_TYPE": "file"})

    :ivar   baseuri: The URL base path to the API.
    :type   baseuri: ``str``
    """

    type = Provider.RANCHER
    name = "Rancher"
    website = "http://rancher.com"
    connectionCls = RancherConnection
    # Holding off on cluster support for now.
    # Only Environment API interaction enabled.
    supports_clusters = False
    # As in the /v1/
    version = "1"

    def __init__(self, key, secret, secure=True, host="localhost", port=443):
        """
        Creates a new Rancher Container driver.

        :param    key: API key or username to used (required)
        :type     key: ``str``

        :param    secret: Secret password to be used (required)
        :type     secret: ``str``

        :param    secure: Whether to use HTTPS or HTTP.
        :type     secure: ``bool``

        :param    host: Override hostname used for connections. This can also
         be a full URL string, including scheme, port, and base path.
        :type     host: ``str``

        :param    port: Override port used for connections.
        :type     port: ``int``

        :return: A newly initialized driver instance.
        """

        # Parse the Given Host
        if "://" not in host and not host.startswith("//"):
            host = "//" + host
        parsed = urlparse.urlparse(host)

        super().__init__(
            key=key,
            secret=secret,
            secure=False if parsed.scheme == "http" else secure,
            host=parsed.hostname,
            port=parsed.port if parsed.port else port,
        )

        self.baseuri = parsed.path if parsed.path else "/v%s" % self.version

    def ex_list_stacks(self):
        """
        List all Rancher Stacks

        http://docs.rancher.com/rancher/v1.2/en/api/api-resources/environment/

        :rtype: ``list`` of ``dict``
        """

        result = self.connection.request("%s/environments" % self.baseuri).object
        return result["data"]

    def ex_deploy_stack(
        self,
        name,
        description=None,
        docker_compose=None,
        environment=None,
        external_id=None,
        rancher_compose=None,
        start=True,
    ):
        """
        Deploy a new stack.

        http://docs.rancher.com/rancher/v1.2/en/api/api-resources/environment/#create

        :param name: The desired name of the stack. (required)
        :type name: ``str``

        :param description: A desired description for the stack.
        :type description: ``str``

        :param docker_compose: The Docker Compose configuration to use.
        :type docker_compose: ``str``

        :param environment: Environment K/V specific to this stack.
        :type environment: ``dict``

        :param external_id: The externalId of the stack.
        :type external_id: ``str``

        :param rancher_compose: The Rancher Compose configuration for this env.
        :type rancher_compose: ``str``

        :param start: Whether to start this stack on creation.
        :type start: ``bool``

        :return: The newly created stack.
        :rtype: ``dict``
        """

        payload = {
            "description": description,
            "dockerCompose": docker_compose,
            "environment": environment,
            "externalId": external_id,
            "name": name,
            "rancherCompose": rancher_compose,
            "startOnCreate": start,
        }
        data = json.dumps({k: v for (k, v) in payload.items() if v is not None})
        result = self.connection.request(
            "%s/environments" % self.baseuri, data=data, method="POST"
        ).object

        return result

    def ex_get_stack(self, env_id):
        """
        Get a stack by ID

        :param env_id: The stack to be obtained.
        :type env_id: ``str``

        :rtype: ``dict``
        """
        result = self.connection.request("{}/environments/{}".format(self.baseuri, env_id)).object

        return result

    def ex_search_stacks(self, search_params):
        """
        Search for stacks matching certain filters

        i.e. ``{ "name": "awesomestack"}``

        :param search_params: A collection of search parameters to use.
        :type search_params: ``dict``

        :rtype: ``list``
        """
        search_list = []
        for f, v in search_params.items():
            search_list.append(f + "=" + v)
        search_items = "&".join(search_list)
        result = self.connection.request(
            "{}/environments?{}".format(self.baseuri, search_items)
        ).object

        return result["data"]

    def ex_destroy_stack(self, env_id):
        """
        Destroy a stack by ID

        http://docs.rancher.com/rancher/v1.2/en/api/api-resources/environment/#delete

        :param env_id: The stack to be destroyed.
        :type env_id: ``str``

        :return: True if destroy was successful, False otherwise.
        :rtype: ``bool``
        """
        result = self.connection.request(
            "{}/environments/{}".format(self.baseuri, env_id), method="DELETE"
        )
        return result.status in VALID_RESPONSE_CODES

    def ex_activate_stack(self, env_id):
        """
        Activate Services for a stack.

        http://docs.rancher.com/rancher/v1.2/en/api/api-resources/environment/#activateservices

        :param env_id: The stack to activate services for.
        :type env_id: ``str``

        :return: True if activate was successful, False otherwise.
        :rtype: ``bool``
        """
        result = self.connection.request(
            "{}/environments/{}?action=activateservices".format(self.baseuri, env_id),
            method="POST",
        )
        return result.status in VALID_RESPONSE_CODES

    def ex_deactivate_stack(self, env_id):
        """
        Deactivate Services for a stack.

        http://docs.rancher.com/rancher/v1.2/en/api/api-resources/environment/#deactivateservices

        :param env_id: The stack to deactivate services for.
        :type env_id: ``str``

        :return: True if deactivate was successful, False otherwise.
        :rtype: ``bool``
        """

        result = self.connection.request(
            "{}/environments/{}?action=deactivateservices".format(self.baseuri, env_id),
            method="POST",
        )
        return result.status in VALID_RESPONSE_CODES

    def ex_list_services(self):
        """
        List all Rancher Services

        http://docs.rancher.com/rancher/v1.2/en/api/api-resources/service/

        :rtype: ``list`` of ``dict``
        """

        result = self.connection.request("%s/services" % self.baseuri).object
        return result["data"]

    def ex_deploy_service(
        self,
        name,
        image,
        environment_id,
        start=True,
        assign_service_ip_address=None,
        service_description=None,
        external_id=None,
        metadata=None,
        retain_ip=None,
        scale=None,
        scale_policy=None,
        secondary_launch_configs=None,
        selector_container=None,
        selector_link=None,
        vip=None,
        **launch_conf,
    ):
        """
        Deploy a Rancher Service under a stack.

        http://docs.rancher.com/rancher/v1.2/en/api/api-resources/service/#create

        *Any further configuration passed applies to the ``launchConfig``*

        :param name: The desired name of the service. (required)
        :type name: ``str``

        :param image: The Image object to deploy. (required)
        :type image: :class:`libcloud.container.base.ContainerImage`

        :param environment_id: The stack ID this service is tied to. (required)
        :type environment_id: ``str``

        :param start: Whether to start the service on creation.
        :type start: ``bool``

        :param assign_service_ip_address: The IP address to assign the service.
        :type assign_service_ip_address: ``bool``

        :param service_description: The service description.
        :type service_description: ``str``

        :param external_id: The externalId for this service.
        :type external_id: ``str``

        :param metadata: K/V Metadata for this service.
        :type metadata: ``dict``

        :param retain_ip: Whether this service should retain its IP.
        :type retain_ip: ``bool``

        :param scale: The scale of containers in this service.
        :type scale: ``int``

        :param scale_policy: The scaling policy for this service.
        :type scale_policy: ``dict``

        :param secondary_launch_configs: Secondary container launch configs.
        :type secondary_launch_configs: ``list``

        :param selector_container: The selectorContainer for this service.
        :type selector_container: ``str``

        :param selector_link: The selectorLink for this service.
        :type selector_link: ``type``

        :param vip: The VIP to assign to this service.
        :type vip: ``str``

        :return: The newly created service.
        :rtype: ``dict``
        """

        launch_conf["imageUuid"] = (self._degen_image(image),)

        service_payload = {
            "assignServiceIpAddress": assign_service_ip_address,
            "description": service_description,
            "environmentId": environment_id,
            "externalId": external_id,
            "launchConfig": launch_conf,
            "metadata": metadata,
            "name": name,
            "retainIp": retain_ip,
            "scale": scale,
            "scalePolicy": scale_policy,
            "secondary_launch_configs": secondary_launch_configs,
            "selectorContainer": selector_container,
            "selectorLink": selector_link,
            "startOnCreate": start,
            "vip": vip,
        }

        data = json.dumps({k: v for (k, v) in service_payload.items() if v is not None})
        result = self.connection.request(
            "%s/services" % self.baseuri, data=data, method="POST"
        ).object

        return result

    def ex_get_service(self, service_id):
        """
        Get a service by ID

        :param service_id: The service_id to be obtained.
        :type service_id: ``str``

        :rtype: ``dict``
        """
        result = self.connection.request("{}/services/{}".format(self.baseuri, service_id)).object

        return result

    def ex_search_services(self, search_params):
        """
        Search for services matching certain filters

        i.e. ``{ "name": "awesomesause", "environmentId": "1e2"}``

        :param search_params: A collection of search parameters to use.
        :type search_params: ``dict``

        :rtype: ``list``
        """
        search_list = []
        for f, v in search_params.items():
            search_list.append(f + "=" + v)
        search_items = "&".join(search_list)
        result = self.connection.request("{}/services?{}".format(self.baseuri, search_items)).object

        return result["data"]

    def ex_destroy_service(self, service_id):
        """
        Destroy a service by ID

        http://docs.rancher.com/rancher/v1.2/en/api/api-resources/service/#delete

        :param service_id: The service to be destroyed.
        :type service_id: ``str``

        :return: True if destroy was successful, False otherwise.
        :rtype: ``bool``
        """
        result = self.connection.request(
            "{}/services/{}".format(self.baseuri, service_id), method="DELETE"
        )
        return result.status in VALID_RESPONSE_CODES

    def ex_activate_service(self, service_id):
        """
        Activate a service.

        http://docs.rancher.com/rancher/v1.2/en/api/api-resources/service/#activate

        :param service_id: The service to activate services for.
        :type service_id: ``str``

        :return: True if activate was successful, False otherwise.
        :rtype: ``bool``
        """
        result = self.connection.request(
            "{}/services/{}?action=activate".format(self.baseuri, service_id), method="POST"
        )
        return result.status in VALID_RESPONSE_CODES

    def ex_deactivate_service(self, service_id):
        """
        Deactivate a service.

        http://docs.rancher.com/rancher/v1.2/en/api/api-resources/service/#deactivate

        :param service_id: The service to deactivate services for.
        :type service_id: ``str``

        :return: True if deactivate was successful, False otherwise.
        :rtype: ``bool``
        """
        result = self.connection.request(
            "{}/services/{}?action=deactivate".format(self.baseuri, service_id),
            method="POST",
        )
        return result.status in VALID_RESPONSE_CODES

    def list_containers(self):
        """
        List the deployed containers.

        http://docs.rancher.com/rancher/v1.2/en/api/api-resources/container/

        :rtype: ``list`` of :class:`libcloud.container.base.Container`
        """

        result = self.connection.request("%s/containers" % self.baseuri).object
        containers = [self._to_container(value) for value in result["data"]]
        return containers

    def deploy_container(self, name, image, parameters=None, start=True, **config):
        """
        Deploy a new container.

        http://docs.rancher.com/rancher/v1.2/en/api/api-resources/container/#create

        **The following is the Image format used for ``ContainerImage``**

        *For a ``imageuuid``*:

        - ``docker:<hostname>:<port>/<namespace>/<imagename>:<version>``

        *The following applies*:

        - ``id`` = ``<imagename>``
        - ``name`` = ``<imagename>``
        - ``path`` = ``<hostname>:<port>/<namespace>/<imagename>``
        - ``version`` = ``<version>``

        *Any extra configuration can also be passed i.e. "environment"*

        :param name: The desired name of the container. (required)
        :type name: ``str``

        :param image: The Image object to deploy. (required)
        :type image: :class:`libcloud.container.base.ContainerImage`

        :param parameters: Container Image parameters (unused)
        :type  parameters: ``str``

        :param start: Whether to start the container on creation(startOnCreate)
        :type start: ``bool``

        :rtype: :class:`Container`
        """

        payload = {
            "name": name,
            "imageUuid": self._degen_image(image),
            "startOnCreate": start,
        }
        config.update(payload)

        data = json.dumps(config)

        result = self.connection.request(
            "%s/containers" % self.baseuri, data=data, method="POST"
        ).object

        return self._to_container(result)

    def get_container(self, con_id):
        """
        Get a container by ID

        :param con_id: The ID of the container to get
        :type  con_id: ``str``

        :rtype: :class:`libcloud.container.base.Container`
        """
        result = self.connection.request("{}/containers/{}".format(self.baseuri, con_id)).object

        return self._to_container(result)

    def start_container(self, container):
        """
        Start a container

        :param container: The container to be started
        :type  container: :class:`libcloud.container.base.Container`

        :return: The container refreshed with current data
        :rtype: :class:`libcloud.container.base.Container`
        """
        result = self.connection.request(
            "{}/containers/{}?action=start".format(self.baseuri, container.id),
            method="POST",
        ).object

        return self._to_container(result)

    def stop_container(self, container):
        """
        Stop a container

        :param container: The container to be stopped
        :type  container: :class:`libcloud.container.base.Container`

        :return: The container refreshed with current data
        :rtype: :class:`libcloud.container.base.Container`
        """
        result = self.connection.request(
            "{}/containers/{}?action=stop".format(self.baseuri, container.id), method="POST"
        ).object

        return self._to_container(result)

    def ex_search_containers(self, search_params):
        """
        Search for containers matching certain filters

        i.e. ``{ "imageUuid": "docker:mysql", "state": "running"}``

        :param search_params: A collection of search parameters to use.
        :type search_params: ``dict``

        :rtype: ``list``
        """
        search_list = []
        for f, v in search_params.items():
            search_list.append(f + "=" + v)
        search_items = "&".join(search_list)
        result = self.connection.request(
            "{}/containers?{}".format(self.baseuri, search_items)
        ).object

        return result["data"]

    def destroy_container(self, container):
        """
        Remove a container

        :param container: The container to be destroyed
        :type  container: :class:`libcloud.container.base.Container`

        :return: True if the destroy was successful, False otherwise.
        :rtype: ``bool``
        """
        result = self.connection.request(
            "{}/containers/{}".format(self.baseuri, container.id), method="DELETE"
        ).object

        return self._to_container(result)

    def _gen_image(self, imageuuid):
        """
        This function converts a valid Rancher ``imageUuid`` string to a valid
        image object. Only supports docker based images hence `docker:` must
        prefix!!

        Please see the deploy_container() for details on the format.

        :param imageuuid: A valid Rancher image string
            i.e. ``docker:rlister/hastebin:8.0``
        :type imageuuid: ``str``

        :return: Converted ContainerImage object.
        :rtype: :class:`libcloud.container.base.ContainerImage`
        """
        # Obtain just the name(:version) for parsing
        if "/" not in imageuuid:
            # String looks like `docker:mysql:8.0`
            image_name_version = imageuuid.partition(":")[2]
        else:
            # String looks like `docker:oracle/mysql:8.0`
            image_name_version = imageuuid.rpartition("/")[2]
        # Parse based on ':'
        if ":" in image_name_version:
            version = image_name_version.partition(":")[2]
            id = image_name_version.partition(":")[0]
            name = id
        else:
            version = "latest"
            id = image_name_version
            name = id
        # Get our path based on if there was a version
        if version != "latest":
            path = imageuuid.partition(":")[2].rpartition(":")[0]
        else:
            path = imageuuid.partition(":")[2]

        return ContainerImage(
            id=id,
            name=name,
            path=path,
            version=version,
            driver=self.connection.driver,
            extra={"imageUuid": imageuuid},
        )

    def _degen_image(self, image):
        """
        Take in an image object to break down into an ``imageUuid``

        :param image:
        :return:
        """

        # Only supporting docker atm
        image_type = "docker"

        if image.version is not None:
            return image_type + ":" + image.path + ":" + image.version
        else:
            return image_type + ":" + image.path

    def _to_container(self, data):
        """
        Convert container in proper Container instance object
        ** Updating is NOT supported!!

        :param data: API data about container i.e. result.object
        :return: Proper Container object:
         see http://libcloud.readthedocs.io/en/latest/container/api.html

        """
        rancher_state = data["state"]

        # A Removed container is purged after x amt of time.
        # Both of these render the container dead (can't be started later)
        terminate_condition = ["removed", "purged"]

        if "running" in rancher_state:
            state = ContainerState.RUNNING
        elif "stopped" in rancher_state:
            state = ContainerState.STOPPED
        elif "restarting" in rancher_state:
            state = ContainerState.REBOOTING
        elif "error" in rancher_state:
            state = ContainerState.ERROR
        elif any(x in rancher_state for x in terminate_condition):
            state = ContainerState.TERMINATED
        elif data["transitioning"] == "yes":
            # Best we can do for current actions
            state = ContainerState.PENDING
        else:
            state = ContainerState.UNKNOWN

        # Everything contained in the json response is dumped in extra
        extra = data

        return Container(
            id=data["id"],
            name=data["name"],
            image=self._gen_image(data["imageUuid"]),
            ip_addresses=[data["primaryIpAddress"]],
            state=state,
            driver=self.connection.driver,
            extra=extra,
        )
