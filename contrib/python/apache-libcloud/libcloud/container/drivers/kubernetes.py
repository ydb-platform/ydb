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

import json
import hashlib
import datetime
from typing import Any, Dict, List, Union, Optional
from collections import OrderedDict

from libcloud.compute.base import Node, NodeSize, NodeImage
from libcloud.compute.types import NodeState
from libcloud.container.base import Container, ContainerImage, ContainerDriver, ContainerCluster
from libcloud.container.types import ContainerState
from libcloud.common.exceptions import BaseHTTPError
from libcloud.common.kubernetes import (
    KubernetesException,
    KubernetesDriverMixin,
    KubernetesBasicAuthConnection,
)
from libcloud.container.providers import Provider

__all__ = [
    "KubernetesContainerDriver",
    "to_n_bytes",
    "to_memory_str",
    "to_cpu_str",
    "to_n_cpus",
]


ROOT_URL = "/api/"


K8S_UNIT_MAP = OrderedDict(
    {
        "K": 1000,
        "Ki": 1024,
        "M": 1000 * 1000,
        "Mi": 1024 * 1024,
        "G": 1000 * 1000 * 1000,
        "Gi": 1024 * 1024 * 1024,
    }
)


def to_n_bytes(memory_str: str) -> int:
    """Convert memory string to number of bytes
    (e.g. '1234Mi'-> 1293942784)
    """

    if memory_str.startswith("0"):
        return 0

    if memory_str.isnumeric():
        return int(memory_str)

    for unit, multiplier in K8S_UNIT_MAP.items():
        if memory_str.endswith(unit):
            return int(memory_str.strip(unit)) * multiplier


def to_memory_str(n_bytes: int, unit: Optional[str] = None) -> str:
    """Convert number of bytes to k8s memory string
    (e.g. 1293942784 -> '1234Mi')
    """

    if n_bytes == 0:
        return "0K"
    n_bytes = int(n_bytes)
    memory_str = None

    if unit is None:
        for unit, multiplier in reversed(K8S_UNIT_MAP.items()):
            converted_n_bytes_float = n_bytes / multiplier
            converted_n_bytes = n_bytes // multiplier
            memory_str = f"{converted_n_bytes}{unit}"

            if converted_n_bytes_float % 1 == 0:
                break
    elif K8S_UNIT_MAP.get(unit):
        memory_str = f"{n_bytes // K8S_UNIT_MAP[unit]}{unit}"

    return memory_str


def to_cpu_str(n_cpus: Union[int, float]) -> str:
    """Convert number of cpus to cpu string
    (e.g. 0.5 -> '500m')
    """

    if n_cpus == 0:
        return "0"
    millicores = n_cpus * 1000

    if millicores % 1 == 0:
        return f"{int(millicores)}m"
    microcores = n_cpus * 1000000

    if microcores % 1 == 0:
        return f"{int(microcores)}u"
    nanocores = n_cpus * 1000000000

    return f"{int(nanocores)}n"


def to_n_cpus(cpu_str: str) -> Union[int, float]:
    """Convert cpu string to number of cpus
    (e.g. '500m' -> 0.5, '2000000000n' -> 2)
    """

    if cpu_str.endswith("n"):
        return int(cpu_str.strip("n")) / 1000000000
    elif cpu_str.endswith("u"):
        return int(cpu_str.strip("u")) / 1000000
    elif cpu_str.endswith("m"):
        return int(cpu_str.strip("m")) / 1000
    elif cpu_str.isnumeric():
        return int(cpu_str)
    else:
        return 0


def sum_resources(*resource_dicts):
    total_cpu = 0
    total_memory = 0

    for rd in resource_dicts:
        total_cpu += to_n_cpus(rd.get("cpu", "0m"))
        total_memory += to_n_bytes(rd.get("memory", "0K"))

    return {"cpu": to_cpu_str(total_cpu), "memory": to_memory_str(total_memory)}


class KubernetesDeployment:
    def __init__(
        self,
        id: str,
        name: str,
        namespace: str,
        created_at: str,
        replicas: int,
        selector: Dict[str, Any],
        extra: Optional[Dict[str, Any]] = None,
    ):
        self.id = id
        self.name = name
        self.namespace = namespace
        self.created_at = created_at
        self.replicas = replicas
        self.selector = selector
        self.extra = extra or {}

    def __repr__(self):
        return "<KubernetesDeployment name={} namespace={} replicas={}>".format(
            self.name,
            self.namespace,
            self.replicas,
        )


class KubernetesPod:
    def __init__(
        self,
        id: str,
        name: str,
        containers: List[Container],
        namespace: str,
        state: str,
        ip_addresses: List[str],
        created_at: datetime.datetime,
        node_name: str,
        extra: Dict[str, Any],
    ):
        """
        A Kubernetes pod
        """
        self.id = id
        self.name = name
        self.containers = containers
        self.namespace = namespace
        self.state = state
        self.ip_addresses = ip_addresses
        self.created_at = created_at
        self.node_name = node_name
        self.extra = extra

    def __repr__(self):
        return "<KubernetesPod name={} namespace={} state={}>".format(
            self.name,
            self.namespace,
            self.state,
        )


class KubernetesNamespace(ContainerCluster):
    """
    A Kubernetes namespace
    """

    def __repr__(self):
        return "<KubernetesNamespace name={}>".format(self.name)


class KubernetesContainerDriver(KubernetesDriverMixin, ContainerDriver):
    type = Provider.KUBERNETES
    name = "Kubernetes"
    website = "http://kubernetes.io"
    connectionCls = KubernetesBasicAuthConnection
    supports_clusters = True

    def list_containers(self, image=None, all=True) -> List[Container]:
        """
        List the deployed container images

        :param image: Filter to containers with a certain image(unused)
        :type  image: :class:`libcloud.container.base.ContainerImage`

        :param all: Show all container (unused)
        :type  all: ``bool``

        :rtype: ``list`` of :class:`libcloud.container.base.Container`
        """
        try:
            result = self.connection.request(ROOT_URL + "v1/pods").object
        except Exception as exc:
            errno = getattr(exc, "errno", None)

            if errno == 111:
                raise KubernetesException(
                    errno,
                    "Make sure kube host is accessible" "and the API port is correct",
                )
            raise

        pods = [self._to_pod(value) for value in result["items"]]
        containers = []

        for pod in pods:
            containers.extend(pod.containers)

        return containers

    def get_container(self, id: str) -> Container:
        """
        Get a container by ID

        :param id: The ID of the container to get
        :type  id: ``str``

        :rtype: :class:`libcloud.container.base.Container`
        """
        containers = self.list_containers()
        match = [container for container in containers if container.id == id]

        return match[0]

    def list_namespaces(self) -> List[KubernetesNamespace]:
        """
        Get a list of namespaces that pods can be deployed into

        :rtype: ``list`` of :class:`.KubernetesNamespace`
        """
        try:
            result = self.connection.request(ROOT_URL + "v1/namespaces/").object
        except Exception as exc:
            errno = getattr(exc, "errno", None)

            if errno == 111:
                raise KubernetesException(
                    errno,
                    "Make sure kube host is accessible" "and the API port is correct",
                )
            raise

        namespaces = [self._to_namespace(value) for value in result["items"]]

        return namespaces

    def get_namespace(self, id: str) -> KubernetesNamespace:
        """
        Get a namespace by ID

        :param id: The ID of the namespace to get
        :type  id: ``str``

        :rtype: :class:`.KubernetesNamespace`
        """
        result = self.connection.request(ROOT_URL + "v1/namespaces/%s" % id).object

        return self._to_namespace(result)

    def delete_namespace(self, namespace: KubernetesNamespace) -> bool:
        """
        Delete a namespace

        :return: ``True`` if the destroy was successful, otherwise ``False``.
        :rtype: ``bool``
        """
        self.connection.request(
            ROOT_URL + "v1/namespaces/%s" % namespace.id, method="DELETE"
        ).object

        return True

    def create_namespace(self, name: str) -> KubernetesNamespace:
        """
        Create a namespace

        :param  name: The name of the namespace
        :type   name: ``str``

        :rtype: :class:`.KubernetesNamespace`
        """
        request = {"metadata": {"name": name}}
        result = self.connection.request(
            ROOT_URL + "v1/namespaces", method="POST", data=json.dumps(request)
        ).object

        return self._to_namespace(result)

    def deploy_container(
        self,
        name: str,
        image: ContainerImage,
        namespace: KubernetesNamespace = None,
        parameters: Optional[str] = None,
        start: Optional[bool] = True,
    ):
        """
        Deploy an installed container image.
        In kubernetes this deploys a single container Pod.
        https://cloud.google.com/container-engine/docs/pods/single-container

        :param name: The name of the new container
        :type  name: ``str``

        :param image: The container image to deploy
        :type  image: :class:`.ContainerImage`

        :param namespace: The namespace to deploy to, None is default
        :type  namespace: :class:`.KubernetesNamespace`

        :param parameters: Container Image parameters(unused)
        :type  parameters: ``str``

        :param start: Start the container on deployment(unused)
        :type  start: ``bool``

        :rtype: :class:`.Container`
        """

        if namespace is None:
            namespace = "default"
        else:
            namespace = namespace.id
        request = {
            "metadata": {"name": name},
            "spec": {"containers": [{"name": name, "image": image.name}]},
        }
        result = self.connection.request(
            ROOT_URL + "v1/namespaces/%s/pods" % namespace,
            method="POST",
            data=json.dumps(request),
        ).object

        return self._to_namespace(result)

    def destroy_container(self, container: Container) -> bool:
        """
        Destroy a deployed container. Because the containers are single
        container pods, this will delete the pod.

        :param container: The container to destroy
        :type  container: :class:`.Container`

        :rtype: ``bool``
        """

        return self.ex_destroy_pod(container.extra["namespace"], container.extra["pod"])

    def ex_list_pods(self, fetch_metrics: bool = False) -> List[KubernetesPod]:
        """
        List available Pods

        :param fetch_metrics: Fetch metrics for pods
        :type  fetch_metrics: ``bool``

        :rtype: ``list`` of :class:`.KubernetesPod`
        """
        result = self.connection.request(ROOT_URL + "v1/pods").object
        metrics = None

        if fetch_metrics:
            try:
                metrics = {
                    (
                        metric["metadata"]["name"],
                        metric["metadata"]["namespace"],
                    ): metric["containers"]
                    for metric in self.ex_list_pods_metrics()
                }
            except BaseHTTPError:
                # Metrics Server may not be installed
                pass

        return [self._to_pod(value, metrics=metrics) for value in result["items"]]

    def ex_destroy_pod(self, namespace: str, pod_name: str) -> bool:
        """
        Delete a pod and the containers within it.

        :param namespace: The pod's namespace
        :type  namespace: ``str``

        :param pod_name: Name of the pod to destroy
        :type  pod_name: ``str``

        :rtype: ``bool``
        """
        self.connection.request(
            ROOT_URL + "v1/namespaces/{}/pods/{}".format(namespace, pod_name),
            method="DELETE",
        ).object

        return True

    def ex_list_nodes(self) -> List[Node]:
        """
        List available Nodes

        :rtype: ``list`` of :class:`.Node`
        """
        result = self.connection.request(ROOT_URL + "v1/nodes").object

        return [self._to_node(node) for node in result["items"]]

    def ex_destroy_node(self, node_name: str) -> bool:
        """
        Destroy a node.

        :param node_name: Name of the node to destroy
        :type  node_name: ``str``

        :rtype: ``bool``
        """
        self.connection.request(ROOT_URL + f"v1/nodes/{node_name}", method="DELETE").object

        return True

    def ex_get_version(self) -> str:
        """Get Kubernetes version

        :rtype: ``str``
        """

        return self.connection.request("/version").object["gitVersion"]

    def ex_list_nodes_metrics(self) -> List[Dict[str, Any]]:
        """Get nodes metrics from Kubernetes Metrics Server

        :rtype: ``list`` of ``dict``
        """

        return self.connection.request("/apis/metrics.k8s.io/v1beta1/nodes").object["items"]

    def ex_list_pods_metrics(self) -> List[Dict[str, Any]]:
        """Get pods metrics from Kubernetes Metrics Server

        :rtype: ``list`` of ``dict``
        """

        return self.connection.request("/apis/metrics.k8s.io/v1beta1/pods").object["items"]

    def ex_list_services(self) -> List[Dict[str, Any]]:
        """Get cluster services

        :rtype: ``list`` of ``dict``
        """

        return self.connection.request(ROOT_URL + "v1/services").object["items"]

    def ex_list_deployments(self) -> List[KubernetesDeployment]:
        """Get cluster deployments

        :rtype: ``list`` of :class:`.KubernetesDeployment`
        """
        items = self.connection.request("/apis/apps/v1/deployments").object["items"]

        return [self._to_deployment(item) for item in items]

    def _to_deployment(self, data):
        id_ = data["metadata"]["uid"]
        name = data["metadata"]["name"]
        namespace = data["metadata"]["namespace"]
        created_at = data["metadata"]["creationTimestamp"]
        replicas = data["spec"]["replicas"]
        selector = data["spec"]["selector"]

        extra = {
            "labels": data["metadata"]["labels"],
            "strategy": data["spec"]["strategy"]["type"],
            "total_replicas": data["status"]["replicas"],
            "updated_replicas": data["status"]["updatedReplicas"],
            "ready_replicas": data["status"]["readyReplicas"],
            "available_replicas": data["status"]["availableReplicas"],
            "conditions": data["status"]["conditions"],
        }

        return KubernetesDeployment(
            id=id_,
            name=name,
            namespace=namespace,
            created_at=created_at,
            replicas=replicas,
            selector=selector,
            extra=extra,
        )

    def _to_node(self, data):
        """
        Convert an API node data object to a `Node` object
        """
        ID = data["metadata"]["uid"]
        name = data["metadata"]["name"]
        driver = self.connection.driver
        memory = data["status"].get("capacity", {}).get("memory", "0K")
        cpu = data["status"].get("capacity", {}).get("cpu", "1")

        if isinstance(cpu, str) and not cpu.isnumeric():
            cpu = to_n_cpus(cpu)
        image_name = data["status"]["nodeInfo"].get("osImage")
        image = NodeImage(image_name, image_name, driver)
        size_name = f"{cpu} vCPUs, {memory} Ram"
        size_id = hashlib.md5(size_name.encode("utf-8")).hexdigest()  # nosec
        extra_size = {"cpus": cpu}
        size = NodeSize(
            id=size_id,
            name=size_name,
            ram=memory,
            disk=0,
            bandwidth=0,
            price=0,
            driver=driver,
            extra=extra_size,
        )
        extra = {"memory": memory, "cpu": cpu}
        extra["os"] = data["status"]["nodeInfo"].get("operatingSystem")
        extra["kubeletVersion"] = data["status"]["nodeInfo"]["kubeletVersion"]
        extra["provider_id"] = data.get("spec", {}).get("providerID")

        for condition in data["status"]["conditions"]:
            if condition["type"] == "Ready" and condition["status"] == "True":
                state = NodeState.RUNNING

                break
        else:
            state = NodeState.UNKNOWN
        public_ips, private_ips = [], []

        for address in data["status"]["addresses"]:
            if address["type"] == "InternalIP":
                private_ips.append(address["address"])
            elif address["type"] == "ExternalIP":
                public_ips.append(address["address"])
        created_at = datetime.datetime.strptime(
            data["metadata"]["creationTimestamp"], "%Y-%m-%dT%H:%M:%SZ"
        )

        return Node(
            id=ID,
            name=name,
            state=state,
            public_ips=public_ips,
            private_ips=private_ips,
            driver=driver,
            image=image,
            size=size,
            extra=extra,
            created_at=created_at,
        )

    def _to_pod(self, data, metrics=None):
        """
        Convert an API response to a Pod object
        """
        id_ = data["metadata"]["uid"]
        name = data["metadata"]["name"]
        namespace = data["metadata"]["namespace"]
        state = data["status"]["phase"].lower()
        node_name = data["spec"].get("nodeName")
        container_statuses = data["status"].get("containerStatuses", {})
        containers = []
        extra = {"resources": {}}

        if metrics:
            try:
                extra["metrics"] = metrics[name, namespace]
            except KeyError:
                pass

        # response contains the status of the containers in a separate field

        for container in data["spec"]["containers"]:
            if container_statuses:
                spec = list(filter(lambda i: i["name"] == container["name"], container_statuses))[0]
            else:
                spec = container_statuses
            container_obj = self._to_container(container, spec, data)
            # Calculate new resources
            resources = extra["resources"]
            container_resources = container_obj.extra.get("resources", {})
            resources["limits"] = sum_resources(
                resources.get("limits", {}), container_resources.get("limits", {})
            )
            extra["resources"]["requests"] = sum_resources(
                resources.get("requests", {}), container_resources.get("requests", {})
            )
            containers.append(container_obj)
        ip_addresses = [ip_dict["ip"] for ip_dict in data["status"].get("podIPs", [])]
        created_at = datetime.datetime.strptime(
            data["metadata"]["creationTimestamp"], "%Y-%m-%dT%H:%M:%SZ"
        )

        return KubernetesPod(
            id=id_,
            name=name,
            namespace=namespace,
            state=state,
            ip_addresses=ip_addresses,
            containers=containers,
            created_at=created_at,
            node_name=node_name,
            extra=extra,
        )

    def _to_container(self, data, container_status, pod_data):
        """
        Convert container in Container instances
        """
        state = container_status.get("state")
        created_at = None

        if state:
            started_at = list(state.values())[0].get("startedAt")

            if started_at:
                created_at = datetime.datetime.strptime(started_at, "%Y-%m-%dT%H:%M:%SZ")
        extra = {
            "pod": pod_data["metadata"]["name"],
            "namespace": pod_data["metadata"]["namespace"],
        }
        resources = data.get("resources")

        if resources:
            extra["resources"] = resources

        return Container(
            id=container_status.get("containerID") or data["name"],
            name=data["name"],
            image=ContainerImage(
                id=container_status.get("imageID") or data["image"],
                name=data["image"],
                path=None,
                version=None,
                driver=self.connection.driver,
            ),
            ip_addresses=None,
            state=(ContainerState.RUNNING if container_status else ContainerState.UNKNOWN),
            driver=self.connection.driver,
            created_at=created_at,
            extra=extra,
        )

    def _to_namespace(self, data):
        """
        Convert an API node data object to a `KubernetesNamespace` object
        """

        return KubernetesNamespace(
            id=data["metadata"]["name"],
            name=data["metadata"]["name"],
            driver=self.connection.driver,
            extra={"phase": data["status"]["phase"]},
        )


def ts_to_str(timestamp):
    """
    Return a timestamp as a nicely formatted datetime string.
    """
    date = datetime.datetime.fromtimestamp(timestamp)
    date_string = date.strftime("%d/%m/%Y %H:%M %Z")

    return date_string
