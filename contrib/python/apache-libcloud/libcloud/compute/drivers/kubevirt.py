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

# NOTE: Re-enable once we add mypy annotations for the base container API
# type: ignore
"""
kubevirt driver with support for nodes (vms)
"""
import copy
import json
import time
import uuid
import hashlib
import warnings
from typing import Union, Optional
from datetime import datetime

from libcloud.common.types import LibcloudError
from libcloud.compute.base import (
    Node,
    NodeSize,
    NodeImage,
    NodeDriver,
    NodeLocation,
    StorageVolume,
    NodeAuthSSHKey,
    NodeAuthPassword,
)
from libcloud.compute.types import Provider, NodeState
from libcloud.common.kubernetes import (
    VALID_RESPONSE_CODES,
    KubernetesDriverMixin,
    KubernetesBasicAuthConnection,
)

__all__ = [
    "KubeVirtNodeDriver",
    "DISK_TYPES",
    "KubeVirtNodeSize",
    "KubeVirtNodeImage",
]

ROOT_URL = "/api/v1/"
KUBEVIRT_URL = "/apis/kubevirt.io/v1alpha3/"

# all valid disk types supported by kubevirt
DISK_TYPES = {
    "containerDisk",
    "ephemeral",
    "configMap",
    "dataVolume",
    "cloudInitNoCloud",
    "persistentVolumeClaim",
    "emptyDisk",
    "cloudInitConfigDrive",
    "hostDisk",
}


class KubeVirtNodeDriver(KubernetesDriverMixin, NodeDriver):
    type = Provider.KUBEVIRT
    name = "kubevirt"
    website = "https://www.kubevirt.io"
    connectionCls = KubernetesBasicAuthConnection
    features = {"create_node": ["ssh_key", "password"]}

    NODE_STATE_MAP = {
        "pending": NodeState.PENDING,
        "running": NodeState.RUNNING,
        "stopped": NodeState.STOPPED,
    }

    def list_nodes(self, location=None):
        namespaces = []

        if location is not None:
            if isinstance(location, NodeLocation):
                namespaces.append(location.name)
            elif isinstance(location, str):
                namespaces.append(location)
            else:
                raise ValueError("location must be a NodeLocation or a string")
        else:
            for ns in self.list_locations():
                namespaces.append(ns.name)

        dormant = []
        live = []

        for ns in namespaces:
            req = KUBEVIRT_URL + "namespaces/" + ns + "/virtualmachines"
            result = self.connection.request(req)

            if result.status != 200:
                continue
            result = result.object

            for item in result["items"]:
                if not item["spec"]["running"]:
                    dormant.append(item)
                else:
                    live.append(item)
        vms = []

        for vm in dormant:
            vms.append(self._to_node(vm, is_stopped=True))

        for vm in live:
            vms.append(self._to_node(vm, is_stopped=False))

        return vms

    def get_node(self, id=None, name=None):
        """get a vm by name or id.

        :param id: id of the vm
        :type id: ``str``

        :param name: name of the vm
        :type name: ``str``
        """

        if not id and not name:
            raise ValueError("This method needs id or name to be specified")
        nodes = self.list_nodes()

        node_gen = None

        if id:
            node_gen = filter(lambda x: x.id == id, nodes)

        if name:
            node_gen = filter(lambda x: x.name == name, nodes)

        if not node_gen:
            raise ValueError("node_gen is not defined")

        try:
            return next(node_gen)
        except StopIteration:
            raise ValueError("Node does not exist")

    def start_node(self, node):
        """Starting a VM.

        :param node: The node to be started.
        :type node: :class:`Node`

        :return: True if the start was successful, False otherwise.
        :rtype: ``bool``
        """
        # make sure it is stopped

        if node.state is NodeState.RUNNING:
            return True
        name = node.name
        namespace = node.extra["namespace"]
        req = KUBEVIRT_URL + "namespaces/" + namespace + "/virtualmachines/" + name
        data = {"spec": {"running": True}}
        headers = {"Content-Type": "application/merge-patch+json"}
        try:
            result = self.connection.request(
                req, method="PATCH", data=json.dumps(data), headers=headers
            )

            return result.status in VALID_RESPONSE_CODES

        except Exception:
            raise

    def stop_node(self, node):
        """Stopping a VM.

        :param node: The node to be stopped.
        :type node: :class:`Node`

        :return: True if the stop was successful, False otherwise.
        :rtype: ``bool``
        """
        # check if running

        if node.state is NodeState.STOPPED:
            return True
        name = node.name
        namespace = node.extra["namespace"]
        req = KUBEVIRT_URL + "namespaces/" + namespace + "/virtualmachines/" + name
        headers = {"Content-Type": "application/merge-patch+json"}
        data = {"spec": {"running": False}}
        try:
            result = self.connection.request(
                req, method="PATCH", data=json.dumps(data), headers=headers
            )

            return result.status in VALID_RESPONSE_CODES

        except Exception:
            raise

    def reboot_node(self, node):
        """
        Rebooting a node.

        :param node: The node to be rebooted.
        :type node: :class:`Node`

        :return: True if the reboot was successful, False otherwise.
        :rtype: ``bool``
        """
        namespace = node.extra["namespace"]
        name = node.name
        method = "DELETE"
        try:
            result = self.connection.request(
                KUBEVIRT_URL + "namespaces/" + namespace + "/virtualmachineinstances/" + name,
                method=method,
            )

            return result.status in VALID_RESPONSE_CODES
        except Exception:
            raise

        return

    def destroy_node(self, node):
        """
        Terminating a VMI and deleting the VM resource backing it.

        :param node: The node to be destroyed.
        :type node: :class:`Node`

        :return: True if the destruction was successful, False otherwise.
        :rtype: ``bool``
        """
        namespace = node.extra["namespace"]
        name = node.name
        # find and delete services for this VM only
        services = self.ex_list_services(namespace=namespace, node_name=name)

        for service in services:
            service_name = service["metadata"]["name"]
            self.ex_delete_service(namespace=namespace, service_name=service_name)
        # stop the vmi
        self.stop_node(node)
        try:
            result = self.connection.request(
                KUBEVIRT_URL + "namespaces/" + namespace + "/virtualmachines/" + name,
                method="DELETE",
            )

            return result.status in VALID_RESPONSE_CODES
        except Exception:
            raise

    def _create_node_with_template(self, name: str, template: dict, namespace="default"):
        """
        Creating a VM defined by the template.

        The template must be a dictionary of kubernetes object that defines the
        KubeVirt VM. Following are the keys:

        - ``apiVersion``: ``str``
        - ``kind``: ``str``
        - ``metadata``: ``dict``
        - ``spec``: ``dict``
            - ``domain``: ``dict``
            - ``volumes``: ``list``
            - ...
        - ...

        See also:

        - https://kubernetes.io/docs/concepts/overview/working-with-objects/
        - https://kubevirt.io/api-reference/

        :param name: A name to give the VM. The VM will be identified by this
                     name, and it must be the same as the name in the
                     ``template["metadata"]["name"]``.
                     Atm, it cannot be changed after it is set.
        :type name: ``str``

        :param template: A dictionary of kubernetes object that defines the VM.
        :type template: ``dict`` with keys:
                        ``apiVersion: str``, ``kind: str``, ``metadata: dict``,
                        ``spec: dict`` etc.

        :param namespace: The namespace where the VM will live.
                          (default is 'default')
        :return:
        """
        # k8s object checks

        if template.get("apiVersion", "") != "kubevirt.io/v1alpha3":
            raise ValueError("The template must have an apiVersion: kubevirt.io/v1alpha3")

        if template.get("kind", "") != "VirtualMachine":
            raise ValueError("The template must contain kind: VirtualMachine")

        if name != template.get("metadata", {}).get("name"):
            raise ValueError(
                "The name of the VM must be the same as the name in the template. "
                "(name={}, template.metadata.name={})".format(
                    name, template.get("metadata", {}).get("name")
                )
            )

        if template.get("spec", {}).get("running", False):
            warnings.warn(
                "The VM will be created in a stopped state, and then started. "
                "Ignoring the `spec.running: True` in the template."
            )
            # assert "spec" in template and "running" in template["spec"]
            template["spec"]["running"] = False

        vm = template

        method = "POST"
        data = json.dumps(vm)
        req = KUBEVIRT_URL + "namespaces/" + namespace + "/virtualmachines/"
        try:
            self.connection.request(req, method=method, data=data)
        except Exception:
            raise

        # check if new node is present
        # But why not just use the resp from the POST request?
        # Or self.get_node()?
        # I don't think a for loop over list_nodes is necessary.
        nodes = self.list_nodes(location=namespace)

        for node in nodes:
            if node.name == name:
                self.start_node(node)

                return node

        raise ValueError(
            "The node was not found after creation, "
            "create_node may have failed. Please check the kubernetes logs."
        )

    @staticmethod
    def _base_vm_template(name=None):  # type: (Optional[str]) -> dict
        """
        A skeleton VM template to be used for creating VMs.

        :param name: A name to give the VM. The VM will be identified by this
                     name. If not provided, a random uuid4 will be used.
                     The generated name can be gotten from the returned dict
                     with the key ``["metadata"]["name"]``.
        :type name: ``str``

        :return: dict: A skeleton VM template.
        """

        if not name:
            name = uuid.uuid4()

        return {
            "apiVersion": "kubevirt.io/v1alpha3",
            "kind": "VirtualMachine",
            "metadata": {"labels": {"kubevirt.io/vm": name}, "name": name},
            "spec": {
                "running": False,
                "template": {
                    "metadata": {"labels": {"kubevirt.io/vm": name}},
                    "spec": {
                        "domain": {
                            "devices": {
                                "disks": [],
                                "interfaces": [],
                                "networkInterfaceMultiqueue": False,
                            },
                            "machine": {"type": ""},
                            "resources": {"requests": {}, "limits": {}},
                        },
                        "networks": [],
                        "terminationGracePeriodSeconds": 0,
                        "volumes": [],
                    },
                },
            },
        }

    @staticmethod
    def _create_node_vm_from_ex_template(
        name, ex_template, other_args
    ):  # type: (str, dict) -> dict
        """
        A part of create_node that deals with the VM template.
        Returns the VM template with the name set.

        :param name: A name to give the VM. The VM will be identified by
                        this name and atm it cannot be changed after it is set.
                        See also the name parameter in create_node.
        :type name: ``str``

        :param ex_template: A dictionary of kubernetes object that defines the
                            KubeVirt VM. See also the ex_template parameter in create_node.
        :type ex_template: ``dict`` with keys:
                            ``apiVersion: str``, ``kind: str``, ``metadata: dict``
                            and ``spec: dict``

        :param other_args: Other parameters passed to the create_node method.
                           This is used to warn the user about ignored parameters.
                           See also the parameters of create_node.
        :type other_args: ``dict``

        :return: dict: The VM template with the name set.
        """
        assert isinstance(ex_template, dict), "ex_template must be a dictionary"

        other_params = {
            "size": other_args.get("size"),
            "image": other_args.get("image"),
            "auth": other_args.get("auth"),
            "ex_disks": other_args.get("ex_disks"),
            "ex_network": other_args.get("ex_network"),
            "ex_termination_grace_period": other_args.get("ex_termination_grace_period"),
            "ex_ports": other_args.get("ex_ports"),
        }
        ignored_non_none_param_keys = list(
            filter(lambda x: other_params[x] is not None, other_params)
        )

        if ignored_non_none_param_keys:
            warnings.warn(
                "ex_template is provided, ignoring the following non-None "
                "parameters: {}".format(ignored_non_none_param_keys)
            )

        vm = copy.deepcopy(ex_template)

        if vm.get("metadata") is None:
            vm["metadata"] = {}

        if vm["metadata"].get("name") is None:
            vm["metadata"]["name"] = name
        elif vm["metadata"]["name"] != name:
            warnings.warn(
                "The name in the ex_template ({}) will be ignored. "
                "The name provided in the arguments ({}) will be used.".format(
                    vm["metadata"]["name"], name
                )
            )
            vm["metadata"]["name"] = name

        return vm

    @staticmethod
    def _create_node_size(
        vm, size=None, ex_cpu=None, ex_memory=None
    ):  # type: (dict, NodeSize, int, int) -> None
        """
        A part of create_node that deals with the size of the VM.
        It will fill the vm with size information.

        :param size: The size of the VM in terms of CPU and memory.
                     See also the size parameter in create_node.
        :type size: ``NodeSize`` with

        :param ex_cpu: The number of CPU cores to allocate to the VM.
                       See also the ex_cpu parameter in create_node.
        :type ex_cpu: ``int`` or ``str``

        :param ex_memory: The amount of memory to allocate to the VM in MiB.
                          See also the ex_memory parameter in create_node.
        :type ex_memory: ``int``

        :return: None
        """
        # size -> cpu and memory limits / requests

        ex_memory_limit = ex_memory_request = ex_cpu_limit = ex_cpu_request = None

        if size is not None:
            assert isinstance(size, NodeSize), "size must be a NodeSize"
            ex_cpu_limit = size.extra["cpu"]
            ex_memory_limit = size.ram
            # optional resc requests: default = limit
            ex_cpu_request = size.extra.get("cpu_request", None) or ex_cpu_limit
            ex_memory_request = size.extra.get("ram_request", None) or ex_memory_limit

        # memory

        if ex_memory is not None:  # legacy
            ex_memory_limit = ex_memory
            ex_memory_request = ex_memory

        def _format_memory(memory_value):  # type: (int) -> str
            assert isinstance(memory_value, int), "memory must be an int in MiB"

            return str(memory_value) + "Mi"

        if ex_memory_limit is not None:
            memory = _format_memory(ex_memory_limit)
            vm["spec"]["template"]["spec"]["domain"]["resources"]["limits"]["memory"] = memory

        if ex_memory_request is not None:
            memory = _format_memory(ex_memory_request)
            vm["spec"]["template"]["spec"]["domain"]["resources"]["requests"]["memory"] = memory

        # cpu

        if ex_cpu is not None:  # legacy
            ex_cpu_limit = ex_cpu
            ex_cpu_request = ex_cpu

        def _format_cpu(cpu_value):  # type: (Union[int, str]) -> Union[str, float]
            if isinstance(cpu_value, str) and cpu_value.endswith("m"):
                return cpu_value
            try:
                return float(cpu_value)
            except ValueError:
                raise ValueError("cpu must be a number or a string ending with 'm'")

        if ex_cpu_limit is not None:
            cpu = _format_cpu(ex_cpu_limit)
            vm["spec"]["template"]["spec"]["domain"]["resources"]["limits"]["cpu"] = cpu

        if ex_cpu_request is not None:
            cpu = _format_cpu(ex_cpu_request)
            vm["spec"]["template"]["spec"]["domain"]["resources"]["requests"]["cpu"] = cpu

    @staticmethod
    def _create_node_termination_grace_period(
        vm, ex_termination_grace_period
    ):  # type: (dict, int) -> None
        """
        A part of create_node that deals with the termination grace period of the VM.
        It will fill the vm with the termination grace period information.

        :param vm: The VM template to be filled.
        :type vm: ``dict``
        :param ex_termination_grace_period: The termination grace period of the VM in seconds.
                                            See also the ex_termination_grace_period parameter in create_node.
        :type ex_termination_grace_period: ``int``
        :return: None
        """
        assert isinstance(
            ex_termination_grace_period, int
        ), "ex_termination_grace_period must be an int"

        vm["spec"]["template"]["spec"][
            "terminationGracePeriodSeconds"
        ] = ex_termination_grace_period

    @staticmethod
    def _create_node_network(vm, ex_network, ex_ports):  # type: (dict, dict, dict) -> None
        """
        A part of create_node that deals with the network of the VM.
        It will fill the vm with network information.

        :param vm: The VM template to be filled.
        :type vm: ``dict``
        :param ex_network: The network configuration of the VM.
                           See also the ex_network parameter in create_node.
        :type ex_network: ``dict``
        :param ex_ports: The ports to expose in the VM.
                         See also the ex_ports parameter in create_node.
        :type ex_ports: ``dict``
        :return: None
        """
        # ex_network -> network and interface

        if ex_network is not None:
            try:
                if isinstance(ex_network, dict):
                    interface = ex_network["interface"]
                    network_name = ex_network["name"]
                    network_type = ex_network["network_type"]
                elif isinstance(ex_network, (tuple, list)):  # legacy 3-tuple
                    network_type = ex_network[0]
                    interface = ex_network[1]
                    network_name = ex_network[2]
                else:
                    raise KeyError("ex_network must be a dictionary or a tuple/list")
            except KeyError:
                msg = (
                    "ex_network: You must provide a dictionary with keys: "
                    "'interface', 'name', 'network_type'."
                )
                raise KeyError(msg)
        # add a default network
        else:
            interface = "masquerade"
            network_name = "netw1"
            network_type = "pod"

        network_dict = {network_type: {}, "name": network_name}
        interface_dict = {interface: {}, "name": network_name}

        # ex_ports -> network.ports
        ex_ports = ex_ports or {}

        if ex_ports.get("ports_tcp"):
            ports_to_expose = []

            for port in ex_ports["ports_tcp"]:
                ports_to_expose.append({"port": port, "protocol": "TCP"})
            interface_dict[interface]["ports"] = ports_to_expose

        if ex_ports.get("ports_udp"):
            ports_to_expose = interface_dict[interface].get("ports", [])

            for port in ex_ports.get("ports_udp"):
                ports_to_expose.append({"port": port, "protocol": "UDP"})
            interface_dict[interface]["ports"] = ports_to_expose

        vm["spec"]["template"]["spec"]["networks"].append(network_dict)
        vm["spec"]["template"]["spec"]["domain"]["devices"]["interfaces"].append(interface_dict)

    @staticmethod
    def _create_node_auth(vm, auth):
        """
        A part of create_node that deals with the authentication of the VM.
        It will fill the vm with a cloud-init volume that injects the authentication.

        :param vm: The VM template to be filled.
        :param auth: The authentication method for the VM.
                        See also the auth parameter in create_node.
        :type auth: ``NodeAuthSSHKey`` or ``NodeAuthPassword``
        :return: None
        """
        # auth requires cloud-init,
        # and only one cloud-init volume is supported by kubevirt.
        # So if both auth and cloud-init are provided, raise an error.

        for volume in vm["spec"]["template"]["spec"]["volumes"]:
            if "cloudInitNoCloud" in volume or "cloudInitConfigDrive" in volume:
                raise ValueError(
                    "Setting auth and cloudInit at the same time is not supported."
                    "Use deploy_node() instead."
                )

        # cloud-init volume
        cloud_init_volume = "auth-cloudinit-" + str(uuid.uuid4())
        disk_dict = {"disk": {"bus": "virtio"}, "name": cloud_init_volume}
        volume_dict = {
            "name": cloud_init_volume,
            "cloudInitNoCloud": {"userData": ""},
        }

        # cloud_init_config reference: https://kubevirt.io/user-guide/virtual_machines/startup_scripts/#injecting-ssh-keys-with-cloud-inits-cloud-config

        if isinstance(auth, NodeAuthSSHKey):
            public_key = auth.pubkey.strip()
            public_key = json.dumps(public_key)
            cloud_init_config = (
                """#cloud-config\n""" """ssh_authorized_keys:\n""" """  - {}\n"""
            ).format(public_key)
        elif isinstance(auth, NodeAuthPassword):
            password = auth.password.strip()
            password = json.dumps(password)
            cloud_init_config = (
                """#cloud-config\n"""
                """password: {}\n"""
                """chpasswd: {{ expire: False }}\n"""
                """ssh_pwauth: True\n"""
            ).format(password)
        else:
            raise ValueError("auth must be NodeAuthSSHKey or NodeAuthPassword")
        volume_dict["cloudInitNoCloud"]["userData"] = cloud_init_config

        # add volume
        vm["spec"]["template"]["spec"]["domain"]["devices"]["disks"].append(disk_dict)
        vm["spec"]["template"]["spec"]["volumes"].append(volume_dict)

    def _create_node_disks(self, vm, ex_disks, image, namespace, location):
        """
        A part of create_node that deals with the disks (and volumes) of the VM.
        It will fill the vm with disk information.
        OS image is added to the disks as well.

        :param vm: The VM template to be filled.
        :type vm: ``dict``
        :param ex_disks: The disk configuration of the VM.
                            See also the ex_disks parameter in create_node.
        :type ex_disks: ``list``
        :param image: The image to be used as the OS disk.
                        See also the image parameter in create_node.
        :param namespace: The namespace where the VM will live.
                            See also the location parameter in create_node.
        :type namespace: ``str``
        :param location: The location where the VM will live.
                            See also the location parameter in create_node.
        :type location: ``str``

        :return: None
        """
        # ex_disks -> disks and volumes

        ex_disks = ex_disks or []

        for i, disk in enumerate(ex_disks):
            disk_type = disk.get("disk_type")
            bus = disk.get("bus", "virtio")
            disk_name = disk.get("name", "disk{}".format(i))
            device = disk.get("device", "disk")

            if disk_type not in DISK_TYPES:
                raise ValueError("The possible values for this " "parameter are: ", DISK_TYPES)

            # depending on disk_type, in the future,
            # when more will be supported,
            # additional elif should be added

            if disk_type == "containerDisk":
                try:
                    image = disk["volume_spec"]["image"]
                except KeyError:
                    raise KeyError("A container disk needs a " "containerized image")

                volumes_dict = {"containerDisk": {"image": image}, "name": disk_name}
            elif disk_type == "persistentVolumeClaim":
                if "volume_spec" not in disk:
                    raise KeyError("You must provide a volume_spec dictionary")

                if "claim_name" not in disk["volume_spec"]:
                    msg = (
                        "You must provide either a claim_name of an "
                        "existing claim or if you want one to be "
                        "created you must additionally provide size "
                        "and the storage_class_name of the "
                        "cluster, which allows dynamic provisioning, "
                        "so a Persistent Volume Claim can be created. "
                        "In the latter case please provide the desired "
                        "size as well."
                    )
                    raise KeyError(msg)

                claim_name = disk["volume_spec"]["claim_name"]

                if claim_name not in self.ex_list_persistent_volume_claims(namespace=namespace):
                    if (
                        "size" not in disk["volume_spec"]
                        or "storage_class_name" not in disk["volume_spec"]
                    ):
                        msg = (
                            "disk['volume_spec']['size'] and "
                            "disk['volume_spec']['storage_class_name'] "
                            "are both required to create "
                            "a new claim."
                        )
                        raise KeyError(msg)
                    size = disk["volume_spec"]["size"]
                    storage_class = disk["volume_spec"]["storage_class_name"]
                    volume_mode = disk["volume_spec"].get("volume_mode", "Filesystem")
                    access_mode = disk["volume_spec"].get("access_mode", "ReadWriteOnce")
                    self.create_volume(
                        size=size,
                        name=claim_name,
                        location=location,
                        ex_storage_class_name=storage_class,
                        ex_volume_mode=volume_mode,
                        ex_access_mode=access_mode,
                    )

                volumes_dict = {
                    "persistentVolumeClaim": {"claimName": claim_name},
                    "name": disk_name,
                }
            else:
                warnings.warn(
                    "The disk type {} is not tested. Use at your own risk.".format(disk_type)
                )
                volumes_dict = {disk_type: disk.get("volume_spec", {}), "name": disk_name}

            disk_dict = {device: {"bus": bus}, "name": disk_name}
            vm["spec"]["template"]["spec"]["domain"]["devices"]["disks"].append(disk_dict)
            vm["spec"]["template"]["spec"]["volumes"].append(volumes_dict)
        # end of for disk in ex_disks

        # image -> containerDisk -> add as the first disk
        self._create_node_image(vm, image)

    @staticmethod
    def _create_node_image(vm, image):  # type: (dict, NodeImage) -> None
        """
        A part of create_node that deals with the image of the VM.
        It will fill the vm with the OS image as the first disk.

        :param vm: The VM template to be filled.
        :param image: The image to be used as the OS disk.
                        See also the image parameter in create_node.
        :type image: ``NodeImage`` or ``str``
        :return: None
        """
        # image -> containerDisk
        # adding image in a container Disk

        if isinstance(image, NodeImage):
            image = image.name

        boot_disk_name = "boot-disk-" + str(uuid.uuid4())
        volumes_dict = {"containerDisk": {"image": image}, "name": boot_disk_name}
        disk_dict = {"disk": {"bus": "virtio"}, "name": boot_disk_name}

        # boot disk should be the first one, otherwise it will not boot
        vm["spec"]["template"]["spec"]["domain"]["devices"]["disks"].insert(0, disk_dict)
        vm["spec"]["template"]["spec"]["volumes"].insert(0, volumes_dict)

    def create_node(
        self,
        name,  # type: str
        size=None,  # type: Optional[NodeSize]
        image=None,  # type: Optional[Union[NodeImage, str]]
        location=None,  # type: Optional[NodeLocation]
        auth=None,  # type: Optional[Union[NodeAuthSSHKey, NodeAuthPassword]]
        ex_cpu=None,  # type: Optional[Union[int, str]]
        ex_memory=None,  # type: Optional[Union[int]]
        ex_disks=None,  # type: Optional[list]
        ex_network=None,  # type: Optional[dict]
        ex_termination_grace_period=0,  # type: Optional[int]
        ex_ports=None,  # type: Optional[dict]
        ex_template=None,  # type: Optional[dict]
    ):  # type: (...) -> Node
        """
        Creating a VM with a containerDisk.

        @inherits: :class:`NodeDriver.create_node`

        The ``size`` parameter should be a ``NodeSize`` object that defines
        the CPU and memory limits of the VM.
        The ``NodeSize`` object must have the following attributes:

        - ram: int (in MiB)
        - extra["cpu"]: int (in cores)

        An example NodeSize for a VM with 1 CPU and 2GiB of memory
        (attributes except for ``ram`` and ``extra["cpu"]`` are not supported atm,
        but they are required by the ``NodeSize`` object)::

            >>> size = NodeSize(
            >>>     id='small',  # str: Size ID
            >>>     name='small',  # str: Size name. Not used atm
            >>>     ram=2048,  # int: Amount of memory (in MB)
            >>>     disk=20,  # int: Amount of disk storage (in GB). Not used atm
            >>>     bandwidth=0,  # int: Amount of bandwidth. Not used atm
            >>>     price=0,  # float: Price (in US dollars) of running this node for an hour. Not used atm
            >>>     driver=KubeVirtNodeDriver  # NodeDriver: Driver this size belongs to
            >>>     extra={
            >>>         'cpu': 1,  # int: Number of CPUs provided by this size.
            >>>     },
            >>> )

        The ``KubeVirtNodeSize`` wrapper can be used to create the ``NodeSize``
        object more easily::

            >>> size = KubeVirtNodeSize(
            >>>     cpu=1,  # int: Number of CPUs provided by this size.
            >>>     ram=2048,  # int: Amount of memory (in MB)
            >>> )

        For legacy support, the ``ex_cpu`` and ``ex_memory`` parameters
        can be used instead of ``size``:

        - ``ex_cpu``: The number of CPU cores to allocate to the VM.
                        ex_cpu must be a number (cores) or a string
                        ending with 'm' (miliCPUs).
        - ``ex_memory``: The amount of memory to allocate to the VM in MiB.

        The ``image`` parameter can be either a ``NodeImage`` object
        with a ``name`` attribute that points to a containerDisk image,
        or a string representing the image URI.

        In both cases, it must point to a Docker image with an embedded disk.
        May be a URI like `kubevirt/cirros-registry-disk-demo`,
        kubevirt will automatically pull it from https://hub.docker.com/u/URI.

        For more info visit: https://kubevirt.io/user-guide/docs/latest/creating-virtual-machines/disks-and-volumes.html#containerdisk

        An example ``NodeImage``::

            >>> image = NodeImage(
            >>>     id="something_unique",
            >>>     name="quay.io/containerdisks/ubuntu:22.04"
            >>>     driver=KubeVirtNodeDriver,
            >>> )

        The ``KubeVirtNodeImage`` wrapper can be used to create the ``NodeImage``
        object more easily::

            >>> image = KubeVirtNodeImage(
            >>>     name="quay.io/containerdisks/ubuntu:22.04"
            >>> )

        The ``location`` parameter is a NodeLocation object
        with the name being the kubernetes namespace where the VM will live.
        If not provided, the VM will be created in the default namespace.
        It can be created as the following::

            >>> location = NodeLocation(
            >>>     name='default',  # str: namespace
            >>>     ... # Other attributes are ignored
            >>> )

        The ``auth`` parameter is the authentication method for the VM,
        either a ``NodeAuthSSHKey`` or a ``NodeAuthPassword``:

        - ``NodeAuthSSHKey(pubkey='pubkey data here')``
        - ``NodeAuthPassword(password='mysecretpassword')``

        If the ``auth`` parameter is provided, the VM will be created with
        a cloud-init volume that will inject the provided authentication
        into the VM.
        For details, see:
        https://kubevirt.io/user-guide/virtual_machines/startup_scripts/#injecting-ssh-keys-with-cloud-inits-cloud-config

        The ``ex_disks`` parameter is a list of disk dictionaries that define
        the disks of the VM.
        Each dictionary should have specific keys for disk configuration.

        The following are optional keys:

        - ``"bus"``: can be "virtio", "sata", or "scsi"
        - ``"device"``: can be "lun" or "disk"

        The following are required keys:

        - ``"disk_type"``:
            One of the supported ``DISK_TYPES``.
            Atm only "persistentVolumeClaim" and
            "containerDisk" are promised to work.
            Other types may work but are not tested.

        - ``"name"``:
            The name of the disk configuration

        - ``"voulme_spec"``:
            the dictionary that defines the volume of the disk.
            The content is depending on the disk_type:

            For `containerDisk` the dictionary should have a key `image` with
            the value being the image URI.

            For `persistentVolumeClaim` the dictionary should have a key
            `claim_name` with the value being the name of the claim.
            If you wish, a new Persistent Volume Claim can be
            created by providing the following:

            - required:
                - size: the desired size (implied in GB)
                - storage_class_name: the name of the storage class to # NOQA
                                       be used for the creation of the
                                       Persistent Volume Claim.
                                       Make sure it allows for
                                       dynamic provisioning.
            - optional:
                - access_mode: default is ReadWriteOnce
                - volume_mode: default is `Filesystem`, it can also be `Block`

            For other disk types, it will be translated to
            a KubeVirt volume object as is::

                {disk_type: <volume_spec>, "name": disk_name}

            This dict will be translated to KubeVirt API object::

              volumes:
                - name: <disk_name>
                  <disk_type>:
                    <voulme_spec>

        Please refer to the KubeVirt API documentation
        for volume specifications:

        https://kubevirt.io/user-guide/virtual_machines/disks_and_volumes

        An example ``ex_disks`` list with a hostDisk that will create a new
        img file in the host machine::

            >>> ex_disks = [
            >>>     {
            >>>         "bus": "virtio",
            >>>         "device": "disk",
            >>>         "disk_type": "hostDisk",
            >>>         "name": "disk114514",
            >>>         "volume_spec": {
            >>>             "capacity": "10Gi",
            >>>             "path": "/tmp/kubevirt/data/disk114514.img",
            >>>             "type": "DiskOrCreate",
            >>>         },
            >>>     },
            >>> ]

        The ``ex_network`` parameter is a dictionary that defines the network
        of the VM.
        Only the ``pod`` type is supported, and in the configuration
        ``masquerade`` or ``bridge`` are the accepted values.
        The parameter must be a dict::

            {
                "network_type": "pod",
                "interface": "masquerade | bridge",
                "name": "network_name"
            }

        For advanced configurations not covered by the other parameters,
        the ``ex_template`` parameter can be used to provide a dictionary of
        kubernetes object that defines the KubeVirt VM.
        Notice, If provided, ``ex_template`` will override all other parameters
        except for ``name`` and ``location``.

        The ``ex_template`` parameter is a dictionary of kubernetes object:

        - ``apiVersion``: ``str``
        - ``kind``: ``str``
        - ``metadata``: ``dict``
        - ``spec``: ``dict``
            - ``domain``: ``dict``
            - ``volumes``: ``list``
            - ...

        See also:

        - https://kubernetes.io/docs/concepts/overview/working-with-objects/
        - https://kubevirt.io/api-reference/

        :param name: A name to give the VM. The VM will be identified by
                     this name and atm it cannot be changed after it is set.
        :type name: ``str``

        :param size: The size of the VM in terms of CPU and memory.
                     A NodeSize object with the attributes
                     ``ram`` (int in MiB) and ``extra["cpu"]`` (int in cores).
        :type size: ``NodeSize`` with

        :param image: Either a libcloud NodeImage or a string.
                      In both cases, it must a URL to the containerDisk image.
        :type image: ``str`` or ``NodeImage``

        :param location: The namespace where the VM will live.
                          (default is ``'default'``)
        :type location: `NodeLocation`` in which the name is the namespace

        :param auth: authentication to a node.
        :type auth: ``NodeAuthSSHKey`` or ``NodeAuthPassword``.

        :param ex_cpu: The number of CPU cores to allocate to the VM.
                       (Legacy support, consider using ``size`` instead)
        :type ex_cpu: ``int`` or ``str``

        :param ex_memory: The amount of memory to allocate to the VM in MiB.
                          (Legacy support, consider using ``size`` instead)
        :type ex_memory: ``int``

        :param ex_disks: A list containing disk dictionaries.
                         Each dictionary should have the following keys:
                         ``bus``: can be ``"virtio"``, ``"sata"``, or ``"scsi"``;
                         ``device``: can be ``"lun"`` or ``"disk"``;
                         ``disk_type``: One of the supported DISK_TYPES;
                         ``name``: The name of the disk configuration;
                         ``voulme_spec``: the dictionary that defines the volume of the disk.

        :type ex_disks: ``list`` of ``dict`` with keys:
                        ``bus: str``, ``device: str``, ``disk_type: str``, ``name: str``
                        and ``voulme_spec: dict``

        :param ex_network: a dictionary that defines the network of the VM.
                           The following keys are required:
                           ``network_type``: ``"pod"``;
                           ``interface``: ``"masquerade"`` or ``"bridge"``;
                           ``name``: ``"network_name"``.
        :type ex_network: ``dict`` with keys:
                          ``network_type: str``, ``interface: str`` and ``name: str``

        :param ex_termination_grace_period: The grace period in seconds before
                                            the VM is forcefully terminated.
                                            (default is 0)
        :type ex_termination_grace_period: `int`

        :param ex_ports: A dictionary with keys: ``"ports_tcp"`` and ``"ports_udp"``
                      ``"ports_tcp"`` value is a list of ints that indicate
                      the ports to be exposed with TCP protocol,
                      and ``"ports_udp"`` is a list of ints that indicate
                      the ports to be exposed with UDP protocol.
        :type  ex_ports: `dict` with keys
                      ``ports_tcp``: ``list`` of ``int``;
                      ``ports_udp``: ``list`` of ``int``.

        :param ex_template: A dictionary of kubernetes object that defines the
                            KubeVirt VM. This is for advanced vm specifications
                            that are not covered by the other parameters.
                            If provided, it will override all other parameters
                            except for `name` and `location`.
        :type ex_template: ``dict`` with keys:
                            ``apiVersion: str``, ``kind: str``, ``metadata: dict``
                            and ``spec: dict``
        """

        # location -> namespace

        if isinstance(location, NodeLocation):
            if location.name not in map(lambda x: x.name, self.list_locations()):
                raise ValueError("The location must be one of the available namespaces")
            namespace = location.name
        else:
            namespace = "default"

        # ex_template exists, use it to create the vm, ignore other parameters

        if ex_template is not None:
            vm = self._create_node_vm_from_ex_template(
                name=name, ex_template=ex_template, other_args=locals()
            )

            return self._create_node_with_template(name=name, template=vm, namespace=namespace)
        # else (ex_template is None): create a vm with other parameters
        vm = self._base_vm_template(name=name)

        # size -> cpu and memory limits
        self._create_node_size(vm=vm, size=size, ex_cpu=ex_cpu, ex_memory=ex_memory)

        # ex_disks -> disks and volumes, image -> containerDisk
        self._create_node_disks(vm, ex_disks, image, namespace, location)

        # auth -> cloud-init

        if auth is not None:
            self._create_node_auth(vm, auth)

        # now, all disks and volumes stuff are done

        # ex_network and ex_ports -> network and interface
        self._create_node_network(vm, ex_network, ex_ports)

        # terminationGracePeriodSeconds

        if ex_termination_grace_period is not None:
            self._create_node_termination_grace_period(vm, ex_termination_grace_period)

        return self._create_node_with_template(name=name, template=vm, namespace=namespace)

    def list_images(self, location=None):
        """
        If location (namespace) is provided only the images
        in that location will be provided. Otherwise all of them.
        """
        nodes = self.list_nodes()

        if location:
            namespace = location.name
            nodes = list(filter(lambda x: x["extra"]["namespace"] == namespace, nodes))
        name_set = set()
        images = []

        for node in nodes:
            if node.image.name in name_set:
                continue
            name_set.add(node.image.name)
            images.append(node.image)

        return images

    def list_locations(self):
        """
        By locations here it is meant namespaces.
        """
        req = ROOT_URL + "namespaces"

        namespaces = []
        result = self.connection.request(req).object

        for item in result["items"]:
            name = item["metadata"]["name"]
            ID = item["metadata"]["uid"]
            namespaces.append(
                NodeLocation(id=ID, name=name, country="", driver=self.connection.driver)
            )

        return namespaces

    def list_sizes(self, location=None):
        namespace = ""

        if location:
            namespace = location.name
        nodes = self.list_nodes()
        sizes = []

        for node in nodes:
            if not namespace:
                sizes.append(node.size)
            elif namespace == node.extra["namespace"]:
                sizes.append(node.size)

        return sizes

    def create_volume(
        self,
        size,
        name,
        location=None,
        ex_storage_class_name="",
        ex_volume_mode="Filesystem",
        ex_access_mode="ReadWriteOnce",
        ex_dynamic=True,
        ex_reclaim_policy="Recycle",
        ex_volume_type=None,
        ex_volume_params=None,
    ):
        """
        :param size: The size in Gigabytes
        :type size: `int`

        :param volume_type: This is the type of volume to be created that is
                            dependent on the underlying cloud where Kubernetes
                            is deployed. K8s is supporting the following types:
                            -gcePersistentDisk
                            -awsElasticBlockStore
                            -azureFile
                            -azureDisk
                            -csi
                            -fc (Fibre Channel)
                            -flexVolume
                            -flocker
                            -nfs
                            -iSCSI
                            -rbd (Ceph Block Device)
                            -cephFS
                            -cinder (OpenStack block storage)
                            -glusterfs
                            -vsphereVolume
                            -quobyte Volumes
                            -hostPath (Single node testing only – local storage is not supported in any way and WILL NOT WORK in a multi-node cluster) # NOQA
                            -portworx Volumes
                            -scaleIO Volumes
                            -storageOS
                            This parameter is a dict in the form {type: {key1:value1, key2:value2,...}},
                            where type is one of the above and key1, key2... are type specific keys and
                            their corresponding values. eg: {nsf: {server: "172.0.0.0", path: "/tmp"}}
                                            {awsElasticBlockStore: {fsType: 'ext4', volumeID: "1234"}}
        :type volume_type: `str`

        :param volume_params: A dict with the key:value that the
                              volume_type needs.
                              This parameter is a dict in the form
                              {key1:value1, key2:value2,...},
                              where type is one of the above and key1, key2...
                              are type specific keys and
                              their corresponding values.
                              eg: for nsf volume_type
                              {server: "172.0.0.0", path: "/tmp"}
                              for awsElasticBlockStore volume_type
                              {fsType: 'ext4', volumeID: "1234"}
        """

        if ex_dynamic:
            if location is None:
                msg = "Please provide a namespace for the PVC."
                raise ValueError(msg)
            vol = self._create_volume_dynamic(
                size=size,
                name=name,
                storage_class_name=ex_storage_class_name,
                namespace=location.name,
                volume_mode=ex_volume_mode,
                access_mode=ex_access_mode,
            )

            return vol
        else:
            if ex_volume_type is None or ex_volume_params is None:
                msg = (
                    "An ex_volume_type must be provided from the list "
                    "of supported clouds, as well as the ex_volume_params "
                    "necessary for your volume type choice."
                )
                raise ValueError(msg)

        pv = {
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {"name": name},
            "spec": {
                "capacity": {"storage": str(size) + "Gi"},
                "volumeMode": ex_volume_mode,
                "accessModes": [ex_access_mode],
                "persistentVolumeReclaimPolicy": ex_reclaim_policy,
                "storageClassName": ex_storage_class_name,
                "mountOptions": [],  # beta, to add in the future
                ex_volume_type: ex_volume_params,
            },
        }

        req = ROOT_URL + "persistentvolumes/"
        method = "POST"
        data = json.dumps(pv)
        try:
            self.connection.request(req, method=method, data=data)

        except Exception:
            raise
        # make sure that the volume was created
        volumes = self.list_volumes()

        for volume in volumes:
            if volume.name == name:
                return volume

    def _create_volume_dynamic(
        self,
        size,
        name,
        storage_class_name,
        volume_mode="Filesystem",
        namespace="default",
        access_mode="ReadWriteOnce",
    ):
        """
        Method to create a Persistent Volume Claim for storage,
        thus storage is required in the arguments.
        This method assumes dynamic provisioning of the
        Persistent Volume so the storage_class given should
        allow for it (by default it usually is), or already
        have unbounded Persistent Volumes created by an admin.

        :param name: The name of the pvc an arbitrary string of lower letters
        :type name: `str`

        :param size: An int of the amount of gigabytes desired
        :type size: `int`

        :param namespace: The namespace where the claim will live
        :type namespace: `str`

        :param storage_class_name: If you want the pvc to be bound to
                                 a particular class of PVs specified here.
        :type storage_class_name: `str`

        :param access_mode: The desired access mode, ie "ReadOnlyMany"
        :type access_mode: `str`

        :param matchLabels: A dictionary with the labels, ie:
                            {'release': 'stable,}
        :type matchLabels: `dict` with keys `str` and values `str`
        """
        pvc = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": name},
            "spec": {
                "accessModes": [],
                "volumeMode": volume_mode,
                "resources": {"requests": {"storage": ""}},
            },
        }

        pvc["spec"]["accessModes"].append(access_mode)

        if storage_class_name is not None:
            pvc["spec"]["storageClassName"] = storage_class_name
        else:
            raise ValueError(
                "The storage class name must be provided of a"
                "storage class which allows for dynamic "
                "provisioning"
            )
        pvc["spec"]["resources"]["requests"]["storage"] = str(size) + "Gi"

        method = "POST"
        req = ROOT_URL + "namespaces/" + namespace + "/persistentvolumeclaims"
        data = json.dumps(pvc)
        try:
            result = self.connection.request(req, method=method, data=data)
        except Exception:
            raise

        if result.object["status"]["phase"] != "Bound":
            for _ in range(3):
                req = ROOT_URL + "namespaces/" + namespace + "/persistentvolumeclaims/" + name
                try:
                    result = self.connection.request(req).object
                except Exception:
                    raise

                if result["status"]["phase"] == "Bound":
                    break
                time.sleep(3)

        # check that the pv was created and bound
        volumes = self.list_volumes()

        for volume in volumes:
            if volume.extra["pvc"]["name"] == name:
                return volume

    def _bind_volume(self, volume, namespace="default"):
        """
        This method is for unbound volumes that were statically made.
        It will bind them to a pvc so they can be used by
        a kubernetes resource.
        """

        if volume.extra["is_bound"]:
            return  # volume already bound

        storage_class = volume.extra["storage_class_name"]
        size = volume.size
        name = volume.name + "-pvc"
        volume_mode = volume.extra["volume_mode"]
        access_mode = volume.extra["access_modes"][0]

        vol = self._create_volume_dynamic(
            size=size,
            name=name,
            storage_class_name=storage_class,
            volume_mode=volume_mode,
            namespace=namespace,
            access_mode=access_mode,
        )

        return vol

    def destroy_volume(self, volume):
        # first delete the pvc
        method = "DELETE"

        if volume.extra["is_bound"]:
            pvc = volume.extra["pvc"]["name"]
            namespace = volume.extra["pvc"]["namespace"]
            req = ROOT_URL + "namespaces/" + namespace + "/persistentvolumeclaims/" + pvc
            try:
                result = self.connection.request(req, method=method)

            except Exception:
                raise

        pv = volume.name
        req = ROOT_URL + "persistentvolumes/" + pv

        try:
            result = self.connection.request(req, method=method)

            return result.status
        except Exception:
            raise

    def attach_volume(self, node, volume, device="disk", ex_bus="virtio", ex_name=None):
        """
        params: bus, name , device (disk or lun)
        """
        # volume must be bound to a claim

        if not volume.extra["is_bound"]:
            volume = self._bind_volume(volume, node.extra["namespace"])

            if volume is None:
                raise LibcloudError(
                    "Selected Volume (PV) could not be bound "
                    "(to a PVC), please select another volume",
                    driver=self,
                )

        claimName = volume.extra["pvc"]["name"]

        if ex_name is None:
            name = claimName
        else:
            name = ex_name
        namespace = volume.extra["pvc"]["namespace"]
        # check if vm is stopped
        self.stop_node(node)
        # check if it is the same namespace

        if node.extra["namespace"] != namespace:
            msg = "The PVC and the VM must be in the same namespace"
            raise ValueError(msg)
        vm = node.name
        req = KUBEVIRT_URL + "namespaces/" + namespace + "/virtualmachines/" + vm
        disk_dict = {device: {"bus": ex_bus}, "name": name}
        volumes_dict = {"persistentVolumeClaim": {"claimName": claimName}, "name": name}
        # Get all the volumes of the vm
        try:
            result = self.connection.request(req).object
        except Exception:
            raise
        disks = result["spec"]["template"]["spec"]["domain"]["devices"]["disks"]
        volumes = result["spec"]["template"]["spec"]["volumes"]
        disks.append(disk_dict)
        volumes.append(volumes_dict)
        # now patch the new volumes and disks lists into the resource
        headers = {"Content-Type": "application/merge-patch+json"}
        data = {
            "spec": {
                "template": {
                    "spec": {
                        "volumes": volumes,
                        "domain": {"devices": {"disks": disks}},
                    }
                }
            }
        }
        try:
            result = self.connection.request(
                req, method="PATCH", data=json.dumps(data), headers=headers
            )

            if "pvcs" in node.extra:
                node.extra["pvcs"].append(claimName)
            else:
                node.extra["pvcs"] = [claimName]

            return result in VALID_RESPONSE_CODES
        except Exception:
            raise

    def detach_volume(self, volume, ex_node):
        """
        Detaches a volume from a node but the node must be given since a PVC
        can have more than one VMI's pointing to it
        """
        # vmi must be stopped
        self.stop_node(ex_node)

        claimName = volume.extra["pvc"]["name"]
        name = ex_node.name
        namespace = ex_node.extra["namespace"]
        req = KUBEVIRT_URL + "namespaces/" + namespace + "/virtualmachines/" + name
        headers = {"Content-Type": "application/merge-patch+json"}
        # Get all the volumes of the vm

        try:
            result = self.connection.request(req).object
        except Exception:
            raise
        disks = result["spec"]["template"]["spec"]["domain"]["devices"]["disks"]
        volumes = result["spec"]["template"]["spec"]["volumes"]
        to_delete = None

        for volume in volumes:
            if "persistentVolumeClaim" in volume:
                if volume["persistentVolumeClaim"]["claimName"] == claimName:
                    to_delete = volume["name"]
                    volumes.remove(volume)

                    break

        if not to_delete:
            msg = "The given volume is not attached to the given VM"
            raise ValueError(msg)

        for disk in disks:
            if disk["name"] == to_delete:
                disks.remove(disk)

                break
        # now patch the new volumes and disks lists into the resource
        data = {
            "spec": {
                "template": {
                    "spec": {
                        "volumes": volumes,
                        "domain": {"devices": {"disks": disks}},
                    }
                }
            }
        }
        try:
            result = self.connection.request(
                req, method="PATCH", data=json.dumps(data), headers=headers
            )
            ex_node.extra["pvcs"].remove(claimName)

            return result in VALID_RESPONSE_CODES
        except Exception:
            raise

    def ex_list_persistent_volume_claims(self, namespace="default"):
        pvc_req = ROOT_URL + "namespaces/" + namespace + "/persistentvolumeclaims"
        try:
            result = self.connection.request(pvc_req).object
        except Exception:
            raise
        pvcs = [item["metadata"]["name"] for item in result["items"]]

        return pvcs

    def ex_list_storage_classes(self):
        # sc = storage class
        sc_req = "/apis/storage.k8s.io/v1/storageclasses"
        try:
            result = self.connection.request(sc_req).object
        except Exception:
            raise
        scs = [item["metadata"]["name"] for item in result["items"]]

        return scs

    def list_volumes(self):
        """
        Location is a namespace of the cluster.
        """
        volumes = []

        pv_rec = ROOT_URL + "/persistentvolumes/"

        try:
            result = self.connection.request(pv_rec).object
        except Exception:
            raise

        for item in result["items"]:
            if item["status"]["phase"] not in {"Available", "Bound"}:
                continue
            ID = item["metadata"]["uid"]
            size = item["spec"]["capacity"]["storage"]
            size = int(size.rstrip("Gi"))
            extra = {"pvc": {}}
            extra["storage_class_name"] = item["spec"]["storageClassName"]
            extra["is_bound"] = item["status"]["phase"] == "Bound"
            extra["access_modes"] = item["spec"]["accessModes"]
            extra["volume_mode"] = item["spec"]["volumeMode"]

            if extra["is_bound"]:
                extra["pvc"]["name"] = item["spec"]["claimRef"]["name"]
                extra["pvc"]["namespace"] = item["spec"]["claimRef"]["namespace"]
                extra["pvc"]["uid"] = item["spec"]["claimRef"]["uid"]
                name = extra["pvc"]["name"]
            else:
                name = item["metadata"]["name"]
            volume = StorageVolume(
                id=ID, name=name, size=size, driver=self.connection.driver, extra=extra
            )
            volumes.append(volume)

        return volumes

    def _ex_connection_class_kwargs(self):
        kwargs = {}

        if hasattr(self, "key_file"):
            kwargs["key_file"] = self.key_file

        if hasattr(self, "cert_file"):
            kwargs["cert_file"] = self.cert_file

        return kwargs

    def _to_node(self, vm, is_stopped=False):  # type: (dict, bool) -> Node
        """
        converts a vm from the kubevirt API to a libcloud node

        :param vm: kubevirt vm object
        :type vm: dict

        :param is_stopped: if the vm is stopped, it will not have a pod
        :type is_stopped: bool

        :return: a libcloud node
        :rtype: :class:`Node`
        """
        ID = vm["metadata"]["uid"]
        name = vm["metadata"]["name"]
        driver = self.connection.driver
        extra = {"namespace": vm["metadata"]["namespace"]}
        extra["pvcs"] = []

        memory = 0

        if "limits" in vm["spec"]["template"]["spec"]["domain"]["resources"]:
            if "memory" in vm["spec"]["template"]["spec"]["domain"]["resources"]["limits"]:
                memory = vm["spec"]["template"]["spec"]["domain"]["resources"]["limits"]["memory"]
        elif vm["spec"]["template"]["spec"]["domain"]["resources"].get("requests", None):
            if vm["spec"]["template"]["spec"]["domain"]["resources"]["requests"].get(
                "memory", None
            ):
                memory = vm["spec"]["template"]["spec"]["domain"]["resources"]["requests"]["memory"]
        memory = _memory_in_MB(memory)

        memory_req = (
            vm["spec"]["template"]["spec"]["domain"]["resources"]
            .get("requests", {})
            .get("memory", None)
        )

        if memory_req:
            memory_req = _memory_in_MB(memory_req)
        else:
            memory_req = memory

        cpu = 1

        if vm["spec"]["template"]["spec"]["domain"]["resources"].get("limits", None):
            if vm["spec"]["template"]["spec"]["domain"]["resources"]["limits"].get("cpu", None):
                cpu = vm["spec"]["template"]["spec"]["domain"]["resources"]["limits"]["cpu"]
        elif vm["spec"]["template"]["spec"]["domain"]["resources"].get("requests", None) and vm[
            "spec"
        ]["template"]["spec"]["domain"]["resources"]["requests"].get("cpu", None):
            cpu = vm["spec"]["template"]["spec"]["domain"]["resources"]["requests"]["cpu"]
            cpu_req = cpu
        elif vm["spec"]["template"]["spec"]["domain"].get("cpu", None):
            cpu = vm["spec"]["template"]["spec"]["domain"]["cpu"].get("cores", 1)

        if not isinstance(cpu, int):
            cpu = int(cpu.rstrip("m"))

        cpu_req = (
            vm["spec"]["template"]["spec"]["domain"]["resources"]
            .get("requests", {})
            .get("cpu", None)
        )

        if cpu_req is None:
            cpu_req = cpu

        extra_size = {"cpu": cpu, "cpu_request": cpu_req, "ram": memory, "ram_request": memory_req}
        size_name = "{} vCPUs, {}MB Ram".format(str(cpu), str(memory))
        size_id = hashlib.md5(size_name.encode("utf-8")).hexdigest()  # nosec
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

        extra["memory"] = memory
        extra["cpu"] = cpu

        image_name = "undefined"

        for volume in vm["spec"]["template"]["spec"]["volumes"]:
            for k, v in volume.items():
                if type(v) is dict:
                    if "image" in v:
                        image_name = v["image"]
        image = NodeImage(image_name, image_name, driver)

        if "volumes" in vm["spec"]["template"]["spec"]:
            for volume in vm["spec"]["template"]["spec"]["volumes"]:
                if "persistentVolumeClaim" in volume:
                    extra["pvcs"].append(volume["persistentVolumeClaim"]["claimName"])

        port_forwards = []
        services = self.ex_list_services(namespace=extra["namespace"], node_name=name)

        for service in services:
            service_type = service["spec"].get("type")

            for port_pair in service["spec"]["ports"]:
                protocol = port_pair.get("protocol")
                public_port = port_pair.get("port")
                local_port = port_pair.get("targetPort")
                try:
                    int(local_port)
                except ValueError:
                    local_port = public_port
                port_forwards.append(
                    {
                        "local_port": local_port,
                        "public_port": public_port,
                        "protocol": protocol,
                        "service_type": service_type,
                    }
                )
        extra["port_forwards"] = port_forwards

        if is_stopped:
            state = NodeState.STOPPED
            public_ips = None
            private_ips = None

            return Node(
                id=ID,
                name=name,
                state=state,
                public_ips=public_ips,
                private_ips=private_ips,
                driver=driver,
                size=size,
                image=image,
                extra=extra,
            )

        # getting image and image_ID from the container
        req = ROOT_URL + "namespaces/" + extra["namespace"] + "/pods"
        result = self.connection.request(req).object
        pod = None

        for pd in result["items"]:
            if "metadata" in pd and "ownerReferences" in pd["metadata"]:
                if pd["metadata"]["ownerReferences"][0]["name"] == name:
                    pod = pd

        if pod is None or "containerStatuses" not in pod["status"]:
            state = NodeState.PENDING
            public_ips = None
            private_ips = None

            return Node(
                id=ID,
                name=name,
                state=state,
                public_ips=public_ips,
                private_ips=private_ips,
                driver=driver,
                size=size,
                image=image,
                extra=extra,
            )
        extra["pod"] = {"name": pod["metadata"]["name"]}

        for cont_status in pod["status"]["containerStatuses"]:
            # only 2 containers are present the launcher and the vmi

            if cont_status["name"] != "compute":
                image = NodeImage(ID, cont_status["image"], driver)
                state = (
                    NodeState.RUNNING if "running" in cont_status["state"] else NodeState.PENDING
                )
        public_ips = None
        created_at = datetime.strptime(vm["metadata"]["creationTimestamp"], "%Y-%m-%dT%H:%M:%SZ")

        if "podIPs" in pod["status"]:
            private_ips = [ip["ip"] for ip in pod["status"]["podIPs"]]
        else:
            private_ips = []

        return Node(
            id=ID,
            name=name,
            state=state,
            public_ips=public_ips,
            private_ips=private_ips,
            driver=driver,
            size=size,
            image=image,
            extra=extra,
            created_at=created_at,
        )

    def ex_list_services(self, namespace="default", node_name=None, service_name=None):
        """
        If node_name is given then the services returned will be those that
        concern the node.
        """
        params = None

        if service_name is not None:
            params = {"fieldSelector": "metadata.name={}".format(service_name)}
        req = ROOT_URL + "/namespaces/{}/services".format(namespace)
        result = self.connection.request(req, params=params).object["items"]

        if node_name:
            res = []

            for service in result:
                if node_name in service["metadata"].get("name", ""):
                    res.append(service)

            return res

        return result

    def ex_create_service(
        self,
        node,
        ports,
        service_type="NodePort",
        cluster_ip=None,
        load_balancer_ip=None,
        override_existing_ports=False,
    ):
        """
        Each node has a single service of one type on which the exposed ports
        are described. If a service exists then the port declared will be
        exposed alongside the existing ones, set override_existing_ports=True
        to delete existing exposed ports and expose just the ones in the port
        variable.

        :param node: the libcloud node for which the ports will be exposed
        :type  node: libcloud `Node` class

        :param ports: a list of dictionaries with keys --> values:
                     'port' --> port to be exposed on the service;
                     'target_port' --> port on the pod/node, optional
                                       if empty then it gets the same
                                       value as 'port' value;
                     'protocol' ---> either 'UDP' or 'TCP', defaults to TCP;
                     'name' --> A name for the service;
                     If ports is an empty `list` and a service exists of this
                     type then the service will be deleted.
        :type  ports: `list` of `dict` where each `dict` has keys --> values:
                     'port' --> `int`;
                     'target_port' --> `int`;
                     'protocol' --> `str`;
                     'name' --> `str`;

        :param service_type: Valid types are ClusterIP, NodePort, LoadBalancer
        :type  service_type: `str`

        :param cluster_ip: This can be set with an IP string value if you want
                          manually set the service's internal IP. If the value
                          is not correct the method will fail, this value can't
                          be updated.
        :type  cluster_ip: `str`

        :param override_existing_ports: Set to True if you want to delete the
                                       existing ports exposed by the service
                                       and keep just the ones declared in the
                                       present ports argument.
                                       By default it is false and if the
                                       service already exists the ports will be
                                       added to the existing ones.
        :type  override_existing_ports: `boolean`
        """
        # check if service exists first
        namespace = node.extra.get("namespace", "default")
        service_name = "service-{}-{}".format(service_type.lower(), node.name)
        service_list = self.ex_list_services(namespace=namespace, service_name=service_name)

        ports_to_expose = []
        # if ports has a falsey value like None or 0

        if not ports:
            ports = []

        for port_group in ports:
            if not port_group.get("target_port", None):
                port_group["target_port"] = port_group["port"]

            if not port_group.get("name", ""):
                port_group["name"] = "port-{}".format(port_group["port"])
            ports_to_expose.append(
                {
                    "protocol": port_group.get("protocol", "TCP"),
                    "port": int(port_group["port"]),
                    "targetPort": int(port_group["target_port"]),
                    "name": port_group["name"],
                }
            )
        headers = None
        data = None

        if len(service_list) > 0:
            if not ports:
                result = True

                for service in service_list:
                    service_name = service["metadata"]["name"]
                    result = result and self.ex_delete_service(
                        namespace=namespace, service_name=service_name
                    )

                return result
            else:
                method = "PATCH"
                spec = {"ports": ports_to_expose}

                if not override_existing_ports:
                    existing_ports = service_list[0]["spec"]["ports"]
                    spec = {"ports": existing_ports.extend(ports_to_expose)}
                data = json.dumps({"spec": spec})
                headers = {"Content-Type": "application/merge-patch+json"}
            req = "{}/namespaces/{}/services/{}".format(ROOT_URL, namespace, service_name)
        else:
            if not ports:
                raise ValueError(
                    "Argument ports is empty but there is no "
                    "service of {} type to be deleted".format(service_type)
                )
            method = "POST"
            service = {
                "kind": "Service",
                "apiVersion": "v1",
                "metadata": {
                    "name": service_name,
                    "labels": {"service": "kubevirt.io"},
                },
                "spec": {
                    "type": "",
                    "selector": {"kubevirt.io/vm": node.name},
                    "ports": [],
                },
            }
            service["spec"]["ports"] = ports_to_expose
            service["spec"]["type"] = service_type

            if cluster_ip is not None:
                service["spec"]["clusterIP"] = cluster_ip

            if service_type == "LoadBalancer" and load_balancer_ip is not None:
                service["spec"]["loadBalancerIP"] = load_balancer_ip
            data = json.dumps(service)
            req = "{}/namespaces/{}/services".format(ROOT_URL, namespace)
        try:
            result = self.connection.request(req, method=method, data=data, headers=headers)
        except Exception:
            raise

        return result.status in VALID_RESPONSE_CODES

    def ex_delete_service(self, namespace, service_name):
        req = "{}/namespaces/{}/services/{}".format(ROOT_URL, namespace, service_name)
        headers = {"Content-Type": "application/yaml"}
        try:
            result = self.connection.request(req, method="DELETE", headers=headers)
        except Exception:
            raise

        return result.status in VALID_RESPONSE_CODES


def _deep_merge_dict(source: dict, destination: dict) -> dict:
    """
    Deep merge two dictionaries: source into destination.
    For conflicts, prefer source's non-zero values over destination's.
    (By non-zero, we mean that bool(value) is True.)

    Extended from https://stackoverflow.com/a/20666342, added zero value handling.

    Example::

        >>> a = {"domain": {"devices": 0}, "volumes": [1, 2, 3], "network": {}}
        >>> b = {"domain": {"machine": "non-exist-in-a", "devices": 1024}, "volumes": [4, 5, 6]}
        >>> _deep_merge_dict(a, b)
        {'domain': {'machine': 'non-exist-in-a', 'devices': 1024}, 'volumes': [1, 2, 3], 'network': {}}

    In the above example:

    - network: exists in source (a) but not in destination (b): add source (a)'s
    - volumes: exists in both, both are non-zero: prefer source (a)'s
    - devices: exists in both: source (a) is zero, destination (b) is non-zero: keep destination (b)'s
    - machine: exists in destination (b) but not in source (a): reserve destination (b)'s

    :param source: RO: A dict to be merged into another.
                   Do not use circular dict (e.g. d = {}; d['d'] = d) as source,
                   otherwise a RecursionError will be raised.
    :param destination: RW: A dict to be merged into. (the value will be modified).

    :return: dict: Updated destination.
    """

    for key, value in source.items():
        if isinstance(value, dict):  # recurse for dicts
            node = destination.setdefault(key, {})  # get node or create one
            _deep_merge_dict(value, node)
        elif key not in destination:  # not existing in destination: add it
            destination[key] = value
        elif value:  # existing: update if source's value is non-zero
            destination[key] = value

    return destination


def _memory_in_MB(memory):  # type: (Union[str, int]) -> int
    """
    parse k8s memory resource units to MiB or MB (depending on input)

    Note:

    - 1 MiB = 1024 KiB = 1024 * 1024 B = 1048576 B
    -  1 MB =  1000 KB = 1000 * 1000 B = 1000000 B

    Reference: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-memory

    :param memory: Limits and requests for memory are measured in bytes.
                   You can express memory as a plain integer or as a fixed-point integer using one of these suffixes:
                   E, P, T, G, M, K, Ei, Pi, Ti, Gi, Mi, Ki
                   For example, the following represent roughly the same value:
                   128974848, 129e6, 129M, 123Mi
    :type memory: ``str`` or ``int``

    :return: memory in MiB (if input is `int` bytes or `str` with suffix `?i`)
             or in MB (if input is `str` with suffix `?`)
    :rtype: ``int``
    """

    try:
        mem_bytes = int(memory)

        return mem_bytes // 1024 // 1024
    except ValueError:
        pass

    if not isinstance(memory, str):
        raise ValueError("memory must be int or str")

    if memory.endswith("Ei"):
        return int(memory.rstrip("Ei")) * 1024 * 1024 * 1024 * 1024
    elif memory.endswith("Pi"):
        return int(memory.rstrip("Pi")) * 1024 * 1024 * 1024
    elif memory.endswith("Ti"):
        return int(memory.rstrip("Ti")) * 1024 * 1024
    elif memory.endswith("Gi"):
        return int(memory.rstrip("Gi")) * 1024
    elif memory.endswith("Mi"):
        return int(memory.rstrip("Mi"))
    elif memory.endswith("Ki"):
        return int(memory.rstrip("Ki")) // 1024
    elif memory.endswith("E"):
        return int(memory.rstrip("E")) * 1000 * 1000 * 1000 * 1000
    elif memory.endswith("P"):
        return int(memory.rstrip("P")) * 1000 * 1000 * 1000
    elif memory.endswith("T"):
        return int(memory.rstrip("T")) * 1000 * 1000
    elif memory.endswith("G"):
        return int(memory.rstrip("G")) * 1000
    elif memory.endswith("M"):
        return int(memory.rstrip("M"))
    elif memory.endswith("K"):
        return int(memory.rstrip("K")) // 1000
    else:
        raise ValueError("memory unit not supported {}".format(memory))


def KubeVirtNodeSize(
    cpu, ram, cpu_request=None, ram_request=None
):  # type: (int, int, Optional[int], Optional[int]) -> NodeSize
    """
    Create a NodeSize object for KubeVirt driver.

    This function is just a shorthand for ``NodeSize(ram=ram, extra={"cpu": cpu})``.

    :param cpu: number of virtual CPUs (max limit)
    :type cpu: ``int``

    :param ram: amount of RAM in MiB (max limit)
    :type ram: ``int``

    :param cpu_request: number of virtual CPUs (min request)
    :type cpu_request: ``int``

    :param ram_request: amount of RAM in MiB (min request)
    :type ram_request: ``int``

    :return: a NodeSize object with ram and extra.cpu set
    :rtype: :class:`NodeSize`
    """
    extra = {"cpu": cpu}

    extra["cpu_request"] = cpu_request or cpu
    extra["ram_request"] = ram_request or ram

    name = "{} vCPUs, {}MB Ram".format(str(cpu), str(ram))
    size_id = hashlib.md5(name.encode("utf-8")).hexdigest()  # nosec

    return NodeSize(
        id=size_id,
        name=name,
        ram=ram,
        disk=0,
        bandwidth=0,
        price=0,
        driver=KubeVirtNodeDriver,
        extra=extra,
    )


def KubeVirtNodeImage(name):  # type: (str) -> NodeImage
    """
    Create a NodeImage object for KubeVirt driver.

    This function is just a shorthand for ``NodeImage(name=name)``.

    :param name: image source
    :type name: ``str``

    :return: a NodeImage object with the name set to the source to a
        containerDisk image (e.g. ``"quay.io/containerdisks/ubuntu:22.04"``)
    :rtype: :class:`NodeImage`
    """
    image_id = hashlib.md5(name.encode("utf-8")).hexdigest()  # nosec

    return NodeImage(id=image_id, name=name, driver=KubeVirtNodeDriver)
