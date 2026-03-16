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
VMware vSphere driver. Uses pyvmomi - https://github.com/vmware/pyvmomi
Code inspired by https://github.com/vmware/pyvmomi-community-samples

Authors: Dimitris Moraitis, Alex Tsiliris, Markos Gogoulos
"""

import ssl
import json
import time
import atexit
import base64
import asyncio
import hashlib
import logging
import warnings
import functools
import itertools

from libcloud.utils.py3 import httplib
from libcloud.common.base import JsonResponse, ConnectionKey
from libcloud.common.types import LibcloudError, ProviderError, InvalidCredsError
from libcloud.compute.base import Node, NodeSize, NodeImage, NodeDriver, NodeLocation
from libcloud.compute.types import Provider, NodeState
from libcloud.utils.networking import is_public_subnet
from libcloud.common.exceptions import BaseHTTPError

try:
    from pyVim import connect
    from pyVmomi import VmomiSupport, vim, vmodl
    from pyVim.task import WaitForTask

    pyvmomi = True
except ImportError:
    pyvmomi = None


logger = logging.getLogger("libcloud.compute.drivers.vsphere")


def recurse_snapshots(snapshot_list):
    ret = []

    for s in snapshot_list:
        ret.append(s)
        ret += recurse_snapshots(getattr(s, "childSnapshotList", []))

    return ret


def format_snapshots(snapshot_list):
    ret = []

    for s in snapshot_list:
        ret.append(
            {
                "id": s.id,
                "name": s.name,
                "description": s.description,
                "created": s.createTime.strftime("%Y-%m-%d %H:%M"),
                "state": s.state,
            }
        )

    return ret


# 6.5 and older, probably won't work on anything earlier than 4.x
class VSphereNodeDriver(NodeDriver):
    name = "VMware vSphere"
    website = "http://www.vmware.com/products/vsphere/"
    type = Provider.VSPHERE

    NODE_STATE_MAP = {
        "poweredOn": NodeState.RUNNING,
        "poweredOff": NodeState.STOPPED,
        "suspended": NodeState.SUSPENDED,
    }

    def __init__(self, host, username, password, port=443, ca_cert=None):
        """Initialize a connection by providing a hostname,
        username and password
        """

        if pyvmomi is None:
            raise ImportError(
                'Missing "pyvmomi" dependency. '
                "You can install it "
                "using pip - pip install pyvmomi"
            )
        self.host = host
        try:
            if ca_cert is None:
                self.connection = connect.SmartConnect(
                    host=host,
                    port=port,
                    user=username,
                    pwd=password,
                )
            else:
                context = ssl.create_default_context(cafile=ca_cert)
                self.connection = connect.SmartConnect(
                    host=host,
                    port=port,
                    user=username,
                    pwd=password,
                    sslContext=context,
                )
            atexit.register(connect.Disconnect, self.connection)
        except Exception as exc:
            error_message = str(exc).lower()

            if "incorrect user name" in error_message:
                raise InvalidCredsError("Check your username and " "password are valid")

            if "connection refused" in error_message or "is not a vim server" in error_message:
                raise LibcloudError(
                    "Check that the host provided is a " "vSphere installation",
                    driver=self,
                )

            if "name or service not known" in error_message:
                raise LibcloudError("Check that the vSphere host is accessible", driver=self)

            if "certificate verify failed" in error_message:
                # bypass self signed certificates
                try:
                    context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
                    context.verify_mode = ssl.CERT_NONE
                except ImportError:
                    raise ImportError(
                        "To use self signed certificates, "
                        "please upgrade to python 2.7.11 and "
                        "pyvmomi 6.0.0+"
                    )

                self.connection = connect.SmartConnect(
                    host=host,
                    port=port,
                    user=username,
                    pwd=password,
                    sslContext=context,
                )
                atexit.register(connect.Disconnect, self.connection)
            else:
                raise LibcloudError("Cannot connect to vSphere", driver=self)

    def list_locations(self, ex_show_hosts_in_drs=True):
        """
        Lists locations
        """
        content = self.connection.RetrieveContent()

        potential_locations = [
            dc
            for dc in content.viewManager.CreateContainerView(
                content.rootFolder,
                [vim.ClusterComputeResource, vim.HostSystem],
                recursive=True,
            ).view
        ]

        # Add hosts and clusters with DRS enabled
        locations = []
        hosts_all = []
        clusters = []

        for location in potential_locations:
            if isinstance(location, vim.HostSystem):
                hosts_all.append(location)
            elif isinstance(location, vim.ClusterComputeResource):
                if location.configuration.drsConfig.enabled:
                    clusters.append(location)

        if ex_show_hosts_in_drs:
            hosts = hosts_all
        else:
            hosts_filter = [host for cluster in clusters for host in cluster.host]
            hosts = [host for host in hosts_all if host not in hosts_filter]

        for cluster in clusters:
            locations.append(self._to_location(cluster))

        for host in hosts:
            locations.append(self._to_location(host))

        return locations

    def _to_location(self, data):
        extra = {}
        try:
            if isinstance(data, vim.HostSystem):
                extra = {
                    "type": "host",
                    "state": data.runtime.connectionState,
                    "hypervisor": data.config.product.fullName,
                    "vendor": data.hardware.systemInfo.vendor,
                    "model": data.hardware.systemInfo.model,
                    "ram": data.hardware.memorySize,
                    "cpu": {
                        "packages": data.hardware.cpuInfo.numCpuPackages,
                        "cores": data.hardware.cpuInfo.numCpuCores,
                        "threads": data.hardware.cpuInfo.numCpuThreads,
                    },
                    "uptime": data.summary.quickStats.uptime,
                    "parent": str(data.parent),
                }
            elif isinstance(data, vim.ClusterComputeResource):
                extra = {
                    "type": "cluster",
                    "overallStatus": data.overallStatus,
                    "drs": data.configuration.drsConfig.enabled,
                    "hosts": [host.name for host in data.host],
                    "parent": str(data.parent),
                }
        except AttributeError as exc:
            logger.error("Cannot convert location {}: {!r}".format(data.name, exc))
            extra = {}

        return NodeLocation(id=data.name, name=data.name, country=None, extra=extra, driver=self)

    def ex_list_networks(self):
        """
        List networks
        """
        content = self.connection.RetrieveContent()
        networks = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.Network], recursive=True
        ).view

        return [self._to_network(network) for network in networks]

    def _to_network(self, data):
        summary = data.summary
        extra = {
            "hosts": [h.name for h in data.host],
            "ip_pool_name": summary.ipPoolName,
            "ip_pool_id": summary.ipPoolId,
            "accessible": summary.accessible,
        }

        return VSphereNetwork(id=data.name, name=data.name, extra=extra)

    def list_sizes(self):
        """
        Returns sizes
        """

        return []

    def list_images(self, location=None, folder_ids=None):
        """
        Lists VM templates as images.
        If folder is given then it will list images contained
        in that folder only.
        """

        images = []

        if folder_ids:
            vms = []

            for folder_id in folder_ids:
                folder_object = self._get_item_by_moid("Folder", folder_id)
                vms.extend(folder_object.childEntity)
        else:
            content = self.connection.RetrieveContent()
            vms = content.viewManager.CreateContainerView(
                content.rootFolder, [vim.VirtualMachine], recursive=True
            ).view

        for vm in vms:
            if vm.config and vm.config.template:
                images.append(self._to_image(vm))

        return images

    def _to_image(self, data):
        summary = data.summary
        name = summary.config.name
        uuid = summary.config.instanceUuid
        memory = summary.config.memorySizeMB
        cpus = summary.config.numCpu
        operating_system = summary.config.guestFullName
        os_type = "unix"

        if "Microsoft" in str(operating_system):
            os_type = "windows"
        annotation = summary.config.annotation
        extra = {
            "path": summary.config.vmPathName,
            "operating_system": operating_system,
            "os_type": os_type,
            "memory_MB": memory,
            "cpus": cpus,
            "overallStatus": str(summary.overallStatus),
            "metadata": {},
            "type": "template_6_5",
            "disk_size": int(summary.storage.committed) // (1024**3),
            "datastore": data.datastore[0].info.name,
        }

        boot_time = summary.runtime.bootTime

        if boot_time:
            extra["boot_time"] = boot_time.isoformat()

        if annotation:
            extra["annotation"] = annotation

        for custom_field in data.customValue:
            key_id = custom_field.key
            key = self.find_custom_field_key(key_id)
            extra["metadata"][key] = custom_field.value

        return NodeImage(id=uuid, name=name, driver=self, extra=extra)

    def _collect_properties(self, content, view_ref, obj_type, path_set=None, include_mors=False):
        """
        Collect properties for managed objects from a view ref
        Check the vSphere API documentation for example on retrieving
        object properties:
            - http://goo.gl/erbFDz
        Args:
            content     (ServiceInstance): ServiceInstance content
            view_ref (pyVmomi.vim.view.*): Starting point of inventory
                                           navigation
            obj_type      (pyVmomi.vim.*): Type of managed object
            path_set               (list): List of properties to retrieve
            include_mors           (bool): If True include the managed objects
                                        refs in the result
        Returns:
            A list of properties for the managed objects
        """
        collector = content.propertyCollector

        # Create object specification to define the starting point of
        # inventory navigation
        obj_spec = vmodl.query.PropertyCollector.ObjectSpec()
        obj_spec.obj = view_ref
        obj_spec.skip = True

        # Create a traversal specification to identify the path for collection
        traversal_spec = vmodl.query.PropertyCollector.TraversalSpec()
        traversal_spec.name = "traverseEntities"
        traversal_spec.path = "view"
        traversal_spec.skip = False
        traversal_spec.type = view_ref.__class__
        obj_spec.selectSet = [traversal_spec]

        # Identify the properties to the retrieved
        property_spec = vmodl.query.PropertyCollector.PropertySpec()
        property_spec.type = obj_type

        if not path_set:
            property_spec.all = True

        property_spec.pathSet = path_set

        # Add the object and property specification to the
        # property filter specification
        filter_spec = vmodl.query.PropertyCollector.FilterSpec()
        filter_spec.objectSet = [obj_spec]
        filter_spec.propSet = [property_spec]

        # Retrieve properties
        props = collector.RetrieveContents([filter_spec])

        data = []

        for obj in props:
            properties = {}

            for prop in obj.propSet:
                properties[prop.name] = prop.val

            if include_mors:
                properties["obj"] = obj.obj

            data.append(properties)

        return data

    def list_nodes(self, enhance=True, max_properties=20):
        """
        List nodes, excluding templates
        """

        vm_properties = [
            "config.template",
            "summary.config.name",
            "summary.config.vmPathName",
            "summary.config.memorySizeMB",
            "summary.config.numCpu",
            "summary.storage.committed",
            "summary.config.guestFullName",
            "summary.runtime.host",
            "summary.config.instanceUuid",
            "summary.config.annotation",
            "summary.runtime.powerState",
            "summary.runtime.bootTime",
            "summary.guest.ipAddress",
            "summary.overallStatus",
            "customValue",
            "snapshot",
        ]
        content = self.connection.RetrieveContent()
        view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.VirtualMachine], True
        )
        i = 0
        vm_dict = {}

        while i < len(vm_properties):
            vm_list = self._collect_properties(
                content,
                view,
                vim.VirtualMachine,
                path_set=vm_properties[i : i + max_properties],
                include_mors=True,
            )
            i += max_properties

            for vm in vm_list:
                if not vm_dict.get(vm["obj"]):
                    vm_dict[vm["obj"]] = vm
                else:
                    vm_dict[vm["obj"]].update(vm)

        vm_list = [vm_dict[k] for k in vm_dict]
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        nodes = loop.run_until_complete(self._to_nodes(vm_list))

        if enhance:
            nodes = self._enhance_metadata(nodes, content)

        return nodes

    def list_nodes_recursive(self, enhance=True):
        """
        Lists nodes, excluding templates
        """
        nodes = []
        content = self.connection.RetrieveContent()
        children = content.rootFolder.childEntity
        # this will be needed for custom VM metadata

        if content.customFieldsManager:
            self.custom_fields = content.customFieldsManager.field
        else:
            self.custom_fields = []

        for child in children:
            if hasattr(child, "vmFolder"):
                datacenter = child
                vm_folder = datacenter.vmFolder
                vm_list = vm_folder.childEntity
                nodes.extend(self._to_nodes_recursive(vm_list))

        if enhance:
            nodes = self._enhance_metadata(nodes, content)

        return nodes

    def _enhance_metadata(self, nodes, content):
        nodemap = {}

        for node in nodes:
            node.extra["vSphere version"] = content.about.version
            nodemap[node.id] = node

        # Get VM deployment events to extract creation dates & images
        filter_spec = vim.event.EventFilterSpec(eventTypeId=["VmBeingDeployedEvent"])
        deploy_events = content.eventManager.QueryEvent(filter_spec)

        for event in deploy_events:
            try:
                uuid = event.vm.vm.config.instanceUuid
            except Exception:
                continue

            if uuid in nodemap:
                node = nodemap[uuid]
                try:  # Get source template as image
                    source_template_vm = event.srcTemplate.vm
                    image_id = source_template_vm.config.instanceUuid
                    node.extra["image_id"] = image_id
                except Exception:
                    logger.error("Cannot get instanceUuid " "from source template")
                try:  # Get creation date
                    node.created_at = event.createdTime
                except AttributeError:
                    logger.error("Cannot get creation date from VM " "deploy event")

        return nodes

    async def _to_nodes(self, vm_list):
        vms = []

        for vm in vm_list:
            if vm.get("config.template"):
                continue  # Do not include templates in node list
            vms.append(vm)
        loop = asyncio.get_event_loop()
        vms = [loop.run_in_executor(None, self._to_node, vms[i]) for i in range(len(vms))]

        return await asyncio.gather(*vms)

    def _to_nodes_recursive(self, vm_list):
        nodes = []

        for virtual_machine in vm_list:
            if hasattr(virtual_machine, "childEntity"):
                # If this is a group it will have children.
                # If it does, recurse into them and then return
                nodes.extend(self._to_nodes_recursive(virtual_machine.childEntity))
            elif isinstance(virtual_machine, vim.VirtualApp):
                # If this is a vApp, it likely contains child VMs
                # (vApps can nest vApps, but it is hardly
                # a common usecase, so ignore that)
                nodes.extend(self._to_nodes_recursive(virtual_machine.vm))
            else:
                if not hasattr(virtual_machine, "config") or (
                    virtual_machine.config and virtual_machine.config.template
                ):
                    continue  # Do not include templates in node list
                nodes.append(self._to_node_recursive(virtual_machine))

        return nodes

    def _to_node(self, vm):
        name = vm.get("summary.config.name")
        path = vm.get("summary.config.vmPathName")
        memory = vm.get("summary.config.memorySizeMB")
        cpus = vm.get("summary.config.numCpu")
        disk = vm.get("summary.storage.committed", 0) // (1024**3)
        id_to_hash = str(memory) + str(cpus) + str(disk)
        size_id = hashlib.md5(id_to_hash.encode("utf-8")).hexdigest()  # nosec
        size_name = name + "-size"
        size_extra = {"cpus": cpus}
        driver = self
        size = NodeSize(
            id=size_id,
            name=size_name,
            ram=memory,
            disk=disk,
            bandwidth=0,
            price=0,
            driver=driver,
            extra=size_extra,
        )
        operating_system = vm.get("summary.config.guestFullName")
        host = vm.get("summary.runtime.host")

        os_type = "unix"

        if "Microsoft" in str(operating_system):
            os_type = "windows"
        uuid = vm.get("summary.config.instanceUuid") or (
            vm.get("obj").config and vm.get("obj").config.instanceUuid
        )

        if not uuid:
            logger.error("No uuid for vm: {}".format(vm))
        annotation = vm.get("summary.config.annotation")
        state = vm.get("summary.runtime.powerState")
        status = self.NODE_STATE_MAP.get(state, NodeState.UNKNOWN)
        boot_time = vm.get("summary.runtime.bootTime")

        ip_addresses = []

        if vm.get("summary.guest.ipAddress"):
            ip_addresses.append(vm.get("summary.guest.ipAddress"))

        overall_status = str(vm.get("summary.overallStatus"))
        public_ips = []
        private_ips = []

        extra = {
            "path": path,
            "operating_system": operating_system,
            "os_type": os_type,
            "memory_MB": memory,
            "cpus": cpus,
            "overall_status": overall_status,
            "metadata": {},
            "snapshots": [],
        }

        if disk:
            extra["disk"] = disk

        if host:
            extra["host"] = host.name
            parent = host.parent

            while parent:
                if isinstance(parent, vim.ClusterComputeResource):
                    extra["cluster"] = parent.name

                    break
                parent = parent.parent

        if boot_time:
            extra["boot_time"] = boot_time.isoformat()

        if annotation:
            extra["annotation"] = annotation

        for ip_address in ip_addresses:
            try:
                if is_public_subnet(ip_address):
                    public_ips.append(ip_address)
                else:
                    private_ips.append(ip_address)
            except Exception:
                # IPV6 not supported
                pass

        if vm.get("snapshot"):
            extra["snapshots"] = format_snapshots(
                recurse_snapshots(vm.get("snapshot").rootSnapshotList)
            )

        for custom_field in vm.get("customValue", []):
            key_id = custom_field.key
            key = self.find_custom_field_key(key_id)
            extra["metadata"][key] = custom_field.value

        node = Node(
            id=uuid,
            name=name,
            state=status,
            size=size,
            public_ips=public_ips,
            private_ips=private_ips,
            driver=self,
            extra=extra,
        )
        node._uuid = uuid

        return node

    def _to_node_recursive(self, virtual_machine):
        summary = virtual_machine.summary
        name = summary.config.name
        path = summary.config.vmPathName
        memory = summary.config.memorySizeMB
        cpus = summary.config.numCpu
        disk = ""

        if summary.storage.committed:
            disk = summary.storage.committed / (1024**3)
        id_to_hash = str(memory) + str(cpus) + str(disk)
        size_id = hashlib.md5(id_to_hash.encode("utf-8")).hexdigest()  # nosec
        size_name = name + "-size"
        size_extra = {"cpus": cpus}
        driver = self
        size = NodeSize(
            id=size_id,
            name=size_name,
            ram=memory,
            disk=disk,
            bandwidth=0,
            price=0,
            driver=driver,
            extra=size_extra,
        )
        operating_system = summary.config.guestFullName
        host = summary.runtime.host

        # mist.io needs this metadata
        os_type = "unix"

        if "Microsoft" in str(operating_system):
            os_type = "windows"
        uuid = summary.config.instanceUuid
        annotation = summary.config.annotation
        state = summary.runtime.powerState
        status = self.NODE_STATE_MAP.get(state, NodeState.UNKNOWN)
        boot_time = summary.runtime.bootTime
        ip_addresses = []

        if summary.guest is not None:
            ip_addresses.append(summary.guest.ipAddress)

        overall_status = str(summary.overallStatus)
        public_ips = []
        private_ips = []

        extra = {
            "path": path,
            "operating_system": operating_system,
            "os_type": os_type,
            "memory_MB": memory,
            "cpus": cpus,
            "overallStatus": overall_status,
            "metadata": {},
            "snapshots": [],
        }

        if disk:
            extra["disk"] = disk

        if host:
            extra["host"] = host.name
            parent = host.parent

            while parent:
                if isinstance(parent, vim.ClusterComputeResource):
                    extra["cluster"] = parent.name

                    break
                parent = parent.parent

        if boot_time:
            extra["boot_time"] = boot_time.isoformat()

        if annotation:
            extra["annotation"] = annotation

        for ip_address in ip_addresses:
            try:
                if is_public_subnet(ip_address):
                    public_ips.append(ip_address)
                else:
                    private_ips.append(ip_address)
            except Exception:
                # IPV6 not supported
                pass

        if virtual_machine.snapshot:
            snapshots = [
                {
                    "id": s.id,
                    "name": s.name,
                    "description": s.description,
                    "created": s.createTime.strftime("%Y-%m-%d %H:%M"),
                    "state": s.state,
                }
                for s in virtual_machine.snapshot.rootSnapshotList
            ]
            extra["snapshots"] = snapshots

        for custom_field in virtual_machine.customValue:
            key_id = custom_field.key
            key = self.find_custom_field_key(key_id)
            extra["metadata"][key] = custom_field.value

        node = Node(
            id=uuid,
            name=name,
            state=status,
            size=size,
            public_ips=public_ips,
            private_ips=private_ips,
            driver=self,
            extra=extra,
        )
        node._uuid = uuid

        return node

    def reboot_node(self, node):
        """ """
        vm = self.find_by_uuid(node.id)

        return self.wait_for_task(vm.RebootGuest())

    def destroy_node(self, node):
        """ """
        vm = self.find_by_uuid(node.id)

        if node.state != NodeState.STOPPED:
            self.stop_node(node)

        return self.wait_for_task(vm.Destroy())

    def stop_node(self, node):
        """ """
        vm = self.find_by_uuid(node.id)

        return self.wait_for_task(vm.PowerOff())

    def start_node(self, node):
        """ """
        vm = self.find_by_uuid(node.id)

        return self.wait_for_task(vm.PowerOn())

    def ex_list_snapshots(self, node):
        """
        List node snapshots
        """
        vm = self.find_by_uuid(node.id)

        if not vm.snapshot:
            return []

        return format_snapshots(recurse_snapshots(vm.snapshot.rootSnapshotList))

    def ex_create_snapshot(
        self, node, snapshot_name, description="", dump_memory=False, quiesce=False
    ):
        """
        Create node snapshot
        """
        vm = self.find_by_uuid(node.id)

        return WaitForTask(vm.CreateSnapshot(snapshot_name, description, dump_memory, quiesce))

    def ex_remove_snapshot(self, node, snapshot_name=None, remove_children=True):
        """
        Remove a snapshot from node.
        If snapshot_name is not defined remove the last one.
        """
        vm = self.find_by_uuid(node.id)

        if not vm.snapshot:
            raise LibcloudError(
                "Remove snapshot failed. No snapshots for node %s" % node.name,
                driver=self,
            )
        snapshots = recurse_snapshots(vm.snapshot.rootSnapshotList)

        if not snapshot_name:
            snapshot = snapshots[-1].snapshot
        else:
            for s in snapshots:
                if snapshot_name == s.name:
                    snapshot = s.snapshot

                    break
            else:
                raise LibcloudError("Snapshot `%s` not found" % snapshot_name, driver=self)

        return self.wait_for_task(snapshot.RemoveSnapshot_Task(removeChildren=remove_children))

    def ex_revert_to_snapshot(self, node, snapshot_name=None):
        """
        Revert node to a specific snapshot.
        If snapshot_name is not defined revert to the last one.
        """
        vm = self.find_by_uuid(node.id)

        if not vm.snapshot:
            raise LibcloudError(
                "Revert failed. No snapshots " "for node %s" % node.name, driver=self
            )
        snapshots = recurse_snapshots(vm.snapshot.rootSnapshotList)

        if not snapshot_name:
            snapshot = snapshots[-1].snapshot
        else:
            for s in snapshots:
                if snapshot_name == s.name:
                    snapshot = s.snapshot

                    break
            else:
                raise LibcloudError("Snapshot `%s` not found" % snapshot_name, driver=self)

        return self.wait_for_task(snapshot.RevertToSnapshot_Task())

    def _find_template_by_uuid(self, template_uuid):
        # on version 5.5 and earlier search index won't return a VM
        try:
            template = self.find_by_uuid(template_uuid)
        except LibcloudError:
            content = self.connection.RetrieveContent()
            vms = content.viewManager.CreateContainerView(
                content.rootFolder, [vim.VirtualMachine], recursive=True
            ).view

            for vm in vms:
                if vm.config.instanceUuid == template_uuid:
                    template = vm
        except Exception as exc:
            raise LibcloudError("Error while searching for template: %s" % exc, driver=self)

        if not template:
            raise LibcloudError("Unable to locate VirtualMachine.", driver=self)

        return template

    def find_by_uuid(self, node_uuid):
        """Searches VMs for a given uuid
        returns pyVmomi.VmomiSupport.vim.VirtualMachine
        """
        vm = self.connection.content.searchIndex.FindByUuid(None, node_uuid, True, True)

        if not vm:
            # perhaps it is a moid
            vm = self._get_item_by_moid("VirtualMachine", node_uuid)

            if not vm:
                raise LibcloudError("Unable to locate VirtualMachine.", driver=self)

        return vm

    def find_custom_field_key(self, key_id):
        """Return custom field key name, provided it's id"""

        if not hasattr(self, "custom_fields"):
            content = self.connection.RetrieveContent()

            if content.customFieldsManager:
                self.custom_fields = content.customFieldsManager.field
            else:
                self.custom_fields = []

        for k in self.custom_fields:
            if k.key == key_id:
                return k.name

        return None

    def get_obj(self, vimtype, name):
        """
        Return an object by name, if name is None the
        first found object is returned
        """
        obj = None
        content = self.connection.RetrieveContent()
        container = content.viewManager.CreateContainerView(content.rootFolder, vimtype, True)

        for c in container.view:
            if name:
                if c.name == name:
                    obj = c

                    break
            else:
                obj = c

                break

        return obj

    def wait_for_task(self, task, timeout=1800, interval=10):
        """wait for a vCenter task to finish"""
        start_time = time.time()
        task_done = False

        while not task_done:
            if time.time() - start_time >= timeout:
                raise LibcloudError(
                    "Timeout while waiting " "for import task Id %s" % task.info.id,
                    driver=self,
                )

            if task.info.state == "success":
                if task.info.result and str(task.info.result) != "success":
                    return task.info.result

                return True

            if task.info.state == "error":
                raise LibcloudError(task.info.error.msg, driver=self)
            time.sleep(interval)

    def create_node(
        self,
        name,
        image,
        size,
        location=None,
        ex_cluster=None,
        ex_network=None,
        ex_datacenter=None,
        ex_folder=None,
        ex_resource_pool=None,
        ex_datastore_cluster=None,
        ex_datastore=None,
    ):
        """
        Creates and returns node.

        :keyword    ex_network: Name of a "Network" to connect the VM to ",
        :type       ex_network: ``str``

        """
        template = self._find_template_by_uuid(image.id)

        if ex_cluster:
            cluster_name = ex_cluster
        else:
            cluster_name = location.name
        cluster = self.get_obj([vim.ClusterComputeResource], cluster_name)

        if not cluster:  # It is a host go with it
            cluster = self.get_obj([vim.HostSystem], cluster_name)

        datacenter = None

        if not ex_datacenter:  # Get datacenter from cluster
            parent = cluster.parent

            while parent:
                if isinstance(parent, vim.Datacenter):
                    datacenter = parent

                    break
                parent = parent.parent

        if ex_datacenter or datacenter is None:
            datacenter = self.get_obj([vim.Datacenter], ex_datacenter)

        if ex_folder:
            folder = self.get_obj([vim.Folder], ex_folder)

            if folder is None:
                folder = self._get_item_by_moid("Folder", ex_folder)
        else:
            folder = datacenter.vmFolder

        if ex_resource_pool:
            resource_pool = self.get_obj([vim.ResourcePool], ex_resource_pool)
        else:
            try:
                resource_pool = cluster.resourcePool
            except AttributeError:
                resource_pool = cluster.parent.resourcePool
        devices = []
        vmconf = vim.vm.ConfigSpec(
            numCPUs=int(size.extra.get("cpu", 1)),
            memoryMB=int(size.ram),
            deviceChange=devices,
        )
        datastore = None
        pod = None
        podsel = vim.storageDrs.PodSelectionSpec()

        if ex_datastore_cluster:
            pod = self.get_obj([vim.StoragePod], ex_datastore_cluster)
        else:
            content = self.connection.RetrieveContent()
            pods = content.viewManager.CreateContainerView(
                content.rootFolder, [vim.StoragePod], True
            ).view

            for pod in pods:
                if cluster.name.lower() in pod.name:
                    break
        podsel.storagePod = pod
        storagespec = vim.storageDrs.StoragePlacementSpec()
        storagespec.podSelectionSpec = podsel
        storagespec.type = "create"
        storagespec.folder = folder
        storagespec.resourcePool = resource_pool
        storagespec.configSpec = vmconf

        try:
            content = self.connection.RetrieveContent()
            rec = content.storageResourceManager.RecommendDatastores(storageSpec=storagespec)
            rec_action = rec.recommendations[0].action[0]
            real_datastore_name = rec_action.destination.name
        except Exception:
            real_datastore_name = template.datastore[0].info.name

        datastore = self.get_obj([vim.Datastore], real_datastore_name)

        if ex_datastore:
            datastore = self.get_obj([vim.Datastore], ex_datastore)

            if datastore is None:
                datastore = self._get_item_by_moid("Datastore", ex_datastore)
        elif not datastore:
            datastore = self.get_obj([vim.Datastore], template.datastore[0].info.name)
        add_network = True

        if ex_network and len(template.network) > 0:
            for nets in template.network:
                if template in nets.vm:
                    add_network = False

        if ex_network and add_network:
            nicspec = vim.vm.device.VirtualDeviceSpec()
            nicspec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
            nicspec.device = vim.vm.device.VirtualVmxnet3()
            nicspec.device.wakeOnLanEnabled = True
            nicspec.device.deviceInfo = vim.Description()

            portgroup = self.get_obj([vim.dvs.DistributedVirtualPortgroup], ex_network)

            if portgroup:
                dvs_port_connection = vim.dvs.PortConnection()
                dvs_port_connection.portgroupKey = portgroup.key
                dvs_port_connection.switchUuid = portgroup.config.distributedVirtualSwitch.uuid
                nicspec.device.backing = (
                    vim.vm.device.VirtualEthernetCard.DistributedVirtualPortBackingInfo()
                )
                nicspec.device.backing.port = dvs_port_connection
            else:
                nicspec.device.backing = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()
                nicspec.device.backing.network = self.get_obj([vim.Network], ex_network)
                nicspec.device.backing.deviceName = ex_network
            nicspec.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
            nicspec.device.connectable.startConnected = True
            nicspec.device.connectable.connected = True
            nicspec.device.connectable.allowGuestControl = True
            devices.append(nicspec)

        # new_disk_kb = int(size.disk) * 1024 * 1024
        # disk_spec = vim.vm.device.VirtualDeviceSpec()
        # disk_spec.fileOperation = "create"
        # disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        # disk_spec.device = vim.vm.device.VirtualDisk()
        # disk_spec.device.backing = \
        #     vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
        # if size.extra.get('disk_type') == 'thin':
        #     disk_spec.device.backing.thinProvisioned = True
        # disk_spec.device.backing.diskMode = 'persistent'
        # disk_spec.device.capacityInKB = new_disk_kb
        # disk_spec.device.controllerKey = controller.key
        # devices.append(disk_spec)

        clonespec = vim.vm.CloneSpec(config=vmconf)
        relospec = vim.vm.RelocateSpec()
        relospec.datastore = datastore
        relospec.pool = resource_pool

        if location:
            host = self.get_obj([vim.HostSystem], location.name)

            if host:
                relospec.host = host
        clonespec.location = relospec
        clonespec.powerOn = True
        task = template.Clone(folder=folder, name=name, spec=clonespec)

        return self._to_node_recursive(self.wait_for_task(task))

    def ex_connect_network(self, vm, network_name):
        spec = vim.vm.ConfigSpec()

        # add Switch here
        dev_changes = []
        network_spec = vim.vm.device.VirtualDeviceSpec()
        # network_spec.fileOperation = "create"
        network_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        network_spec.device = vim.vm.device.VirtualVmxnet3()

        network_spec.device.backing = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()

        network_spec.device.backing.useAutoDetect = False
        network_spec.device.backing.network = self.get_obj([vim.Network], network_name)

        network_spec.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
        network_spec.device.connectable.startConnected = True
        network_spec.device.connectable.connected = True
        network_spec.device.connectable.allowGuestControl = True

        dev_changes.append(network_spec)

        spec.deviceChange = dev_changes
        output = vm.ReconfigVM_Task(spec=spec)
        print(output.info)

    def _get_item_by_moid(self, type_, moid):
        vm_obj = VmomiSupport.templateOf(type_)(moid, self.connection._stub)

        return vm_obj

    def ex_list_folders(self):
        content = self.connection.RetrieveContent()
        folders_raw = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.Folder], True
        ).view
        folders = []

        for folder in folders_raw:
            to_add = {"type": list(folder.childType)}
            to_add["name"] = folder.name
            to_add["id"] = folder._moId
            folders.append(to_add)

        return folders

    def ex_list_datastores(self):
        content = self.connection.RetrieveContent()
        datastores_raw = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.Datastore], True
        ).view
        datastores = []

        for dstore in datastores_raw:
            to_add = {"type": dstore.summary.type}
            to_add["name"] = dstore.name
            to_add["id"] = dstore._moId
            to_add["free_space"] = int(dstore.summary.freeSpace)
            to_add["capacity"] = int(dstore.summary.capacity)
            datastores.append(to_add)

        return datastores

    def ex_open_console(self, vm_uuid):
        vm = self.find_by_uuid(vm_uuid)
        ticket = vm.AcquireTicket(ticketType="webmks")

        return "wss://{}:{}/ticket/{}".format(ticket.host, ticket.port, ticket.ticket)

    def _get_version(self):
        content = self.connection.RetrieveContent()

        return content.about.version


class VSphereNetwork:
    """
    Represents information about a VPC (Virtual Private Cloud) network

    Note: This class is VSphere specific.
    """

    def __init__(self, id, name, extra=None):
        self.id = id
        self.name = name
        self.extra = extra or {}

    def __repr__(self):
        return ("<VSphereNetwork: id=%s, name=%s") % (self.id, self.name)


# 6.7
class VSphereResponse(JsonResponse):
    def parse_error(self):
        if self.body:
            message = self.body
            message += "-- code: {}".format(self.status)

            return message

        return self.body


class VSphereConnection(ConnectionKey):
    responseCls = VSphereResponse
    session_token = None

    def add_default_headers(self, headers):
        """
        VSphere needs an initial connection to a specific API endpoint to
        generate a session-token, which will be used for the purpose of
        authenticating for the rest of the session.
        """
        headers["Content-Type"] = "application/json"
        headers["Accept"] = "application/json"

        if self.session_token is None:
            to_encode = "{}:{}".format(self.key, self.secret)
            b64_user_pass = base64.b64encode(to_encode.encode())
            headers["Authorization"] = "Basic {}".format(b64_user_pass.decode())
        else:
            headers["vmware-api-session-id"] = self.session_token

        return headers


class VSphereException(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        self.args = (code, message)

    def __str__(self):
        return "{} {}".format(self.code, self.message)

    def __repr__(self):
        return "VSphereException {} {}".format(self.code, self.message)


class VSphere_REST_NodeDriver(NodeDriver):
    name = "VMware vSphere"
    website = "http://www.vmware.com/products/vsphere/"
    type = Provider.VSPHERE
    connectionCls = VSphereConnection
    session_token = None

    NODE_STATE_MAP = {
        "powered_on": NodeState.RUNNING,
        "powered_off": NodeState.STOPPED,
        "suspended": NodeState.SUSPENDED,
    }

    VALID_RESPONSE_CODES = [
        httplib.OK,
        httplib.ACCEPTED,
        httplib.CREATED,
        httplib.NO_CONTENT,
    ]

    def __init__(self, key, secret=None, secure=True, host=None, port=443, ca_cert=None):
        if not key or not secret:
            raise InvalidCredsError("Please provide both username " "(key) and password (secret).")
        super().__init__(key=key, secure=secure, host=host, port=port)
        prefixes = ["http://", "https://"]

        for prefix in prefixes:
            if host.startswith(prefix):
                host = host.lstrip(prefix)

        if ca_cert:
            self.connection.connection.ca_cert = ca_cert
        else:
            self.connection.connection.ca_cert = False

        self.connection.secret = secret
        self.host = host
        self.username = key
        # getting session token
        self._get_session_token()
        self.driver_soap = None

    def _get_soap_driver(self):
        if pyvmomi is None:
            raise ImportError(
                'Missing "pyvmomi" dependency. '
                "You can install it "
                "using pip - pip install pyvmomi"
            )
        self.driver_soap = VSphereNodeDriver(
            self.host,
            self.username,
            self.connection.secret,
            ca_cert=self.connection.connection.ca_cert,
        )

    def _get_session_token(self):
        uri = "/rest/com/vmware/cis/session"
        try:
            result = self.connection.request(uri, method="POST")
        except Exception:
            raise
        self.session_token = result.object["value"]
        self.connection.session_token = self.session_token

    def list_sizes(self):
        return []

    def list_nodes(
        self,
        ex_filter_power_states=None,
        ex_filter_folders=None,
        ex_filter_names=None,
        ex_filter_hosts=None,
        ex_filter_clusters=None,
        ex_filter_vms=None,
        ex_filter_datacenters=None,
        ex_filter_resource_pools=None,
        max_properties=20,
    ):
        """
        The ex parameters are search options and must be an array of strings,
        even ex_filter_power_states which can have at most two items but makes
        sense to keep only one ("POWERED_ON" or "POWERED_OFF")
        Keep in mind that this method will return up to 1000 nodes so if your
        network has more, do use the provided filters and call it multiple
        times.
        """

        req = "/rest/vcenter/vm"
        kwargs = {
            "filter.power_states": ex_filter_power_states,
            "filter.folders": ex_filter_folders,
            "filter.names": ex_filter_names,
            "filter.hosts": ex_filter_hosts,
            "filter.clusters": ex_filter_clusters,
            "filter.vms": ex_filter_vms,
            "filter.datacenters": ex_filter_datacenters,
            "filter.resource_pools": ex_filter_resource_pools,
        }
        params = {}

        for param, value in kwargs.items():
            if value:
                params[param] = value
        result = self._request(req, params=params).object["value"]
        vm_ids = [[item["vm"]] for item in result]
        vms = []
        interfaces = self._list_interfaces()

        for vm_id in vm_ids:
            vms.append(self._to_node(vm_id, interfaces))

        return vms

    def async_list_nodes(self):
        """
        In this case filtering is not possible.
        Use this method when the cloud has
        a lot of vms and you want to return them all.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(self._get_all_vms())
        vm_ids = [(item["vm"], item["host"]) for item in result]
        interfaces = self._list_interfaces()

        return loop.run_until_complete(self._list_nodes_async(vm_ids, interfaces))

    async def _list_nodes_async(self, vm_ids, interfaces):
        loop = asyncio.get_event_loop()
        vms = [
            loop.run_in_executor(None, self._to_node, vm_ids[i], interfaces)
            for i in range(len(vm_ids))
        ]

        return await asyncio.gather(*vms)

    async def _get_all_vms(self):
        """
        6.7 doesn't offer any pagination, if we get 1000 vms we will try
        this roundabout  way: First get all the datacenters, for each
        datacenter get the hosts and for each host the vms it has.
        This assumes that datacenters, hosts per datacenter and vms per
        host don't exceed 1000.
        """
        datacenters = self.ex_list_datacenters()
        loop = asyncio.get_event_loop()
        hosts_futures = [
            loop.run_in_executor(
                None,
                functools.partial(self.ex_list_hosts, ex_filter_datacenters=datacenter["id"]),
            )
            for datacenter in datacenters
        ]
        hosts = await asyncio.gather(*hosts_futures)

        vm_resp_futures = [
            loop.run_in_executor(None, functools.partial(self._get_vms_with_host, host))
            for host in itertools.chain(*hosts)
        ]

        vm_resp = await asyncio.gather(*vm_resp_futures)
        # return a flat list

        return [item for vm_list in vm_resp for item in vm_list]

    def _get_vms_with_host(self, host):
        req = "/rest/vcenter/vm"
        host_id = host["host"]
        response = self._request(req, params={"filter.hosts": host_id})
        vms = response.object["value"]

        for vm in vms:
            vm["host"] = host

        return vms

    def list_locations(self, ex_show_hosts_in_drs=True):
        """
        Location in the general sense means any resource that allows for node
        creation. In vSphere's case that usually is a host but if a cluster
        has rds enabled, a cluster can be assigned to create the VM, thus the
        clusters with rds enabled will be added to locations.

        :param ex_show_hosts_in_drs: A DRS cluster schedules automatically
                                     on which host should be placed thus it
                                     may not be desired to show the hosts
                                     in a DRS enabled cluster. Set False to
                                     not show these hosts.
        :type ex_show_hosts_in_drs:  `boolean`
        """
        clusters = self.ex_list_clusters()
        hosts_all = self.ex_list_hosts()
        hosts = []

        if ex_show_hosts_in_drs:
            hosts = hosts_all
        else:
            cluster_filter = [cluster["cluster"] for cluster in clusters]
            filter_hosts = self.ex_list_hosts(ex_filter_clusters=cluster_filter)
            hosts = [host for host in hosts_all if host not in filter_hosts]
        driver = self.connection.driver
        locations = []

        for cluster in clusters:
            if cluster["drs_enabled"]:
                extra = {"type": "cluster", "drs": True, "ha": cluster["ha_enabled"]}
                locations.append(
                    NodeLocation(
                        id=cluster["cluster"],
                        name=cluster["name"],
                        country="",
                        driver=driver,
                        extra=extra,
                    )
                )

        for host in hosts:
            extra = {
                "type": "host",
                "status": host["connection_state"],
                "state": host["power_state"],
            }

            locations.append(
                NodeLocation(
                    id=host["host"],
                    name=host["name"],
                    country="",
                    driver=driver,
                    extra=extra,
                )
            )

        return locations

    def stop_node(self, node):
        if node.state == NodeState.STOPPED:
            return True

        method = "POST"
        req = "/rest/vcenter/vm/{}/power/stop".format(node.id)

        result = self._request(req, method=method)

        return result.status in self.VALID_RESPONSE_CODES

    def start_node(self, node):
        if isinstance(node, str):
            node_id = node
        else:
            if node.state is NodeState.RUNNING:
                return True
            node_id = node.id
        method = "POST"
        req = "/rest/vcenter/vm/{}/power/start".format(node_id)
        result = self._request(req, method=method)

        return result.status in self.VALID_RESPONSE_CODES

    def reboot_node(self, node):
        if node.state is not NodeState.RUNNING:
            return False

        method = "POST"
        req = "/rest/vcenter/vm/{}/power/reset".format(node.id)
        result = self._request(req, method=method)

        return result.status in self.VALID_RESPONSE_CODES

    def destroy_node(self, node):
        # make sure the machine is stopped

        if node.state is not NodeState.STOPPED:
            self.stop_node(node)
        # wait to make sure it stopped
        # in the future this can be made asynchronously
        # for i in range(6):
        #     if node.state is NodeState.STOPPED:
        #         break
        #     time.sleep(10)
        req = "/rest/vcenter/vm/{}".format(node.id)
        resp = self._request(req, method="DELETE")

        return resp.status in self.VALID_RESPONSE_CODES

    def ex_suspend_node(self, node):
        if node.state is not NodeState.RUNNING:
            return False

        method = "POST"
        req = "/rest/vcenter/vm/{}/power/suspend".format(node.id)
        result = self._request(req, method=method)

        return result.status in self.VALID_RESPONSE_CODES

    def _list_interfaces(self):
        request = "/rest/appliance/networking/interfaces"
        response = self._request(request).object["value"]
        interfaces = [
            {
                "name": interface["name"],
                "mac": interface["mac"],
                "status": interface["status"],
                "ip": interface["ipv4"]["address"],
            }
            for interface in response
        ]

        return interfaces

    def _to_node(self, vm_id_host, interfaces):
        """
        id, name, state, public_ips, private_ips,
                driver, size=None, image=None, extra=None, created_at=None)
        """
        vm_id = vm_id_host[0]
        req = "/rest/vcenter/vm/" + vm_id
        vm = self._request(req).object["value"]
        name = vm["name"]
        state = self.NODE_STATE_MAP[vm["power_state"].lower()]

        # IP's
        private_ips = []
        nic_macs = set()

        for nic in vm["nics"]:
            nic_macs.add(nic["value"]["mac_address"])

        for interface in interfaces:
            if interface["mac"] in nic_macs:
                private_ips.append(interface["ip"])
                nic_macs.remove(interface["mac"])

                if len(nic_macs) == 0:
                    break
        public_ips = []  # should default_getaway be the public?

        driver = self.connection.driver

        # size
        total_size = 0
        gb_converter = 1024**3

        for disk in vm["disks"]:
            total_size += int(int(disk["value"]["capacity"] / gb_converter))
        ram = int(vm["memory"]["size_MiB"])
        cpus = int(vm["cpu"]["count"])
        id_to_hash = str(ram) + str(cpus) + str(total_size)
        size_id = hashlib.md5(id_to_hash.encode("utf-8")).hexdigest()  # nosec
        size_name = name + "-size"
        size_extra = {"cpus": cpus}
        size = NodeSize(
            id=size_id,
            name=size_name,
            ram=ram,
            disk=total_size,
            bandwidth=0,
            price=0,
            driver=driver,
            extra=size_extra,
        )

        # image
        image_name = vm["guest_OS"]
        image_id = image_name + "_id"
        image_extra = {"type": "guest_OS"}
        image = NodeImage(id=image_id, name=image_name, driver=driver, extra=image_extra)
        extra = {"snapshots": []}

        if len(vm_id_host) > 1:
            extra["host"] = vm_id_host[1].get("name", "")

        return Node(
            id=vm_id,
            name=name,
            state=state,
            public_ips=public_ips,
            private_ips=private_ips,
            driver=driver,
            size=size,
            image=image,
            extra=extra,
        )

    def ex_list_hosts(
        self,
        ex_filter_folders=None,
        ex_filter_standalone=None,
        ex_filter_hosts=None,
        ex_filter_clusters=None,
        ex_filter_names=None,
        ex_filter_datacenters=None,
        ex_filter_connection_states=None,
    ):
        kwargs = {
            "filter.folders": ex_filter_folders,
            "filter.names": ex_filter_names,
            "filter.hosts": ex_filter_hosts,
            "filter.clusters": ex_filter_clusters,
            "filter.standalone": ex_filter_standalone,
            "filter.datacenters": ex_filter_datacenters,
            "filter.connection_states": ex_filter_connection_states,
        }

        params = {}

        for param, value in kwargs.items():
            if value:
                params[param] = value
        req = "/rest/vcenter/host"
        result = self._request(req, params=params).object["value"]

        return result

    def ex_list_clusters(
        self,
        ex_filter_folders=None,
        ex_filter_names=None,
        ex_filter_datacenters=None,
        ex_filter_clusters=None,
    ):
        kwargs = {
            "filter.folders": ex_filter_folders,
            "filter.names": ex_filter_names,
            "filter.datacenters": ex_filter_datacenters,
            "filter.clusters": ex_filter_clusters,
        }
        params = {}

        for param, value in kwargs.items():
            if value:
                params[param] = value
        req = "/rest/vcenter/cluster"
        result = self._request(req, params=params).object["value"]

        return result

    def ex_list_datacenters(
        self, ex_filter_folders=None, ex_filter_names=None, ex_filter_datacenters=None
    ):
        req = "/rest/vcenter/datacenter"
        kwargs = {
            "filter.folders": ex_filter_folders,
            "filter.names": ex_filter_names,
            "filter.datacenters": ex_filter_datacenters,
        }
        params = {}

        for param, value in kwargs.items():
            if value:
                params[param] = value
        result = self._request(req, params=params)
        to_return = [
            {"name": item["name"], "id": item["datacenter"]} for item in result.object["value"]
        ]

        return to_return

    def ex_list_content_libraries(self):
        req = "/rest/com/vmware/content/library"
        try:
            result = self._request(req).object

            return result["value"]
        except BaseHTTPError:
            return []

    def ex_list_content_library_items(self, library_id):
        req = "/rest/com/vmware/content/library/item"
        params = {"library_id": library_id}
        try:
            result = self._request(req, params=params).object

            return result["value"]
        except BaseHTTPError:
            logger.error(
                "Library was cannot be accessed, "
                " most probably the VCenter service "
                "is stopped"
            )

            return []

    def ex_list_folders(self):
        req = "/rest/vcenter/folder"
        response = self._request(req).object
        folders = response["value"]

        for folder in folders:
            folder["id"] = folder["folder"]

        return folders

    def ex_list_datastores(
        self,
        ex_filter_folders=None,
        ex_filter_names=None,
        ex_filter_datacenters=None,
        ex_filter_types=None,
        ex_filter_datastores=None,
    ):
        req = "/rest/vcenter/datastore"
        kwargs = {
            "filter.folders": ex_filter_folders,
            "filter.names": ex_filter_names,
            "filter.datacenters": ex_filter_datacenters,
            "filter.types": ex_filter_types,
            "filter.datastores": ex_filter_datastores,
        }
        params = {}

        for param, value in kwargs.items():
            if value:
                params[param] = value
        result = self._request(req, params=params).object["value"]

        for datastore in result:
            datastore["id"] = datastore["datastore"]

        return result

    def ex_update_memory(self, node, ram):
        """
        :param ram: The amount of ram in MB.
        :type ram: `str` or `int`
        """

        if isinstance(node, str):
            node_id = node
        else:
            node_id = node.id
        request = "/rest/vcenter/vm/{}/hardware/memory".format(node_id)
        ram = int(ram)

        body = {"spec": {"size_MiB": ram}}
        response = self._request(request, method="PATCH", data=json.dumps(body))

        return response.status in self.VALID_RESPONSE_CODES

    def ex_update_cpu(self, node, cores):
        """
        Assuming 1 Core per socket
        :param cores: Integer or string indicating number of cores
        :type cores: `int` or `str`
        """

        if isinstance(node, str):
            node_id = node
        else:
            node_id = node.id
        request = "/rest/vcenter/vm/{}/hardware/cpu".format(node_id)
        cores = int(cores)
        body = {"spec": {"count": cores}}
        response = self._request(request, method="PATCH", data=json.dumps(body))

        return response.status in self.VALID_RESPONSE_CODES

    def ex_update_capacity(self, node, capacity):
        # Should be added when REST API supports it
        pass

    def ex_add_nic(self, node, network):
        """
        Creates a network adapter that will connect to the specified network
        for the given node. Returns a boolean indicating success or not.
        """

        if isinstance(node, str):
            node_id = node
        else:
            node_id = node.id
        spec = {}
        spec["mac_type"] = "GENERATED"
        spec["backing"] = {}
        spec["backing"]["type"] = "STANDARD_PORTGROUP"
        spec["backing"]["network"] = network
        spec["start_connected"] = True

        data = json.dumps({"spec": spec})
        req = "/rest/vcenter/vm/{}/hardware/ethernet".format(node_id)
        method = "POST"
        resp = self._request(req, method=method, data=data)

        return resp.status

    def _get_library_item(self, item_id):
        req = "/rest/com/vmware/content/library/item/id:{}".format(item_id)
        result = self._request(req).object

        return result["value"]

    def _get_resource_pool(self, host_id=None, cluster_id=None, name=None):
        pms = None

        if host_id:
            pms = {"filter.hosts": host_id}

        if cluster_id:
            pms = {"filter.clusters": cluster_id}

        if name:
            pms = {"filter.names": name}

        if not pms:
            raise ValueError("Either host_id, cluster_id or name must be provided")

        rp_request = "/rest/vcenter/resource-pool"
        resource_pool = self._request(rp_request, params=pms).object

        return resource_pool["value"][0]["resource_pool"]

    def _request(self, req, method="GET", params=None, data=None):
        try:
            result = self.connection.request(req, method=method, params=params, data=data)
        except BaseHTTPError as exc:
            if exc.code == 401:
                self.connection.session_token = None
                self._get_session_token()
                result = self.connection.request(req, method=method, params=params, data=data)
            else:
                raise
        except Exception:
            raise

        return result

    def list_images(self, **kwargs):
        libraries = self.ex_list_content_libraries()
        item_ids = []

        if libraries:
            for library in libraries:
                item_ids.extend(self.ex_list_content_library_items(library))
        items = []

        if item_ids:
            for item_id in item_ids:
                items.append(self._get_library_item(item_id))
        images = []
        names = set()

        if items:
            driver = self.connection.driver

            for item in items:
                names.add(item["name"])
                extra = {"type": item["type"]}

                if item["type"] == "vm-template":
                    capacity = item["size"] // (1024**3)
                    extra["disk_size"] = capacity
                images.append(
                    NodeImage(id=item["id"], name=item["name"], driver=driver, extra=extra)
                )

        if self.driver_soap is None:
            self._get_soap_driver()
        templates_in_hosts = self.driver_soap.list_images()

        for template in templates_in_hosts:
            if template.name not in names:
                images += [template]

        return images

    def ex_list_networks(self):
        request = "/rest/vcenter/network"
        response = self._request(request).object["value"]
        networks = []

        for network in response:
            networks.append(
                VSphereNetwork(
                    id=network["network"],
                    name=network["name"],
                    extra={"type": network["type"]},
                )
            )

        return networks

    def create_node(
        self,
        name,
        image,
        size=None,
        location=None,
        ex_datastore=None,
        ex_disks=None,
        ex_folder=None,
        ex_network=None,
        ex_turned_on=True,
    ):
        """
        Image can be either a vm template , a ovf template or just
        the guest OS.

        ex_folder is necessary if the image is a vm-template, this method
        will attempt to put the VM in a random folder and a warning about it
        will be issued in case the value remains `None`.
        """
        create_request = None
        data = {}

        # image is in the host then need the 6.5 driver

        if image.extra["type"] == "template_6_5":
            kwargs = {}
            kwargs["name"] = name
            kwargs["image"] = image
            kwargs["size"] = size
            kwargs["ex_network"] = ex_network
            kwargs["location"] = location

            for dstore in self.ex_list_datastores():
                if dstore["id"] == ex_datastore:
                    kwargs["ex_datastore"] = dstore["name"]

                    break
            kwargs["folder"] = ex_folder

            if self.driver_soap is None:
                self._get_soap_driver()
            result = self.driver_soap.create_node(**kwargs)

            return result

        # post creation checks
        create_nic = False
        update_memory = False
        update_cpu = False
        create_disk = False
        update_capacity = False

        if image.extra["type"] == "guest_OS":
            spec = {}
            spec["guest_OS"] = image.name
            spec["name"] = name
            spec["placement"] = {}

            if ex_folder is None:
                warn = (
                    "The API(6.7) requires the folder to be given, I will"
                    " place it into a random folder, after creation you "
                    "might find it convenient to move it into a better "
                    "folder."
                )
                warnings.warn(warn)
                folders = self.ex_list_folders()

                for folder in folders:
                    if folder["type"] == "VIRTUAL_MACHINE":
                        ex_folder = folder["folder"]

                if ex_folder is None:
                    msg = "No suitable folder for VMs found, please create one"
                    raise ProviderError(msg, 404)
            spec["placement"]["folder"] = ex_folder

            if location.extra["type"] == "host":
                spec["placement"]["host"] = location.id
            elif location.extra["type"] == "cluster":
                spec["placement"]["cluster"] = location.id
            elif location.extra["type"] == "resource_pool":
                spec["placement"]["resource_pool"] = location.id
            spec["placement"]["datastore"] = ex_datastore
            cpu = size.extra.get("cpu", 1)
            spec["cpu"] = {"count": cpu}
            spec["memory"] = {"size_MiB": size.ram}

            if size.disk:
                disk = {}
                disk["new_vmdk"] = {}
                disk["new_vmdk"]["capacity"] = size.disk * (1024**3)
                spec["disks"] = [disk]

            if ex_network:
                nic = {}
                nic["mac_type"] = "GENERATED"
                nic["backing"] = {}
                nic["backing"]["type"] = "STANDARD_PORTGROUP"
                nic["backing"]["network"] = ex_network
                nic["start_connected"] = True
                spec["nics"] = [nic]
            create_request = "/rest/vcenter/vm"
            data = json.dumps({"spec": spec})

        elif image.extra["type"] == "ovf":
            ovf_request = (
                "/rest/com/vmware/vcenter/ovf/library-item" "/id:{}?~action=filter".format(image.id)
            )
            spec = {}
            spec["target"] = {}

            if location.extra.get("type") == "resource-pool":
                spec["target"]["resource_pool_id"] = location.id
            elif location.extra.get("type") == "host":
                resource_pool = self._get_resource_pool(host_id=location.id)

                if not resource_pool:
                    msg = (
                        "Could not find resource-pool for given location "
                        "(host). Please make sure the location is valid."
                    )
                    raise VSphereException(code="504", message=msg)
                spec["target"]["resource_pool_id"] = resource_pool
                spec["target"]["host_id"] = location.id
            elif location.extra.get("type") == "cluster":
                resource_pool = self._get_resource_pool(cluster_id=location.id)

                if not resource_pool:
                    msg = (
                        "Could not find resource-pool for given location "
                        "(cluster). Please make sure the location "
                        "is valid."
                    )
                    raise VSphereException(code="504", message=msg)
                spec["target"]["resource_pool_id"] = resource_pool
            ovf = self._request(ovf_request, method="POST", data=json.dumps(spec)).object["value"]
            spec["deployment_spec"] = {}
            spec["deployment_spec"]["name"] = name
            # assuming that since you want to make a vm you don't need reminder
            spec["deployment_spec"]["accept_all_EULA"] = True
            # network

            if ex_network and ovf["networks"]:
                spec["deployment_spec"]["network_mappings"] = [
                    {"key": ovf["networks"][0], "value": ex_network}
                ]
            elif not ovf["networks"] and ex_network:
                create_nic = True
            # storage

            if ex_datastore:
                spec["deployment_spec"]["storage_mappings"] = []
                store_map = {"type": "DATASTORE", "datastore_id": ex_datastore}
                spec["deployment_spec"]["storage_mappings"].append(store_map)

            if size and size.ram:
                update_memory = True

            if size and size.extra and size.extra.get("cpu"):
                update_cpu = True

            if size and size.disk:
                # TODO Should update capacity but it is not possible with 6.7
                pass

            if ex_disks:
                create_disk = True

            create_request = (
                "/rest/com/vmware/vcenter/ovf/library-item" "/id:{}?~action=deploy".format(image.id)
            )
            data = json.dumps(
                {"target": spec["target"], "deployment_spec": spec["deployment_spec"]}
            )

        elif image.extra["type"] == "vm-template":
            tp_request = "/rest/vcenter/vm-template/library-items/" + image.id
            template = self._request(tp_request).object["value"]
            spec = {}
            spec["name"] = name

            # storage

            if ex_datastore:
                spec["disk_storage"] = {}
                spec["disk_storage"]["datastore"] = ex_datastore

            # location :: folder,resource group, datacenter, host
            spec["placement"] = {}

            if not ex_folder:
                warn = (
                    "The API(6.7) requires the folder to be given, I will"
                    " place it into a random folder, after creation you "
                    "might find it convenient to move it into a better "
                    "folder."
                )
                warnings.warn(warn)
                folders = self.ex_list_folders()

                for folder in folders:
                    if folder["type"] == "VIRTUAL_MACHINE":
                        ex_folder = folder["folder"]

                if ex_folder is None:
                    msg = "No suitable folder for VMs found, please create one"
                    raise ProviderError(msg, 404)
            spec["placement"]["folder"] = ex_folder

            if location.extra["type"] == "host":
                spec["placement"]["host"] = location.id
            elif location.extra["type"] == "cluster":
                spec["placement"]["cluster"] = location.id
            # network changes the network to existing nics if
            # there are no adapters
            # in the template then we will make on in the vm
            # after the creation finishes
            # only one network atm??
            spec["hardware_customization"] = {}

            if ex_network:
                nics = template["nics"]

                if len(nics) > 0:
                    nic = nics[0]
                    spec["hardware_customization"]["nics"] = [
                        {"key": nic["key"], "value": {"network": ex_network}}
                    ]
                else:
                    create_nic = True
            spec["powered_on"] = False
            # hardware

            if size:
                if size.ram:
                    spec["hardware_customization"]["memory_update"] = {"memory": int(size.ram)}

                if size.extra.get("cpu"):
                    spec["hardware_customization"]["cpu_update"] = {"num_cpus": size.extra["cpu"]}

                if size.disk:
                    if not len(template["disks"]) > 0:
                        create_disk = True
                    else:
                        capacity = size.disk * 1024 * 1024 * 1024
                        dsk = template["disks"][0]["key"]

                        if template["disks"][0]["value"]["capacity"] < capacity:
                            update = {"capacity": capacity}
                            spec["hardware_customization"]["disks_to_update"] = [
                                {"key": dsk, "value": update}
                            ]

            create_request = "/rest/vcenter/vm-template/library-items/" "{}/?action=deploy".format(
                image.id
            )
            data = json.dumps({"spec": spec})

        if not create_request:
            raise ValueError("Missing create_request")

        # deploy the node
        result = self._request(create_request, method="POST", data=data)
        # wait until the node is up and then add extra config
        node_id = result.object["value"]

        if image.extra["type"] == "ovf":
            node_id = node_id["resource_id"]["id"]

        node = self.list_nodes(ex_filter_vms=node_id)[0]

        if create_nic:
            self.ex_add_nic(node, ex_network)

        if update_memory:
            self.ex_update_memory(node, size.ram)

        if update_cpu:
            self.ex_update_cpu(node, size.extra["cpu"])

        if create_disk:
            pass  # until volumes are added

        if update_capacity:
            pass  # until API method is added

        if ex_turned_on:
            self.start_node(node)

        return node

    # TODO As soon as snapshot support gets added to the REST api
    # these methods should be rewritten with REST api calls
    def ex_list_snapshots(self, node):
        """
        List node snapshots
        """

        if self.driver_soap is None:
            self._get_soap_driver()

        return self.driver_soap.ex_list_snapshots(node)

    def ex_create_snapshot(
        self, node, snapshot_name, description="", dump_memory=False, quiesce=False
    ):
        """
        Create node snapshot
        """

        if self.driver_soap is None:
            self._get_soap_driver()

        return self.driver_soap.ex_create_snapshot(
            node,
            snapshot_name,
            description=description,
            dump_memory=dump_memory,
            quiesce=False,
        )

    def ex_remove_snapshot(self, node, snapshot_name=None, remove_children=True):
        """
        Remove a snapshot from node.
        If snapshot_name is not defined remove the last one.
        """

        if self.driver_soap is None:
            self._get_soap_driver()

        return self.driver_soap.ex_remove_snapshot(
            node, snapshot_name=snapshot_name, remove_children=remove_children
        )

    def ex_revert_to_snapshot(self, node, snapshot_name=None):
        """
        Revert node to a specific snapshot.
        If snapshot_name is not defined revert to the last one.
        """

        if self.driver_soap is None:
            self._get_soap_driver()

        return self.driver_soap.ex_revert_to_snapshot(node, snapshot_name=snapshot_name)

    def ex_open_console(self, vm_id):
        if self.driver_soap is None:
            self._get_soap_driver()

        return self.driver_soap.ex_open_console(vm_id)
