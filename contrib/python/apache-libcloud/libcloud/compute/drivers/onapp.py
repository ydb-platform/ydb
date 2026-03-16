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

from libcloud.utils.py3 import httplib
from libcloud.common.onapp import OnAppConnection
from libcloud.compute.base import Node, KeyPair, NodeImage, NodeDriver
from libcloud.utils.networking import is_private_subnet
from libcloud.compute.providers import Provider

__all__ = ["OnAppNodeDriver"]

"""
Define the extra dictionary for specific resources
"""
RESOURCE_EXTRA_ATTRIBUTES_MAP = {
    "node": {
        "add_to_marketplace": {
            "key_name": "add_to_marketplace",
            "transform_func": bool,
        },
        "admin_note": {"key_name": "admin_note", "transform_func": str},
        "allow_resize_without_reboot": {
            "key_name": "allow_resize_without_reboot",
            "transform_func": bool,
        },
        "allowed_hot_migrate": {
            "key_name": "allowed_hot_migrate",
            "transform_func": bool,
        },
        "allowed_swap": {"key_name": "allowed_swap", "transform_func": bool},
        "booted": {"key_name": "booted", "transform_func": bool},
        "built": {"key_name": "built", "transform_func": bool},
        "cpu_priority": {"key_name": "cpu_priority", "transform_func": int},
        "cpu_shares": {"key_name": "cpu_shares", "transform_func": int},
        "cpu_sockets": {"key_name": "cpu_sockets", "transform_func": int},
        "cpu_threads": {"key_name": "cpu_threads", "transform_func": int},
        "cpu_units": {"key_name": "cpu_units", "transform_func": int},
        "cpus": {"key_name": "cpus", "transform_func": int},
        "created_at": {"key_name": "created_at", "transform_func": str},
        "customer_network_id": {
            "key_name": "customer_network_id",
            "transform_func": str,
        },
        "deleted_at": {"key_name": "deleted_at", "transform_func": str},
        "edge_server_type": {"key_name": "edge_server_type", "transform_func": str},
        "enable_autoscale": {"key_name": "enable_autoscale", "transform_func": bool},
        "enable_monitis": {"key_name": "enable_monitis", "transform_func": bool},
        "firewall_notrack": {"key_name": "firewall_notrack", "transform_func": bool},
        "hostname": {"key_name": "hostname", "transform_func": str},
        "hypervisor_id": {"key_name": "hypervisor_id", "transform_func": int},
        "id": {"key_name": "id", "transform_func": int},
        "initial_root_password": {
            "key_name": "initial_root_password",
            "transform_func": str,
        },
        "initial_root_password_encrypted": {
            "key_name": "initial_root_password_encrypted",
            "transform_func": bool,
        },
        "local_remote_access_ip_address": {
            "key_name": "local_remote_access_ip_address",
            "transform_func": str,
        },
        "local_remote_access_port": {
            "key_name": "local_remote_access_port",
            "transform_func": int,
        },
        "locked": {"key_name": "locked", "transform_func": bool},
        "memory": {"key_name": "memory", "transform_func": int},
        "min_disk_size": {"key_name": "min_disk_size", "transform_func": int},
        "monthly_bandwidth_used": {
            "key_name": "monthly_bandwidth_used",
            "transform_func": int,
        },
        "note": {"key_name": "note", "transform_func": str},
        "operating_system": {"key_name": "operating_system", "transform_func": str},
        "operating_system_distro": {
            "key_name": "operating_system_distro",
            "transform_func": str,
        },
        "preferred_hvs": {"key_name": "preferred_hvs", "transform_func": list},
        "price_per_hour": {"key_name": "price_per_hour", "transform_func": float},
        "price_per_hour_powered_off": {
            "key_name": "price_per_hour_powered_off",
            "transform_func": float,
        },
        "recovery_mode": {"key_name": "recovery_mode", "transform_func": bool},
        "remote_access_password": {
            "key_name": "remote_access_password",
            "transform_func": str,
        },
        "service_password": {"key_name": "service_password", "transform_func": str},
        "state": {"key_name": "state", "transform_func": str},
        "storage_server_type": {
            "key_name": "storage_server_type",
            "transform_func": str,
        },
        "strict_virtual_machine_id": {
            "key_name": "strict_virtual_machine_id",
            "transform_func": str,
        },
        "support_incremental_backups": {
            "key_name": "support_incremental_backups",
            "transform_func": bool,
        },
        "suspended": {"key_name": "suspended", "transform_func": bool},
        "template_id": {"key_name": "template_id", "transform_func": int},
        "template_label": {"key_name": "template_label", "transform_func": str},
        "total_disk_size": {"key_name": "total_disk_size", "transform_func": int},
        "updated_at": {"key_name": "updated_at", "transform_func": str},
        "user_id": {"key_name": "user_id", "transform_func": int},
        "vip": {"key_name": "vip", "transform_func": bool},
        "xen_id": {"key_name": "xen_id", "transform_func": int},
    }
}


class OnAppNodeDriver(NodeDriver):
    """
    Base OnApp node driver.
    """

    connectionCls = OnAppConnection
    type = Provider.ONAPP
    name = "OnApp"
    website = "http://onapp.com/"

    def create_node(
        self,
        name,
        ex_memory,
        ex_cpus,
        ex_cpu_shares,
        ex_hostname,
        ex_template_id,
        ex_primary_disk_size,
        ex_swap_disk_size,
        ex_required_virtual_machine_build=1,
        ex_required_ip_address_assignment=1,
        **kwargs,
    ):
        """
        Add a VS

        :param  kwargs: All keyword arguments to create a VS
        :type   kwargs: ``dict``

        :rtype: :class:`OnAppNode`
        """
        server_params = dict(
            label=name,
            memory=ex_memory,
            cpus=ex_cpus,
            cpu_shares=ex_cpu_shares,
            hostname=ex_hostname,
            template_id=ex_template_id,
            primary_disk_size=ex_primary_disk_size,
            swap_disk_size=ex_swap_disk_size,
            required_virtual_machine_build=ex_required_virtual_machine_build,
            required_ip_address_assignment=ex_required_ip_address_assignment,
            rate_limit=kwargs.get("rate_limit"),
        )

        server_params.update(OnAppNodeDriver._create_args_to_params(**kwargs))
        data = json.dumps({"virtual_machine": server_params})

        response = self.connection.request(
            "/virtual_machines.json",
            data=data,
            headers={"Content-type": "application/json"},
            method="POST",
        )

        return self._to_node(response.object["virtual_machine"])

    def destroy_node(self, node, ex_convert_last_backup=0, ex_destroy_all_backups=0):
        """
        Delete a VS

        :param node: OnApp node
        :type  node: :class: `OnAppNode`

        :param convert_last_backup: set 1 to convert the last VS's backup to
                                    template, otherwise set 0
        :type  convert_last_backup: ``int``

        :param destroy_all_backups: set 1 to destroy all existing backups of
                                    this VS, otherwise set 0
        :type  destroy_all_backups: ``int``
        """
        server_params = {
            "convert_last_backup": ex_convert_last_backup,
            "destroy_all_backups": ex_destroy_all_backups,
        }
        action = "/virtual_machines/{identifier}.json".format(identifier=node.id)

        self.connection.request(action, params=server_params, method="DELETE")
        return True

    def list_nodes(self):
        """
        List all VS

        :rtype: ``list`` of :class:`OnAppNode`
        """
        response = self.connection.request("/virtual_machines.json")
        nodes = []
        for vm in response.object:
            nodes.append(self._to_node(vm["virtual_machine"]))
        return nodes

    def list_images(self):
        """
        List all images

        :rtype: ``list`` of :class:`NodeImage`
        """
        response = self.connection.request("/templates.json")
        templates = []
        for template in response.object:
            templates.append(self._to_image(template["image_template"]))
        return templates

    def list_key_pairs(self):
        """
        List all the available key pair objects.

        :rtype: ``list`` of :class:`.KeyPair` objects
        """
        user_id = self.connection.request("/profile.json").object["user"]["id"]
        response = self.connection.request("/users/%s/ssh_keys.json" % user_id)
        ssh_keys = []
        for ssh_key in response.object:
            ssh_keys.append(self._to_key_pair(ssh_key["ssh_key"]))
        return ssh_keys

    def get_key_pair(self, name):
        """
        Retrieve a single key pair.

        :param name: ID of the key pair to retrieve.
        :type name: ``str``

        :rtype: :class:`.KeyPair` object
        """
        user_id = self.connection.request("/profile.json").object["user"]["id"]
        response = self.connection.request("/users/{}/ssh_keys/{}.json".format(user_id, name))
        return self._to_key_pair(response.object["ssh_key"])

    def import_key_pair_from_string(self, name, key_material):
        """
        Import a new public key from string.

        :param name: Key pair name (unused).
        :type name: ``str``

        :param key_material: Public key material.
        :type key_material: ``str``

        :rtype: :class:`.KeyPair` object
        """
        data = json.dumps({"key": key_material})
        user_id = self.connection.request("/profile.json").object["user"]["id"]
        response = self.connection.request(
            "/users/%s/ssh_keys.json" % user_id,
            data=data,
            headers={"Content-type": "application/json"},
            method="POST",
        )
        return self._to_key_pair(response.object["ssh_key"])

    def delete_key_pair(self, key):
        """
        Delete an existing key pair.

        :param key_pair: Key pair object.
        :type key_pair: :class:`.KeyPair`

        :return: True on success
        :rtype: ``bool``
        """
        key_id = key.name
        response = self.connection.request("/settings/ssh_keys/%s.json" % key_id, method="DELETE")
        return response.status == httplib.NO_CONTENT

    #
    # Helper methods
    #

    def _to_key_pair(self, data):
        extra = {"created_at": data["created_at"], "updated_at": data["updated_at"]}
        return KeyPair(
            name=data["id"],
            fingerprint=None,
            public_key=data["key"],
            private_key=None,
            driver=self,
            extra=extra,
        )

    def _to_image(self, template):
        extra = {
            "distribution": template["operating_system_distro"],
            "operating_system": template["operating_system"],
            "operating_system_arch": template["operating_system_arch"],
            "allow_resize_without_reboot": template["allow_resize_without_reboot"],
            "allowed_hot_migrate": template["allowed_hot_migrate"],
            "allowed_swap": template["allowed_swap"],
            "min_disk_size": template["min_disk_size"],
            "min_memory_size": template["min_memory_size"],
            "created_at": template["created_at"],
        }
        return NodeImage(id=template["id"], name=template["label"], driver=self, extra=extra)

    def _to_node(self, data):
        identifier = data["identifier"]
        name = data["label"]
        private_ips = []
        public_ips = []
        for ip in data["ip_addresses"]:
            address = ip["ip_address"]["address"]
            if is_private_subnet(address):
                private_ips.append(address)
            else:
                public_ips.append(address)

        extra = OnAppNodeDriver._get_extra_dict(data, RESOURCE_EXTRA_ATTRIBUTES_MAP["node"])
        return Node(identifier, name, extra["state"], public_ips, private_ips, self, extra=extra)

    @staticmethod
    def _get_extra_dict(response, mapping):
        """
        Extract attributes from the element based on rules provided in the
        mapping dictionary.

        :param   response: The JSON response to parse the values from.
        :type    response: ``dict``

        :param   mapping: Dictionary with the extra layout
        :type    mapping: ``dict``

        :rtype:  ``dict``
        """
        extra = {}
        for attribute, values in mapping.items():
            transform_func = values["transform_func"]
            value = response.get(values["key_name"])

            extra[attribute] = transform_func(value) if value else None
        return extra

    @staticmethod
    def _create_args_to_params(**kwargs):
        """
        Extract server params from keyword args to create a VS

        :param   kwargs: keyword args
        :return: ``dict``
        """
        params = [
            "ex_cpu_sockets",
            "ex_cpu_threads",
            "ex_enable_autoscale",
            "ex_data_store_group_primary_id",
            "ex_data_store_group_swap_id",
            "ex_hypervisor_group_id",
            "ex_hypervisor_id",
            "ex_initial_root_password",
            "ex_note",
            "ex_primary_disk_min_iops",
            "ex_primary_network_id",
            "ex_primary_network_group_id",
            "ex_recipe_ids",
            "ex_required_automatic_backup",
            "ex_required_virtual_machine_startup",
            "ex_required_virtual_machine_startup",
            "ex_selected_ip_address_id",
            "ex_swap_disk_min_iops",
            "ex_type_of_format",
            "ex_custom_recipe_variables",
            "ex_licensing_key",
            "ex_licensing_server_id",
            "ex_licensing_type",
        ]
        server_params = {}

        for p in params:
            value = kwargs.get(p)
            if value:
                server_params[p[3:]] = value
        return server_params
