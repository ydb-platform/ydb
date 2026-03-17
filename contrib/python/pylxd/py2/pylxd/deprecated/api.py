
# Copyright (c) 2015 Canonical Ltd
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
import warnings

from pylxd.deprecated import certificate
from pylxd.deprecated import connection
from pylxd.deprecated import container
from pylxd.deprecated import hosts
from pylxd.deprecated import image
from pylxd.deprecated import network
from pylxd.deprecated import operation
from pylxd.deprecated import profiles


class API(object):

    def __init__(self, host=None, port=8443):
        warnings.warn(
            "pylxd.api.API is deprecated. Please use pylxd.Client.",
            DeprecationWarning)
        conn = self.connection = connection.LXDConnection(host=host, port=port)
        self.hosts = hosts.LXDHost(conn)
        self.image = image.LXDImage(conn)
        self.alias = image.LXDAlias(conn)
        self.network = network.LXDNetwork(conn)
        self.operation = operation.LXDOperation(conn)
        self.profiles = profiles.LXDProfile(conn)
        self.certificate = certificate.LXDCertificate(conn)
        self.container = container.LXDContainer(conn)

    # host
    def host_ping(self):
        return self.hosts.host_ping()

    def host_info(self):
        return self.hosts.host_info()

    def get_lxd_api_compat(self, data=None):
        return self.hosts.get_lxd_api_compat(data)

    def get_lxd_host_trust(self, data=None):
        return self.hosts.get_lxd_host_trust(data)

    def get_lxd_backing_fs(self, data=None):
        return self.hosts.get_lxd_backing_fs(data)

    def get_lxd_driver(self, data=None):
        return self.hosts.get_lxd_driver(data)

    def get_lxc_version(self, data=None):
        return self.hosts.get_lxc_version(data)

    def get_lxd_version(self, data=None):
        return self.hosts.get_lxd_version(data)

    def get_kernel_version(self, data=None):
        return self.hosts.get_kernel_version(data)

    def get_host_certificate(self):
        return self.hosts.get_certificate()

    def host_config(self):
        return self.hosts.host_config()

    # images
    def image_list(self):
        return self.image.image_list()

    def image_defined(self, image):
        return self.image.image_defined(image)

    def image_search(self, params):
        return self.image.image_list_by_key(params)

    def image_info(self, image):
        return self.image.image_info(image)

    def image_upload_date(self, image, data=None):
        return self.image.get_image_date(image, data, 'uploaded_at')

    def image_create_date(self, image, data=None):
        return self.image.get_image_date(image, data, 'created_at')

    def image_expire_date(self, image, data=None):
        return self.image.get_image_date(image, data, 'expires_at')

    def image_upload(self, path=None, data=None, headers={}):
        return self.image.image_upload(path=path, data=data, headers=headers)

    def image_delete(self, image):
        return self.image.image_delete(image)

    def image_export(self, image):
        return self.image.image_export(image)

    def image_update(self, image, data):
        return self.image.image_update(image, data)

    def image_rename(self, image, data):
        return self.image.image_rename(image, data)

    # alias
    def alias_list(self):
        return self.alias.alias_list()

    def alias_defined(self, alias):
        return self.alias.alias_defined(alias)

    def alias_create(self, data):
        return self.alias.alias_create(data)

    def alias_update(self, alias, data):
        return self.alias.alias_update(alias, data)

    def alias_show(self, alias):
        return self.alias.alias_show(alias)

    def alias_rename(self, alias, data):
        return self.alias.alias_rename(alias, data)

    def alias_delete(self, alias):
        return self.alias.alias_delete(alias)

    # containers:
    def container_list(self):
        return self.container.container_list()

    def container_defined(self, container):
        return self.container.container_defined(container)

    def container_running(self, container):
        return self.container.container_running(container)

    def container_init(self, container):
        return self.container.container_init(container)

    def container_update(self, container, config):
        return self.container.container_update(container, config)

    def container_state(self, container):
        return self.container.container_state(container)

    def container_start(self, container, timeout):
        return self.container.container_start(container, timeout)

    def container_stop(self, container, timeout):
        return self.container.container_stop(container, timeout)

    def container_suspend(self, container, timeout):
        return self.container.container_suspend(container, timeout)

    def container_resume(self, container, timeout):
        return self.container.container_resume(container, timeout)

    def container_reboot(self, container, timeout):
        return self.container.container_reboot(container, timeout)

    def container_destroy(self, container):
        return self.container.container_destroy(container)

    def get_container_log(self, container):
        return self.container.get_container_log(container)

    def get_container_config(self, container):
        return self.container.get_container_config(container)

    def get_container_websocket(self, container):
        return self.container.get_container_websocket(container)

    def container_info(self, container):
        return self.container.container_info(container)

    def container_local_copy(self, container):
        return self.container.container_local_copy(container)

    def container_local_move(self, instance, container):
        return self.container.container_local_move(instance, container)

    # file operations
    def get_container_file(self, container, filename):
        return self.container.get_container_file(container, filename)

    def container_publish(self, container):
        return self.container.container_publish(container)

    def put_container_file(self, container, src_file,
                           dst_file, uid=0, gid=0, mode=0o644):
        return self.container.put_container_file(
            container, src_file, dst_file, uid, gid, mode)

    # snapshots
    def container_snapshot_list(self, container):
        return self.container.snapshot_list(container)

    def container_snapshot_create(self, container, config):
        return self.container.snapshot_create(container, config)

    def container_snapshot_info(self, container, snapshot):
        return self.container.snapshot_info(container, snapshot)

    def container_snapshot_rename(self, container, snapshot, config):
        return self.container.snapshot_rename(container, snapshot, config)

    def container_snapshot_delete(self, container, snapshot):
        return self.container.snapshot_delete(container, snapshot)

    def container_migrate(self, container):
        return self.container.container_migrate(container)

    def container_migrate_sync(self, operation_id, container_secret):
        return self.container.container_migrate_sync(
            operation_id, container_secret)

    # misc container
    def container_run_command(self, container, args, interactive=False,
                              web_sockets=False, env=None):
        return self.container.run_command(container, args, interactive,
                                          web_sockets, env)

    # certificates
    def certificate_list(self):
        return self.certificate.certificate_list()

    def certificate_show(self, fingerprint):
        return self.certificate.certificate_show(fingerprint)

    def certificate_delete(self, fingerprint):
        return self.certificate.certificate_delete(fingerprint)

    def certificate_create(self, fingerprint):
        return self.certificate.certificate_create(fingerprint)

    # profiles
    def profile_create(self, profile):
        '''Create LXD profile'''
        return self.profiles.profile_create(profile)

    def profile_show(self, profile):
        '''Show LXD profile'''
        return self.profiles.profile_show(profile)

    def profile_defined(self, profile):
        '''Check to see if profile is defined'''
        return self.profiles.profile_defined(profile)

    def profile_list(self):
        '''List LXD profiles'''
        return self.profiles.profile_list()

    def profile_update(self, profile, config):
        '''Update LXD profile'''
        return self.profiles.profile_update(profile, config)

    def profile_rename(self, profile, config):
        '''Rename LXD profile'''
        return self.profiles.profile_rename(profile, config)

    def profile_delete(self, profile):
        '''Delete LXD profile'''
        return self.profiles.profile_delete(profile)

    # lxd operations
    def list_operations(self):
        return self.operation.operation_list()

    def wait_container_operation(self, operation, status_code, timeout):
        return self.operation.operation_wait(operation, status_code, timeout)

    def operation_delete(self, operation):
        return self.operation.operation_delete(operation)

    def operation_info(self, operation):
        return self.operation.operation_info(operation)

    def operation_show_create_time(self, operation, data=None):
        return self.operation.operation_create_time(operation, data)

    def operation_show_update_time(self, operation, data=None):
        return self.operation.operation_update_time(operation, data)

    def operation_show_status(self, operation, data=None):
        return self.operation.operation_status_code(operation, data)

    def operation_stream(self, operation, operation_secret):
        return self.operation.operation_stream(operation, operation_secret)

    # networks
    def network_list(self):
        return self.network.network_list()

    def network_show(self, network):
        return self.network.network_show(network)

    def network_show_name(self, network, data=None):
        return self.network.show_network_name(network, data)

    def network_show_type(self, network, data=None):
        return self.network.show_network_type(network, data)

    def network_show_members(self, network, data=None):
        return self.network.show_network_members(network, data)
