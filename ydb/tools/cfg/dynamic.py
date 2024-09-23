#!/usr/bin/env python
# -*- coding: utf-8 -*-
import itertools
import os

from ydb.core.protos import blobstorage_config_pb2 as bs_config
from ydb.core.protos import blobstorage_pdisk_config_pb2 as pdisk_config
from ydb.core.protos import flat_scheme_op_pb2 as flat_scheme_op
from ydb.core.protos import msgbus_pb2 as msgbus
from ydb.core.protos import tx_proxy_pb2 as tx_proxy
from ydb.tools.cfg import base, static, utils
from ydb.tools.cfg.types import DistinctionLevels, Erasure, FailDomainType, PDiskCategory


class DynamicConfigGenerator(object):
    def __init__(
        self,
        template,
        binary_path,
        output_dir,
        grpc_endpoint=None,
        local_binary_path=None,
        walle_provider=None,
        **kwargs
    ):
        self._template = template
        self._binary_path = binary_path
        self._local_binary_path = local_binary_path or binary_path
        self._output_dir = output_dir
        self._walle_provider = walle_provider
        self._cluster_details = base.ClusterDetailsProvider(template, walle_provider=self._walle_provider)
        self._grpc_endpoint = grpc_endpoint
        self.__configure_request = None
        self.__static_config = static.StaticConfigGenerator(
            template, binary_path, output_dir, walle_provider=walle_provider, local_binary_path=local_binary_path
        )

    @property
    def grpc_endpoint(self):
        if self._grpc_endpoint is not None:
            return self._grpc_endpoint
        node = self._cluster_details.static_bs_group_hosts[0]
        return "grpc://%s:%s" % (node.hostname, self._cluster_details.grpc_port)

    def __init_storage_command(self, proto_file):
        return " ".join(
            [
                self._binary_path,
                "-s",
                self.grpc_endpoint,
                "admin",
                "bs",
                "config",
                "invoke",
                "--proto-file",
                os.path.join(self._output_dir, proto_file),
            ]
        )

    def __add_storage_pool(
        self,
        request,
        box_id,
        storage_pool_id,
        erasure,
        filter_properties,
        num_groups,
        fail_domain_type,
        kind=None,
        name=None,
        vdisk_kind='Default',
        encryption_mode=0,
        generation=0,
    ):
        cmd = request.Command.add()
        cmd.DefineStoragePool.BoxId = box_id
        name = "Storage Pool with id: %d" % storage_pool_id if name is None else name
        cmd.DefineStoragePool.ItemConfigGeneration = generation
        cmd.DefineStoragePool.Name = name
        cmd.DefineStoragePool.StoragePoolId = storage_pool_id
        cmd.DefineStoragePool.ErasureSpecies = str(erasure)
        cmd.DefineStoragePool.VDiskKind = vdisk_kind
        cmd.DefineStoragePool.NumGroups = num_groups
        cmd.DefineStoragePool.EncryptionMode = encryption_mode
        if kind is not None:
            cmd.DefineStoragePool.Kind = kind
        pdisk_filter = cmd.DefineStoragePool.PDiskFilter.add()
        fail_domain_type = FailDomainType.from_string(fail_domain_type)
        if fail_domain_type != FailDomainType.Rack:
            rx_begin, rx_end, dx_begin, dx_end = DistinctionLevels[fail_domain_type]
            erasure = Erasure.from_string(erasure)
            cmd.DefineStoragePool.Geometry.RealmLevelBegin = rx_begin
            cmd.DefineStoragePool.Geometry.RealmLevelEnd = rx_end
            cmd.DefineStoragePool.Geometry.DomainLevelBegin = dx_begin
            cmd.DefineStoragePool.Geometry.DomainLevelEnd = dx_end
            cmd.DefineStoragePool.Geometry.NumVDisksPerFailDomain = 1
            cmd.DefineStoragePool.Geometry.NumFailDomainsPerFailRealm = erasure.min_fail_domains
            num_fail_realms = 3 if erasure == Erasure.MIRROR_3_DC else 1
            cmd.DefineStoragePool.Geometry.NumFailRealms = num_fail_realms

        if 'type' in filter_properties:
            pdisk_category = int(PDiskCategory.from_string(filter_properties['type']))
            pdisk_filter.Property.add(Type=pdisk_category)

        if 'SharedWithOs' in filter_properties:
            pdisk_filter.Property.add(SharedWithOs=filter_properties['SharedWithOs'])

        if 'kind' in filter_properties:
            pdisk_filter.Property.add(Kind=filter_properties['kind'])

    def __bind_storage_with_root(self, proto_file):
        return " ".join(
            [
                self._binary_path,
                "--server=%s" % self.grpc_endpoint,
                "db",
                "schema",
                "execute",
                os.path.join(self._output_dir, proto_file),
            ]
        )

    def __cms_init_cmd(self, proto_file, domain_name):
        return " ".join(
            [
                self._binary_path,
                "-s",
                self.grpc_endpoint,
                "admin",
                "console",
                "execute",
                "--domain=%s" % domain_name,
                "--retry=10",
                os.path.join(self._output_dir, proto_file),
            ]
        )

    def __cms_init_cmds(self):
        domain_names = [domain.domain_name for domain in self._cluster_details.domains]

        commands = ["set -eu"]
        commands += [self.__cms_init_cmd('Configure-%s.txt' % name, name) for name in domain_names]

        for domain in self._cluster_details.domains:
            name = domain.domain_name
            for init_id, filename in enumerate(domain.console_initializers, 1):
                commands.append(self.__cms_init_cmd('Configure-%s-init-%d.txt' % (name, init_id), name))

        return '\n'.join(commands)

    def init_storage_commands(self):
        return '\n'.join(
            [
                "set -eu",
                self.__init_storage_command("DefineBoxAndStoragePools.txt"),
            ]
        )

    def init_compute_commands(self):
        commands = ["set -eu"]
        return '\n'.join(commands)

    def init_root_storage(self):
        commands = ["set -eu"]
        commands += [
            self.__bind_storage_with_root('BindRootStorageRequest-%s.txt' % domain.domain_name)
            for domain in self._cluster_details.domains
            if len(domain.storage_pools)
        ]
        return '\n'.join(commands)

    @property
    def cms_init_cmd(self):
        return self.__cms_init_cmds()

    def define_box_and_storage_pools_request(self):
        request = bs_config.TConfigRequest()
        box_id = 1
        drives_to_config_id = {}
        host_config_id_iter = itertools.count(start=1)
        at_least_one_host_config_defined = False

        def add_drive(array, drive):
            kwargs = dict(
                Path=drive.path,
                SharedWithOs=drive.shared_with_os,
                Type=drive.type,
                Kind=drive.kind,
            )
            if drive.expected_slot_count is not None:
                pc = pdisk_config.TPDiskConfig(ExpectedSlotCount=drive.expected_slot_count)
                kwargs.update(PDiskConfig=pc)
            array.add(**kwargs)

        for host_config in self._cluster_details.host_configs:
            at_least_one_host_config_defined = True
            cmd = request.Command.add()
            cmd.DefineHostConfig.HostConfigId = host_config.host_config_id
            cmd.DefineHostConfig.ItemConfigGeneration = host_config.generation
            drives_to_config_id[host_config.drives] = host_config.host_config_id
            for drive in host_config.drives:
                add_drive(cmd.DefineHostConfig.Drive, drive)

        for host in self._cluster_details.hosts:
            if host.drives in drives_to_config_id:
                continue

            if at_least_one_host_config_defined:
                raise RuntimeError(
                    "At least one host config defined manually, but you still use drives directly attached to hosts"
                )

            host_config_id = next(host_config_id_iter)
            cmd = request.Command.add()
            cmd.DefineHostConfig.HostConfigId = host_config_id
            for drive in host.drives:
                add_drive(cmd.DefineHostConfig.Drive, drive)

            drives_to_config_id[host.drives] = host_config_id

        box_cmd = request.Command.add()
        box_cmd.DefineBox.BoxId = box_id
        box_cmd.DefineBox.ItemConfigGeneration = self._cluster_details.storage_config_generation
        for host in self._cluster_details.hosts:
            box_host = box_cmd.DefineBox.Host.add()
            box_host.Key.Fqdn = host.hostname
            box_host.Key.IcPort = host.ic_port
            if host.host_config_id is not None:
                box_host.HostConfigId = host.host_config_id
            else:
                box_host.HostConfigId = drives_to_config_id[host.drives]

        storage_pool_id = itertools.count(start=1)

        if self._cluster_details.storage_pools_deprecated:
            for storage_pool in self._cluster_details.storage_pools_deprecated:
                self.__add_storage_pool(request, storage_pool_id=next(storage_pool_id), **storage_pool.to_dict())

        # for tablets in domain lets make pools
        # but it is not supposed to make tablets in domain directly
        for domain in self._cluster_details.domains:
            for storage_pool in domain.storage_pools:
                self.__add_storage_pool(request, storage_pool_id=next(storage_pool_id), **storage_pool.to_dict())

        return request

    @staticmethod
    def make_bind_root_storage_request(domain):
        assert len(domain.storage_pools)

        scheme_transaction = tx_proxy.TTransaction()
        scheme_operation = scheme_transaction.ModifyScheme
        scheme_operation.WorkingDir = '/'
        scheme_operation.OperationType = flat_scheme_op.ESchemeOpAlterSubDomain

        domain_description = scheme_operation.SubDomain
        domain_description.Name = domain.domain_name
        for storage in domain.storage_pools:
            domain_description.StoragePools.add(
                Name=storage.name,
                Kind=storage.kind,
            )

        return scheme_transaction

    def make_configure_request(self, domain):
        configure_request = msgbus.TConsoleRequest()
        action = configure_request.ConfigureRequest.Actions.add()
        app_config = self.__static_config.get_app_config()
        if self._cluster_details.use_auto_config:
            app_config.ActorSystemConfig.CpuCount = self._cluster_details.dynamic_cpu_count
            app_config.ActorSystemConfig.NodeType = app_config.ActorSystemConfig.ENodeType.Value('COMPUTE')
            app_config.ActorSystemConfig.ForceIOPoolThreads = self._cluster_details.force_io_pool_threads
        action.AddConfigItem.ConfigItem.Config.CopyFrom(app_config)
        action.AddConfigItem.EnableAutoSplit = True

        if domain.config_cookie:
            action.AddConfigItem.ConfigItem.Cookie = domain.config_cookie
            configure_request.ConfigureRequest.Actions.add().RemoveConfigItems.CookieFilter.Cookies.append(
                domain.config_cookie
            )

        action = configure_request.ConfigureRequest.Actions.add()
        action.AddConfigItem.ConfigItem.UsageScope.TenantAndNodeTypeFilter.Tenant = "dynamic"
        pool = action.AddConfigItem.ConfigItem.Config.TenantPoolConfig
        pool.IsEnabled = True
        slot = pool.Slots.add()
        slot.Id = 'dynamic-slot-1'
        slot.DomainName = domain.domain_name
        slot.IsDynamic = True
        slot.Type = 'default'

        return configure_request

    def get_storage_requests(self):
        return {
            'DefineBoxAndStoragePools.txt': utils.message_to_string(self.define_box_and_storage_pools_request()),
        }

    @staticmethod
    def _construct_create_tenant_request(domain, tenant):
        console_request = msgbus.TConsoleRequest()
        console_request.CreateTenantRequest.Request.path = os.path.join('/', domain.domain_name, tenant.name)

        if tenant.shared:
            resources = console_request.CreateTenantRequest.Request.shared_resources
        else:
            resources = console_request.CreateTenantRequest.Request.resources

        for storage_unit in tenant.storage_units:
            pool = resources.storage_units.add()
            pool.unit_kind = storage_unit.kind
            pool.count = storage_unit.count

        overridden_configs = tenant.overridden_configs or {}
        nbs = overridden_configs.get('nbs', {})
        nfs = overridden_configs.get('nfs', {})
        if nbs.get('enable', False) or nfs.get('enable', False):
            console_request.CreateTenantRequest.Request.options.disable_tx_service = True

        if tenant.plan_resolution is not None:
            console_request.CreateTenantRequest.Request.options.plan_resolution = tenant.plan_resolution

        return utils.message_to_string(console_request)

    def get_create_tenant_requests(self):
        files = {}
        tn_id = itertools.count(start=1)
        tenants_count = 0
        for domain in self._cluster_details.domains:
            for tenant in domain.tenants:
                tenants_count += 1
                files['CreateTenant-%d.txt' % next(tn_id)] = self._construct_create_tenant_request(domain, tenant)
        return tenants_count, files

    def get_create_tenant_commands(self):
        tn_id = itertools.count(start=1)
        commands = []
        for domain in self._cluster_details.domains:
            for _ in domain.tenants:
                commands.append(self.__cms_init_cmd('CreateTenant-%d.txt' % next(tn_id), domain.domain_name))
        commands.append('exit 0')
        return "\n".join(commands)

    def get_all_configs(self):
        self.__static_config.get_all_configs()
        all_configs = self.get_storage_requests()
        all_configs.update(
            {
                'BindRootStorageRequest-%s.txt'
                % domain.domain_name: utils.message_to_string(self.make_bind_root_storage_request(domain))
                for domain in self._cluster_details.domains
                if len(domain.storage_pools)
            }
        )
        all_configs.update(
            {
                'init_compute.bash': self.init_compute_commands(),
                'init_storage.bash': self.init_storage_commands(),
                'init_root_storage.bash': self.init_root_storage(),
            }
        )
        for domain in self._cluster_details.domains:
            name = domain.domain_name
            all_configs['Configure-%s.txt' % name] = utils.message_to_string(self.make_configure_request(domain))
            for init_id, filename in enumerate(domain.console_initializers, 1):
                data = utils.get_resource_data('console_initializers', filename)
                all_configs['Configure-%s-init-%d.txt' % (name, init_id)] = data
        all_configs['init_cms.bash'] = self.cms_init_cmd

        tenants_count, files = self.get_create_tenant_requests()
        if tenants_count > 0:
            all_configs.update(files)
            all_configs['init_databases.bash'] = self.get_create_tenant_commands()

        return all_configs
