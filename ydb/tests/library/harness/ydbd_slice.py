#!/usr/bin/env python
# -*- coding: utf-8 -*-
import itertools
import logging
import time

import ydb
import ydb.tools.ydbd_slice as ydbd_slice
from ydb.tools.cfg import walle

from .kikimr_cluster import DEFAULT_INTERCONNECT_PORT, DEFAULT_MBUS_PORT, DEFAULT_MON_PORT, DEFAULT_GRPC_PORT, load_yaml
from .kikimr_runner import KikimrExternalNode
from .kikimr_cluster_interface import KiKiMRClusterInterface

logger = logging.getLogger(__name__)


class YdbdSlice(KiKiMRClusterInterface):
    def __init__(self, config_path, binary_path=None):
        kikimr_bin = binary_path
        self.__yaml_config = load_yaml(config_path)
        self.__hosts = [host['name'] for host in self.__yaml_config.get('hosts')]
        ssh_user = "yc-user"
        walle_provider = walle.NopHostsInformationProvider()
        components = {'kikimr': ['bin', 'cfg'], 'dynamic_slots': ['all']}
        self.cluster_details = ydbd_slice.safe_load_cluster_details(config_path, walle_provider=walle_provider)
        self.hosts_names = self.cluster_details.hosts_names
        nodes = ydbd_slice.nodes.Nodes(self.hosts_names, dry_run=False, ssh_user=ssh_user)

        configurator = ydbd_slice.cluster_description.Configurator(
            self.cluster_details,
            out_dir="/tmp",
            kikimr_bin=kikimr_bin,
            kikimr_compressed_bin=None,
            walle_provider=walle_provider
        )

        self.__slice = ydbd_slice.handlers.Slice(
            components,
            nodes,
            self.cluster_details,
            configurator,
            do_clear_logs=True,
            yav_version=None,
            walle_provider=walle_provider,
        )

        self.db_name = list(self.cluster_details.databases.values())[0][0].name
        self.domain = self.cluster_details.domains[0].domain_name
        self.db_path = "/{}/{}".format(self.domain, self.db_name)
        super(YdbdSlice, self).__init__()

    @property
    def config(self):
        return self.__config

    def add_storage_pool(self, erasure=None):
        raise NotImplementedError()

    def _wait_for_start(self):
        driver_config = ydb.DriverConfig(
            database=self.db_path,
            endpoint="%s:%s" % (
                self.nodes[1].host, self.nodes[1].port
            )
        )

        with ydb.Driver(driver_config) as driver:
            with ydb.SessionPool(driver, size=1) as pool:
                with pool.checkout() as session:
                    retry_count = 60
                    # wait for readiness
                    for _ in range(retry_count):
                        try:
                            table_for_check = self.db_path + "/sample_table"
                            session.create_table(
                                table_for_check,
                                ydb.TableDescription()
                                .with_column(ydb.Column("key", ydb.PrimitiveType.Uint64))
                                .with_primary_key("key")
                            )
                        except ydb.issues.Unavailable:
                            time.sleep(1)
                        else:
                            break

                    session.drop_table(table_for_check)

    def start(self):
        self.__slice.slice_install()
        self._wait_for_start()
        return self

    def stop(self):
        self.__slice.slice_stop()
        return self

    def restart(self):
        self.stop()
        self.stop()
        return self

    @property
    def nodes(self):
        # TODO: use from config?
        return {
            node_id: KikimrExternalNode(
                node_id=node_id,
                host=host,
                port=DEFAULT_GRPC_PORT,
                mon_port=DEFAULT_MON_PORT,
                ic_port=DEFAULT_INTERCONNECT_PORT,
                mbus_port=DEFAULT_MBUS_PORT,
                configurator=None,
            ) for node_id, host in zip(itertools.count(start=1), self.__hosts)
        }

    @property
    def slots(self):
        raise NotImplementedError()
