# -*- coding: utf-8 -*-
import os
import yatest


kikimr_binary_deploy_path = '/Berkanavt/kikimr/bin/kikimr'
kikimr_configure_binary_deploy_path = '/Berkanavt/kikimr/bin/kikimr_configure'
kikimr_configuration_deploy_path = '/Berkanavt/kikimr/cfg'
kikimr_cluster_yaml_deploy_path = '/Berkanavt/kikimr/cfg/cluster.yaml'
kikimr_home = '/Berkanavt/kikimr'


def generate_configs_cmd(configs_type="", deploy_path=None):
    deploy_path = kikimr_configuration_deploy_path if deploy_path is None else deploy_path
    return "sudo {} cfg {} {} {} --enable-cores {}".format(
        kikimr_configure_binary_deploy_path,
        kikimr_cluster_yaml_deploy_path, kikimr_binary_deploy_path, deploy_path, configs_type)


def kikimr_driver_path():
    if os.getenv("YDB_DRIVER_BINARY"):
        return yatest.common.binary_path(os.getenv("YDB_DRIVER_BINARY"))

    return yatest.common.binary_path("kikimr/driver/kikimr")


def kikimr_configure_binary_path():
    return yatest.common.binary_path("ydb/tools/cfg/bin/ydb_configure")
