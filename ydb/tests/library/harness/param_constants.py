# -*- coding: utf-8 -*-
import os
import ydb.tests.library.common.yatest_common as yatest_common


config_name = yatest_common.get_param("kikimr.ci.cluster_name", None)
ssh_username = yatest_common.get_param("kikimr.ci.ssh_username", os.getenv('NEMESIS_USER', 'robot-nemesis'))
deploy_cluster = yatest_common.get_bool_param("kikimr.ci.deploy_cluster", False)
use_packages = yatest_common.get_bool_param('kikimr.ci.packages', True)

log_level = int(yatest_common.get_param('kikimr.ci.driver.log_level', 5))

kikimr_binary_deploy_path = '/Berkanavt/kikimr/bin/kikimr'
kikimr_configure_binary_deploy_path = '/Berkanavt/kikimr/bin/kikimr_configure'
kikimr_configuration_deploy_path = '/Berkanavt/kikimr/cfg'
kikimr_cluster_yaml_deploy_path = '/Berkanavt/kikimr/cfg/cluster.yaml'
blockstore_configs_deploy_path = '/Berkanavt/nbs-server/cfg'
kikimr_next_version_deploy_path = '/Berkanavt/kikimr/bin/kikimr_next'
kikimr_last_version_deploy_path = '/Berkanavt/kikimr/bin/kikimr_last'
kikimr_home = '/Berkanavt/kikimr'


def generate_configs_cmd(configs_type="", deploy_path=None):
    deploy_path = kikimr_configuration_deploy_path if deploy_path is None else deploy_path
    return "sudo {} cfg {} {} {} --enable-cores {}".format(
        kikimr_configure_binary_deploy_path,
        kikimr_cluster_yaml_deploy_path, kikimr_binary_deploy_path, deploy_path, configs_type)


def next_version_kikimr_driver_path():
    return yatest_common.get_param("kikimr.ci.kikimr_driver_next", None)


def kikimr_stderr_to_console():
    return yatest_common.get_bool_param("kikimr.ci.stderr_to_console", False)


def kikimr_driver_path():
    built_binary = yatest_common.get_param("kikimr.ci.kikimr_driver", None)
    if os.getenv("YDB_DRIVER_BINARY"):
        return yatest_common.binary_path(os.getenv("YDB_DRIVER_BINARY"))

    if built_binary is None:
        return yatest_common.binary_path("kikimr/driver/kikimr")
    return built_binary


def kikimr_configure_binary_path():
    return yatest_common.binary_path("ydb/tools/cfg/bin/ydb_configure")
