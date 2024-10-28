# -*- coding: utf-8 -*-
import os
import yatest


def _get_param(name, default):
    # TODO: remove yatest dependency from harness
    try:
        return yatest.common.get_param(name, default)
    except yatest.common.NoRuntimeFormed:
        return default


ssh_username = _get_param("kikimr.ci.ssh_username", os.getenv('NEMESIS_USER', 'robot-nemesis'))
deploy_cluster = _get_param("kikimr.ci.deploy_cluster", "false") == "true"

kikimr_binary_deploy_path = '/Berkanavt/kikimr/bin/kikimr'
kikimr_configure_binary_deploy_path = '/Berkanavt/kikimr/bin/kikimr_configure'
kikimr_configuration_deploy_path = '/Berkanavt/kikimr/cfg'
kikimr_cluster_yaml_deploy_path = '/Berkanavt/kikimr/cfg/cluster.yaml'
kikimr_next_version_deploy_path = '/Berkanavt/kikimr/bin/kikimr_next'
kikimr_last_version_deploy_path = '/Berkanavt/kikimr/bin/kikimr_last'
kikimr_home = '/Berkanavt/kikimr'


def generate_configs_cmd(configs_type="", deploy_path=None):
    deploy_path = kikimr_configuration_deploy_path if deploy_path is None else deploy_path
    return "sudo {} cfg {} {} {} --enable-cores {}".format(
        kikimr_configure_binary_deploy_path,
        kikimr_cluster_yaml_deploy_path, kikimr_binary_deploy_path, deploy_path, configs_type)


def next_version_kikimr_driver_path():
    return _get_param("kikimr.ci.kikimr_driver_next", None)


def kikimr_stderr_to_console():
    return _get_param("kikimr.ci.stderr_to_console", "false") == "true"


def kikimr_driver_path():
    built_binary = _get_param("kikimr.ci.kikimr_driver", None)
    if os.getenv("YDB_DRIVER_BINARY"):
        return yatest.common.binary_path(os.getenv("YDB_DRIVER_BINARY"))

    if built_binary is None:
        return yatest.common.binary_path("kikimr/driver/kikimr")
    return built_binary


def kikimr_configure_binary_path():
    return yatest.common.binary_path("ydb/tools/cfg/bin/ydb_configure")
