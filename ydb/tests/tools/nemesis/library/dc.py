# -*- coding: utf-8 -*-

from ydb.tests.library.nemesis.dc_nemesis_network import (
    DataCenterNetworkNemesis,
    SingleDataCenterFailureNemesis,
    DataCenterRouteUnreachableNemesis,
    DataCenterIptablesBlockPortsNemesis
)


def datacenter_nemesis_list(cluster):
    return [
        DataCenterNetworkNemesis(cluster),
        SingleDataCenterFailureNemesis(cluster),
        DataCenterRouteUnreachableNemesis(cluster),
        DataCenterIptablesBlockPortsNemesis(cluster)
    ]