# -*- coding: utf-8 -*-

from ydb.tests.library.nemesis.dc_nemesis_network import (
    SingleDataCenterFailureNemesis, 
    DataCenterRouteUnreachableNemesis,
    DataCenterIptablesBlockPortsNemesis
)


def datacenter_nemesis_list(cluster):
    return [
        SingleDataCenterFailureNemesis(cluster),
        DataCenterRouteUnreachableNemesis(cluster),
        DataCenterIptablesBlockPortsNemesis(cluster)
    ] 
