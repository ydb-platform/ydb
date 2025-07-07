# -*- coding: utf-8 -*-

from ydb.tests.library.nemesis.nemesis_datacenter import (
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
