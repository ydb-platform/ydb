# -*- coding: utf-8 -*-

from ydb.tests.library.nemesis.nemesis_datacenter import (
    DataCenterStopNodesNemesis,
    DataCenterRouteUnreachableNemesis,
    DataCenterIptablesBlockPortsNemesis
)


def datacenter_nemesis_list(cluster):
    return [
        DataCenterStopNodesNemesis(cluster),
        DataCenterRouteUnreachableNemesis(cluster),
        DataCenterIptablesBlockPortsNemesis(cluster)
    ]
