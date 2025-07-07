# -*- coding: utf-8 -*-
"""
Инструментальная поддержка для сетевых DC nemesis.

Этот модуль содержит интеграцию DC nemesis с каталогом nemesis.
"""

from ydb.tests.library.nemesis.dc_nemesis_network import (
    DataCenterNetworkNemesis,
    SingleDataCenterFailureNemesis, 
    DataCenterRouteUnreachableNemesis,
    DataCenterIptablesBlockPortsNemesis
)


def datacenter_nemesis_list(cluster):
    """
    Создает список nemesis для тестирования отказов на уровне ДЦ.
    
    :param cluster: Кластер YDB
    :return: Список nemesis объектов для тестирования различных DC сбоев
    """
    return [
        DataCenterNetworkNemesis(cluster),
        SingleDataCenterFailureNemesis(cluster),
        DataCenterRouteUnreachableNemesis(cluster),
        DataCenterIptablesBlockPortsNemesis(cluster)
    ] 