"""Tests for Zabbix Datasource"""

import grafanalib.core as G
import grafanalib.zabbix as Z
from grafanalib import _gen

import sys
if sys.version_info[0] < 3:
    from io import BytesIO as StringIO
else:
    from io import StringIO


def test_serialization_zabbix_target():
    """Serializing a graph doesn't explode."""
    graph = G.Graph(
        title="CPU Usage",
        dataSource="Zabbix data source",
        targets=[
            Z.zabbixMetricTarget(
                group="Zabbix Group",
                host="Zabbix Host",
                application="CPU",
                item="/CPU (load)/",
                functions=[
                    Z.ZabbixSetAliasFunction("View alias"),
                ]),
        ],
        id=1,
        yAxes=G.YAxes(
            G.YAxis(format=G.SHORT_FORMAT, label="CPU seconds / second"),
            G.YAxis(format=G.SHORT_FORMAT),
        ),
    )
    stream = StringIO()
    _gen.write_dashboard(graph, stream)
    assert stream.getvalue() != ''


def test_serialization_zabbix_trigger_panel():
    """Serializing a graph doesn't explode."""
    graph = Z.ZabbixTriggersPanel(
        id=1,
        title="Zabbix Triggers",
        dataSource="Zabbix data source",
        triggers=Z.ZabbixTrigger(
            group='Zabbix Group',
            application="",
            trigger="/trigger.regexp/",
            host="/zabbix.host/"))
    stream = StringIO()
    _gen.write_dashboard(graph, stream)
    assert stream.getvalue() != ''
