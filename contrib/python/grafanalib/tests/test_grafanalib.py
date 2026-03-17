"""Tests for Grafanalib."""

import grafanalib.core as G
from grafanalib import _gen

import sys
if sys.version_info[0] < 3:
    from io import BytesIO as StringIO
else:
    from io import StringIO

# TODO: Use Hypothesis to generate a more thorough battery of smoke tests.


def test_serialization():
    """Serializing a graph doesn't explode."""
    graph = G.Graph(
        title="CPU Usage by Namespace (rate[5m])",
        dataSource="My data source",
        targets=[
            G.Target(
                expr='namespace:container_cpu_usage_seconds_total:sum_rate',
                legendFormat='{{namespace}}',
                refId='A',
            ),
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


def test_auto_id():
    """auto_panel_ids() provides IDs for all panels without IDs already set."""
    dashboard = G.Dashboard(
        title="Test dashboard",
        rows=[
            G.Row(panels=[
                G.Graph(
                    title="CPU Usage by Namespace (rate[5m])",
                    dataSource="My data source",
                    targets=[
                        G.Target(
                            expr='whatever',
                            legendFormat='{{namespace}}',
                            refId='A',
                        ),
                    ],
                    yAxes=G.YAxes(
                        G.YAxis(format=G.SHORT_FORMAT, label="CPU seconds"),
                        G.YAxis(format=G.SHORT_FORMAT),
                    ),
                )
            ]),
        ],
    ).auto_panel_ids()
    assert dashboard.rows[0].panels[0].id == 1

    dashboard = G.Dashboard(
        title="Test dashboard",
        panels=[
            G.RowPanel(gridPos=G.GridPos(h=1, w=24, x=0, y=8)),
            G.Graph(
                title="CPU Usage by Namespace (rate[5m])",
                dataSource="My data source",
                targets=[
                    G.Target(
                        expr='whatever',
                        legendFormat='{{namespace}}',
                        refId='A',
                    ),
                ],
                yAxes=G.YAxes(
                    G.YAxis(format=G.SHORT_FORMAT, label="CPU seconds"),
                    G.YAxis(format=G.SHORT_FORMAT),
                ),
                gridPos=G.GridPos(h=1, w=24, x=0, y=8)
            )
        ],
    ).auto_panel_ids()
    assert dashboard.panels[0].id == 1


def test_auto_refids_preserves_provided_ids():
    """
    auto_ref_ids() provides refIds for all targets without refIds already
    set.
    """
    dashboard = G.Dashboard(
        title="Test dashboard",
        rows=[
            G.Row(panels=[
                G.Graph(
                    title="CPU Usage by Namespace (rate[5m])",
                    targets=[
                        G.Target(
                            expr='whatever #Q',
                            legendFormat='{{namespace}}',
                        ),
                        G.Target(
                            expr='hidden whatever',
                            legendFormat='{{namespace}}',
                            refId='Q',
                        ),
                        G.Target(
                            expr='another target'
                        ),
                    ],
                ).auto_ref_ids()
            ]),
        ],
    )
    assert dashboard.rows[0].panels[0].targets[0].refId == 'A'
    assert dashboard.rows[0].panels[0].targets[1].refId == 'Q'
    assert dashboard.rows[0].panels[0].targets[2].refId == 'B'

    dashboard = G.Dashboard(
        title="Test dashboard",
        panels=[
            G.Graph(
                title="CPU Usage by Namespace (rate[5m])",
                dataSource="My data source",
                targets=[
                    G.Target(
                        expr='whatever #Q',
                        legendFormat='{{namespace}}',
                    ),
                    G.Target(
                        expr='hidden whatever',
                        legendFormat='{{namespace}}',
                        refId='Q',
                    ),
                    G.Target(
                        expr='another target'
                    ),
                ],
                yAxes=G.YAxes(
                    G.YAxis(format=G.SHORT_FORMAT, label="CPU seconds"),
                    G.YAxis(format=G.SHORT_FORMAT),
                ),
                gridPos=G.GridPos(h=1, w=24, x=0, y=8)
            ).auto_ref_ids()
        ],
    ).auto_panel_ids()
    assert dashboard.panels[0].targets[0].refId == 'A'
    assert dashboard.panels[0].targets[1].refId == 'Q'
    assert dashboard.panels[0].targets[2].refId == 'B'


def test_auto_refids():
    """
    auto_ref_ids() provides refIds for all targets without refIds already
    set.
    """
    dashboard = G.Dashboard(
        title="Test dashboard",
        rows=[
            G.Row(panels=[
                G.Graph(
                    title="CPU Usage by Namespace (rate[5m])",
                    targets=[G.Target(expr="metric %d" % i)
                             for i in range(53)],
                ).auto_ref_ids()
            ]),
        ],
    )
    assert dashboard.rows[0].panels[0].targets[0].refId == 'A'
    assert dashboard.rows[0].panels[0].targets[25].refId == 'Z'
    assert dashboard.rows[0].panels[0].targets[26].refId == 'AA'
    assert dashboard.rows[0].panels[0].targets[51].refId == 'AZ'
    assert dashboard.rows[0].panels[0].targets[52].refId == 'BA'


def test_row_show_title():
    row = G.Row().to_json_data()
    assert row['title'] == 'New row'
    assert not row['showTitle']

    row = G.Row(title='My title').to_json_data()
    assert row['title'] == 'My title'
    assert row['showTitle']

    row = G.Row(title='My title', showTitle=False).to_json_data()
    assert row['title'] == 'My title'
    assert not row['showTitle']


def test_row_panel_show_title():
    row = G.RowPanel().to_json_data()
    assert row['title'] == ''
    assert row['panels'] == []

    row = G.RowPanel(title='My title').to_json_data()
    assert row['title'] == 'My title'

    row = G.RowPanel(title='My title', panels=['a', 'b']).to_json_data()
    assert row['title'] == 'My title'
    assert row['panels'][0] == 'a'


def test_row_panel_collapsed():
    row = G.RowPanel().to_json_data()
    assert row['collapsed'] is False

    row = G.RowPanel(collapsed=True).to_json_data()
    assert row['collapsed'] is True
