import pytest

from ydb.tests.olap.lib.fill_tables import TableFiller
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


def _node(host, disconnected=False, **ports):
    return YdbCluster.Node({
        'Disconnected': disconnected,
        'SystemState': {
            'Host': host,
            'Roles': ['Storage'],
            'Endpoints': [{'Name': name, 'Address': f':{port}'} for name, port in ports.items()],
        },
    })


@pytest.mark.parametrize(('scheme', 'expected'), [
    ('grpc', {'dual': 'grpc://dual:2135', 'plain-only': 'grpc://plain-only:3135'}),
    ('grpcs', {'dual': 'grpcs://dual:2136', '2001:db8::1': 'grpcs://[2001:db8::1]:3136'}),
])
def test_discover_static_nodes_uses_matching_port(monkeypatch, scheme, expected):
    nodes = [
        _node('dual', grpc=2135, grpcs=2136),
        _node('plain-only', grpc=3135),
        _node('2001:db8::1', grpcs=3136),
        _node('disconnected', disconnected=True, grpc=4135, grpcs=4136),
    ]
    monkeypatch.setattr(YdbCluster, 'ydb_endpoint', f'{scheme}://seed:1234')
    monkeypatch.setattr(YdbCluster, 'ydb_mon_port', 8765)
    monkeypatch.setattr(YdbCluster, 'get_cluster_nodes', lambda **kwargs: nodes)

    assert TableFiller._discover_static_nodes() == expected
