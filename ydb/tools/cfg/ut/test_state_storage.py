from collections import Counter

import pytest

from ydb.tools.cfg.static import StaticConfigGenerator


@pytest.mark.parametrize(
    'erasure,node_limit,n_to_select',
    [
        ('block-4-2', 8, 5),
        ('mirror-3-dc', 9, 9),
    ],
)
@pytest.mark.parametrize('mode', ['rack', 'body', 'allow_incorrect', 'new_style'])
@pytest.mark.parametrize('large_cluster', [False, True])
def test_default_state_storage_node_limit(erasure, node_limit, n_to_select, mode, large_cluster):
    host_count = 36 if large_cluster else node_limit
    hosts = [
        {
            'name': 'node%d.example.net' % i,
            'node_id': i + 1,
            'rack': i // 2 if large_cluster else i,
            'datacenter': i // (host_count // 3) if erasure == 'mirror-3-dc' else 1,
            'body': i + 1,
            'drives': [{'path': '/dev/disk/ssd', 'type': 'SSD'}],
        }
        for i in range(host_count)
    ]
    template = {
        'static_erasure': erasure,
        'fail_domain_type': 'body' if mode == 'body' else 'rack',
        'hosts': hosts,
    }
    if mode == 'allow_incorrect':
        template['state_storage'] = {'allow_incorrect': True}
    elif mode == 'new_style':
        template['use_new_style_config_yaml'] = True
        fail_domains = [{'vdisk_locations': [{'node_id': i + 1}]} for i in range(host_count)]
        template['blob_storage_config'] = {
            'service_set': {'groups': [{'rings': [{'fail_domains': fail_domains}]}]},
        }

    generator = StaticConfigGenerator(template, binary_path="", output_dir="")
    ring = generator.domains_txt.StateStorage[0].Ring
    node_ids = list(ring.Node)
    assert len(node_ids) == node_limit
    assert len(set(node_ids)) == node_limit
    assert ring.NToSelect == n_to_select
    assert set(node_ids) <= {host['node_id'] for host in hosts}
    if mode == 'rack':
        selected_hosts = [host for host in hosts if host['node_id'] in node_ids]
        assert len({host['rack'] for host in selected_hosts}) == node_limit
        if erasure == 'mirror-3-dc':
            assert sorted(Counter(host['datacenter'] for host in selected_hosts).values()) == [3, 3, 3]


@pytest.mark.parametrize('erasure,n_to_select', [('block-4-2', 5), ('mirror-3-dc', 9)])
@pytest.mark.parametrize('mode', ['rack', 'body', 'allow_incorrect', 'new_style'])
def test_explicit_state_storage_node_ids(erasure, n_to_select, mode):
    node_ids = [1, 9, 17, 25, 34, 43, 53, 64, 74]
    template = {
        'static_erasure': erasure,
        'fail_domain_type': 'body' if mode == 'body' else 'rack',
        'hosts': [
            {
                'name': 'node%d.example.net' % i,
                'node_id': i + 1,
                'rack': 1,
                'datacenter': 1,
                'body': i + 1,
                'drives': [{'path': '/dev/disk/ssd', 'type': 'SSD'}],
            }
            for i in range(81)
        ],
        'state_storage': {'node_ids': node_ids},
    }
    if mode == 'allow_incorrect':
        template['state_storage']['allow_incorrect'] = True
    elif mode == 'new_style':
        template['use_new_style_config_yaml'] = True
        template['blob_storage_config'] = {
            'service_set': {'groups': [{'rings': [{'fail_domains': [
                {'vdisk_locations': [{'node_id': i}]} for i in range(1, 10)
            ]}]}]},
        }

    generator = StaticConfigGenerator(template, binary_path="", output_dir="")
    ring = generator.domains_txt.StateStorage[0].Ring
    assert list(ring.Node) == node_ids
    assert ring.NToSelect == n_to_select
