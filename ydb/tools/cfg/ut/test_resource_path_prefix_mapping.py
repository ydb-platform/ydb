import pytest
import yaml

from ydb.tools.cfg.static import StaticConfigGenerator


@pytest.mark.parametrize(
    'mapping',
    [
        None,
        {},
        {'rules': []},
        {
            'rules': [
                {'src': '/source/first', 'dst': '/destination/first'},
                {'src': '/source/second', 'dst': '/destination/second'},
            ]
        },
    ],
)
def test_resource_path_prefix_mapping(mapping):
    template = {
        'static_erasure': 'none',
        'fail_domain_type': 'body',
        'hosts': [
            {
                'name': 'node.example.net',
                'node_id': 1,
                'rack': 1,
                'datacenter': 1,
                'body': 1,
                'drives': [{'path': '/dev/disk/ssd', 'type': 'SSD'}],
            }
        ],
        'blob_storage_config': {'service_set': {'groups': []}},
    }
    if mapping is not None:
        template['resource_path_prefix_mapping'] = mapping
    generator = StaticConfigGenerator(template, binary_path='', output_dir='')
    configs = generator.get_all_configs()
    for config in (
        yaml.safe_load(configs['config.yaml']),
        yaml.safe_load(configs['dynconfig.yaml'])['config'],
    ):
        if mapping is None:
            assert 'resource_path_prefix_mapping' not in config
        else:
            assert config['resource_path_prefix_mapping'] == mapping
