import copy
import json

import pytest
import yatest.common

from ydb.udfs.wasm.sdk.services.generate import generate, method_id


def description(module='echo'):
    with open(yatest.common.source_path('ydb/udfs/wasm/' + module + '/service.json')) as source:
        return json.load(source)


@pytest.mark.parametrize('module', ['profile', 'echo'])
def test_generates_both_artifacts_without_public_ids(module):
    source = description(module)
    manifest, header = generate(source)
    result = json.loads(manifest)
    assert source == description(module)
    assert result['service_abi_version'] == 2
    assert result['service_methods'] == source['service_methods']
    for method in result['service_methods']:
        assert 'id' not in method
        assert f'Method{method["name"]} = {method_id(method["name"])}u;' in header
    assert generate(source) == (manifest, header)


def test_reordering_or_adding_methods_preserves_ids():
    source = description()
    _, header = generate(source)
    source['service_methods'].reverse()
    extra = copy.deepcopy(source['service_methods'][0])
    extra['name'] = 'Additional'
    source['service_methods'].append(extra)
    _, changed = generate(source)
    for line in header.splitlines():
        if line.startswith('inline constexpr uint32_t Method'):
            assert line in changed


@pytest.mark.parametrize('identifier', [0, 7, None])
def test_rejects_manual_ids(identifier):
    source = description()
    source['service_methods'][0]['id'] = identifier
    with pytest.raises(ValueError, match='IDs are generated'):
        generate(source)


def test_rejects_hash_collisions():
    source = description()
    first, second = source['service_methods']
    first['name'], second['name'] = 'M15119', 'M203802'
    assert method_id(first['name']) == method_id(second['name']) == 683710779
    with pytest.raises(ValueError, match='ID collision'):
        generate(source)


def test_rejects_duplicate_methods():
    source = description()
    source['service_methods'][1]['name'] = 'Echo'
    with pytest.raises(ValueError, match='Duplicate service method name'):
        generate(source)


def test_rejects_manual_abi_version():
    source = description()
    source['service_abi_version'] = 1
    with pytest.raises(ValueError, match='ABI version is generated'):
        generate(source)


def test_rejects_oversized_output_schema():
    source = description()
    source['service_methods'][0]['max_output_row_bytes'] = 1
    with pytest.raises(ValueError, match='reservation is too small'):
        generate(source)
