import copy
import json

import pytest
import yatest.common

from ydb.udfs.wasm.sdk.services.generate import generate_header, main, method_id


def manifest(module='echo'):
    with open(yatest.common.source_path('ydb/udfs/wasm/' + module + '/manifest.json')) as source:
        return json.load(source)


@pytest.mark.parametrize('module', ['profile', 'echo'])
def test_generates_header_without_modifying_manifest(module):
    source = manifest(module)
    header = generate_header(source)
    assert source == manifest(module)
    assert 'ServiceAbiVersion = 2;' in header
    for method in source['service_methods']:
        assert 'id' not in method
        assert f'Method{method["name"]} = {method_id(method["name"])}u;' in header
    assert generate_header(source) == header


def test_reordering_or_adding_methods_preserves_ids():
    source = manifest()
    header = generate_header(source)
    source['service_methods'].reverse()
    extra = copy.deepcopy(source['service_methods'][0])
    extra['name'] = 'Additional'
    source['service_methods'].append(extra)
    changed = generate_header(source)
    for line in header.splitlines():
        if line.startswith('inline constexpr uint32_t Method'):
            assert line in changed


@pytest.mark.parametrize('identifier', [0, 7, None])
def test_rejects_manual_ids(identifier):
    source = manifest()
    source['service_methods'][0]['id'] = identifier
    with pytest.raises(ValueError, match='IDs are generated'):
        generate_header(source)


def test_rejects_hash_collisions():
    source = manifest()
    first, second = source['service_methods']
    first['name'], second['name'] = 'M15119', 'M203802'
    assert method_id(first['name']) == method_id(second['name']) == 683710779
    with pytest.raises(ValueError, match='ID collision'):
        generate_header(source)


def test_rejects_duplicate_methods():
    source = manifest()
    source['service_methods'][1]['name'] = 'Echo'
    with pytest.raises(ValueError, match='Duplicate service method name'):
        generate_header(source)


@pytest.mark.parametrize('version', [None, 1, 3, True])
def test_rejects_unsupported_or_missing_abi_version(version):
    source = manifest()
    if version is None:
        del source['service_abi_version']
    else:
        source['service_abi_version'] = version
    with pytest.raises(ValueError, match='Unsupported service ABI version'):
        generate_header(source)


def test_rejects_oversized_output_schema():
    source = manifest()
    source['service_methods'][0]['max_output_row_bytes'] = 1
    with pytest.raises(ValueError, match='reservation is too small'):
        generate_header(source)


def test_cli_outputs_only_header_and_preserves_manifest(tmp_path, monkeypatch):
    source = tmp_path / 'manifest.json'
    output = tmp_path / 'service_methods.h'
    contents = json.dumps(manifest(), indent=2) + '\n'
    source.write_text(contents)
    monkeypatch.setattr('sys.argv', ['generate.py', str(source), str(output)])
    main()
    assert source.read_text() == contents
    assert output.read_text() == generate_header(manifest())
    assert sorted(path.name for path in tmp_path.iterdir()) == ['manifest.json', 'service_methods.h']
