import argparse
import json
import re
from pathlib import Path


SERVICE_VERSION = 2
MAX_BYTES = 32768
WIDTHS = {'Uint64': 8, 'Uint32': 4, 'Int64': 8, 'Bool': 1, 'String': 4, 'Utf8': 4}


def method_id(name):
    value = 2166136261
    for byte in name.encode('ascii'):
        value = ((value ^ byte) * 16777619) & 0xffffffff
    return value


def require(condition, message):
    if not condition:
        raise ValueError(message)


def name(value):
    require(isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_]{1,128}', value), 'Invalid service name')
    return value


def number(value, maximum):
    require(type(value) is int and 0 <= value <= maximum, 'Invalid service numeric limit')
    return value


def fields(items):
    require(isinstance(items, list) and 1 <= len(items) <= 32, 'Invalid service field count')
    seen = set()
    maximum = 0
    for item in items:
        field_name = name(item['name'])
        require(field_name not in seen, 'Duplicate service field')
        seen.add(field_name)
        kind = item['type']
        require(kind in WIDTHS, 'Unsupported service field type')
        maximum += WIDTHS[kind]
        if kind in ('String', 'Utf8'):
            limit = number(item['max_bytes'], MAX_BYTES)
            require(limit > 0, 'Service string limit must be positive')
            maximum += limit
        require(maximum <= MAX_BYTES - 32, 'Service row exceeds byte limit')
    return maximum


def generate(description):
    require(description['module_type'] == 'module' and description['module_kind'] == 'wasm'
            and description['module_extension'] == 'wasm', 'Expected a WASM service module')
    module = name(description['module_name'])
    require('service_abi_version' not in description, 'Service ABI version is generated')
    methods = description['service_methods']
    require(isinstance(methods, list) and 1 <= len(methods) <= 64, 'Invalid service method count')
    names, ids = set(), set()
    constants = []
    for method in methods:
        require('id' not in method, 'Method IDs are generated; remove id from the description')
        method_name = name(method['name'])
        require(method_name not in names, 'Duplicate service method name')
        names.add(method_name)
        identifier = method_id(method_name)
        require(identifier not in ids, 'Service method ID collision; rename a method')
        ids.add(identifier)
        require(type(method['batch']) is bool, 'Invalid service batch capability')
        count = number(method['max_batch_rows'], 64)
        require(count > 0 and (method['batch'] or count == 1), 'Invalid service batch capability')
        fields(method['input'])
        output_size = fields(method['output'])
        require(number(method['max_output_row_bytes'], MAX_BYTES - 16) >= output_size,
                'Service output row reservation is too small')
        constants.append(f'inline constexpr uint32_t Method{method_name} = {identifier}u;')
    manifest = dict(description, service_abi_version=SERVICE_VERSION)
    header = '\n'.join([
        '// Generated from service.json. Do not edit.', '#pragma once', '#include <cstdint>', '',
        f'namespace NYdb::NWasm::NServices::NGenerated::NModule{module} {{',
        f'inline constexpr uint32_t ServiceAbiVersion = {SERVICE_VERSION};', *constants, '}', '',
    ])
    return json.dumps(manifest, indent=2, ensure_ascii=True) + '\n', header


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('description')
    parser.add_argument('manifest')
    parser.add_argument('header')
    args = parser.parse_args()
    with open(args.description) as source:
        manifest, header = generate(json.load(source))
    Path(args.manifest).write_text(manifest, encoding='ascii')
    Path(args.header).write_text(header, encoding='ascii')


if __name__ == '__main__':
    main()
