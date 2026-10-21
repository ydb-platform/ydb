"""Descriptor-driven editing of YDB configuration without materializing defaults."""

import base64
import copy
import hashlib
import json
import math
import re
from functools import lru_cache

import yaml
from google.protobuf.descriptor import FieldDescriptor as FD

from ydb.core.protos import config_pb2
from ydb.library.yaml_config.protos import config_pb2 as yaml_config_pb2
from ydb.tools.ydb_bench.lib.common import BenchmarkError


def yaml_name(name):
    # Match NProtobufJson::ToSnakeCaseDense, including acronym boundaries.
    return ''.join(
        ('_' if i and c.isupper() and name[i - 1] != '_' and not name[i - 1].isupper() else '') + c.lower()
        for i, c in enumerate(name)
    )


@lru_cache(maxsize=1)
def schema():
    messages, pending = {}, [config_pb2.TAppConfig.DESCRIPTOR, yaml_config_pb2.TEphemeralInputFields.DESCRIPTOR]
    while pending:
        message = pending.pop()
        if message.full_name in messages:
            continue
        fields = []
        messages[message.full_name] = fields
        for field in message.fields:
            item = {
                'name': yaml_name(field.name),
                'proto_name': field.name,
                'type': field.type,
                'repeated': field.label == FD.LABEL_REPEATED,
                'required': field.label == FD.LABEL_REQUIRED,
                'oneof': field.containing_oneof.name if field.containing_oneof else None,
                'deprecated': field.GetOptions().deprecated,
            }
            if field.message_type:
                item['message'] = field.message_type.full_name
                item['map'] = field.message_type.GetOptions().map_entry
                pending.append(field.message_type)
            elif field.enum_type:
                item['enum'] = {v.name: v.number for v in field.enum_type.values}
                item['default'] = field.enum_type.values[0].number if item['repeated'] else field.default_value
            elif field.type == FD.TYPE_BYTES:
                item['default'] = '' if item['repeated'] else base64.b64encode(field.default_value).decode()
            else:
                item['default'] = (
                    (False if field.type == FD.TYPE_BOOL else '' if field.type == FD.TYPE_STRING else 0)
                    if item['repeated']
                    else field.default_value
                )
                if type(item['default']) is float and not math.isfinite(item['default']):
                    item['default'] = str(item['default'])
                if field.type in (FD.TYPE_INT64, FD.TYPE_UINT64, FD.TYPE_SINT64, FD.TYPE_FIXED64, FD.TYPE_SFIXED64):
                    item['default'] = str(item['default'])
            fields.append(item)
    root = config_pb2.TAppConfig.DESCRIPTOR.full_name
    messages[root] = messages[root] + messages[yaml_config_pb2.TEphemeralInputFields.DESCRIPTOR.full_name]
    result = {'root': root, 'messages': messages}
    result['fingerprint'] = hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()
    return result


def _json_tree(value, depth=0, budget=None):
    if budget is None:
        budget = [100000]
    budget[0] -= 1
    if depth > 40 or budget[0] < 0:
        raise BenchmarkError('YDB configuration is too deeply nested or too large')
    if isinstance(value, dict):
        if any(not isinstance(k, str) or k in ('__proto__', 'constructor', 'prototype') for k in value):
            raise BenchmarkError('YDB configuration requires string keys without reserved JavaScript names')
        return {k: _json_tree(v, depth + 1, budget) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_tree(v, depth + 1, budget) for v in value]
    if value is None or type(value) in (str, bool, int):
        # Preserve int64 precision when the document passes through JavaScript.
        return str(value) if type(value) is int and abs(value) > 2**53 - 1 else value
    if type(value) is float and math.isfinite(value):
        return value
    raise BenchmarkError('YDB configuration must contain finite JSON-compatible values')


def validate(value):
    if not isinstance(value, dict):
        raise BenchmarkError('YDB configuration must be an object')
    value = _json_tree(value)
    if len(json.dumps(value)) > 2 * 1024 * 1024:
        raise BenchmarkError('YDB configuration exceeds 2 MiB')
    unknown = []
    model = schema()

    def scalar(field, item, path):
        kind = field['type']
        valid = True
        if 'message' in field:
            message(item, field['message'], path)
            return
        if kind == FD.TYPE_BOOL:
            valid = type(item) is bool
        elif kind in (FD.TYPE_STRING, FD.TYPE_BYTES):
            valid = isinstance(item, str)
            if valid and kind == FD.TYPE_BYTES:
                try:
                    base64.b64decode(item, validate=True)
                except ValueError:
                    valid = False
        elif kind == FD.TYPE_ENUM:
            names = field['enum']
            valid = (
                (type(item) is int and item in names.values())
                or (isinstance(item, str) and item.upper() in {name.upper() for name in names})
                or (type(item) is bool and ('VALUE_TRUE' if item else 'VALUE_FALSE') in names)
            )
        elif kind in (FD.TYPE_FLOAT, FD.TYPE_DOUBLE):
            valid = type(item) in (int, float) and math.isfinite(item)
        else:
            unsigned = kind in (FD.TYPE_UINT32, FD.TYPE_UINT64, FD.TYPE_FIXED32, FD.TYPE_FIXED64)
            bits = (
                64 if kind in (FD.TYPE_INT64, FD.TYPE_UINT64, FD.TYPE_SINT64, FD.TYPE_FIXED64, FD.TYPE_SFIXED64) else 32
            )
            try:
                number = int(item)
                valid = type(item) is int or (isinstance(item, str) and str(number) == item)
                valid = valid and (0 if unsigned else -(2 ** (bits - 1))) <= number < 2 ** (
                    bits if unsigned else bits - 1
                )
            except (ValueError, TypeError, OverflowError):
                valid = False
        if not valid:
            raise BenchmarkError('Invalid protobuf value at ' + path)

    def message(data, name, path):
        if not isinstance(data, dict):
            raise BenchmarkError('Expected object at ' + path)
        fields = {f['name']: f for f in model['messages'][name]}
        choices = set()
        for key, item in data.items():
            here = path + '.' + key if path else key
            field = fields.get(key)
            if field is None:
                unknown.append(here)
                continue
            if field['oneof']:
                if field['oneof'] in choices:
                    raise BenchmarkError('Multiple oneof values at ' + here)
                choices.add(field['oneof'])
            if field.get('map'):
                if not isinstance(item, dict):
                    raise BenchmarkError('Expected map at ' + here)
                key_field, value_field = model['messages'][field['message']]
                for map_key, map_value in item.items():
                    parsed_key = (
                        {'true': True, 'false': False}.get(map_key, map_key)
                        if key_field['type'] == FD.TYPE_BOOL
                        else map_key
                    )
                    scalar(key_field, parsed_key, here + '.key')
                    scalar(value_field, map_value, here + '[' + map_key + ']')
            elif field['repeated']:
                if not isinstance(item, list):
                    raise BenchmarkError('Expected list at ' + here)
                for index, element in enumerate(item):
                    scalar(field, element, here + '[' + str(index) + ']')
            else:
                scalar(field, item, here)

    message(value, model['root'], '')
    return value, unknown


class _ConfigLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        self.flatten_mapping(node)
        keys = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str) or key in keys:
                raise BenchmarkError('YDB YAML requires unique string keys')
            keys.add(key)
        return super().construct_mapping(node, deep=deep)


def parse(text):
    if not isinstance(text, str) or len(text) > 2 * 1024 * 1024:
        raise BenchmarkError('YDB YAML must be at most 2 MiB')
    try:
        value = yaml.load(text, Loader=_ConfigLoader)
    except (yaml.YAMLError, RecursionError) as error:
        raise BenchmarkError('Invalid YDB YAML: ' + str(error)) from error
    # The editor owns the config mapping, not dynamic YAML selectors/metadata.
    if isinstance(value, dict) and 'config' in value and ('metadata' in value or 'selector_config' in value):
        raise BenchmarkError('Paste only the config mapping, without metadata or selectors')
    return validate(value)


def response(value):
    normalized, unknown = validate(value)
    return {'config': normalized, 'yaml': yaml.safe_dump(normalized, sort_keys=False), 'unknown': unknown}


class _Inherited(dict):
    pass


class _DocumentLoader(_ConfigLoader):
    pass


class _DocumentDumper(yaml.SafeDumper):
    pass


_DocumentLoader.add_constructor('!inherit', lambda loader, node: _Inherited(loader.construct_mapping(node, deep=True)))
_DocumentDumper.add_representer(_Inherited, lambda dumper, value: dumper.represent_mapping('!inherit', value.items()))


def _inherited(value, replacements=(), path=(), enabled=True):
    if isinstance(value, dict):
        return (_Inherited if enabled and list(path) not in replacements else dict)(
            (k, _inherited(v, replacements, (*path, k), enabled)) for k, v in value.items()
        )
    if isinstance(value, list):
        return [_inherited(v, enabled=False) for v in value]
    return value


def tenant_replacements(value, overrides):
    value = _json_tree(value)
    if not isinstance(value, dict) or set(value) - set(overrides):
        raise BenchmarkError('Replacement settings must refer to configured tenants')
    for tenant, paths in value.items():
        if not isinstance(paths, list):
            raise BenchmarkError('Expected a list of replacement mapping paths')
        seen = set()
        for path in paths:
            if not isinstance(path, list) or not path or any(not isinstance(key, str) for key in path):
                raise BenchmarkError('Expected a nonempty mapping path')
            item = overrides[tenant]
            for key in path:
                item = item.get(key) if isinstance(item, dict) else None
            if not isinstance(item, dict) or tuple(path) in seen:
                raise BenchmarkError('Replacement path must identify a unique configuration mapping')
            seen.add(tuple(path))
    return value


def tenant_configs(value, paths, execution=False):
    value = _json_tree(value)
    if not isinstance(value, dict) or set(value) - set(paths):
        raise BenchmarkError('Tenant configuration must refer to tenants in this template')
    result = {}
    for path, config in value.items():
        result[path] = validate(config)[0]
        if execution:
            execution_config(config)
            ephemeral = {yaml_name(field.name) for field in yaml_config_pb2.TEphemeralInputFields.DESCRIPTOR.fields}
            if (MANAGED | ephemeral).intersection(config):
                raise BenchmarkError('Tenant selectors cannot override placement, bootstrap or actor-system settings')
    return result


def document(config, overrides, paths, cluster='', replacements=None):
    replacements = tenant_replacements({} if replacements is None else replacements, overrides)
    return {
        'metadata': {'kind': 'MainConfig', 'cluster': cluster, 'version': 0},
        'config': config,
        'allowed_labels': {'tenant': {'type': 'string'}},
        'selector_config': [
            {
                'description': 'Tenant ' + path,
                'selector': {'tenant': path},
                'config': _inherited(overrides.get(path, {}), replacements.get(path, [])),
            }
            for path in paths
        ],
    }


def dump_document(value):
    return yaml.dump(value, Dumper=_DocumentDumper, sort_keys=False)


def document_response(config, overrides, paths, replacements=None):
    if not isinstance(paths, list) or any(not isinstance(p, str) for p in paths) or len(set(paths)) != len(paths):
        raise BenchmarkError('Tenant paths must be a unique list of strings')
    normalized, unknown = validate(config)
    overrides = tenant_configs(overrides, paths)
    replacements = tenant_replacements({} if replacements is None else replacements, overrides)
    for path, value in overrides.items():
        unknown.extend(path + ': ' + key for key in validate(value)[1])
    return {
        'config': normalized,
        'tenant_configs': overrides,
        'tenant_replacements': replacements,
        'unknown': unknown,
        'yaml': dump_document(document(normalized, overrides, paths, replacements=replacements)),
    }


def parse_document(text, paths=None):
    if not isinstance(text, str) or len(text) > 2 * 1024 * 1024:
        raise BenchmarkError('YDB YAML must be at most 2 MiB')
    try:
        value = yaml.load(text, Loader=_DocumentLoader)
    except (yaml.YAMLError, RecursionError) as error:
        raise BenchmarkError('Invalid YDB YAML: ' + str(error)) from error
    _json_tree(value)
    if not isinstance(value, dict):
        raise BenchmarkError('YDB YAML must be an object')
    if 'config' not in value:
        return document_response(value, {}, [] if paths is None else paths)
    if set(value) - {'metadata', 'config', 'allowed_labels', 'selector_config'}:
        raise BenchmarkError('Unsupported configuration document sections')
    metadata = value.get('metadata', {})
    if (
        not isinstance(metadata, dict)
        or set(metadata) - {'kind', 'cluster', 'version'}
        or metadata.get('kind', 'MainConfig') != 'MainConfig'
    ):
        raise BenchmarkError('Expected MainConfig metadata')
    if metadata.get('cluster', '') != '' or metadata.get('version', 0) != 0:
        raise BenchmarkError('Template config is a new-cluster draft: use metadata.cluster: "" and version: 0')
    if value.get('allowed_labels', {}) not in ({}, {'tenant': {'type': 'string'}}):
        raise BenchmarkError('Only the tenant string label is supported by the template editor')
    selectors = value.get('selector_config', [])
    if not isinstance(selectors, list) or len(selectors) > 64:
        raise BenchmarkError('Expected at most 64 tenant selectors')
    overrides, replacements = {}, {}

    def replacement_paths(item, path=(), in_list=False):
        if isinstance(item, list):
            for child in item:
                replacement_paths(child, in_list=True)
            return []
        if not isinstance(item, dict):
            return []
        if in_list and isinstance(item, _Inherited):
            raise BenchmarkError('List items cannot use !inherit: the list replaces inherited values')
        result = [list(path)] if path and not in_list and not isinstance(item, _Inherited) else []
        for key, child in item.items():
            result.extend(replacement_paths(child, (*path, key), in_list))
        return result

    for selector in selectors:
        if not isinstance(selector, dict) or set(selector) - {'description', 'selector', 'config'}:
            raise BenchmarkError('Invalid tenant selector')
        match = selector.get('selector', {})
        if not isinstance(match, dict) or set(match) != {'tenant'} or not isinstance(match['tenant'], str):
            raise BenchmarkError('Only exact tenant selectors are supported')
        path = match['tenant']
        if path in overrides:
            raise BenchmarkError('Use one selector per tenant')
        config = selector.get('config', {})
        replacement = replacement_paths(config)
        if replacement:
            replacements[path] = replacement
        overrides[path] = config
    return document_response(value['config'], overrides, list(overrides) if paths is None else paths, replacements)


# Runtime topology and per-role actor settings have a separate owner. Never let
# a generic override bypass disk admission or change leased endpoints.
MANAGED = {
    'hosts',
    'host_configs',
    'system_tablets',
    'tls',
    'storage_config_generation',
    'security_config',
    'nameservice_config',
    'dynamic_nameservice_config',
    'dynamic_node_config',
    'blob_storage_config',
    'domains_config',
    'bootstrap_config',
    'tablets_config',
    'tenant_pool_config',
    'actor_system_config',
    'grpc_config',
    'interconnect_config',
    'monitoring_config',
    'self_management_config',
    'cluster_yaml_config',
    'config_dir_path',
    'stored_config_yaml',
    'startup_config_yaml',
    'startup_storage_yaml',
    'bridge_config',
}


def execution_config(value):
    normalized, unknown = validate(value)
    conflicts = sorted((MANAGED - {'domains_config', 'self_management_config'}).intersection(normalized))
    domains = normalized.get('domains_config', {})
    if set(domains) - {'domain', 'state_storage'}:
        conflicts.append('domains_config: only domain and state_storage are supported for execution')
    domain = domains.get('domain', [{'domain_id': 1, 'name': 'Root'}])
    if (
        len(domain) != 1
        or set(domain[0]) - {'domain_id', 'name', 'storage_pool_types'}
        or domain[0].get('domain_id', 1) != 1
    ):
        conflicts.append('domains_config.domain: exactly one domain with ID 1 is required')
    elif not re.fullmatch(r'[A-Za-z0-9_-]+', domain_name(normalized)):
        conflicts.append('domains_config.domain.name: invalid domain name')
    management = normalized.get('self_management_config', {})
    if set(management) - {'erasure_species'} or erasure_name(normalized) not in ERASURES:
        conflicts.append('self_management_config: only supported erasure_species is allowed')
    if normalized.get('fail_domain_type', 'rack') not in ('rack', 'Rack', 0):
        conflicts.append('Benchmark placement currently requires fail_domain_type: rack')
    if normalized.get('default_disk_type', 'SSD') not in ('SSD', 'ROT'):
        conflicts.append('Benchmark disks support default_disk_type SSD or ROT')
    if normalized.get('domain_name', domain_name(normalized)) != domain_name(normalized):
        conflicts.append('domain_name conflicts with domains_config.domain.name')
    if 'storage_pool_types' in normalized and domain and domain[0].get('storage_pool_types'):
        conflicts.append('Specify storage_pool_types at only one level')
    if unknown or conflicts:
        raise BenchmarkError(
            'YDB configuration cannot be applied to benchmark execution: ' + ', '.join(conflicts + unknown)
        )
    return normalized


ERASURES = ('none', 'block-4-2', 'mirror-3-dc')


def domain_name(value):
    domains = value.get('domains_config', {}).get('domain', [])
    return domains[0].get('name', value.get('domain_name', 'Root')) if domains else value.get('domain_name', 'Root')


def erasure_name(value):
    values = [value[key] for key in ('static_erasure', 'erasure') if key in value]
    management = value.get('self_management_config', {})
    if 'erasure_species' in management:
        values.append(management['erasure_species'])
    if len(set(values)) > 1:
        raise BenchmarkError('Conflicting erasure, static_erasure and self_management_config.erasure_species')
    return values[0] if values else 'none'


def validate_placement(config, nodes):
    """Check supported benchmark geometry before acquiring worker resources."""
    static = [(i, n) for i, n in enumerate(nodes, 1) if n['role'] == 'static']
    erasure = erasure_name(config)
    pools = config.get(
        'storage_pool_types', config.get('domains_config', {}).get('domain', [{}])[0].get('storage_pool_types', [])
    )
    checks = []
    for pool in pools:
        kind, settings = pool.get('kind'), pool.get('pool_config', {})
        if kind not in ('ssd', 'hdd') or set(settings) - {
            'box_id',
            'kind',
            'erasure_species',
            'vdisk_kind',
            'pdisk_filter',
        }:
            raise BenchmarkError('Benchmark storage pools must use ssd/hdd kinds and standard disk filters')
        media = 'SSD' if kind == 'ssd' else 'ROT'
        if settings.get('box_id', 1) != 1 or settings.get('pdisk_filter') != [{'property': [{'type': media}]}]:
            raise BenchmarkError('Storage pool requires box 1 and a matching SSD/ROT disk filter')
        checks.append((kind, settings.get('erasure_species', erasure)))
    if len({p.get('kind') for p in pools}) != len(pools):
        raise BenchmarkError('Storage pool kinds must be unique')
    checks += [(None, erasure)]
    for kind, species in checks:
        locations = {}
        for _, node in static:
            if kind and not any(d['media'] == kind for d in node['disks']):
                continue
            location = node['location']
            locations.setdefault(location['data_center'], set()).add(location['rack'])
        if species not in ERASURES:
            raise BenchmarkError('Unsupported benchmark erasure: ' + str(species))
        if species == 'block-4-2' and sum(map(len, locations.values())) < 8:
            raise BenchmarkError('block-4-2 requires at least 8 distinct racks with matching disks')
        if species == 'mirror-3-dc' and sum(len(racks) >= 3 for racks in locations.values()) < 3:
            raise BenchmarkError('mirror-3-dc requires at least 3 DCs with 3 racks each and matching disks')
    storages = config.get('domains_config', {}).get('state_storage', [])
    if len(storages) > 1:
        raise BenchmarkError('Benchmark supports one state storage configuration')
    for storage in storages:
        ring = storage.get('ring', {})
        members = ring.get('node', [])
        count = ring.get('nto_select')
        if (
            set(storage) - {'ssid', 'ring'}
            or storage.get('ssid', 1) != 1
            or set(ring) - {'node', 'nto_select'}
            or not members
            or len(members) != len(set(members))
            or not set(members).issubset({i for i, _ in static})
            or not isinstance(count, int)
            or not 1 <= count <= len(members)
            or count % 2 == 0
        ):
            raise BenchmarkError(
                'State storage requires SSID 1, static node IDs and an odd NToSelect within the ring size'
            )


def merge(base, overrides):
    result = copy.deepcopy(base)
    for key, value in overrides.items():
        result[key] = (
            merge(result[key], value)
            if isinstance(value, dict) and isinstance(result.get(key), dict)
            else copy.deepcopy(value)
        )
    return result
