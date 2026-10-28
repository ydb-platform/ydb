#!/usr/bin/env python3
"""Generate version-local Diplodoc variables from protobuf descriptors and C++ literals."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
import tempfile

import grpc_tools
from grpc_tools import protoc
from google.protobuf import descriptor_pb2 as pb
import yaml

BEGIN = '  # BEGIN GENERATED FEATURE FLAG DEFAULTS\n'
END = '  # END GENERATED FEATURE FLAG DEFAULTS\n'
BEGIN_MARKER = BEGIN.strip()
END_MARKER = END.strip()
CANONICAL = 'NKikimrConfig.TFeatureFlags'
TRIBOOL = '.NKikimrConfig.TFeatureFlags.Tribool'
API_STATUS = '.Ydb.FeatureFlag.Status'


class Error(ValueError):
    pass


class UniqueLoader(yaml.SafeLoader):
    """Do not silently accept duplicate YAML keys, including generated namespaces."""


def unique_mapping(loader, node, deep=False):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise Error(f'Duplicate YAML key: {key}')
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping)


def snake(name: str) -> str:
    name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def qualified_mapping(namespace: dict, parts: list[str]) -> dict:
    mapping = namespace
    for part in parts:
        value = mapping.setdefault(part, {})
        if not isinstance(value, dict):
            raise Error(f'Qualified variable collision: {".".join(parts)}')
        mapping = value
    return mapping


def source_files(root: Path, config: dict) -> tuple[list[str], list[str]]:
    wanted = set(config['proto_sources'])
    for pattern in config.get('proto_globs', []):
        wanted.update(p.relative_to(root).as_posix() for p in root.glob(pattern))
    missing = sorted(p for p in wanted if not (root / p).is_file())
    required = 'ydb/core/protos/feature_flags.proto'
    if required in missing:
        raise Error(f'Missing required source: {required}')
    # Older stable branches need not contain sources introduced in later versions.
    return sorted(wanted - set(missing)), missing


def compile_schema(root: Path, paths: list[str]) -> pb.FileDescriptorSet:
    with tempfile.TemporaryDirectory(prefix='docs-flag-descriptors-') as temp:
        output = Path(temp) / 'schema.pb'
        includes = [root, Path(grpc_tools.__file__).parent / '_proto']
        for extra in ('contrib/libs/protobuf/src', 'contrib/libs/googleapis'):
            if (root / extra).is_dir():
                includes.append(root / extra)
        args = ['protoc', *(f'-I{path}' for path in includes), '--include_imports',
                f'--descriptor_set_out={output}', *(str(root / p) for p in paths)]
        if protoc.main(args):
            raise Error('protoc could not compile the checked-out sources')
        schema = pb.FileDescriptorSet()
        schema.ParseFromString(output.read_bytes())
        return schema


def messages(schema):
    def walk(node, parent, file):
        name = parent + '.' + node.name if parent else node.name
        yield name, node, file
        for child in node.nested_type:
            yield from walk(child, name, file)
    for file in schema.file:
        for node in file.message_type:
            yield from walk(node, file.package, file)


def enum_defaults(schema):
    result = {}
    for file in schema.file:
        for enum in file.enum_type:
            result['.' + file.package + '.' + enum.name] = enum.value[0].name
    for name, node, _ in messages(schema):
        for enum in node.enum_type:
            result['.' + name + '.' + enum.name] = enum.value[0].name
    return result


def category(file: str) -> str:
    if '/public/api/' in file:
        return 'public_api'
    if '/viewer/' in file:
        return 'viewer_snapshot'
    if file.endswith(('kqp.proto', 'kqp_physical.proto')):
        return 'query_plan'
    if file.endswith('flat_scheme_op.proto'):
        return 'per_table_or_operation'
    return 'service_configuration'


def variables_from_schema(schema, selected):
    variables = {'feature_flags': {}, 'proto_flags': {}, 'compile_time_flags': {}}
    catalog = []
    first_enums = enum_defaults(schema)
    canonical_found = False
    for name, node, file in messages(schema):
        if file.name not in selected:
            continue
        canonical_found |= name == CANONICAL
        if node.options.map_entry:
            continue
        values = {}
        for field in node.field:
            is_bool = field.type == pb.FieldDescriptorProto.TYPE_BOOL
            is_status = field.type == pb.FieldDescriptorProto.TYPE_ENUM and field.type_name in (TRIBOOL, API_STATUS)
            if not (is_bool or is_status) or field.label == pb.FieldDescriptorProto.LABEL_REPEATED:
                continue
            if is_bool:
                value = field.default_value == 'true' if field.HasField('default_value') else False
            else:
                value = field.default_value if field.HasField('default_value') else first_enums[field.type_name]
                if field.type_name == TRIBOOL:
                    value = {'VALUE_TRUE': True, 'VALUE_FALSE': False, 'UNSET': None}[value]
            key = snake(field.name)
            if key in values:
                raise Error(f'Variable name collision: {name}.{key}')
            values[key] = value
            catalog.append({'source': file.name, 'message': name, 'field': field.name,
                            'key': key, 'default': value, 'type': 'bool' if is_bool else field.type_name,
                            'explicit_default': field.HasField('default_value'), 'kind': category(file.name)})
        for retired in node.reserved_name:
            values.setdefault(snake(retired), None)
        if values:
            qualified_mapping(variables['proto_flags'], name.split('.')).update(values)
        if name == CANONICAL:
            variables['feature_flags'] = dict(values)
    if not canonical_found:
        raise Error(f'Required message not found: {CANONICAL}')
    return variables, catalog


def cpp_variables(root: Path, sources: list[dict]):
    values, catalog, missing = {}, [], []
    for source in sources:
        path = root / source['path']
        if not path.is_file():
            missing.append(source['path'])
            continue
        # Only reviewed literal definitions are evaluated. Arbitrary C++ is never executed.
        text = re.sub(r'//[^\n]*|/\*.*?\*/', '', path.read_text(), flags=re.S)
        namespace = re.search(r'\bnamespace\s+' + re.escape(source['namespace']) + r'\s*\{', text)
        if not namespace:
            raise Error(f'Cannot read registered C++ namespace: {source["path"]}:{source["namespace"]}')
        depth, end = 1, namespace.end()
        while end < len(text) and depth:
            depth += (text[end] == '{') - (text[end] == '}')
            end += 1
        if depth:
            raise Error(f'Unclosed C++ namespace in {source["path"]}')
        text = text[namespace.end():end - 1]
        for struct in source['structs']:
            match = re.search(r'\bstruct\s+' + re.escape(struct) + r'\s*\{([^{}]*)\}', text, re.S)
            if not match:
                raise Error(f'Cannot read registered C++ struct: {source["path"]}:{struct}')
            qualified = source['namespace'] + '::' + struct
            flags = {}
            for field, expression in re.findall(r'\bstatic\s+constexpr\s+bool\s+(\w+)\s*=\s*([^;]+);', match[1]):
                expression = expression.strip()
                value = {'true': True, 'false': False}.get(expression)
                key = snake(field)
                if key in flags:
                    raise Error(f'C++ variable collision: {qualified}.{key}')
                flags[key] = value
                catalog.append({'source': source['path'], 'message': qualified, 'field': field,
                                'key': key, 'default': value, 'kind': 'compile_time_literal',
                                'expression': expression})
            qualified_mapping(values, qualified.split('::')).update(flags)
    return values, catalog, missing


def retired_variables(variables: dict, references: list[dict], active: set | None = None):
    """Explicit tombstones distinguish deletions without reserved names from spelling mistakes."""
    for reference in references:
        namespace = reference['namespace']
        if namespace not in variables:
            raise Error(f'Unknown retired namespace: {namespace}')
        key = reference['field']
        if not re.fullmatch(r'[a-z][a-z0-9_]*', key):
            raise Error(f'Retired field must use snake_case: {key}')
        if namespace == 'feature_flags':
            mapping = variables[namespace]
        else:
            parts = reference['message'].split('::' if namespace == 'compile_time_flags' else '.')
            mapping = qualified_mapping(variables[namespace], parts)
        identity = (namespace, reference.get('message', ''), key)
        if (active and identity in active) or (key in mapping and mapping[key] is not None):
            raise Error(f'Retired reference still names an active field: {reference}')
        mapping[key] = None


REFERENCE = re.compile(r'\b(feature_flags|proto_flags|compile_time_flags)((?:\.[A-Za-z_][A-Za-z0-9_]*)+)')
TEMPLATE = re.compile(r'\{%.*?%\}|\{\{.*?\}\}', re.S)


def strip_fences(text: str) -> str:
    # Preserve offsets/line numbers while excluding examples of snippets in code blocks.
    result, fence = [], None
    for line in text.splitlines(keepends=True):
        marker = re.match(r'^\s*(`{3,}|~{3,})', line)
        if marker:
            if fence is None:
                fence = marker[1][0], len(marker[1])
            elif marker[1][0] == fence[0] and len(marker[1]) >= fence[1]:
                fence = None
            result.append('\n' if line.endswith('\n') else '')
        elif fence:
            result.append('\n' if line.endswith('\n') else '')
        else:
            result.append(line)
    return ''.join(result)


def validate_references(docs: Path, variables: dict) -> int:
    checked, errors = 0, []
    for path in sorted(docs.rglob('*.md')):
        text = strip_fences(path.read_text())
        for template in TEMPLATE.finditer(text):
            expression = template[0]
            recognized = []
            for match in REFERENCE.finditer(expression):
                recognized.append(match.span())
                mapping = variables[match[1]]
                parts = match[2].lstrip('.').split('.')
                checked += 1
                if re.match(r'\s*\[', expression[match.end():]):
                    errors.append(f'{path.relative_to(docs)}: use a static, qualified flag reference: {expression}')
                for key in parts:
                    if not isinstance(mapping, dict) or key not in mapping:
                        errors.append(f'{path.relative_to(docs)}: unknown flag {match[0]}')
                        break
                    mapping = mapping[key]
                else:
                    if isinstance(mapping, dict):
                        errors.append(f'{path.relative_to(docs)}: reference must name a field: {match[0]}')
            remainder = expression
            for start, end in reversed(recognized):
                remainder = remainder[:start] + ' ' * (end - start) + remainder[end:]
            if re.search(r'\b(?:feature_flags|proto_flags|compile_time_flags)\b', remainder):
                errors.append(f'{path.relative_to(docs)}: use a static, qualified flag reference: {expression}')
    if errors:
        raise Error('\n'.join(errors))
    return checked


def inject_presets(text: str, variables: dict) -> str:
    lines = text.splitlines(keepends=True)
    begins = [index for index, line in enumerate(lines) if line.strip() == BEGIN_MARKER]
    ends = [index for index, line in enumerate(lines) if line.strip() == END_MARKER]
    if len(begins) != len(ends) or len(begins) > 1 or (begins and ends[0] <= begins[0]):
        raise Error('Malformed generated presets block')
    had_generated = bool(begins)
    if had_generated:
        text = ''.join(lines[:begins[0]] + lines[ends[0] + 1:])
    loaded = yaml.load(text, Loader=UniqueLoader)
    if not isinstance(loaded, dict):
        raise Error('presets.yaml must contain a default mapping')
    if 'default' not in loaded:
        raise Error('presets.yaml must contain a default mapping')
    default_mapping = loaded['default']
    if default_mapping is None and had_generated:
        default_mapping = {}
    if not isinstance(default_mapping, dict):
        raise Error('presets.yaml must contain a default mapping')
    overlap = set(variables) & set(default_mapping)
    if overlap:
        raise Error(f'Manually defined generated namespaces: {sorted(overlap)}')
    document = yaml.compose(text)
    default_key, default = next((key, value) for key, value in document.value if key.value == 'default')
    generated = yaml.safe_dump(variables, sort_keys=True, allow_unicode=True, width=120)
    if not isinstance(default, yaml.MappingNode):
        indent = ' ' * (default_key.start_mark.column + 2)
        offset = default.end_mark.index
        block = (indent + BEGIN_MARKER + '\n'
                 + ''.join(indent + line + '\n' for line in generated.splitlines())
                 + indent + END_MARKER + '\n')
        prefix = text[:offset].rstrip(' \t')
        suffix = text[offset:]
        if prefix and not prefix.endswith('\n'):
            prefix += '\n'
        if suffix.startswith('\n'):
            suffix = suffix[1:]
        text = prefix + block + suffix
        yaml.load(text, Loader=UniqueLoader)
        return text
    if default.flow_style:
        indent = ' ' * (default_key.start_mark.column + 2)
        manual = yaml.safe_dump(default_mapping, sort_keys=False, allow_unicode=True, width=120)
        manual_block = ''.join(indent + line + '\n' for line in manual.splitlines()) if default_mapping else ''
        block = (indent + BEGIN_MARKER + '\n'
                 + ''.join(indent + line + '\n' for line in generated.splitlines())
                 + indent + END_MARKER + '\n')
        suffix = text[default.end_mark.index:]
        if suffix.startswith(' #'):
            comment, separator, rest = suffix.partition('\n')
            text = text[:default.start_mark.index].rstrip(' \t') + comment + '\n' + manual_block + block + (rest if separator else '')
        else:
            if suffix.startswith('\n'):
                suffix = suffix[1:]
            text = text[:default.start_mark.index].rstrip(' \t') + '\n' + manual_block + block + suffix
        yaml.load(text, Loader=UniqueLoader)
        return text
    indent_width = (default.value[0][0].start_mark.column if default.value
                    else default_key.start_mark.column + 2)
    indent = ' ' * indent_width
    offset = default.end_mark.index
    block = (indent + BEGIN_MARKER + '\n'
             + ''.join(indent + line + '\n' for line in generated.splitlines())
             + indent + END_MARKER + '\n')
    prefix = text[:offset]
    suffix = text[offset:]
    if prefix and not prefix.endswith('\n'):
        prefix += '\n'
    if suffix.startswith('\n'):
        suffix = suffix[1:]
    text = prefix + block + suffix
    yaml.load(text, Loader=UniqueLoader)
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument('--docs-root', type=Path)
    parser.add_argument('--catalog', type=Path, help='Optional JSON inventory for build artifacts')
    args = parser.parse_args()
    root = args.repo_root.resolve()
    docs = (args.docs_root or root / 'ydb/docs').resolve()
    config = json.loads((Path(__file__).parent / 'sources.json').read_text())
    paths, missing = source_files(root, config)
    schema = compile_schema(root, paths)
    # Service configs can delegate flags to types declared in imported YDB schemas.
    selected = set(paths) | {file.name for file in schema.file if file.name.startswith('ydb/')}
    variables, catalog = variables_from_schema(schema, selected)
    cpp, cpp_catalog, cpp_missing = cpp_variables(root, config.get('cpp_sources', []))
    variables['compile_time_flags'] = cpp
    active = {('proto_flags', item['message'], item['key']) for item in catalog}
    active.update(('feature_flags', '', item['key']) for item in catalog if item['message'] == CANONICAL)
    active.update(('compile_time_flags', item['message'], item['key']) for item in cpp_catalog)
    retired_variables(variables, config.get('retired_references', []), active)
    checked = validate_references(docs, variables)
    presets = docs / 'presets.yaml'
    updated = inject_presets(presets.read_text(), variables)
    # Do not modify the file until descriptors, references and YAML have all passed validation.
    presets.write_text(updated)
    inventory = {'sources': paths, 'descriptor_sources': sorted(selected),
                 'missing_in_this_version': missing + cpp_missing,
                 'references_checked': checked, 'flags': catalog + cpp_catalog}
    if args.catalog:
        args.catalog.parent.mkdir(parents=True, exist_ok=True)
        args.catalog.write_text(json.dumps(inventory, ensure_ascii=False, indent=2) + '\n')
    print(f'Generated {len(variables["feature_flags"])} canonical defaults and {len(catalog) + len(cpp_catalog)} inventory entries; checked {checked} documentation references.')
    if inventory['missing_in_this_version']:
        print('Sources absent in this version: ' + ', '.join(inventory['missing_in_this_version']))


if __name__ == '__main__':
    try:
        main()
    except (Error, OSError, ValueError) as error:
        print(f'Feature flag generation failed: {error}', file=sys.stderr)
        sys.exit(1)
