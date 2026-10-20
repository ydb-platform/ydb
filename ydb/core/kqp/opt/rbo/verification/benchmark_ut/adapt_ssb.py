"""Adapt the pinned QueryBrew SSB SQL and scale-one schema for YDB.

Only FROM paths and the dialect pragma change in query bodies. No data is
generated. Schema differences and all source/output hashes are retained.
"""

import hashlib
import json
from pathlib import Path
import re
import sys


TABLES = frozenset({'date', 'customer', 'supplier', 'part', 'lineorder'})
QUERY_NAMES = tuple(f'{flight}{query}.sql'
                    for flight, count in ((1, 3), (2, 3), (3, 4), (4, 3))
                    for query in range(1, count + 1))


def adapt_query(source):
    select, rest = re.split(r'(?m)^from ', source)
    tables, predicate = re.split(r'(?m)^where ', rest)
    names = [name.strip() for name in tables.strip().split(',')]
    if len(set(names)) != len(names) or not set(names) <= TABLES:
        raise ValueError('Unexpected SSB table references')
    references = ', '.join(f'`/Root/{name}` AS `{name}`' for name in names)
    return 'PRAGMA AnsiImplicitCrossJoin;\n' + select + 'from ' + references + '\nwhere ' + predicate


def adapt_schema(source):
    schema = json.loads(source)
    if {table['name'] for table in schema['tables']} != TABLES:
        raise ValueError('Unexpected SSB table set')
    statements = []
    for table in schema['tables']:
        fields = {}
        for column in table['columns']:
            condition = column.get('_eval')
            if condition == 'dataset.scale >= 1000':
                continue
            if condition not in (None, 'dataset.scale < 1000'):
                raise ValueError('Unknown schema scale condition')
            # Interpret the two explicit schema choices; never eval source code.
            match = re.fullmatch(r'(integer|char\(\d+\)|varchar\(\d+\)|numeric\(18, 2\)) not null', column['type'])
            if not match or column['name'] in fields:
                raise ValueError('Unknown or duplicate SSB column')
            kind = match.group(1)
            mapped = 'Int32' if kind == 'integer' else 'Decimal(18,2)' if kind == 'numeric(18, 2)' else 'String'
            fields[column['name']] = mapped
        key = table['primary key']
        keys = key['columns'] if 'columns' in key else [key['column']]
        if not keys or not set(keys) <= set(fields):
            raise ValueError('Invalid SSB primary key')
        lines = [f'    {name} {kind} NOT NULL' for name, kind in fields.items()]
        lines.append('    PRIMARY KEY (' + ', '.join(keys) + ')')
        statements.append(f"CREATE TABLE `/Root/{table['name']}` (\n" + ',\n'.join(lines) + '\n);')
    return '\n\n'.join(statements) + '\n'


def generate(source, destination):
    provenance = json.loads((source / 'source.json').read_text())
    expected = {*QUERY_NAMES, 'ssb.dbschema.json', 'LICENSE'}
    if set(provenance['files']) != expected:
        raise ValueError('Incomplete pinned SSB source')
    for name, digest in provenance['files'].items():
        if hashlib.sha256((source / name).read_bytes()).hexdigest() != digest:
            raise ValueError('Source hash mismatch: ' + name)
    outputs = {'schema.yql': adapt_schema((source / 'ssb.dbschema.json').read_text()),
               'LICENSE': (source / 'LICENSE').read_text()}
    queries = []
    for number, name in enumerate(QUERY_NAMES, 1):
        filename = f'q{number}.yql'
        result = adapt_query((source / name).read_text())
        outputs[filename] = result
        queries.append({'query_id': number, 'ssb_query': name[0] + '.' + name[1],
                        'source': name, 'source_sha256': provenance['files'][name],
                        'adapted': filename, 'adapted_sha256': hashlib.sha256(result.encode()).hexdigest()})
    manifest = {
        **provenance, 'source_path': 'benchmarks/ssb', 'schema_scale_choice': 1,
        'paper': 'https://www.cs.umb.edu/~poneil/StarSchemaB.PDF',
        'scope': 'YDB optimizer correctness on the adapted schema; not a benchmark performance claim',
        'adaptations': [
            'FROM tables use /Root paths; SELECT, WHERE, GROUP BY and ORDER BY are unchanged',
            'AnsiImplicitCrossJoin enables comma joins before WHERE filtering',
            'integer maps to Int32; numeric(18,2) maps to Decimal(18,2)',
            'char/varchar map to String: length, blank-padding and ASCII restrictions are not modeled',
            'All primary keys and NOT NULL declarations are preserved',
            'Foreign keys and generator value ranges are not assumed by the proofs',
            'Schema uses the pinned QueryBrew financial Decimal types and its scale<1000 order-key choice',
            'No data generator or dataset is downloaded or executed',
        ], 'queries': queries,
        'adapted_schema_sha256': hashlib.sha256(outputs['schema.yql'].encode()).hexdigest(),
    }
    outputs['source.json'] = json.dumps(manifest, indent=2) + '\n'
    destination.mkdir(parents=True, exist_ok=True)
    for name, body in outputs.items():
        (destination / name).write_text(body)


if __name__ == '__main__':
    generate(Path(sys.argv[1]), Path(sys.argv[2]))
