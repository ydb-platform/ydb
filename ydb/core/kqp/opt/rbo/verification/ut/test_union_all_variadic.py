"""Native UnionAll preserves every input and its positional column mapping."""
import copy
import unittest

from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import SnapshotError, parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import build_problem, build_transformation_prefix_problem, query_solver
if __package__:
    from .test_verify import SOLVER
else:
    from test_verify import SOLVER


def _snapshot(count=3, *, staged=False, nested=False, ordered=False):
    names = [f't{index}' for index in range(count)]
    tables = [{"name": name, "columns": [
        {"name": column, "type": "Int32", "nullable": False} for column in ("k", "x")],
        "unique_keys": []} for name in names]
    nodes = [{"id": name, "op": "scan", "table": name, "columns": [
        {"source": column, "output": f'{name}.{column}'} for column in ("k", "x")]} for name in names]
    inputs = [{"node": name, "columns": [f'{name}.k']} for name in names]
    unions = []
    if nested:
        previous = inputs[0]
        for index, branch in enumerate(inputs[1:]):
            node = {"id": f'union{index}', "op": "union_all", "inputs": [previous, branch],
                    "output": ["u.k"], "ordered": ordered}
            unions.append(node)
            previous = {"node": node['id'], "columns": ["u.k"]}
    else:
        unions = [{"id": "union", "op": "union_all", "inputs": inputs,
                   "output": ["u.k"], "ordered": ordered}]
    nodes.extend(unions)
    graph = None
    if staged:
        stages = [{"id": name, "nodes": [name], "inputs": [],
                   "outputs": [{"index": 0, "node": name}], "source_storage": "row"}
                  for name in names]
        edges = []
        for node in unions:
            stages.append({"id": node['id'], "nodes": [node['id']],
                           "inputs": [item['node'] for item in node['inputs']],
                           "outputs": [{"index": 0, "node": node['id']}], "source_storage": None})
            occurrences = {}
            for index, item in enumerate(node['inputs']):
                producer = item['node']
                occurrence = occurrences.get(producer, 0)
                occurrences[producer] = occurrence + 1
                edges.append({"id": f"{node['id']}_{index}", "producer": producer,
                              "consumer": node['id'], "occurrence": occurrence,
                              "producer_output": 0, "consumer_input": index,
                              "kind": "union_all", "parallel": False})
        graph = {"root_stage": unions[-1]['id'], "stages": stages, "edges": edges, "assumptions": []}
    return {"format": "ydb-rbo-semantic-snapshot", "version": 1, "schema": {"tables": tables},
            "plan": {"nodes": nodes, "root": unions[-1]['id'], "output": ["u.k"], "subplans": []},
            "stage_graph": graph}


class VariadicUnionValidationTest(unittest.TestCase):
    def test_every_branch_must_match_the_output_schema(self):
        for field, value in (("columns", []), ("columns", ["missing"]), ("node", "missing")):
            raw = _snapshot()
            raw['plan']['nodes'][-1]['inputs'][-1][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(SnapshotError):
                parse_snapshot(raw)

    def test_missing_or_single_input_is_rejected(self):
        for count in (0, 1):
            raw = _snapshot()
            raw['plan']['nodes'][-1]['inputs'] = raw['plan']['nodes'][-1]['inputs'][:count]
            with self.subTest(count=count), self.assertRaisesRegex(SnapshotError, 'at least two inputs'):
                parse_snapshot(raw)


@unittest.skipUnless(SOLVER, 'requires the pinned solver')
class VariadicUnionProofTest(unittest.TestCase):
    def test_three_and_four_inputs_match_nested_binary_unions(self):
        for count in (3, 4):
            for ordered in (False, True):
                for nested_final in (False, True):
                    before = _snapshot(count, nested=not nested_final, ordered=ordered)
                    after = _snapshot(count, staged=True, nested=nested_final, ordered=ordered)
                    with self.subTest(count=count, ordered=ordered, nested_final=nested_final):
                        problem = build_problem(parse_snapshot(before), parse_snapshot(after), 2)
                        self.assertEqual(query_solver(problem, SOLVER, timeout_ms=10_000).status, 'unsat')

    def test_a_changed_third_branch_is_a_counterexample(self):
        for ordered in (False, True):
            before = _snapshot(ordered=ordered)
            after = _snapshot(staged=True, ordered=ordered)
            after['plan']['nodes'][-1]['inputs'][2]['columns'] = ['t2.x']
            with self.subTest(ordered=ordered):
                problem = build_problem(parse_snapshot(before), parse_snapshot(after), 2)
                self.assertEqual(query_solver(problem, SOLVER, timeout_ms=10_000).status, 'sat')

    def test_a_repeated_input_preserves_its_multiplicity(self):
        before = _snapshot()
        before['plan']['nodes'][-1]['inputs'].append(copy.deepcopy(before['plan']['nodes'][-1]['inputs'][0]))
        after = _snapshot(staged=True)
        problem = build_problem(parse_snapshot(before), parse_snapshot(after), 2)
        self.assertEqual(query_solver(problem, SOLVER, timeout_ms=10_000).status, 'sat')


def _counted_snapshot(count=3, *, staged=False, nested=False, ordered=False, zero_width=False):
    raw = _snapshot(count, staged=staged, nested=nested, ordered=ordered)
    nodes = raw['plan']['nodes']
    previous = raw['plan']['root']
    extra = []
    if zero_width:
        for node in nodes:
            if node['op'] == 'union_all':
                node['output'] = []
                for item in node['inputs']:
                    item['columns'] = []
        extra.append({'id': 'unit', 'op': 'project', 'input': previous, 'ordered': False,
                      'columns': [{'output': '_unit', 'expression': {'kind': 'void'}}]})
        previous = 'unit'
    extra.append({'id': 'count', 'op': 'aggregate', 'input': previous, 'keys': [],
                  'aggregates': [{'input': '_unit' if zero_width else 'u.k', 'function': 'count',
                                  'output': 'n', 'type': 'Uint64', 'nullable': False,
                                  'distinct': False, 'unwrap': False}],
                  'phase': 'undefined', 'distinct_all': False})
    nodes.extend(extra)
    raw['plan'].update(root='count', output=['n'])
    if staged:
        stage = raw['stage_graph']['stages'][-1]
        stage['nodes'].extend(node['id'] for node in extra)
        stage['outputs'] = [{'index': 0, 'node': 'count'}]
    return raw


class ZeroWidthUnionValidationTest(unittest.TestCase):
    def test_empty_output_still_requires_matching_input_arity(self):
        raw = _counted_snapshot(zero_width=True)
        node = next(node for node in raw['plan']['nodes'] if node['op'] == 'union_all')
        node['inputs'][-1]['columns'] = ['t2.k']
        with self.assertRaisesRegex(SnapshotError, 'every union input must match the output arity'):
            parse_snapshot(raw)

    def test_empty_output_still_requires_reachable_inputs(self):
        raw = _counted_snapshot(zero_width=True)
        node = next(node for node in raw['plan']['nodes'] if node['op'] == 'union_all')
        node['inputs'][-1]['node'] = 'missing'
        with self.assertRaises(SnapshotError):
            parse_snapshot(raw)

    def test_private_count_carrier_cannot_escape_as_a_result(self):
        raw = _counted_snapshot(zero_width=True)
        raw['plan']['nodes'].pop()
        raw['plan'].update(root='unit', output=['_unit'])
        with self.assertRaises(SnapshotError):
            parse_snapshot(raw)


@unittest.skipUnless(SOLVER, 'requires the pinned solver')
class ZeroWidthUnionProofTest(unittest.TestCase):
    def test_pruning_all_union_columns_keeps_the_count(self):
        for count in (2, 3, 4):
            for ordered in (False, True):
                for nested in (False, True):
                    for staged in (False, True):
                        before = _counted_snapshot(count, nested=not nested, ordered=ordered)
                        after = _counted_snapshot(count, nested=nested, ordered=ordered,
                                                  staged=staged, zero_width=True)
                        with self.subTest(count=count, ordered=ordered, nested=nested, staged=staged):
                            problem = build_transformation_prefix_problem(
                                parse_snapshot(before), parse_snapshot(after), 2)
                            self.assertEqual(query_solver(problem, SOLVER, timeout_ms=10_000).status, 'unsat')

    def test_replacing_a_branch_changes_the_count_even_without_columns(self):
        for ordered in (False, True):
            for staged in (False, True):
                before = _counted_snapshot(ordered=ordered)
                after = _counted_snapshot(ordered=ordered, staged=staged, zero_width=True)
                # Preserve the graph and input occurrences, but read the first table twice.
                after['plan']['nodes'][2]['table'] = 't0'
                with self.subTest(ordered=ordered, staged=staged):
                    problem = build_transformation_prefix_problem(parse_snapshot(before), parse_snapshot(after), 2)
                    self.assertEqual(query_solver(problem, SOLVER, timeout_ms=10_000).status, 'sat')


if __name__ == '__main__':
    unittest.main()
