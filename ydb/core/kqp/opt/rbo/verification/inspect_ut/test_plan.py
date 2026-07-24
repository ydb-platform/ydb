import unittest
from dataclasses import dataclass, replace

from ydb.core.kqp.opt.rbo.verification.inspector.plan import (
    InspectionError,
    render_edge,
    render_expression,
    render_node,
    render_snapshot,
    snapshot_digest,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import decimal, ir


def _column(name="x"):
    return ir.Expr(kind="column", column=name)


def _literal(value=1, scalar_type="Int64"):
    return ir.Expr(kind="literal", value=value, result_type=scalar_type, nullable=False)


def _u64(value):
    return _literal(value, "Uint64")


class ExpressionRendererTest(unittest.TestCase):
    def test_every_expression_variant_has_explicit_stable_text(self):
        one = _literal()
        two = _literal(2)
        cases = (
            (_column('a."x"'), 'column("a.\\\"x\\\"")'),
            (ir.Expr(kind="bound", depth=2), "bound(depth=2)"),
            (ir.Expr(kind="void", result_type="Void", nullable=False), "void()"),
            (
                _literal("line\n", "String"),
                'literal(type="String", value="line\\n")',
            ),
            (
                _literal(decimal.Literal(decimal.FINITE, -123), "Decimal(5,2)"),
                'literal(type="Decimal(5,2)", '
                'value={"kind": "finite", "scaled": "-123"})',
            ),
            (
                ir.Expr(kind="null", result_type="Int64", nullable=True),
                'null(type="Int64")',
            ),
            (
                ir.Expr(kind="and", args=(one, two)),
                'and(args=[literal(type="Int64", value=1), '
                'literal(type="Int64", value=2)])',
            ),
            (ir.Expr(kind="or", args=(one,)), 'or(args=[literal(type="Int64", value=1)])'),
            (ir.Expr(kind="not", args=(one,)), 'not(arg=literal(type="Int64", value=1))'),
            (ir.Expr(kind="exists", args=(_column(),)), 'exists(arg=column("x"))'),
            (
                ir.Expr(kind="in", args=(_column("x"), one, two)),
                'in(lookup=column("x"), items=[literal(type="Int64", value=1), '
                'literal(type="Int64", value=2)])',
            ),
            (
                ir.Expr(kind="eq", args=(one, two), null_safe=True),
                'eq(left=literal(type="Int64", value=1), '
                'right=literal(type="Int64", value=2), null_safe=true)',
            ),
            (
                ir.Expr(kind="lt", args=(one, two)),
                'lt(left=literal(type="Int64", value=1), '
                'right=literal(type="Int64", value=2))',
            ),
            (
                ir.Expr(kind="lte", args=(one, two)),
                'lte(left=literal(type="Int64", value=1), '
                'right=literal(type="Int64", value=2))',
            ),
            (
                ir.Expr(kind="gt", args=(one, two)),
                'gt(left=literal(type="Int64", value=1), '
                'right=literal(type="Int64", value=2))',
            ),
            (
                ir.Expr(kind="gte", args=(one, two)),
                'gte(left=literal(type="Int64", value=1), '
                'right=literal(type="Int64", value=2))',
            ),
            (
                ir.Expr(kind="add", args=(one, two), result_type="Int64", nullable=False),
                'add(left=literal(type="Int64", value=1), '
                'right=literal(type="Int64", value=2), type="Int64", nullable=false)',
            ),
            (
                ir.Expr(kind="sub", args=(one, two), result_type="Int64", nullable=False),
                'sub(left=literal(type="Int64", value=1), '
                'right=literal(type="Int64", value=2), type="Int64", nullable=false)',
            ),
            (
                ir.Expr(kind="mul", args=(one, two), result_type="Int64", nullable=False),
                'mul(left=literal(type="Int64", value=1), '
                'right=literal(type="Int64", value=2), type="Int64", nullable=false)',
            ),
            (
                ir.Expr(
                    kind="div",
                    args=(one, two),
                    result_type="Decimal(15,4)",
                    nullable=False,
                ),
                'div(left=literal(type="Int64", value=1), '
                'right=literal(type="Int64", value=2), '
                'type="Decimal(15,4)", nullable=false)',
            ),
            (
                ir.Expr(
                    kind="cast_decimal",
                    args=(_column(),),
                    source_type="Int64",
                    result_type="Decimal(15,4)",
                    nullable=False,
                ),
                'cast_decimal(arg=column("x"), source_type="Int64", '
                'type="Decimal(15,4)", nullable=false)',
            ),
            (
                ir.Expr(
                    kind="cast_integral",
                    args=(_column(),),
                    result_type="Int32",
                    nullable=True,
                ),
                'cast_integral(arg=column("x"), type="Int32", nullable=true)',
            ),
            (
                ir.Expr(
                    kind="if",
                    args=(
                        _column("condition"),
                        _literal(1),
                        _literal(0),
                    ),
                    result_type="Int64",
                    nullable=True,
                ),
                'if(condition=column("condition"), '
                'then=literal(type="Int64", value=1), '
                'else=literal(type="Int64", value=0), '
                'type="Int64", nullable=true)',
            ),
            (
                ir.Expr(
                    kind="if_present",
                    args=(
                        _column("optional"),
                        ir.Expr(kind="bound", depth=0),
                        _literal(0),
                    ),
                    result_type="Int64",
                    nullable=False,
                ),
                'if_present(optional=column("optional"), present=bound(depth=0), '
                'missing=literal(type="Int64", value=0), '
                'type="Int64", nullable=false)',
            ),
            (
                ir.Expr(
                    kind="opaque",
                    args=(_column(),),
                    result_type="Bool",
                    nullable=True,
                    fingerprint="f($0)",
                ),
                'opaque(fingerprint="f($0)", type="Bool", nullable=true, '
                'args=[column("x")])',
            ),
        )
        for expression, expected in cases:
            with self.subTest(kind=expression.kind):
                self.assertEqual(render_expression(expression), expected)

    def test_unknown_or_malformed_expression_fails_closed(self):
        with self.assertRaisesRegex(InspectionError, "unknown expression kind"):
            render_expression(ir.Expr(kind="future"))
        with self.assertRaisesRegex(InspectionError, "exactly two arguments"):
            render_expression(ir.Expr(kind="eq", args=(_literal(),)))
        with self.assertRaisesRegex(InspectionError, "between 1 and 512 items"):
            render_expression(ir.Expr(kind="in", args=(_column(),)))
        with self.assertRaisesRegex(InspectionError, "exactly one argument"):
            render_expression(ir.Expr(kind="cast_decimal", args=()))
        with self.assertRaisesRegex(InspectionError, "source_type"):
            render_expression(
                ir.Expr(
                    kind="cast_decimal",
                    args=(_literal(),),
                    result_type="Decimal(15,4)",
                    nullable=False,
                )
            )
        with self.assertRaisesRegex(InspectionError, "exactly one argument"):
            render_expression(ir.Expr(kind="cast_integral", args=()))
        with self.assertRaisesRegex(InspectionError, "exactly one argument"):
            render_expression(ir.Expr(kind="exists", args=()))
        with self.assertRaisesRegex(InspectionError, "non-negative integer"):
            render_expression(ir.Expr(kind="bound", depth=-1))
        with self.assertRaisesRegex(InspectionError, "exactly three arguments"):
            render_expression(ir.Expr(kind="if", args=()))
        with self.assertRaisesRegex(InspectionError, "exactly three arguments"):
            render_expression(ir.Expr(kind="if_present", args=()))
        with self.assertRaisesRegex(InspectionError, "not Boolean"):
            render_expression(
                ir.Expr(
                    kind="if_present",
                    args=(_column(), _literal(), _literal()),
                    result_type="Int64",
                    nullable=0,
                )
            )


class OperatorRendererTest(unittest.TestCase):
    def test_every_operator_variant_has_explicit_stable_text(self):
        predicate = ir.Expr(kind="eq", args=(_column("a.k"), _literal()), null_safe=False)
        order = (ir.SortOrder("a.k", ascending=False, nulls_first=True),)
        nodes = (
            (ir.EmptySource("empty"), 'node "empty" empty_source'),
            (
                ir.Scan(
                    "scan",
                    "A",
                    (ir.ScanColumn("k", "a.k"),),
                    predicate,
                    _u64(3),
                ),
                'node "scan" scan table="A" '
                'columns=[{source="k", output="a.k"}] '
                'predicate=eq(left=column("a.k"), '
                'right=literal(type="Int64", value=1), null_safe=false) '
                'pushed_limit=literal(type="Uint64", value=3)',
            ),
            (
                ir.Project(
                    "project",
                    "scan",
                    (ir.Projection("out", _column("a.k")),),
                    True,
                ),
                'node "project" project input="scan" '
                'columns=[{output="out", expression=column("a.k")}] ordered=true',
            ),
            (
                ir.Filter("filter", "scan", predicate),
                'node "filter" filter input="scan" '
                'predicate=eq(left=column("a.k"), '
                'right=literal(type="Int64", value=1), null_safe=false)',
            ),
            (
                ir.OuterBind(
                    "outer_bind",
                    "scan",
                    "outer.k",
                    "Int64",
                    True,
                ),
                'node "outer_bind" outer_bind input="scan" '
                'dependency="outer.k" type="Int64" nullable=true',
            ),
            (
                ir.Limit("limit", "scan", _u64(2), None, "final"),
                'node "limit" limit input="scan" '
                'count=literal(type="Uint64", value=2) offset=none phase=final '
                'ensure_at_most_one=false',
            ),
            (
                ir.Sort("sort", "scan", order, _u64(5), "intermediate"),
                'node "sort" sort input="scan" '
                'order=[{column="a.k", direction=desc, nulls=first}] '
                'limit=literal(type="Uint64", value=5) phase=intermediate',
            ),
            (
                ir.Aggregate(
                    "aggregate",
                    "scan",
                    ("a.k",),
                    (
                        ir.AggregateTrait(
                            "a.k", "count", "count", "Uint64", False, False, True
                        ),
                    ),
                    "undefined",
                    False,
                ),
                'node "aggregate" aggregate input="scan" keys=["a.k"] '
                'aggregates=[{input="a.k", function="count", output="count", '
                'type="Uint64", nullable=false, distinct=false, unwrap=true, '
                'state=none}] '
                'phase=undefined distinct_all=false',
            ),
            (
                ir.Aggregate(
                    "average",
                    "scan",
                    (),
                    (
                        ir.AggregateTrait(
                            "a.k",
                            "avg",
                            "average",
                            "Decimal(7,2)",
                            True,
                            False,
                            False,
                            ir.AverageStateType(
                                "Decimal(35,2)",
                                "Uint64",
                                True,
                            ),
                        ),
                    ),
                    "intermediate",
                    False,
                ),
                'node "average" aggregate input="scan" keys=[] '
                'aggregates=[{input="a.k", function="avg", output="average", '
                'type="Decimal(7,2)", nullable=true, distinct=false, '
                'unwrap=false, state={sum_type="Decimal(35,2)", '
                'count_type="Uint64", nullable=true}}] '
                'phase=intermediate distinct_all=false',
            ),
            (
                ir.Join(
                    "join",
                    "left",
                    "right",
                    "inner",
                    (ir.JoinKey("a.k", "b.k"),),
                    predicate,
                ),
                'node "join" join left="left" right="right" kind=inner '
                'keys=[{left="a.k", right="b.k"}] '
                'predicate=eq(left=column("a.k"), '
                'right=literal(type="Int64", value=1), null_safe=false)',
            ),
            (
                ir.UnionAll(
                    "union",
                    (
                        ir.UnionInput("left", ("l.k",)),
                        ir.UnionInput("right", ("r.k",)),
                    ),
                    ("k",),
                    False,
                ),
                'node "union" union_all '
                'inputs=[{node="left", columns=["l.k"]}, '
                '{node="right", columns=["r.k"]}] output=["k"] ordered=false',
            ),
        )
        for node, expected in nodes:
            with self.subTest(node=node.id):
                self.assertEqual(render_node(node), expected)

        @dataclass(frozen=True)
        class FutureNode:
            id: str

        with self.assertRaisesRegex(InspectionError, "unknown plan node class"):
            render_node(FutureNode("future"))  # type: ignore[arg-type]


class StageEdgeRendererTest(unittest.TestCase):
    def test_every_connection_variant_has_explicit_stable_text(self):
        common = dict(
            id="edge",
            producer="left",
            consumer="root",
            occurrence=1,
            producer_output=2,
            consumer_input=3,
        )
        prefix = (
            'edge "edge" producer="left" producer_output=2 consumer="root" '
            'consumer_input=3 occurrence=1 kind='
        )
        cases = (
            (ir.StageEdge(**common, kind="map"), prefix + "map"),
            (ir.StageEdge(**common, kind="broadcast"), prefix + "broadcast"),
            (
                ir.StageEdge(
                    **common,
                    kind="hash_shuffle",
                    keys=("k", "x"),
                    hash_function="HashV2",
                    use_spilling=True,
                ),
                prefix
                + 'hash_shuffle keys=["k", "x"] hash_function="HashV2" '
                'use_spilling=true',
            ),
            (
                ir.StageEdge(**common, kind="union_all", parallel=False),
                prefix + "union_all parallel=false",
            ),
            (
                ir.StageEdge(
                    **common,
                    kind="merge",
                    order=(ir.SortOrder("k", True, False),),
                ),
                prefix + 'merge order=[{column="k", direction=asc, nulls=last}]',
            ),
        )
        for edge, expected in cases:
            with self.subTest(kind=edge.kind):
                self.assertEqual(render_edge(edge), expected)

        with self.assertRaisesRegex(InspectionError, "unknown StageGraph connection kind"):
            render_edge(ir.StageEdge(**common, kind="future"))


def _stage_snapshot():
    tables = (
        ir.Table(
            "A",
            (ir.Column("k", "Int64", False),),
            (ir.UniqueKey(("k",), False),),
        ),
        ir.Table("B", (ir.Column("k", "Int64", True),), ()),
    )
    nodes = (
        ir.Scan("a", "A", (ir.ScanColumn("k", "a.k"),), None, None),
        ir.Scan("b", "B", (ir.ScanColumn("k", "b.k"),), None, None),
        ir.Join(
            "join",
            "a",
            "b",
            "inner",
            (ir.JoinKey("a.k", "b.k"),),
            ir.Expr(kind="eq", args=(_column("a.k"), _column("b.k")), null_safe=False),
        ),
    )
    stages = (
        ir.Stage("sa", ("a",), (), (ir.StageOutput(0, "a"),), "column"),
        ir.Stage("sb", ("b",), (), (ir.StageOutput(0, "b"),), "row"),
        ir.Stage(
            "root",
            ("join",),
            ("a", "b"),
            (ir.StageOutput(0, "join"),),
            None,
        ),
    )
    edges = (
        ir.StageEdge("broadcast", "sa", "root", 0, 0, 0, "broadcast"),
        ir.StageEdge(
            "hash",
            "sb",
            "root",
            0,
            0,
            1,
            "hash_shuffle",
            keys=("b.k",),
            hash_function="HashV1",
            use_spilling=False,
        ),
    )
    return ir.Snapshot(
        tables,
        ir.Plan(nodes, "join", ("b.k", "a.k"), ()),
        ir.StageGraph("root", stages, edges),
    )


class SnapshotRendererTest(unittest.TestCase):
    def test_complete_stage_snapshot_is_deterministic_and_exact(self):
        snapshot = _stage_snapshot()
        expected = """semantic_snapshot format="ydb-rbo-semantic-snapshot" version=1
schema tables=2
  table "A" columns=[{name="k", type="Int64", not_null}] unique_keys=[{columns=["k"], nulls_distinct=false}]
  table "B" columns=[{name="k", type="Int64", nullable}] unique_keys=[]
plan root="join" output=["b.k", "a.k"] subplans=0
  output_schema=[{name="b.k", type="Int64", nullable}, {name="a.k", type="Int64", not_null}]
  node "a" scan table="A" columns=[{source="k", output="a.k"}] predicate=none pushed_limit=none
  node "b" scan table="B" columns=[{source="k", output="b.k"}] predicate=none pushed_limit=none
  node "join" join left="a" right="b" kind=inner keys=[{left="a.k", right="b.k"}] predicate=eq(left=column("a.k"), right=column("b.k"), null_safe=false)
stage_graph root_stage="root" stages=3 edges=2 assumptions=[]
  stage "sa" tasks=2 source_storage="column" nodes=["a"] inputs=[] outputs=[{index=0, node="a"}]
  stage "sb" tasks=2 source_storage="row" nodes=["b"] inputs=[] outputs=[{index=0, node="b"}]
  stage "root" tasks=2 source_storage=none nodes=["join"] inputs=["a", "b"] outputs=[{index=0, node="join"}]
  edge "broadcast" producer="sa" producer_output=0 consumer="root" consumer_input=0 occurrence=0 kind=broadcast
  edge "hash" producer="sb" producer_output=0 consumer="root" consumer_input=1 occurrence=0 kind=hash_shuffle keys=["b.k"] hash_function="HashV1" use_spilling=false
"""
        self.assertEqual(render_snapshot(snapshot), expected)
        self.assertEqual(render_snapshot(snapshot), render_snapshot(snapshot))

    def test_absent_stage_graph_is_explicit(self):
        rendered = render_snapshot(replace(_stage_snapshot(), stage_graph=None))
        self.assertTrue(rendered.endswith("stage_graph none\n"))

    def test_scalar_subplan_descriptor_is_explicit_and_digest_sensitive(self):
        nodes = (
            ir.EmptySource("main_source"),
            ir.Project(
                "main_project",
                "main_source",
                (ir.Projection("result", _column("$scalar")),),
                False,
            ),
            ir.EmptySource("sub_source"),
            ir.Project(
                "sub_value",
                "sub_source",
                (ir.Projection("sub.value", _literal(7)),),
                False,
            ),
        )
        descriptor = ir.ScalarSubplan(
            binding="$scalar",
            root="sub_value",
            output=ir.SubplanOutput("sub.value", "Int64", False),
            consumers=("main_project",),
        )
        snapshot = ir.Snapshot(
            (),
            ir.Plan(nodes, "main_project", ("result",), (descriptor,)),
            None,
        )
        rendered = render_snapshot(snapshot)
        self.assertIn(
            'plan root="main_project" output=["result"] subplans=1',
            rendered,
        )
        self.assertIn(
            'subplan binding="$scalar" kind=scalar root="sub_value" '
            'output={column="sub.value", type="Int64", nullable=false} '
            'type="Int64" nullable=true dependencies=[] '
            'consumers=["main_project"]',
            rendered,
        )
        without_subplan = ir.Snapshot(
            (),
            ir.Plan(nodes[:2], "main_project", ("result",), ()),
            None,
        )
        with self.assertRaisesRegex(ir.SnapshotError, "not available"):
            snapshot_digest(without_subplan)
        renamed_nodes = (
            nodes[0],
            replace(
                nodes[1],
                columns=(ir.Projection("result", _column("$renamed")),),
            ),
            *nodes[2:],
        )
        renamed = ir.Snapshot(
            (),
            ir.Plan(
                renamed_nodes,
                "main_project",
                ("result",),
                (replace(descriptor, binding="$renamed"),),
            ),
            None,
        )
        self.assertNotEqual(snapshot_digest(snapshot), snapshot_digest(renamed))

    def test_exists_subplan_descriptor_is_explicit(self):
        nodes = (
            ir.EmptySource("main_source"),
            ir.Filter("main_filter", "main_source", _column("$exists")),
            ir.Project(
                "main_project",
                "main_filter",
                (ir.Projection("result", _literal(1)),),
                False,
            ),
            ir.EmptySource("sub_source"),
        )
        descriptor = ir.ExistsSubplan(
            binding="$exists",
            root="sub_source",
            predicate=None,
            dependency=None,
            consumers=("main_filter",),
        )
        snapshot = ir.Snapshot(
            (),
            ir.Plan(nodes, "main_project", ("result",), (descriptor,)),
            None,
        )

        rendered = render_snapshot(snapshot)

        self.assertIn(
            'subplan binding="$exists" kind=exists root="sub_source" '
            'predicate=none type="Bool" nullable=false dependencies=[] '
            'consumers=["main_filter"]',
            rendered,
        )

        correlated_nodes = (
            ir.EmptySource("main_unit"),
            ir.Project(
                "outer",
                "main_unit",
                (ir.Projection("outer.x", _literal()),),
                False,
            ),
            ir.Filter("main_filter", "outer", _column("$exists")),
            ir.EmptySource("sub_unit"),
            ir.Project(
                "inner",
                "sub_unit",
                (ir.Projection("inner.x", _literal()),),
                False,
            ),
        )
        correlation = ir.Expr(
            kind="eq",
            args=(_column("outer.x"), _column("inner.x")),
            null_safe=False,
        )
        correlated_descriptor = ir.ExistsSubplan(
            binding="$exists",
            root="inner",
            predicate=correlation,
            dependency="outer.x",
            consumers=("main_filter",),
        )
        correlated = ir.Snapshot(
            (),
            ir.Plan(
                correlated_nodes,
                "main_filter",
                ("outer.x",),
                (correlated_descriptor,),
            ),
            None,
        )
        correlated_rendering = render_snapshot(correlated)
        self.assertIn(
            'predicate=eq(left=column("outer.x"), right=column("inner.x"), '
            'null_safe=false) type="Bool" nullable=false '
            'dependencies=["outer.x"]',
            correlated_rendering,
        )
        reversed_correlation = replace(
            correlated_descriptor,
            predicate=replace(
                correlation,
                args=tuple(reversed(correlation.args)),
            ),
        )
        self.assertNotEqual(
            snapshot_digest(correlated),
            snapshot_digest(
                replace(
                    correlated,
                    plan=replace(
                        correlated.plan,
                        subplans=(reversed_correlation,),
                    ),
                )
            ),
        )

    def test_in_subplan_descriptor_is_explicit_and_digest_sensitive(self):
        nodes = (
            ir.EmptySource("main_unit"),
            ir.Project(
                "outer",
                "main_unit",
                (
                    ir.Projection("outer.lookup", _literal(1)),
                    ir.Projection("outer.other", _literal(2)),
                ),
                False,
            ),
            ir.Filter("main_filter", "outer", _column("$in")),
            ir.Project(
                "main_project",
                "main_filter",
                (ir.Projection("result", _column("outer.lookup")),),
                False,
            ),
            ir.EmptySource("sub_unit"),
            ir.Project(
                "sub_value",
                "sub_unit",
                (
                    ir.Projection("inner.value", _literal(3)),
                    ir.Projection("inner.other", _literal(4)),
                ),
                False,
            ),
        )
        descriptor = ir.InSubplan(
            binding="$in",
            root="sub_value",
            lookup=ir.SubplanOutput("outer.lookup", "Int64", False),
            output=ir.SubplanOutput("inner.value", "Int64", False),
            consumers=("main_filter",),
        )
        snapshot = ir.Snapshot(
            (),
            ir.Plan(nodes, "main_project", ("result",), (descriptor,)),
            None,
        )

        rendered = render_snapshot(snapshot)

        self.assertIn(
            'subplan binding="$in" kind=in root="sub_value" '
            'lookup={column="outer.lookup", type="Int64", nullable=false} '
            'output={column="inner.value", type="Int64", nullable=false} '
            'type="Bool" nullable=false dependencies=[] '
            'consumers=["main_filter"]',
            rendered,
        )
        self.assertNotEqual(
            snapshot_digest(snapshot),
            snapshot_digest(
                replace(
                    snapshot,
                    plan=replace(
                        snapshot.plan,
                        subplans=(
                            replace(
                                descriptor,
                                lookup=ir.SubplanOutput(
                                    "outer.other",
                                    "Int64",
                                    False,
                                ),
                            ),
                        ),
                    ),
                )
            ),
        )
        self.assertNotEqual(
            snapshot_digest(snapshot),
            snapshot_digest(
                replace(
                    snapshot,
                    plan=replace(
                        snapshot.plan,
                        subplans=(
                            replace(
                                descriptor,
                                output=ir.SubplanOutput(
                                    "inner.other",
                                    "Int64",
                                    False,
                                ),
                            ),
                        ),
                    ),
                )
            ),
        )

    def test_semantic_digest_is_stable_and_field_sensitive(self):
        snapshot = _stage_snapshot()
        digest = snapshot_digest(snapshot)
        self.assertEqual(len(digest), 64)
        self.assertEqual(digest, snapshot_digest(snapshot))
        self.assertNotEqual(digest, snapshot_digest(replace(snapshot, stage_graph=None)))

        join = snapshot.plan.nodes[-1]
        self.assertIsInstance(join, ir.Join)
        changed_join = replace(
            join,
            keys=(),
        )
        changed_plan = replace(
            snapshot.plan,
            nodes=(*snapshot.plan.nodes[:-1], changed_join),
        )
        self.assertNotEqual(
            digest,
            snapshot_digest(replace(snapshot, plan=changed_plan)),
        )


if __name__ == "__main__":
    unittest.main()
