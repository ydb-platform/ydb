import copy
import io
import os
import subprocess
import unittest
from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from itertools import product
from unittest import mock

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Column,
    Expr,
    OPAQUE_DOUBLE_FINGERPRINT_PREFIX,
    SnapshotError,
    SortOrder,
    parse_snapshot,
    stage_task_counts,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import cli, decimal
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import relation as relation_model
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import stages as stage_model
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import verify as verifier
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Database,
    Evaluator as RelationEvaluator,
    RelationError,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import (
    Encoder as ScalarEncoder,
    IntegralAverageCertificate,
    IntegralAverageState,
    Value,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.stages import (
    Evaluator as StageEvaluator,
    Router,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.types import (
    INTEGER_TYPES,
    integer_bounds,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    Problem,
    SchemaMismatch,
    SolverError,
    VerificationError,
    build_logical_kernel_problem_for_tests,
    build_problem,
    build_transformation_prefix_problem,
    solve,
)


SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None
    else os.environ.get("RBO_Z3")
)
REFERENCE_DECIMAL_INF = 10**35
REFERENCE_DECIMAL_NAN = REFERENCE_DECIMAL_INF + 1


def schema():
    return {
        "tables": [
            {
                "name": "A",
                "columns": [
                    {"name": "k", "type": "Int64", "nullable": False},
                    {"name": "x", "type": "Int64", "nullable": False},
                ],
                "unique_keys": [],
            },
            {
                "name": "B",
                "columns": [
                    {"name": "k", "type": "Int64", "nullable": False},
                    {"name": "x", "type": "Int64", "nullable": False},
                ],
                "unique_keys": [],
            },
        ]
    }


SCAN_A = {
    "id": "a",
    "op": "scan",
    "table": "A",
    "columns": [
        {"source": "k", "output": "a.k"},
        {"source": "x", "output": "a.x"},
    ],
    "pushed_limit": None,
}
SCAN_B = {
    "id": "b",
    "op": "scan",
    "table": "B",
    "columns": [
        {"source": "k", "output": "b.k"},
        {"source": "x", "output": "b.x"},
    ],
    "pushed_limit": None,
}
KEY_EQUALITY = {
    "kind": "eq",
    "left": {"kind": "column", "column": "a.k"},
    "right": {"kind": "column", "column": "b.k"},
}
RESIDUAL = {
    "kind": "opaque",
    "fingerprint": "greater_than($0,$1)",
    "type": "Bool",
    "nullable": False,
    "args": [
        {"kind": "column", "column": "a.x"},
        {"kind": "column", "column": "b.x"},
    ],
}


def _stage_schema(*names):
    available = {table["name"]: table for table in schema()["tables"]}
    return {"tables": [copy.deepcopy(available[name]) for name in names]}


def _stage(
    stage_id,
    nodes,
    inputs,
    outputs,
    source_storage=None,
):
    return {
        "id": stage_id,
        "nodes": nodes,
        "inputs": inputs,
        "outputs": [
            {"index": index, "node": node}
            for index, node in enumerate(outputs)
        ],
        "source_storage": source_storage,
    }


def _edge(
    edge_id,
    producer,
    consumer,
    producer_output,
    consumer_input,
    kind,
    occurrence=0,
    **settings,
):
    return {
        "id": edge_id,
        "producer": producer,
        "consumer": consumer,
        "occurrence": occurrence,
        "producer_output": producer_output,
        "consumer_input": consumer_input,
        "kind": kind,
        **settings,
    }


def _snapshot_with_stage_graph(schema_value, nodes, root, output, stages=None, edges=None):
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": schema_value,
        "plan": {
            "nodes": nodes,
            "root": root,
            "output": output,
            "subplans": [],
        },
        "stage_graph": None if stages is None else {
            "root_stage": stages[-1]["id"],
            "stages": stages,
            "edges": edges,
            "assumptions": [],
        },
    }


def _set_connection(edge, connection):
    common = {
        name: edge[name]
        for name in (
            "id",
            "producer",
            "consumer",
            "occurrence",
            "producer_output",
            "consumer_input",
        )
    }
    edge.clear()
    edge.update(common, **connection)


def passthrough_stage_snapshot(connection=None):
    project = {
        "id": "project",
        "op": "project",
        "input": "a",
        "ordered": False,
        "columns": [
            {
                "output": "result",
                "expression": {"kind": "column", "column": "a.k"},
            }
        ],
    }
    nodes = [copy.deepcopy(SCAN_A), project]
    if connection is None:
        return parse_snapshot(
            _snapshot_with_stage_graph(_stage_schema("A"), nodes, "project", ["result"])
        )
    return parse_snapshot(
        _snapshot_with_stage_graph(
            _stage_schema("A"),
            nodes,
            "project",
            ["result"],
            [
                _stage("source", ["a"], [], ["a"], "row"),
                _stage("consumer", ["project"], ["a"], ["project"]),
            ],
            [_edge("edge", "source", "consumer", 0, 0, **connection)],
        )
    )


def local_join_stage_snapshot(left_connection=None, right_connection=None):
    join = {
        "id": "join",
        "op": "join",
        "left": "a",
        "right": "b",
        "kind": "inner",
        "predicate": copy.deepcopy(KEY_EQUALITY),
    }
    nodes = [copy.deepcopy(SCAN_A), copy.deepcopy(SCAN_B), join]
    if left_connection is None:
        return parse_snapshot(
            _snapshot_with_stage_graph(
                _stage_schema("A", "B"),
                nodes,
                "join",
                ["a.k", "b.k"],
            )
        )
    assert right_connection is not None
    return parse_snapshot(
        _snapshot_with_stage_graph(
            _stage_schema("A", "B"),
            nodes,
            "join",
            ["a.k", "b.k"],
            [
                _stage("left", ["a"], [], ["a"], "row"),
                _stage("right", ["b"], [], ["b"], "row"),
                _stage("join_stage", ["join"], ["a", "b"], ["join"]),
            ],
            [
                _edge("left_edge", "left", "join_stage", 0, 0, **left_connection),
                _edge("right_edge", "right", "join_stage", 0, 1, **right_connection),
            ],
        )
    )


def shared_name_semi_join_stage_snapshot(kind):
    schema_value = {
        "tables": [
            {
                "name": table,
                "columns": [
                    {"name": "k", "type": "Int64", "nullable": True},
                ],
                "unique_keys": [],
            }
            for table in ("A", "B")
        ]
    }
    left = {
        "id": "left",
        "op": "scan",
        "table": "A",
        "columns": [{"source": "k", "output": "shared.k"}],
        "pushed_limit": None,
    }
    right = {
        "id": "right",
        "op": "scan",
        "table": "B",
        "columns": [{"source": "k", "output": "shared.k"}],
        "pushed_limit": None,
    }
    join = {
        "id": "join",
        "op": "join",
        "left": "left",
        "right": "right",
        "kind": kind,
        "keys": [{"left": "shared.k", "right": "shared.k"}],
        "predicate": {
            "kind": "literal",
            "type": "Bool",
            "value": True,
        },
    }
    shuffle = {
        "kind": "hash_shuffle",
        "keys": ["shared.k"],
        "hash_function": "HashV1",
        "use_spilling": False,
    }
    return parse_snapshot(
        _snapshot_with_stage_graph(
            schema_value,
            [left, right, join],
            "join",
            ["shared.k"],
            [
                _stage("left_source", ["left"], [], ["left"], "row"),
                _stage("right_source", ["right"], [], ["right"], "row"),
                _stage("join_stage", ["join"], ["left", "right"], ["join"]),
            ],
            [
                _edge(
                    "left_edge",
                    "left_source",
                    "join_stage",
                    0,
                    0,
                    **shuffle,
                ),
                _edge(
                    "right_edge",
                    "right_source",
                    "join_stage",
                    0,
                    1,
                    **shuffle,
                ),
            ],
        )
    )


def duplicate_edge_stage_value():
    union = {
        "id": "union",
        "op": "union_all",
        "inputs": [
            {"node": "a", "columns": ["a.k"]},
            {"node": "a", "columns": ["a.k"]},
        ],
        "output": ["result"],
        "ordered": False,
    }
    return _snapshot_with_stage_graph(
        _stage_schema("A"),
        [copy.deepcopy(SCAN_A), union],
        "union",
        ["result"],
        [
            _stage("source", ["a"], [], ["a", "a"], "row"),
            _stage("consumer", ["union"], ["a", "a"], ["union"]),
        ],
        [
            _edge("first", "source", "consumer", 0, 0, "map", occurrence=0),
            _edge(
                "second",
                "source",
                "consumer",
                1,
                1,
                "union_all",
                occurrence=1,
                parallel=True,
            ),
        ],
    )


def serial_then_parallel_stage_snapshot(staged):
    gather = {
        "id": "gather",
        "op": "project",
        "input": "a",
        "ordered": False,
        "columns": [
            {
                "output": "middle",
                "expression": {"kind": "column", "column": "a.k"},
            }
        ],
    }
    project = {
        "id": "project",
        "op": "project",
        "input": "gather",
        "ordered": False,
        "columns": [
            {
                "output": "result",
                "expression": {"kind": "column", "column": "middle"},
            }
        ],
    }
    stages = None
    edges = None
    if staged:
        stages = [
            _stage("source", ["a"], [], ["a"], "row"),
            _stage("gather_stage", ["gather"], ["a"], ["gather"]),
            _stage("root", ["project"], ["gather"], ["project"]),
        ]
        edges = [
            _edge(
                "gather_edge",
                "source",
                "gather_stage",
                0,
                0,
                "union_all",
                parallel=False,
            ),
            _edge(
                "parallel_edge",
                "gather_stage",
                "root",
                0,
                0,
                "union_all",
                parallel=True,
            ),
        ]
    return parse_snapshot(
        _snapshot_with_stage_graph(
            _stage_schema("A"),
            [copy.deepcopy(SCAN_A), gather, project],
            "project",
            ["result"],
            stages,
            edges,
        )
    )


def aggregate_stage_snapshot(
    function,
    grouped,
    staged,
    final_function=None,
    final_phase="final",
    nullable_input=False,
    nullable_key=False,
    shuffle_key="a.k",
    input_type="Int64",
    aggregate_input="a.x",
    project_average_away=False,
    duplicate_input=False,
):
    if duplicate_input and (not staged or grouped):
        raise ValueError(
            "duplicate aggregate input is supported only by the staged "
            "keyless fixture"
        )
    decimal_sum_type = decimal.sum_type(input_type)
    integral_average = (
        function == "avg"
        and input_type == "Int64"
        and nullable_input
    )
    if function == "count":
        output_type = "Uint64"
    elif function in {"avg", "max", "min"}:
        if function == "avg" and decimal_sum_type is None and not integral_average:
            raise ValueError("the aggregate test fixture only models Decimal avg")
        output_type = "Double" if integral_average else input_type
    elif input_type.startswith("Uint"):
        output_type = "Uint64"
    else:
        output_type = decimal_sum_type or "Int64"
    final_function = final_function or (
        "sum" if staged and function == "count" else function
    )
    keys = ["a.k"] if grouped else []
    nullable_aggregate = function in {"avg", "max", "min", "sum"}
    logical_nullable = nullable_aggregate and (nullable_input or not grouped)
    trait = {
        "input": aggregate_input,
        "function": function,
        "output": "result",
        "type": output_type,
        "nullable": logical_nullable,
        "distinct": False,
        "unwrap": False,
    }
    if function == "avg":
        trait["state"] = (
            {
                "kind": "integral_double_v1",
                "source_type": "Int64",
                "sum_type": "Double",
                "count_type": "Uint64",
                "nullable": True,
                "exact_when_count_at_most": 2,
            }
            if integral_average
            else {
                "sum_type": decimal_sum_type,
                "count_type": "Uint64",
                "nullable": nullable_input,
            }
        )
    aggregate = {
        "id": "aggregate",
        "op": "aggregate",
        "input": "a",
        "keys": keys,
        "aggregates": [trait],
        "phase": "undefined",
        "distinct_all": False,
    }
    nodes = [copy.deepcopy(SCAN_A), aggregate]
    stages = None
    edges = None
    root = "aggregate"
    if staged:
        partial = copy.deepcopy(aggregate)
        partial.update(id="partial", phase="intermediate")
        partial["aggregates"][0].update(
            output="_state",
            nullable=(nullable_aggregate and nullable_input),
        )
        final = copy.deepcopy(aggregate)
        final.update(id="final", input="partial", phase=final_phase)
        final["aggregates"][0].update(
            input="_state",
            function=final_function,
            nullable=(nullable_aggregate and (nullable_input or not grouped)),
        )
        if duplicate_input:
            copied_scan = copy.deepcopy(SCAN_A)
            copied_scan.update(id="a_copy")
            copied_scan["columns"] = [
                {"source": "k", "output": "copy.k"},
                {"source": "x", "output": "copy.x"},
            ]
            duplicate = {
                "id": "duplicate",
                "op": "union_all",
                "inputs": [
                    {"node": "a", "columns": ["a.k", "a.x"]},
                    {
                        "node": "a_copy",
                        "columns": ["copy.k", "copy.x"],
                    },
                ],
                "output": ["duplicate.k", "duplicate.x"],
                "ordered": False,
            }
            partial["input"] = "duplicate"
            partial["aggregates"][0]["input"] = "duplicate.x"
            nodes = [
                copy.deepcopy(SCAN_A),
                copied_scan,
                duplicate,
                partial,
                final,
            ]
            stages = [
                _stage("source", ["a"], [], ["a"], "column"),
                _stage(
                    "copy_source",
                    ["a_copy"],
                    [],
                    ["a_copy"],
                    "column",
                ),
                _stage(
                    "partial_stage",
                    ["duplicate", "partial"],
                    ["a", "a_copy"],
                    ["partial"],
                ),
                _stage("root", ["final"], ["partial"], ["final"]),
            ]
            edges = [
                _edge(
                    "original_input",
                    "source",
                    "partial_stage",
                    0,
                    0,
                    "map",
                ),
                _edge(
                    "copied_input",
                    "copy_source",
                    "partial_stage",
                    0,
                    1,
                    "union_all",
                    parallel=True,
                ),
                _edge(
                    "aggregate_edge",
                    "partial_stage",
                    "root",
                    0,
                    0,
                    "union_all",
                    parallel=False,
                ),
            ]
        else:
            nodes = [copy.deepcopy(SCAN_A), partial, final]
            stages = [
                _stage(
                    "source",
                    ["a", "partial"],
                    [],
                    ["partial"],
                    "column",
                ),
                _stage("root", ["final"], ["partial"], ["final"]),
            ]
        connection = (
            {
                "kind": "hash_shuffle",
                "keys": [shuffle_key],
                "hash_function": "HashV1",
                "use_spilling": False,
            }
            if grouped
            else {"kind": "union_all", "parallel": False}
        )
        if edges is None:
            edges = [
                _edge(
                    "aggregate_edge",
                    "source",
                    "root",
                    0,
                    0,
                    **connection,
                )
            ]
        root = "final"
    output = (["a.k"] if grouped else []) + ["result"]
    if project_average_away:
        if not grouped:
            raise ValueError("projecting AVG away requires a grouped fixture")
        project = {
            "id": "project",
            "op": "project",
            "input": root,
            "ordered": False,
            "columns": [
                {
                    "output": "only_key",
                    "expression": {"kind": "column", "column": "a.k"},
                }
            ],
        }
        nodes.append(project)
        root = "project"
        output = ["only_key"]
        if stages is not None:
            stages[-1]["nodes"].append("project")
            stages[-1]["outputs"] = [{"index": 0, "node": "project"}]
    schema_value = _stage_schema("A")
    schema_value["tables"][0]["columns"][1]["type"] = input_type
    if nullable_key:
        schema_value["tables"][0]["columns"][0]["nullable"] = True
    if nullable_input:
        schema_value["tables"][0]["columns"][1]["nullable"] = True
    return parse_snapshot(
        _snapshot_with_stage_graph(
            schema_value,
            nodes,
            root,
            output,
            stages,
            edges,
        )
    )


def unwrapped_uint64_sum_snapshot():
    schema_value = _stage_schema("A")
    schema_value["tables"][0]["columns"][1].update(
        type="Uint64",
        nullable=True,
    )
    aggregate = {
        "id": "aggregate",
        "op": "aggregate",
        "input": "a",
        "keys": [],
        "aggregates": [
            {
                "input": "a.x",
                "function": "sum",
                "output": "result",
                "type": "Uint64",
                # This is the raw prephysical type. Scalar Unwrap is lowered
                # to Coalesce(..., Uint64(0)), so the inferred node schema is
                # non-nullable.
                "nullable": True,
                "distinct": False,
                "unwrap": True,
            }
        ],
        "phase": "final",
        "distinct_all": False,
    }
    return parse_snapshot(
        _snapshot_with_stage_graph(
            schema_value,
            [copy.deepcopy(SCAN_A), aggregate],
            "aggregate",
            ["result"],
        )
    )


def distinct_all_stage_snapshot(staged, connection_kind="hash_shuffle"):
    trait = {
        "input": "a.k",
        "function": "distinct",
        "output": "result",
        "type": "Int64",
        "nullable": True,
        "distinct": False,
        "unwrap": False,
    }
    aggregate = {
        "id": "aggregate",
        "op": "aggregate",
        "input": "a",
        "keys": ["a.k"],
        "aggregates": [trait],
        "phase": "undefined",
        "distinct_all": True,
    }
    nodes = [copy.deepcopy(SCAN_A), aggregate]
    root = "aggregate"
    stages = None
    edges = None
    if staged:
        partial = copy.deepcopy(aggregate)
        partial.update(id="partial", phase="intermediate")
        partial["aggregates"][0]["output"] = "_distinct"
        final = copy.deepcopy(aggregate)
        final.update(
            id="final",
            input="partial",
            keys=["_distinct"],
            phase="final",
        )
        final["aggregates"][0].update(
            input="_distinct",
            output="result",
        )
        nodes = [copy.deepcopy(SCAN_A), partial, final]
        root = "final"
        stages = [
            _stage("source", ["a", "partial"], [], ["partial"], "column"),
            _stage("root", ["final"], ["partial"], ["final"]),
        ]
        connection = (
            {
                "kind": "hash_shuffle",
                "keys": ["_distinct"],
                "hash_function": "HashV1",
                "use_spilling": False,
            }
            if connection_kind == "hash_shuffle"
            else {"kind": connection_kind}
        )
        edges = [
            _edge(
                "distinct_edge",
                "source",
                "root",
                0,
                0,
                **connection,
            )
        ]

    schema_value = _stage_schema("A")
    schema_value["tables"][0]["columns"][0]["nullable"] = True
    return parse_snapshot(
        _snapshot_with_stage_graph(
            schema_value,
            nodes,
            root,
            ["result"],
            stages,
            edges,
        )
    )


def composite_distinct_all_snapshot():
    columns = (("a.k", "first"), ("a.x", "second"))
    aggregate = {
        "id": "aggregate",
        "op": "aggregate",
        "input": "a",
        "keys": [source for source, _ in columns],
        "aggregates": [
            {
                "input": source,
                "function": "distinct",
                "output": output,
                "type": "Int64",
                "nullable": True,
                "distinct": False,
                "unwrap": False,
            }
            for source, output in columns
        ],
        "phase": "undefined",
        "distinct_all": True,
    }
    schema_value = _stage_schema("A")
    for column in schema_value["tables"][0]["columns"]:
        column["nullable"] = True
    return parse_snapshot(
        _snapshot_with_stage_graph(
            schema_value,
            [copy.deepcopy(SCAN_A), aggregate],
            "aggregate",
            [output for _, output in columns],
        )
    )


def duplicated_grouped_aggregate_snapshot(function, nullable_key=False):
    def project(node_id, prefix):
        return {
            "id": node_id,
            "op": "project",
            "input": "a",
            "ordered": False,
            "columns": [
                {
                    "output": f"{prefix}.k",
                    "expression": {
                        "kind": "add",
                        "left": {"kind": "column", "column": "a.k"},
                        "right": {
                            "kind": "literal",
                            "type": "Int64",
                            "value": 0,
                        },
                        "type": "Int64",
                        "nullable": nullable_key,
                    },
                },
                {
                    "output": f"{prefix}.x",
                    "expression": {"kind": "column", "column": "a.x"},
                },
            ],
        }

    union = {
        "id": "union",
        "op": "union_all",
        "inputs": [
            {"node": "left", "columns": ["left.k", "left.x"]},
            {"node": "right", "columns": ["right.k", "right.x"]},
        ],
        "output": ["u.k", "u.x"],
        "ordered": False,
    }
    aggregate = {
        "id": "aggregate",
        "op": "aggregate",
        "input": "union",
        "keys": ["u.k"],
        "aggregates": [
            {
                "input": "u.x",
                "function": function,
                "output": "result",
                "type": "Uint64" if function == "count" else "Int64",
                "nullable": False,
                "distinct": False,
                "unwrap": False,
            }
        ],
        "phase": "undefined",
        "distinct_all": False,
    }
    schema_value = _stage_schema("A")
    schema_value["tables"][0]["columns"][0]["nullable"] = nullable_key
    return parse_snapshot(
        _snapshot_with_stage_graph(
            schema_value,
            [
                copy.deepcopy(SCAN_A),
                project("left", "left"),
                project("right", "right"),
                union,
                aggregate,
            ],
            "aggregate",
            ["u.k", "result"],
        )
    )


def constant_grouped_stage_snapshot(connection_kind):
    project = {
        "id": "project",
        "op": "project",
        "input": "a",
        "ordered": False,
        "columns": [
            {
                "output": "g",
                "expression": {
                    "kind": "literal",
                    "type": "Int64",
                    "value": 0,
                },
            },
            {
                "output": "x",
                "expression": {"kind": "column", "column": "a.x"},
            },
        ],
    }
    partial = {
        "id": "partial",
        "op": "aggregate",
        "input": "project",
        "keys": ["g"],
        "aggregates": [
            {
                "input": "x",
                "function": "count",
                "output": "_state",
                "type": "Uint64",
                "nullable": False,
                "distinct": False,
                "unwrap": False,
            }
        ],
        "phase": "intermediate",
        "distinct_all": False,
    }
    final = {
        "id": "final",
        "op": "aggregate",
        "input": "partial",
        "keys": ["g"],
        "aggregates": [
            {
                "input": "_state",
                "function": "sum",
                "output": "result",
                "type": "Uint64",
                "nullable": False,
                "distinct": False,
                "unwrap": False,
            }
        ],
        "phase": "final",
        "distinct_all": False,
    }
    connection = (
        {
            "kind": "hash_shuffle",
            "keys": ["g"],
            "hash_function": "HashV1",
            "use_spilling": False,
        }
        if connection_kind == "hash_shuffle"
        else {"kind": "broadcast"}
    )
    return parse_snapshot(
        _snapshot_with_stage_graph(
            _stage_schema("A"),
            [copy.deepcopy(SCAN_A), project, partial, final],
            "final",
            ["g", "result"],
            [
                _stage(
                    "source",
                    ["a", "project", "partial"],
                    [],
                    ["partial"],
                    "column",
                ),
                _stage("root", ["final"], ["partial"], ["final"]),
            ],
            [
                _edge(
                    "aggregate_edge",
                    "source",
                    "root",
                    0,
                    0,
                    **connection,
                )
            ],
        )
    )


def projected_decimal_sum_snapshot(expression, input_type="Int64"):
    schema_value = _stage_schema("A")
    schema_value["tables"][0]["columns"][1]["type"] = input_type
    project = {
        "id": "project",
        "op": "project",
        "input": "a",
        "ordered": False,
        "columns": [
            {
                "output": "value",
                "expression": copy.deepcopy(expression),
            }
        ],
    }
    result_type = decimal.sum_type(expression["type"])
    assert result_type is not None
    aggregate = {
        "id": "aggregate",
        "op": "aggregate",
        "input": "project",
        "keys": [],
        "aggregates": [
            {
                "input": "value",
                "function": "sum",
                "output": "result",
                "type": result_type,
                "nullable": True,
                "distinct": False,
                "unwrap": False,
            }
        ],
        "phase": "undefined",
        "distinct_all": False,
    }
    return parse_snapshot(
        _snapshot_with_stage_graph(
            schema_value,
            [copy.deepcopy(SCAN_A), project, aggregate],
            "aggregate",
            ["result"],
        )
    )


def count_star_snapshot():
    schema_value = _stage_schema("A")
    schema_value["tables"][0]["columns"][1]["nullable"] = True
    project = {
        "id": "count_input",
        "op": "project",
        "input": "a",
        "ordered": False,
        "columns": [
            {
                "output": "_count_input",
                "expression": {"kind": "void"},
            }
        ],
    }
    aggregate = {
        "id": "aggregate",
        "op": "aggregate",
        "input": "count_input",
        "keys": [],
        "aggregates": [
            {
                "input": "_count_input",
                "function": "count",
                "output": "result",
                "type": "Uint64",
                "nullable": False,
                "distinct": False,
                "unwrap": False,
            }
        ],
        "phase": "undefined",
        "distinct_all": False,
    }
    return parse_snapshot(
        _snapshot_with_stage_graph(
            schema_value,
            [copy.deepcopy(SCAN_A), project, aggregate],
            "aggregate",
            ["result"],
        )
    )


def count_distinct_int64_snapshot():
    aggregate = {
        "id": "aggregate",
        "op": "aggregate",
        "input": "a",
        "keys": [],
        "aggregates": [
            {
                "input": "a.x",
                "function": "count",
                "output": "result",
                "type": "Uint64",
                "nullable": False,
                "distinct": True,
                "unwrap": False,
            }
        ],
        "phase": "undefined",
        "distinct_all": False,
    }
    return parse_snapshot(
        _snapshot_with_stage_graph(
            _stage_schema("A"),
            [copy.deepcopy(SCAN_A), aggregate],
            "aggregate",
            ["result"],
        )
    )


def partial_only_count_snapshot():
    aggregate = {
        "id": "partial",
        "op": "aggregate",
        "input": "a",
        "keys": [],
        "aggregates": [
            {
                "input": "a.x",
                "function": "count",
                "output": "result",
                "type": "Uint64",
                "nullable": False,
                "distinct": False,
                "unwrap": False,
            }
        ],
        "phase": "intermediate",
        "distinct_all": False,
    }
    return parse_snapshot(
        _snapshot_with_stage_graph(
            _stage_schema("A"),
            [copy.deepcopy(SCAN_A), aggregate],
            "partial",
            ["result"],
            [_stage("source", ["a", "partial"], [], ["partial"], "column")],
            [],
        )
    )


def quantified_opaque_snapshot(result_type):
    nodes = [
        copy.deepcopy(SCAN_A),
        {
            "id": "sort",
            "op": "sort",
            "input": "a",
            "order": [
                {"column": "a.x", "ascending": True, "nulls_first": False}
            ],
            "limit": None,
            "phase": "undefined",
        },
        {
            "id": "limit",
            "op": "limit",
            "input": "sort",
            "count": {"kind": "literal", "type": "Uint64", "value": 1},
            "offset": None,
            "phase": "undefined",
        },
        {
            "id": "aggregate",
            "op": "aggregate",
            "input": "limit",
            "keys": [],
            "aggregates": [
                {
                    "input": "a.x",
                    "function": "count",
                    "output": "n",
                    "type": "Uint64",
                    "nullable": False,
                    "distinct": False,
                    "unwrap": False,
                }
            ],
            "phase": "undefined",
            "distinct_all": False,
        },
        {
            "id": "render",
            "op": "project",
            "input": "aggregate",
            "ordered": False,
            "columns": [
                {
                    "output": "result",
                    "expression": {
                        "kind": "opaque",
                        "fingerprint": "render($0)",
                        "type": result_type,
                        "nullable": False,
                        "args": [{"kind": "column", "column": "n"}],
                    },
                }
            ],
        },
    ]
    return parse_snapshot(
        _snapshot_with_stage_graph(
            _stage_schema("A"),
            nodes,
            "render",
            ["result"],
        )
    )


def unordered_take_opaque_snapshot(result_type):
    return parse_snapshot(
        _snapshot_with_stage_graph(
            _stage_schema("A"),
            [
                copy.deepcopy(SCAN_A),
                {
                    "id": "limit",
                    "op": "limit",
                    "input": "a",
                    "count": {
                        "kind": "literal",
                        "type": "Uint64",
                        "value": 1,
                    },
                    "offset": None,
                    "phase": "undefined",
                },
                {
                    "id": "render",
                    "op": "project",
                    "input": "limit",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "result",
                            "expression": {
                                "kind": "opaque",
                                "fingerprint": "render($0)",
                                "type": result_type,
                                "nullable": False,
                                "args": [
                                    {
                                        "kind": "column",
                                        "column": "a.x",
                                    }
                                ],
                            },
                        }
                    ],
                },
            ],
            "render",
            ["result"],
        )
    )


def right_join(predicate):
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": schema(),
            "plan": {
                "nodes": [
                    SCAN_A,
                    SCAN_B,
                    {
                        "id": "join",
                        "op": "join",
                        "left": "a",
                        "right": "b",
                        "kind": "right",
                        "predicate": predicate,
                    },
                ],
                "root": "join",
                "output": ["a.x", "b.x"],
                "subplans": [],
            },
            "stage_graph": None,
        }
    )


def union_snapshot(duplicate):
    if duplicate:
        nodes = [
            SCAN_A,
            {
                "id": "union",
                "op": "union_all",
                "inputs": [
                    {"node": "a", "columns": ["a.k"]},
                    {"node": "a", "columns": ["a.k"]},
                ],
                "output": ["u.k"],
                "ordered": False,
            },
        ]
    else:
        nodes = [
            {
                "id": "a",
                "op": "scan",
                "table": "A",
                "columns": [{"source": "k", "output": "u.k"}],
                "pushed_limit": None,
            }
        ]
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": schema(),
            "plan": {
                "nodes": nodes,
                "root": "union" if duplicate else "a",
                "output": ["u.k"],
                "subplans": [],
            },
            "stage_graph": None,
        }
    )


def filtered_snapshot(predicate):
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {
                "tables": [
                    {
                        "name": "T",
                        "columns": [{"name": "flag", "type": "Bool", "nullable": True}],
                        "unique_keys": [],
                    }
                ]
            },
            "plan": {
                "nodes": [
                    {
                        "id": "scan",
                        "op": "scan",
                        "table": "T",
                        "columns": [{"source": "flag", "output": "t.flag"}],
                        "pushed_limit": None,
                    },
                    {
                        "id": "filter",
                        "op": "filter",
                        "input": "scan",
                        "predicate": predicate,
                    },
                ],
                "root": "filter",
                "output": ["t.flag"],
                "subplans": [],
            },
            "stage_graph": None,
        }
    )


def integral_division_snapshot(reverse_operands=False):
    left = {"kind": "column", "column": "a.x"}
    right = {"kind": "column", "column": "a.k"}
    if reverse_operands:
        left, right = right, left
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {
                "tables": [copy.deepcopy(schema()["tables"][0])]
            },
            "plan": {
                "nodes": [
                    copy.deepcopy(SCAN_A),
                    {
                        "id": "project",
                        "op": "project",
                        "input": "a",
                        "ordered": False,
                        "columns": [
                            {
                                "output": "quotient",
                                "expression": {
                                    "kind": "div",
                                    "left": left,
                                    "right": right,
                                    "type": "Int64",
                                    "nullable": True,
                                },
                            }
                        ],
                    },
                ],
                "root": "project",
                "output": ["quotient"],
                "subplans": [],
            },
            "stage_graph": None,
        }
    )


def string_predicate_snapshot(fingerprint, reverse_arguments=False):
    arguments = [
        {"kind": "column", "column": "t.s"},
        {"kind": "literal", "type": "String", "value": "tail"},
    ]
    if reverse_arguments:
        arguments.reverse()
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {
                "tables": [
                    {
                        "name": "T",
                        "columns": [
                            {"name": "s", "type": "String", "nullable": True}
                        ],
                        "unique_keys": [],
                    }
                ]
            },
            "plan": {
                "nodes": [
                    {
                        "id": "scan",
                        "op": "scan",
                        "table": "T",
                        "columns": [{"source": "s", "output": "t.s"}],
                        "pushed_limit": None,
                    },
                    {
                        "id": "filter",
                        "op": "filter",
                        "input": "scan",
                        "predicate": {
                            "kind": "opaque",
                            "fingerprint": fingerprint,
                            "type": "Bool",
                            "nullable": True,
                            "args": arguments,
                        },
                    },
                ],
                "root": "filter",
                "output": ["t.s"],
                "subplans": [],
            },
            "stage_graph": None,
        }
    )


DATE_YEAR_FINGERPRINT = "yql-datetime-year-v1"


def date_year_snapshot(
    fingerprint=DATE_YEAR_FINGERPRINT,
    *,
    opaque_argument=None,
):
    if opaque_argument is None:
        opaque_argument = {"kind": "bound", "depth": 0}
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {
                "tables": [
                    {
                        "name": "T",
                        "columns": [
                            {
                                "name": "shipdate",
                                "type": "Date",
                                "nullable": True,
                            }
                        ],
                        "unique_keys": [],
                    }
                ]
            },
            "plan": {
                "nodes": [
                    {
                        "id": "scan",
                        "op": "scan",
                        "table": "T",
                        "columns": [
                            {
                                "source": "shipdate",
                                "output": "t.shipdate",
                            }
                        ],
                        "pushed_limit": None,
                    },
                    {
                        "id": "project",
                        "op": "project",
                        "input": "scan",
                        "ordered": False,
                        "columns": [
                            {
                                "output": "t.year",
                                "expression": {
                                    "kind": "if_present",
                                    "optional": {
                                        "kind": "column",
                                        "column": "t.shipdate",
                                    },
                                    "present": {
                                        "kind": "if",
                                        "condition": {
                                            "kind": "literal",
                                            "type": "Bool",
                                            "value": True,
                                        },
                                        "then": {
                                            "kind": "opaque",
                                            "fingerprint": fingerprint,
                                            "type": "Uint16",
                                            "nullable": False,
                                            "args": [opaque_argument],
                                        },
                                        "else": {
                                            "kind": "null",
                                            "type": "Uint16",
                                        },
                                        "type": "Uint16",
                                        "nullable": True,
                                    },
                                    "missing": {
                                        "kind": "null",
                                        "type": "Uint16",
                                    },
                                    "type": "Uint16",
                                    "nullable": True,
                                },
                            }
                        ],
                    },
                ],
                "root": "project",
                "output": ["t.year"],
                "subplans": [],
            },
            "stage_graph": None,
        }
    )


def passive_double_snapshot(
    fingerprint=OPAQUE_DOUBLE_FINGERPRINT_PREFIX + "identity",
    argument_columns=("a.k", "a.x", "a.y"),
    *,
    staged=False,
    sort_merge=False,
):
    schema_value = _stage_schema("A")
    schema_value["tables"][0]["columns"].append(
        {"name": "y", "type": "Int64", "nullable": True}
    )
    for column in schema_value["tables"][0]["columns"]:
        column["nullable"] = True
    scan = copy.deepcopy(SCAN_A)
    scan["columns"].append({"source": "y", "output": "a.y"})
    compute = {
        "id": "compute_double",
        "op": "project",
        "input": "a",
        "ordered": False,
        "columns": [
            {
                "output": "double_value",
                "expression": {
                    "kind": "opaque_double",
                    "fingerprint": fingerprint,
                    "type": "Double",
                    "nullable": True,
                    "args": [
                        {"kind": "column", "column": column}
                        for column in argument_columns
                    ],
                },
            },
            {
                "output": "key",
                "expression": {"kind": "column", "column": "a.k"},
            },
        ],
    }
    passthrough = {
        "id": "pass_double",
        "op": "project",
        "input": "sort_double" if sort_merge else "compute_double",
        "ordered": False,
        "columns": [
            {
                "output": "result",
                "expression": {
                    "kind": "column",
                    "column": "double_value",
                },
            }
        ],
    }
    nodes = [scan, compute]
    if sort_merge:
        nodes.append(
            {
                "id": "sort_double",
                "op": "sort",
                "input": "compute_double",
                "order": [
                    {
                        "column": "key",
                        "ascending": True,
                        "nulls_first": True,
                    }
                ],
                "limit": None,
                "phase": "undefined",
            }
        )
    nodes.append(passthrough)
    stages = None
    edges = None
    if staged:
        source_nodes = ["a", "compute_double"]
        source_output = "compute_double"
        connection = {
            "kind": "hash_shuffle",
            "keys": ["key"],
            "hash_function": "HashV1",
            "use_spilling": False,
        }
        if sort_merge:
            source_nodes.append("sort_double")
            source_output = "sort_double"
            connection = {
                "kind": "merge",
                "order": [
                    {
                        "column": "key",
                        "ascending": True,
                        "nulls_first": True,
                    }
                ],
            }
        stages = [
            _stage(
                "source",
                source_nodes,
                [],
                [source_output],
                "column",
            ),
            _stage(
                "root",
                ["pass_double"],
                [source_output],
                ["pass_double"],
            ),
        ]
        edges = [
            _edge(
                "merge" if sort_merge else "shuffle",
                "source",
                "root",
                0,
                0,
                **connection,
            )
        ]
    return parse_snapshot(
        _snapshot_with_stage_graph(
            schema_value,
            nodes,
            "pass_double",
            ["result"],
            stages,
            edges,
        )
    )


def mixed_width_membership_snapshot(lowered):
    lookup = {"kind": "column", "column": "t.year"}
    items = [
        {"kind": "literal", "type": "Int32", "value": value}
        for value in (0, 1, 1998)
    ]
    direct = {"kind": "in", "lookup": lookup, "items": items}
    predicate = direct
    if lowered:
        predicate = {
            "kind": "if",
            "condition": {"kind": "exists", "arg": lookup},
            "then": {
                "kind": "if_present",
                "optional": {
                    "kind": "cast_integral",
                    "arg": lookup,
                    "type": "Int32",
                    "nullable": True,
                },
                "present": {
                    "kind": "in",
                    "lookup": {"kind": "bound", "depth": 0},
                    "items": items,
                },
                "missing": {"kind": "literal", "type": "Bool", "value": False},
                "type": "Bool",
                "nullable": False,
            },
            "else": {"kind": "literal", "type": "Bool", "value": False},
            "type": "Bool",
            "nullable": False,
        }
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {
                "tables": [
                    {
                        "name": "T",
                        "columns": [
                            {"name": "year", "type": "Int64", "nullable": True}
                        ],
                        "unique_keys": [],
                    }
                ]
            },
            "plan": {
                "nodes": [
                    {
                        "id": "scan",
                        "op": "scan",
                        "table": "T",
                        "columns": [{"source": "year", "output": "t.year"}],
                        "pushed_limit": None,
                    },
                    {
                        "id": "filter",
                        "op": "filter",
                        "input": "scan",
                        "predicate": predicate,
                    },
                ],
                "root": "filter",
                "output": ["t.year"],
                "subplans": [],
            },
            "stage_graph": None,
        }
    )


def date_filtered_snapshot(kind, day):
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {
                "tables": [
                    {
                        "name": "D",
                        "columns": [
                            {"name": "day", "type": "Date", "nullable": True}
                        ],
                        "unique_keys": [],
                    }
                ]
            },
            "plan": {
                "nodes": [
                    {
                        "id": "scan",
                        "op": "scan",
                        "table": "D",
                        "columns": [{"source": "day", "output": "d.day"}],
                        "pushed_limit": None,
                    },
                    {
                        "id": "filter",
                        "op": "filter",
                        "input": "scan",
                        "predicate": {
                            "kind": kind,
                            "left": {"kind": "column", "column": "d.day"},
                            "right": {"kind": "literal", "type": "Date", "value": day},
                        },
                    },
                ],
                "root": "filter",
                "output": ["d.day"],
                "subplans": [],
            },
            "stage_graph": None,
        }
    )


def date_if_present_snapshot(
    *,
    source="d.day",
    present=None,
    missing=0,
):
    if present is None:
        present = {"kind": "bound", "depth": 0}
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {
                "tables": [
                    {
                        "name": "D",
                        "columns": [
                            {"name": "day", "type": "Date", "nullable": True},
                            {
                                "name": "other_day",
                                "type": "Date",
                                "nullable": True,
                            },
                        ],
                        "unique_keys": [],
                    }
                ]
            },
            "plan": {
                "nodes": [
                    {
                        "id": "scan",
                        "op": "scan",
                        "table": "D",
                        "columns": [
                            {"source": "day", "output": "d.day"},
                            {"source": "other_day", "output": "d.other_day"},
                        ],
                        "pushed_limit": None,
                    },
                    {
                        "id": "project",
                        "op": "project",
                        "input": "scan",
                        "ordered": False,
                        "columns": [
                            {
                                "output": "d.result",
                                "expression": {
                                    "kind": "if_present",
                                    "optional": {
                                        "kind": "column",
                                        "column": source,
                                    },
                                    "present": present,
                                    "missing": {
                                        "kind": "literal",
                                        "type": "Date",
                                        "value": missing,
                                    },
                                    "type": "Date",
                                    "nullable": False,
                                },
                            }
                        ],
                    },
                ],
                "root": "project",
                "output": ["d.result"],
                "subplans": [],
            },
            "stage_graph": None,
        }
    )


def left_join_elimination_snapshot(with_join, right_key_is_unique):
    value = schema()
    value["tables"][1]["unique_keys"] = (
        [{"columns": ["k"], "nulls_distinct": False}] if right_key_is_unique else []
    )
    nodes = [SCAN_A]
    if with_join:
        nodes.extend(
            [
                SCAN_B,
                {
                    "id": "join",
                    "op": "join",
                    "left": "a",
                    "right": "b",
                    "kind": "left",
                    "predicate": KEY_EQUALITY,
                },
            ]
        )
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": value,
            "plan": {
                "nodes": nodes,
                "root": "join" if with_join else "a",
                "output": ["a.x"],
                "subplans": [],
            },
            "stage_graph": None,
        }
    )


def constant_snapshot(value, output="result", scalar_type=None):
    scalar_type = scalar_type or ("String" if isinstance(value, str) else "Int64")
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {"tables": []},
            "plan": {
                "nodes": [
                    {"id": "source", "op": "empty_source"},
                    {
                        "id": "project",
                        "op": "project",
                        "input": "source",
                        "ordered": False,
                        "columns": [
                            {
                                "output": output,
                                "expression": {"kind": "literal", "type": scalar_type, "value": value},
                            }
                        ],
                    },
                ],
                "root": "project",
                "output": [output],
                "subplans": [],
            },
            "stage_graph": None,
        }
    )


def scalar_subplan_inline_snapshot(staged, aggregate_input="a.x"):
    aggregate = {
        "id": "scalar_aggregate",
        "op": "aggregate",
        "input": "a",
        "keys": [],
        "aggregates": [
            {
                "input": aggregate_input,
                "function": "sum",
                "output": "sub.value",
                "type": "Int64",
                "nullable": True,
                "distinct": False,
                "unwrap": False,
            }
        ],
        "phase": "undefined",
        "distinct_all": False,
    }
    result_project = {
        "id": "result_project",
        "op": "project",
        "input": "cross" if staged else "b",
        "ordered": False,
        "columns": [
            {
                "output": "key",
                "expression": {"kind": "column", "column": "b.k"},
            },
            {
                "output": "result",
                "expression": {
                    "kind": "column",
                    "column": "sub.value" if staged else "$scalar",
                },
            },
        ],
    }
    nodes = [
        copy.deepcopy(SCAN_A),
        copy.deepcopy(SCAN_B),
        aggregate,
    ]
    stages = None
    edges = None
    if staged:
        nodes.extend(
            [
                {
                    "id": "cross",
                    "op": "join",
                    "left": "b",
                    "right": "scalar_aggregate",
                    "kind": "cross",
                    "predicate": {
                        "kind": "literal",
                        "type": "Bool",
                        "value": True,
                    },
                },
                result_project,
            ]
        )
        stages = [
            _stage("a_source", ["a"], [], ["a"], "column"),
            _stage(
                "scalar",
                ["scalar_aggregate"],
                ["a"],
                ["scalar_aggregate"],
            ),
            _stage("b_source", ["b"], [], ["b"], "row"),
            _stage(
                "root",
                ["cross", "result_project"],
                ["b", "scalar_aggregate"],
                ["result_project"],
            ),
        ]
        edges = [
            _edge(
                "gather_scalar_input",
                "a_source",
                "scalar",
                0,
                0,
                "union_all",
                parallel=False,
            ),
            _edge("map_main_input", "b_source", "root", 0, 0, "map"),
            _edge(
                "broadcast_scalar",
                "scalar",
                "root",
                0,
                1,
                "broadcast",
            ),
        ]
    else:
        nodes.append(result_project)

    raw = _snapshot_with_stage_graph(
        _stage_schema("A", "B"),
        nodes,
        "result_project",
        ["key", "result"],
        stages,
        edges,
    )
    if not staged:
        raw["plan"]["subplans"] = [
            {
                "binding": "$scalar",
                "kind": "scalar",
                "root": "scalar_aggregate",
                "output": {
                    "column": "sub.value",
                    "type": "Int64",
                    "nullable": True,
                },
                "type": "Int64",
                "nullable": True,
                "dependencies": [],
                "consumers": ["result_project"],
            }
        ]
    return parse_snapshot(raw)


class SolverProtocolTest(unittest.TestCase):
    @staticmethod
    def _branch_problem(count=2):
        script = smt.Script()
        requested = script.fresh_constant("requested", smt.BOOL)
        obligation = script.fresh_constant("whole_mismatch", smt.BOOL)
        predicates = tuple(
            script.fresh_constant(f"branch_{index}", smt.BOOL)
            for index in range(count)
        )
        script.assert_obligation(obligation)
        branches = tuple(
            relation_model.MismatchBranch(f"branch_{index}", predicate)
            for index, predicate in enumerate(predicates)
        )
        return Problem(script, {}, branches), requested, predicates

    @staticmethod
    def _integral_average_problem():
        script = smt.Script()
        exact = script.fresh_constant("semantic_mismatch", smt.BOOL)
        inexact = script.fresh_constant("integral_average_count_gt_2", smt.BOOL)
        script.assert_obligation(smt.or_(exact, inexact))
        exact_branch = relation_model.MismatchBranch(
            "semantic_mismatch",
            exact,
        )
        inexact_branch = relation_model.MismatchBranch(
            "integral_avg_count_gt_2",
            inexact,
        )
        problem = Problem(
            script=script,
            witness={},
            mismatch_branches=(
                relation_model.MismatchBranch("exact_component", exact),
            ),
            semantic_mismatch=exact_branch,
            soundness_exclusion=relation_model.MismatchBranch(
                "integral_avg_model_domain",
                inexact,
            ),
            soundness_exclusions=(inexact_branch,),
        )
        return problem, exact, inexact

    def test_reachable_integral_average_inexact_region_is_inconclusive(self):
        problem, _, _ = self._integral_average_problem()
        sat = subprocess.CompletedProcess(["z3"], 0, "sat\n", "")

        with mock.patch.object(verifier, "_run_solver", return_value=sat) as run:
            query = verifier.query_solver(problem, "z3")

        self.assertEqual(query.status, "unknown")
        self.assertEqual(query.phase, "soundness")
        self.assertIn("greater than two is reachable", query.reason)
        run.assert_called_once()

    def test_unresolved_integral_average_inexact_region_is_inconclusive(self):
        problem, _, _ = self._integral_average_problem()
        unknown = subprocess.CompletedProcess(["z3"], 0, "unknown\n", "")

        with mock.patch.object(
            verifier,
            "_run_solver",
            return_value=unknown,
        ) as run:
            query = verifier.query_solver(problem, "z3")

        self.assertEqual(query.status, "unknown")
        self.assertEqual(query.phase, "soundness")
        self.assertIn("could not rule out", query.reason)
        run.assert_called_once()

    def test_semantic_mismatch_is_solved_only_after_inexact_region_is_unsat(self):
        problem, exact, inexact = self._integral_average_problem()
        unsat = subprocess.CompletedProcess(["z3"], 0, "unsat\n", "")

        with mock.patch.object(
            verifier,
            "_run_solver",
            side_effect=(unsat, unsat),
        ) as run:
            query = verifier.query_solver(problem, "z3")

        self.assertEqual(query.status, "unsat")
        self.assertEqual(run.call_count, 2)
        safety_formula, exact_formula = (
            call.args[1] for call in run.call_args_list
        )
        self.assertIn(f"(assert {inexact.render()})", safety_formula)
        self.assertNotIn(f"(assert {exact.render()})", safety_formula)
        self.assertIn(f"(assert {exact.render()})", exact_formula)
        self.assertNotIn(f"(assert {inexact.render()})", exact_formula)

    def test_abstract_integral_average_mismatch_requires_binary64_replay(self):
        problem, exact, _ = self._integral_average_problem()
        responses = (
            subprocess.CompletedProcess(["z3"], 0, "unsat\n", ""),
            subprocess.CompletedProcess(["z3"], 0, "sat\n", ""),
        )

        with mock.patch.object(
            verifier,
            "_run_solver",
            side_effect=responses,
        ) as run:
            query = verifier.query_solver(problem, "z3", (exact,))

        self.assertEqual(query.status, "unknown")
        self.assertEqual(query.phase, "abstract")
        self.assertIn("exact binary64 replay is required", query.reason)
        self.assertEqual(run.call_count, 2)
        self.assertTrue(
            all("(get-value" not in call.args[1] for call in run.call_args_list)
        )

    def test_abstract_integral_average_branch_candidate_requires_replay(self):
        problem, _, _ = self._integral_average_problem()
        responses = (
            subprocess.CompletedProcess(["z3"], 0, "unsat\n", ""),
            subprocess.CompletedProcess(["z3"], 0, "unknown\n", ""),
            subprocess.CompletedProcess(["z3"], 0, "sat\n", ""),
        )

        with mock.patch.object(
            verifier,
            "_run_solver",
            side_effect=responses,
        ) as run:
            query = verifier.query_solver(problem, "z3")

        self.assertEqual(query.status, "unknown")
        self.assertEqual(query.phase, "abstract")
        self.assertIn("exact binary64 replay is required", query.reason)
        self.assertEqual(run.call_count, 3)

    def test_string_rank_decoder_is_exact_and_rejects_out_of_universe_values(self):
        representatives = {0: "", 1: "a", 2: "é"}
        self.assertEqual(verifier.decode_string_atom(2, representatives), "é")
        for value in (-1, 3, True, "1"):
            with self.subTest(value=value):
                with self.assertRaises(SolverError):
                    verifier.decode_string_atom(value, representatives)

    def test_canonical_unsat_can_verify_without_branch_checks(self):
        problem, _, _ = self._branch_problem(3)
        unsat = subprocess.CompletedProcess(["z3"], 0, "unsat\n", "")

        with mock.patch.object(verifier, "_run_solver", return_value=unsat) as run:
            query = verifier.query_solver(problem, "z3")

        self.assertEqual(query.status, "unsat")
        run.assert_called_once()

    def test_all_exact_branches_must_be_unsat_after_canonical_unknown(self):
        problem, _, _ = self._branch_problem(3)
        responses = [
            subprocess.CompletedProcess(["z3"], 0, "unknown\n", ""),
            *(
                subprocess.CompletedProcess(["z3"], 0, "unsat\n", "")
                for _ in range(3)
            ),
        ]

        with mock.patch.object(verifier, "_run_solver", side_effect=responses) as run:
            query = verifier.query_solver(problem, "z3")

        self.assertEqual(query.status, "unsat")
        self.assertEqual(run.call_count, 4)

    def test_later_sat_wins_after_unknown_and_reuses_its_branch_for_model(self):
        problem, requested, predicates = self._branch_problem()
        responses = [
            subprocess.CompletedProcess(["z3"], 0, "unknown\n", ""),
            subprocess.CompletedProcess(["z3"], 0, "unknown\n", ""),
            subprocess.CompletedProcess(["z3"], 0, "sat\n", ""),
            subprocess.CompletedProcess(["z3"], 0, "sat\n((v_0 true))\n", ""),
        ]

        with mock.patch.object(verifier, "_run_solver", side_effect=responses) as run:
            query = verifier.query_solver(problem, "z3", (requested,))

        self.assertEqual(query.status, "sat")
        self.assertEqual(query.values, {"v_0": True})
        formulas = [call.args[1] for call in run.call_args_list]
        first_assertion = f"(assert {predicates[0].render()})"
        winning_assertion = f"(assert {predicates[1].render()})"
        self.assertNotIn(first_assertion, formulas[0])
        self.assertIn(first_assertion, formulas[1])
        self.assertNotIn(winning_assertion, formulas[1])
        self.assertIn(winning_assertion, formulas[2])
        self.assertIn(winning_assertion, formulas[3])
        self.assertNotIn(first_assertion, formulas[2])
        self.assertNotIn(first_assertion, formulas[3])
        self.assertNotIn("(get-value", formulas[2])
        self.assertIn("(get-value (v_0))", formulas[3])

    def test_unknown_branch_is_retained_after_all_other_branches_are_unsat(self):
        problem, _, _ = self._branch_problem(3)
        responses = [
            subprocess.CompletedProcess(["z3"], 0, "unknown\n", ""),
            subprocess.CompletedProcess(["z3"], 0, "unsat\n", ""),
            subprocess.CompletedProcess(["z3"], 0, "unknown\n", ""),
            subprocess.CompletedProcess(["z3"], 0, "unsat\n", ""),
        ]

        with mock.patch.object(verifier, "_run_solver", side_effect=responses) as run:
            query = verifier.query_solver(problem, "z3")

        self.assertEqual(query.status, "unknown")
        self.assertIn("branch 2/3 (branch_1)", query.reason)
        self.assertEqual(run.call_count, 4)

    def test_problem_without_exact_branches_keeps_the_legacy_single_query(self):
        script = smt.Script()
        obligation = script.fresh_constant("obligation", smt.BOOL)
        script.assert_term(obligation)
        problem = Problem(script, {})
        unsat = subprocess.CompletedProcess(["z3"], 0, "unsat\n", "")

        with mock.patch.object(verifier, "_run_solver", return_value=unsat) as run:
            query = verifier.query_solver(problem, "z3")

        self.assertEqual(query.status, "unsat")
        run.assert_called_once()
        self.assertEqual(run.call_args.args[1], script.render())

    def test_empty_exact_branch_set_cannot_verify(self):
        with self.assertRaisesRegex(SolverError, "has no branches"):
            verifier.query_solver(Problem(smt.Script(), {}, ()), "z3")

    def test_portfolio_shares_one_decreasing_solver_deadline(self):
        problem, _, _ = self._branch_problem()
        responses = [
            subprocess.CompletedProcess(["z3"], 0, "unknown\n", ""),
            subprocess.CompletedProcess(["z3"], 0, "unsat\n", ""),
            subprocess.CompletedProcess(["z3"], 0, "unsat\n", ""),
        ]
        tick = iter(index / 40 for index in range(40))

        with (
            mock.patch.object(verifier.time, "monotonic", side_effect=tick),
            mock.patch.object(verifier, "_run_solver", side_effect=responses) as run,
        ):
            query = verifier.query_solver(problem, "z3", timeout_ms=1000)

        self.assertEqual(query.status, "unsat")
        formulas = [call.args[1] for call in run.call_args_list]
        solver_timeouts = [
            int(
                next(
                    line
                    for line in formula.splitlines()
                    if line.startswith("(set-option :timeout ")
                ).removeprefix("(set-option :timeout ").removesuffix(")")
            )
            for formula in formulas
        ]
        process_timeouts = [call.args[2] for call in run.call_args_list]
        self.assertEqual(solver_timeouts[0], 750)
        self.assertGreater(solver_timeouts[1], solver_timeouts[2])
        self.assertGreater(process_timeouts[1], process_timeouts[2])

    def test_expired_global_deadline_stops_before_the_next_branch(self):
        problem, _, _ = self._branch_problem()
        responses = [
            subprocess.CompletedProcess(["z3"], 0, "unknown\n", ""),
            subprocess.CompletedProcess(["z3"], 0, "unsat\n", ""),
        ]
        clock = iter((0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.1))

        with (
            mock.patch.object(verifier.time, "monotonic", side_effect=clock),
            mock.patch.object(verifier, "_run_solver", side_effect=responses) as run,
        ):
            query = verifier.query_solver(problem, "z3", timeout_ms=1000)

        self.assertEqual(query.status, "unknown")
        self.assertIn("deadline expired before branch 2/2", query.reason)
        self.assertEqual(run.call_count, 2)

    def test_unsat_arriving_during_process_grace_cannot_verify(self):
        problem, _, _ = self._branch_problem(1)
        unsat = subprocess.CompletedProcess(["z3"], 0, "unsat\n", "")
        clock = iter((0.0, 0.1, 0.2, 0.3, 1.1, 1.2))

        with (
            mock.patch.object(verifier.time, "monotonic", side_effect=clock),
            mock.patch.object(verifier, "_run_solver", return_value=unsat),
        ):
            query = verifier.query_solver(problem, "z3", timeout_ms=1000)

        self.assertEqual(query.status, "unknown")
        self.assertIn("deadline expired before branch 1/1", query.reason)

    def test_solver_process_timeout_is_unknown(self):
        with mock.patch.object(
            verifier,
            "_run_solver",
            side_effect=verifier._ProcessDeadlineExceeded,
        ):
            result = solve(Problem(smt.Script(), {}), "z3", 0)

        self.assertEqual(result.status, "UNKNOWN")
        self.assertIn("process exceeded", result.reason)

    def test_model_process_timeout_preserves_the_confirmed_counterexample(self):
        script = smt.Script()
        present = script.fresh_constant("present", smt.BOOL)
        problem = Problem(
            script,
            {"A": (relation_model.WitnessRow(present, {}),)},
        )
        sat = subprocess.CompletedProcess(["z3"], 0, "sat\n", "")

        with mock.patch.object(
            verifier,
            "_run_solver",
            side_effect=[sat, verifier._ProcessDeadlineExceeded],
        ):
            result = solve(problem, "z3", 1)

        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertIsNone(result.witness)
        self.assertIn("extracting its model", result.reason)

    def test_solver_error_cannot_be_reported_as_verified(self):
        process = subprocess.CompletedProcess(
            args=["z3"],
            returncode=1,
            stdout="unsat\n",
            stderr="parser error\n",
        )
        with mock.patch.object(verifier, "_run_solver", return_value=process):
            with self.assertRaisesRegex(SolverError, "parser error"):
                solve(Problem(smt.Script(), {}), "z3", 0)

    def test_extra_solver_responses_cannot_be_reported_as_verified(self):
        for output in (
            "unsat\nsat\n",
            "unsat\nsuccess\n",
            'unsat\n(error\n"late")\n',
        ):
            process = subprocess.CompletedProcess(
                args=["z3"],
                returncode=0,
                stdout=output,
                stderr="",
            )
            with self.subTest(output=output):
                with mock.patch.object(verifier, "_run_solver", return_value=process):
                    with self.assertRaisesRegex(SolverError, "exactly one solver status"):
                        solve(Problem(smt.Script(), {}), "z3", 0)

    def test_extra_witness_response_is_rejected(self):
        with self.assertRaisesRegex(SolverError, "SAT and one get-value response"):
            verifier._get_values("sat\n((v_0 true))\nsat\n")

    def test_model_unknown_does_not_hide_solver_protocol_errors(self):
        script = smt.Script()
        requested = script.fresh_constant("requested", smt.BOOL)
        first = subprocess.CompletedProcess(["z3"], 0, "sat\n", "")
        malformed = (
            subprocess.CompletedProcess(["z3"], 1, "unknown\n", "fatal\n"),
            subprocess.CompletedProcess(["z3"], 0, "unknown\nsat\n", ""),
        )
        for second in malformed:
            with self.subTest(second=second):
                with mock.patch.object(verifier, "_run_solver", side_effect=[first, second]):
                    with self.assertRaises(SolverError):
                        verifier.query_solver(Problem(script, {}), "z3", (requested,))

    def test_model_unknown_preserves_the_confirmed_counterexample(self):
        script = smt.Script()
        present = script.fresh_constant("present", smt.BOOL)
        problem = Problem(script, {"A": (relation_model.WitnessRow(present, {}),)})
        responses = [
            subprocess.CompletedProcess(["z3"], 0, "sat\n", ""),
            subprocess.CompletedProcess(
                ["z3"],
                1,
                'unknown\n(error "model is not available")\n',
                "",
            ),
        ]
        with mock.patch.object(verifier, "_run_solver", side_effect=responses):
            result = solve(problem, "z3", 1)
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertIsNone(result.witness)
        self.assertIn("model", result.reason)

    def test_schema_mismatch_is_a_correctness_verdict(self):
        output = io.StringIO()
        with (
            mock.patch.object(cli, "load_snapshot", return_value=mock.sentinel.snapshot),
            mock.patch.object(cli, "build_problem", side_effect=SchemaMismatch("changed root")),
            redirect_stdout(output),
        ):
            exit_code = cli.main(["before.json", "after.json", "--emit-smt", "formula.smt2"])
        self.assertEqual(exit_code, 1)
        self.assertIn('"status": "SCHEMA_MISMATCH"', output.getvalue())
        self.assertIn('"task_bound": 2', output.getvalue())

    def test_root_nullability_is_part_of_the_contract(self):
        nullable = parse_snapshot(
            {
                "format": "ydb-rbo-semantic-snapshot",
                "version": 1,
                "schema": {"tables": []},
                "plan": {
                    "nodes": [
                        {"id": "source", "op": "empty_source"},
                        {
                            "id": "project",
                            "op": "project",
                            "input": "source",
                            "ordered": False,
                            "columns": [
                                {
                                    "output": "result",
                                    "expression": {
                                        "kind": "opaque",
                                        "fingerprint": "nullable_constant",
                                        "type": "Int64",
                                        "nullable": True,
                                        "args": [],
                                    },
                                }
                            ],
                        },
                    ],
                    "root": "project",
                    "output": ["result"],
                    "subplans": [],
                },
                "stage_graph": None,
            }
        )
        with self.assertRaisesRegex(SchemaMismatch, "root output nullability differs"):
            build_logical_kernel_problem_for_tests(constant_snapshot(1), nullable, 0)

    def test_root_output_name_is_part_of_the_contract(self):
        with self.assertRaisesRegex(SchemaMismatch, "root output names or order differ"):
            build_logical_kernel_problem_for_tests(
                constant_snapshot(1, "before_name"),
                constant_snapshot(1, "after_name"),
                0,
            )

    def test_exact_integer_width_is_part_of_the_contract(self):
        with self.assertRaisesRegex(SchemaMismatch, "root output type differs"):
            build_logical_kernel_problem_for_tests(
                constant_snapshot(1, scalar_type="Int32"),
                constant_snapshot(1, scalar_type="Int64"),
                0,
            )


class BoundaryContractTest(unittest.TestCase):
    def test_initial_boundary_must_be_logical(self):
        staged = passthrough_stage_snapshot({"kind": "map"})
        logical = passthrough_stage_snapshot()
        with self.assertRaisesRegex(VerificationError, "initial snapshot.*stage_graph:null"):
            build_problem(staged, logical, 1)

    def test_final_boundary_must_have_a_stage_graph(self):
        logical = passthrough_stage_snapshot()
        with self.assertRaisesRegex(VerificationError, "final snapshot.*non-null stage_graph"):
            build_problem(logical, logical, 1)

    def test_logical_kernel_escape_hatch_rejects_staged_snapshots(self):
        logical = passthrough_stage_snapshot()
        staged = passthrough_stage_snapshot({"kind": "map"})
        with self.assertRaisesRegex(VerificationError, "test comparisons require stage_graph:null"):
            build_logical_kernel_problem_for_tests(logical, staged, 1)

    def test_transformation_prefix_accepts_logical_or_complete_staged_rhs(self):
        logical = passthrough_stage_snapshot()
        staged = passthrough_stage_snapshot({"kind": "map"})
        self.assertIsInstance(
            build_transformation_prefix_problem(logical, logical, 1), Problem
        )
        self.assertIsInstance(
            build_transformation_prefix_problem(logical, staged, 1), Problem
        )
        with self.assertRaisesRegex(
            VerificationError,
            "transformation-prefix comparison requires a logical initial",
        ):
            build_transformation_prefix_problem(staged, logical, 1)

    def test_cli_enforces_boundary_roles(self):
        logical = passthrough_stage_snapshot()
        errors = io.StringIO()
        with (
            mock.patch.object(cli, "load_snapshot", side_effect=[logical, logical]),
            redirect_stderr(errors),
        ):
            exit_code = cli.main(
                ["initial.json", "final.json", "--emit-smt", "unused.smt2"]
            )
        self.assertEqual(exit_code, 2)
        self.assertIn('"status": "UNSUPPORTED"', errors.getvalue())
        self.assertIn("final snapshot", errors.getvalue())

    def test_cli_labels_the_explicit_transformation_prefix_scope(self):
        logical = passthrough_stage_snapshot()
        output = io.StringIO()
        problem = Problem(smt.Script(), {})
        with (
            mock.patch.object(cli, "load_snapshot", side_effect=[logical, logical]),
            mock.patch.object(
                cli, "build_transformation_prefix_problem", return_value=problem
            ) as builder,
            mock.patch.object(cli.Path, "write_text"),
            redirect_stdout(output),
        ):
            exit_code = cli.main([
                "initial.json",
                "prefix.json",
                "--diagnostic-transformation-prefix",
                "--emit-smt",
                "unused.smt2",
            ])
        self.assertEqual(exit_code, 0)
        self.assertIn(
            '"comparison_scope": "OPTIMIZER_TRANSFORMATION_PREFIX"',
            output.getvalue(),
        )
        builder.assert_called_once_with(logical, logical, 2, 10_000)


class _MissingFunction(Exception):
    def __init__(self, key, sort):
        super().__init__(key)
        self.key = key
        self.sort = sort


def _restricted_domain_has_model(script):
    """Exhaust a tiny data domain and all Boolean/UF choices.

    This is deliberately test-only and independent of SMT-LIB rendering. It
    produces valid witnesses when true. False only means that this restricted
    domain has no witness; solver-backed tests establish actual UNSAT results.
    """

    root = smt.and_(*script.assertions)
    symbols = {}
    integer_literals = {0, 1}

    def collect(term):
        if term.operation == "symbol":
            symbols[term.atom] = term.sort
        elif term.operation == "int" and -2 <= term.atom <= 2:
            integer_literals.add(term.atom)
        for argument in term.arguments:
            collect(argument)

    collect(root)
    names = tuple(sorted(symbols))
    domains = {smt.BOOL: (False, True), smt.INT: tuple(sorted(integer_literals))}

    def evaluate(term, constants, functions):
        if term.operation == "symbol":
            return constants[term.atom]
        if term.operation in {"bool", "int"}:
            return term.atom
        if term.operation == "not":
            return not evaluate(term.arguments[0], constants, functions)
        if term.operation == "and":
            return all(evaluate(argument, constants, functions) for argument in term.arguments)
        if term.operation == "or":
            return any(evaluate(argument, constants, functions) for argument in term.arguments)
        if term.operation == "=":
            return evaluate(term.arguments[0], constants, functions) == evaluate(
                term.arguments[1], constants, functions
            )
        if term.operation == "<":
            return evaluate(term.arguments[0], constants, functions) < evaluate(
                term.arguments[1], constants, functions
            )
        if term.operation == "ite":
            branch = term.arguments[1] if evaluate(term.arguments[0], constants, functions) else term.arguments[2]
            return evaluate(branch, constants, functions)
        if term.operation == "+":
            return sum(evaluate(argument, constants, functions) for argument in term.arguments)
        if term.operation == "mod":
            return evaluate(term.arguments[0], constants, functions) % evaluate(
                term.arguments[1], constants, functions
            )
        if term.operation.startswith("f_"):
            key = (
                term.operation,
                tuple(evaluate(argument, constants, functions) for argument in term.arguments),
            )
            if key not in functions:
                raise _MissingFunction(key, term.sort)
            return functions[key]
        raise AssertionError(f"unsupported test SMT operation {term.operation!r}")

    def choose_functions(constants, functions):
        try:
            return evaluate(root, constants, functions) is True
        except _MissingFunction as missing:
            for value in domains[missing.sort]:
                functions[missing.key] = value
                if choose_functions(constants, functions):
                    return True
            del functions[missing.key]
            return False

    for values in product(*(domains[symbols[name]] for name in names)):
        if choose_functions(dict(zip(names, values)), {}):
            return True
    return False


def _evaluate_ground_term(term, constants, function_value=None):
    if term.operation == "symbol":
        return constants[term.atom]
    if term.operation in {"bool", "int"}:
        return term.atom
    if term.operation == "not":
        return not _evaluate_ground_term(term.arguments[0], constants, function_value)
    if term.operation == "and":
        return all(
            _evaluate_ground_term(argument, constants, function_value)
            for argument in term.arguments
        )
    if term.operation == "or":
        return any(
            _evaluate_ground_term(argument, constants, function_value)
            for argument in term.arguments
        )
    if term.operation == "=":
        return _evaluate_ground_term(
            term.arguments[0], constants, function_value
        ) == _evaluate_ground_term(
            term.arguments[1], constants, function_value
        )
    if term.operation == "<":
        return _evaluate_ground_term(
            term.arguments[0], constants, function_value
        ) < _evaluate_ground_term(
            term.arguments[1], constants, function_value
        )
    if term.operation == "ite":
        branch = (
            term.arguments[1]
            if _evaluate_ground_term(term.arguments[0], constants, function_value)
            else term.arguments[2]
        )
        return _evaluate_ground_term(branch, constants, function_value)
    if term.operation == "+":
        return sum(
            _evaluate_ground_term(argument, constants, function_value)
            for argument in term.arguments
        )
    if term.operation == "-":
        left, right = term.arguments
        return _evaluate_ground_term(
            left, constants, function_value
        ) - _evaluate_ground_term(
            right, constants, function_value
        )
    if term.operation == "*":
        left, right = term.arguments
        return _evaluate_ground_term(
            left, constants, function_value
        ) * _evaluate_ground_term(
            right, constants, function_value
        )
    if term.operation == "div":
        dividend = _evaluate_ground_term(
            term.arguments[0], constants, function_value
        )
        divisor = _evaluate_ground_term(
            term.arguments[1], constants, function_value
        )
        if dividend < 0 or divisor <= 0:
            raise AssertionError(
                "the concrete test evaluator only admits nonnegative "
                "division by a positive integer"
            )
        return dividend // divisor
    if term.operation == "mod":
        return _evaluate_ground_term(
            term.arguments[0], constants, function_value
        ) % _evaluate_ground_term(
            term.arguments[1], constants, function_value
        )
    if term.operation.startswith("f_") and function_value is not None:
        return function_value(
            term.operation,
            tuple(
                _evaluate_ground_term(argument, constants, function_value)
                for argument in term.arguments
            ),
        )
    raise AssertionError(f"non-ground SMT operation {term.operation!r}")


class ConstructionAuditBoundTest(unittest.TestCase):
    def test_database_row_bound_is_reported_as_unsupported(self):
        snapshot = right_join(KEY_EQUALITY)
        with mock.patch.object(relation_model, "MAX_RELATION_ROWS", 1):
            with self.assertRaisesRegex(
                VerificationError,
                "database table requires 2 candidate rows.*1 row construction",
            ):
                build_logical_kernel_problem_for_tests(snapshot, snapshot, 2)

    def test_relation_row_invariant_is_a_fail_closed_backup(self):
        snapshot = right_join(KEY_EQUALITY)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        source = database.relations["A"]
        with mock.patch.object(relation_model, "MAX_RELATION_ROWS", 1):
            with self.assertRaisesRegex(
                RelationError,
                "relation requires 2 candidate rows.*1 row construction",
            ):
                relation_model.Relation(source.columns, source.rows)

    def test_join_matching_and_output_are_checked_before_construction(self):
        snapshot = right_join(KEY_EQUALITY)
        with mock.patch.object(relation_model, "MAX_RELATION_ROW_PAIRS", 3):
            with self.assertRaisesRegex(
                VerificationError,
                "join matching requires 4 candidate-row pairs.*3 pair construction",
            ):
                build_logical_kernel_problem_for_tests(snapshot, snapshot, 2)

        with mock.patch.object(relation_model, "MAX_RELATION_ROWS", 5):
            with self.assertRaisesRegex(
                VerificationError,
                "join output requires 6 candidate rows.*5 row construction",
            ):
                build_logical_kernel_problem_for_tests(snapshot, snapshot, 2)

    def test_union_and_grouped_aggregate_fail_before_quadratic_expansion(self):
        union = union_snapshot(True)
        with mock.patch.object(relation_model, "MAX_RELATION_ROWS", 3):
            with self.assertRaisesRegex(
                VerificationError,
                "union-all requires 4 candidate rows.*3 row construction",
            ):
                build_logical_kernel_problem_for_tests(union, union, 2)

        aggregate = aggregate_stage_snapshot("count", True, False)
        with mock.patch.object(relation_model, "MAX_RELATION_ROW_PAIRS", 2):
            with self.assertRaisesRegex(
                VerificationError,
                "grouped aggregate requires 3 candidate-row pairs.*2 pair construction",
            ):
                build_logical_kernel_problem_for_tests(aggregate, aggregate, 2)

        with mock.patch.object(relation_model, "MAX_RELATION_ROW_PAIRS", 3):
            build_logical_kernel_problem_for_tests(aggregate, aggregate, 2)

        compactable = duplicated_grouped_aggregate_snapshot("count")
        with mock.patch.object(relation_model, "MAX_RELATION_ROW_PAIRS", 7):
            with self.assertRaisesRegex(
                VerificationError,
                "grouped aggregate requires 10 candidate-row pairs.*"
                "7 pair construction",
            ):
                build_logical_kernel_problem_for_tests(
                    compactable,
                    compactable,
                    2,
                )

    def test_distinct_aggregate_pair_cap_precedes_construction(self):
        snapshot = count_distinct_int64_snapshot()
        script = smt.Script()
        database = Database(snapshot, 3, script)
        with mock.patch.object(relation_model, "MAX_RELATION_ROW_PAIRS", 2):
            with self.assertRaisesRegex(
                RelationError,
                "distinct aggregate requires 3 candidate-row pairs.*"
                "2 pair construction",
            ):
                RelationEvaluator(
                    snapshot,
                    database,
                    ScalarEncoder(script),
                ).root()

        with mock.patch.object(relation_model, "MAX_RELATION_ROW_PAIRS", 3):
            RelationEvaluator(
                snapshot,
                database,
                ScalarEncoder(script),
            ).root()


class AggregateConcreteDifferentialTest(unittest.TestCase):
    def test_staged_shared_name_join_keys_remain_side_explicit(self):
        states = (
            (False, None),
            (True, None),
            (True, 0),
            (True, 1),
        )
        for kind in ("left_semi", "left_anti", "right_semi", "right_anti"):
            snapshot = shared_name_semi_join_stage_snapshot(kind)
            script = smt.Script()
            database = Database(snapshot, 1, script)
            router = Router(script)
            relation = StageEvaluator(
                snapshot,
                database,
                ScalarEncoder(script),
                router,
            ).root().certain()

            for left, right, left_task, right_task in product(
                states,
                states,
                (False, True),
                (False, True),
            ):
                constants = {}
                for table, state in (("A", left), ("B", right)):
                    witness = database.witness[table][0]
                    present, value = state
                    constants[witness.present.atom] = present
                    cell = witness.cells["k"]
                    constants[cell.is_null.atom] = value is None
                    constants[cell.value.atom] = 0 if value is None else value
                constants[router.source_task("A", 0).atom] = left_task
                constants[router.source_task("B", 0).atom] = right_task

                matches = (
                    left[0]
                    and right[0]
                    and left[1] is not None
                    and right[1] is not None
                    and left[1] == right[1]
                )
                selected = left if kind.startswith("left_") else right
                keep = selected[0] and (
                    matches if kind.endswith("_semi") else not matches
                )
                expected = (
                    Counter({(selected[1],): 1})
                    if keep
                    else Counter()
                )
                with self.subTest(
                    kind=kind,
                    left=left,
                    right=right,
                    left_task=left_task,
                    right_task=right_task,
                ):
                    self.assertEqual(
                        self._symbolic_bag(
                            relation,
                            constants,
                            self._hash_choice,
                        ),
                        expected,
                    )

    def test_scalar_uint64_unwrap_coalesces_an_empty_sum_to_zero(self):
        snapshot = unwrapped_uint64_sum_snapshot()
        self.assertEqual(
            [
                (column.type, column.nullable)
                for column in snapshot.output_schema()
            ],
            [("Uint64", False)],
        )
        script = smt.Script()
        database = Database(snapshot, 2, script)
        relation = RelationEvaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root().certain()

        states = (None, (0, None), (0, 0), (0, 1), (0, (1 << 64) - 1))
        for rows in product(states, repeat=2):
            non_null = [
                row[1]
                for row in rows
                if row is not None and row[1] is not None
            ]
            expected = Counter({(sum(non_null) % (1 << 64),): 1})
            with self.subTest(rows=rows):
                self.assertEqual(
                    self._symbolic_bag(
                        relation,
                        self._constants(database, rows),
                    ),
                    expected,
                )

    def test_scalar_count_distinct_matches_a_tiny_set_reference(self):
        snapshot = count_distinct_int64_snapshot()
        script = smt.Script()
        database = Database(snapshot, 3, script)
        relation = RelationEvaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root().certain()

        states = (None, (0, -1), (0, 0), (0, 1))
        for rows in product(states, repeat=3):
            expected = Counter({
                (len({row[1] for row in rows if row is not None}),): 1,
            })
            with self.subTest(rows=rows):
                self.assertEqual(
                    self._symbolic_bag(
                        relation,
                        self._constants(database, rows),
                    ),
                    expected,
                )

    def test_composite_distinct_all_matches_nullable_tuple_set_reference(self):
        snapshot = composite_distinct_all_snapshot()
        script = smt.Script()
        database = Database(snapshot, 2, script)
        relation = RelationEvaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root().certain()

        states = (None,) + tuple(product((None, 0, 1), repeat=2))
        for rows in product(states, repeat=2):
            with self.subTest(rows=rows):
                actual = self._symbolic_bag(
                    relation,
                    self._constants(database, rows),
                )
                expected = Counter(
                    row for row in set(rows) if row is not None
                )
                self.assertEqual(actual, expected)

    def test_structural_key_classes_are_exact_and_nonrecursive(self):
        left = smt.symbol("same_key", smt.INT)
        right = smt.symbol("same_key", smt.INT)
        for _ in range(2000):
            left = smt.Term(smt.INT, "deep_key", (left,))
            right = smt.Term(smt.INT, "deep_key", (right,))

        same_null_left = smt.symbol("same_null", smt.BOOL)
        same_null_right = smt.symbol("same_null", smt.BOOL)
        rows = (
            relation_model.Row(
                smt.TRUE,
                {"k": Value("Int64", same_null_left, left)},
            ),
            relation_model.Row(
                smt.TRUE,
                {"k": Value("Int64", same_null_right, right)},
            ),
            relation_model.Row(
                smt.TRUE,
                {"k": Value("Int64", same_null_left, smt.symbol("other", smt.INT))},
            ),
            relation_model.Row(
                smt.TRUE,
                {"k": Value("Uint64", same_null_left, left)},
            ),
        )

        self.assertEqual(
            relation_model._aggregate_key_classes(("k",), rows),
            ((0, 1), (2,), (3,)),
        )

    def test_structural_key_classes_preserve_multiplicity_and_dynamic_equality(self):
        for function in ("count", "sum"):
            for pair_bound in (8, 16):
                with self.subTest(function=function, pair_bound=pair_bound):
                    snapshot = duplicated_grouped_aggregate_snapshot(
                        function,
                        nullable_key=True,
                    )
                    script = smt.Script()
                    database = Database(snapshot, 2, script)
                    evaluator = RelationEvaluator(
                        snapshot,
                        database,
                        ScalarEncoder(script),
                    )
                    source = evaluator.node("union").certain()
                    self.assertIsNot(
                        source.rows[0].values["u.k"].value,
                        source.rows[2].values["u.k"].value,
                    )
                    self.assertEqual(
                        relation_model._aggregate_key_classes(
                            ("u.k",),
                            source.rows,
                        ),
                        ((0, 2), (1, 3)),
                    )
                    with (
                        mock.patch.object(
                            relation_model,
                            "MAX_RELATION_ROW_PAIRS",
                            pair_bound,
                        ),
                        mock.patch.object(
                            evaluator,
                            "_same_group",
                            wraps=evaluator._same_group,
                        ) as same_group,
                        mock.patch.object(
                            relation_model,
                            "_require_relation_row_pairs",
                            wraps=relation_model._require_relation_row_pairs,
                        ) as require_pairs,
                    ):
                        result = evaluator.root().certain()

                    self.assertEqual(len(result.rows), 2)
                    self.assertEqual(same_group.call_count, 3)
                    self.assertIn(
                        mock.call(8, "grouped aggregate class membership"),
                        require_pairs.call_args_list,
                    )
                    self.assertIn(
                        mock.call(3, "grouped aggregate class comparison"),
                        require_pairs.call_args_list,
                    )
                    self.assertTrue(all(
                        row.occurrence is None
                        for row in result.rows
                    ))
                    states = (None,) + tuple(product((None, 0, 1), (-2, 3)))
                    for rows in product(states, repeat=2):
                        expanded = tuple(
                            row
                            for row in rows
                            for _ in range(2)
                        )
                        self.assertEqual(
                            self._symbolic_bag(
                                result,
                                self._constants(database, rows),
                            ),
                            self._reference_bag(
                                function,
                                True,
                                expanded,
                                False,
                            ),
                            rows,
                        )

    def test_equal_cost_classes_stay_directional_and_shared_metadata_is_conservative(self):
        snapshot = duplicated_grouped_aggregate_snapshot("count")
        script = smt.Script()
        evaluator = RelationEvaluator(
            snapshot,
            Database(snapshot, 1, script),
            ScalarEncoder(script),
        )
        shared = relation_model.PartitionFact(
            smt.symbol("shared_fact", smt.BOOL),
            True,
        )
        left_only = relation_model.PartitionFact(
            smt.symbol("left_fact", smt.BOOL),
            True,
        )
        right_only = relation_model.PartitionFact(
            smt.symbol("right_fact", smt.BOOL),
            False,
        )

        def row(
            ordinal,
            key,
            facts,
        ):
            return relation_model.Row(
                smt.symbol(f"present_{ordinal}", smt.BOOL),
                {
                    "u.k": Value("Int64", smt.FALSE, key),
                    "u.x": Value(
                        "Int64",
                        smt.FALSE,
                        smt.symbol(f"value_{ordinal}", smt.INT),
                    ),
                },
                relation_model.Occurrence("input", "source", ordinal),
                frozenset(facts),
            )

        source = relation_model.Relation(
            evaluator._columns("union"),
            (
                row(
                    0,
                    smt.symbol("same_key", smt.INT),
                    (shared, left_only),
                ),
                row(
                    1,
                    smt.symbol("same_key", smt.INT),
                    (shared, right_only),
                ),
                row(
                    2,
                    smt.symbol("other_key", smt.INT),
                    (shared, left_only),
                ),
            ),
        )
        classes = relation_model._aggregate_key_classes(("u.k",), source.rows)
        self.assertEqual(classes, ((0, 1), (2,)))
        with mock.patch.object(
            evaluator,
            "_same_group",
            wraps=evaluator._same_group,
        ) as same_group:
            directional = evaluator._grouped_aggregate_rows(
                evaluator.nodes["aggregate"],
                source,
            )
        self.assertEqual(len(directional), 3)
        self.assertEqual(same_group.call_count, 12)

        result = evaluator._shared_grouped_aggregate_rows(
            evaluator.nodes["aggregate"],
            source,
            classes,
        )

        self.assertIsNone(result[0].occurrence)
        self.assertEqual(result[0].partition_facts, frozenset({shared}))
        self.assertEqual(
            result[1].occurrence,
            relation_model.Occurrence(
                "aggregate",
                "aggregate",
                inputs=(source.rows[2].occurrence,),
            ),
        )
        self.assertEqual(
            result[1].partition_facts,
            frozenset({shared, left_only}),
        )

    def test_compacted_partial_states_survive_hash_and_broadcast(self):
        for connection_kind in ("hash_shuffle", "broadcast"):
            with self.subTest(connection_kind=connection_kind):
                snapshot = constant_grouped_stage_snapshot(connection_kind)
                script = smt.Script()
                database = Database(snapshot, 2, script)
                router = Router(script)
                edge_inputs = []

                def observe(_edge, task, family):
                    edge_inputs.append((task, family.certain()))

                result = StageEvaluator(
                    snapshot,
                    database,
                    ScalarEncoder(script),
                    router,
                    edge_observer=observe,
                ).root().certain()

                self.assertEqual(
                    len(edge_inputs),
                    2 if connection_kind == "hash_shuffle" else 1,
                )
                for _, edge_input in edge_inputs:
                    self.assertEqual(len(edge_input.rows), 2)
                    self.assertTrue(
                        all(row.occurrence is None for row in edge_input.rows)
                    )

                states = (None, (0, 7))
                for rows, placements in product(
                    product(states, repeat=2),
                    product((False, True), repeat=2),
                ):
                    constants = self._constants(database, rows)
                    for slot, placement in enumerate(placements):
                        constants[router.source_task("A", slot).atom] = placement
                    present = sum(row is not None for row in rows)
                    expected = (
                        Counter()
                        if present == 0
                        else Counter({(0, present): 1})
                    )
                    self.assertEqual(
                        self._symbolic_bag(
                            result,
                            constants,
                            self._hash_choice,
                        ),
                        expected,
                        (rows, placements),
                    )

    def test_group_comparisons_share_one_symmetric_triangle(self):
        snapshot = aggregate_stage_snapshot("count", True, False)
        script = smt.Script()
        database = Database(snapshot, 3, script)
        evaluator = RelationEvaluator(snapshot, database, ScalarEncoder(script))
        source_rows = evaluator.node("a").certain().rows

        with (
            mock.patch.object(relation_model, "MAX_RELATION_ROW_PAIRS", 6),
            mock.patch.object(
                evaluator,
                "_same_group",
                wraps=evaluator._same_group,
            ) as same_group,
        ):
            relation = evaluator.root().certain()

        self.assertEqual(len(relation.rows), 3)
        self.assertEqual(same_group.call_count, 6)
        expected_pairs = (
            (source_rows[0], source_rows[0]),
            (source_rows[0], source_rows[1]),
            (source_rows[0], source_rows[2]),
            (source_rows[1], source_rows[1]),
            (source_rows[1], source_rows[2]),
            (source_rows[2], source_rows[2]),
        )
        for call, expected in zip(same_group.call_args_list, expected_pairs):
            self.assertIs(call.args[1], expected[0])
            self.assertIs(call.args[2], expected[1])

    def test_shared_group_comparisons_keep_presence_directional(self):
        snapshot = aggregate_stage_snapshot(
            "count",
            True,
            False,
            nullable_key=True,
        )
        script = smt.Script()
        database = Database(snapshot, 2, script)
        with mock.patch.object(relation_model, "MAX_RELATION_ROW_PAIRS", 3):
            relation = RelationEvaluator(
                snapshot,
                database,
                ScalarEncoder(script),
            ).root().certain()

        cases = (
            ((None, (None, 7)), Counter({(None, 1): 1})),
            (((None, 7), None), Counter({(None, 1): 1})),
            (((None, 7), (None, 8)), Counter({(None, 2): 1})),
        )
        for rows, expected in cases:
            with self.subTest(rows=rows):
                self.assertEqual(
                    self._symbolic_bag(relation, self._constants(database, rows)),
                    expected,
                )

    def test_void_count_input_implements_count_star(self):
        snapshot = count_star_snapshot()
        script = smt.Script()
        database = Database(snapshot, 2, script)
        relation = RelationEvaluator(
            snapshot, database, ScalarEncoder(script)
        ).root().certain()

        states = (None, (0, None), (0, 7))
        for rows in product(states, repeat=2):
            with self.subTest(rows=rows):
                actual = self._symbolic_bag(
                    relation,
                    self._constants(database, rows),
                )
                expected = Counter({(sum(row is not None for row in rows),): 1})
                self.assertEqual(actual, expected)

    def test_partial_sum_canonicalization_preserves_64_bit_wrapping(self):
        cases = (
            ("Int64", ((1 << 63) - 1, 1), -(1 << 63)),
            ("Int64", (-(1 << 63), -1), (1 << 63) - 1),
            ("Uint64", ((1 << 64) - 1, 1), 0),
        )
        for scalar_type, inputs, expected in cases:
            with self.subTest(scalar_type=scalar_type, inputs=inputs):
                raw_terms = tuple(
                    smt.symbol(f"partial_{scalar_type}_{index}", smt.INT)
                    for index in range(len(inputs))
                )
                partials = tuple(
                    relation_model._wrap_sum(raw, scalar_type)
                    for raw in raw_terms
                )
                lifted = relation_model._wrap_sum(
                    smt.add(
                        *(
                            relation_model._unwrap_sum(
                                Value(scalar_type, smt.FALSE, partial)
                            )
                            for partial in partials
                        )
                    ),
                    scalar_type,
                )
                constants = {
                    raw.atom: concrete for raw, concrete in zip(raw_terms, inputs)
                }
                self.assertEqual(_evaluate_ground_term(lifted, constants), expected)

    def test_symbolic_aggregate_matches_independent_tiny_reference(self):
        cases = (
            ("count", "Int64"),
            ("sum", "Int8"),
            ("sum", "Int64"),
            ("sum", "Uint8"),
            ("sum", "Uint64"),
        )
        bounds = {
            "Int8": (-(1 << 7), (1 << 7) - 1),
            "Int64": (-(1 << 63), (1 << 63) - 1),
            "Uint8": (0, (1 << 8) - 1),
            "Uint64": (0, (1 << 64) - 1),
        }
        for (function, input_type), grouped, nullable_input in product(
            cases, (False, True), (False, True)
        ):
            nullable_keys = (False, True) if grouped else (False,)
            for nullable_key in nullable_keys:
                with self.subTest(
                    function=function,
                    input_type=input_type,
                    grouped=grouped,
                    nullable_input=nullable_input,
                    nullable_key=nullable_key,
                ):
                    snapshot = aggregate_stage_snapshot(
                        function,
                        grouped,
                        False,
                        nullable_input=nullable_input,
                        nullable_key=nullable_key,
                        input_type=input_type,
                    )
                    script = smt.Script()
                    database = Database(snapshot, 2, script)
                    pair_bound = (
                        3 if grouped else relation_model.MAX_RELATION_ROW_PAIRS
                    )
                    with mock.patch.object(
                        relation_model,
                        "MAX_RELATION_ROW_PAIRS",
                        pair_bound,
                    ):
                        relation = RelationEvaluator(
                            snapshot, database, ScalarEncoder(script)
                        ).root().certain()
                    key_values = (None, 0, 1) if nullable_key else (0, 1)
                    minimum, maximum = bounds[input_type]
                    concrete_values = tuple(dict.fromkeys((minimum, 0, 1, maximum)))
                    input_values = (
                        (None,) + concrete_values
                        if nullable_input
                        else concrete_values
                    )
                    states = (None,) + tuple(product(key_values, input_values))
                    for rows in product(states, repeat=2):
                        constants = self._constants(database, rows)
                        actual = self._symbolic_bag(relation, constants)
                        expected = self._reference_bag(
                            function,
                            grouped,
                            rows,
                            input_type.startswith("Uint"),
                        )
                        self.assertEqual(actual, expected, rows)

    def test_split_stage_aggregate_matches_independent_task_reference(self):
        states = (None,) + tuple(
            product((None, 0, 1), (None, -1, 0, 1))
        )
        for function, grouped in product(("count", "sum"), (False, True)):
            with self.subTest(function=function, grouped=grouped):
                snapshot = aggregate_stage_snapshot(
                    function,
                    grouped,
                    True,
                    nullable_input=True,
                    nullable_key=True,
                )
                script = smt.Script()
                database = Database(snapshot, 2, script)
                router = Router(script)
                pair_bound = (
                    10 if grouped else relation_model.MAX_RELATION_ROW_PAIRS
                )
                with mock.patch.object(
                    relation_model,
                    "MAX_RELATION_ROW_PAIRS",
                    pair_bound,
                ):
                    relation = StageEvaluator(
                        snapshot,
                        database,
                        ScalarEncoder(script),
                        router,
                    ).root().certain()
                for rows, placements in product(
                    product(states, repeat=2),
                    product((False, True), repeat=2),
                ):
                    constants = self._constants(database, rows)
                    for slot, placement in enumerate(placements):
                        source_task = router.source_task("A", slot)
                        constants[source_task.atom] = placement
                    actual = self._symbolic_bag(
                        relation,
                        constants,
                        self._hash_choice,
                    )
                    expected = self._split_reference_bag(
                        function,
                        grouped,
                        rows,
                        placements,
                    )
                    self.assertEqual(actual, expected, (rows, placements))

    def test_split_distinct_all_matches_independent_task_reference(self):
        snapshot = distinct_all_stage_snapshot(True)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        router = Router(script)
        relation = StageEvaluator(
            snapshot,
            database,
            ScalarEncoder(script),
            router,
        ).root().certain()

        states = (None,) + tuple((key, 0) for key in (None, 0, 1))
        for rows, placements in product(
            product(states, repeat=2),
            product((False, True), repeat=2),
        ):
            constants = self._constants(database, rows)
            for slot, placement in enumerate(placements):
                constants[router.source_task("A", slot).atom] = placement
            actual = self._symbolic_bag(
                relation,
                constants,
                self._hash_choice,
            )
            expected = Counter(
                (key,)
                for key in {
                    row[0]
                    for row in rows
                    if row is not None
                }
            )
            self.assertEqual(actual, expected, (rows, placements))

    def test_decimal_aggregates_match_independent_special_null_and_group_reference(self):
        self.assertEqual(decimal.INF, REFERENCE_DECIMAL_INF)
        self.assertEqual(decimal.NAN, REFERENCE_DECIMAL_NAN)
        concrete_values = (
            -REFERENCE_DECIMAL_INF,
            -1,
            0,
            1,
            REFERENCE_DECIMAL_INF,
            REFERENCE_DECIMAL_NAN,
        )
        for function, grouped, nullable_input, nullable_key in product(
            ("max", "min", "sum"),
            (False, True),
            (False, True),
            (False, True),
        ):
            if not grouped and nullable_key:
                continue
            snapshot = aggregate_stage_snapshot(
                function,
                grouped,
                False,
                nullable_input=nullable_input,
                nullable_key=nullable_key,
                input_type="Decimal(2,0)",
            )
            script = smt.Script()
            database = Database(snapshot, 2, script)
            pair_bound = 3 if grouped else relation_model.MAX_RELATION_ROW_PAIRS
            with mock.patch.object(
                relation_model,
                "MAX_RELATION_ROW_PAIRS",
                pair_bound,
            ):
                relation = RelationEvaluator(
                    snapshot,
                    database,
                    ScalarEncoder(script),
                ).root().certain()
            keys = (None, 0, 1) if nullable_key else (0, 1)
            values = (
                (None,) + concrete_values
                if nullable_input
                else concrete_values
            )
            states = (None,) + tuple(product(keys, values))
            for rows in product(states, repeat=2):
                with self.subTest(
                    function=function,
                    grouped=grouped,
                    nullable_input=nullable_input,
                    nullable_key=nullable_key,
                    rows=rows,
                ):
                    actual = self._symbolic_bag(
                        relation,
                        self._constants(database, rows),
                    )
                    expected = self._decimal_reference_bag(function, grouped, rows)
                    self.assertEqual(actual, expected)

    def test_decimal_avg_matches_independent_exhaustive_reference(self):
        for values, expected in (
            ((0, 1), 0),
            ((1, 2), 2),
            ((-1, 0), 0),
            ((-2, -1), -2),
        ):
            with self.subTest(tie=values):
                self.assertEqual(
                    self._reference_decimal_average(values),
                    expected,
                )

        concrete_values = (
            -REFERENCE_DECIMAL_INF,
            -2,
            -1,
            0,
            1,
            2,
            REFERENCE_DECIMAL_INF,
            REFERENCE_DECIMAL_NAN,
        )
        for grouped in (False, True):
            snapshot = aggregate_stage_snapshot(
                "avg",
                grouped,
                False,
                nullable_input=True,
                input_type="Decimal(2,0)",
            )
            script = smt.Script()
            database = Database(snapshot, 2, script)
            pair_bound = 3 if grouped else relation_model.MAX_RELATION_ROW_PAIRS
            with mock.patch.object(
                relation_model,
                "MAX_RELATION_ROW_PAIRS",
                pair_bound,
            ):
                relation = RelationEvaluator(
                    snapshot,
                    database,
                    ScalarEncoder(script),
                ).root().certain()
            states = (None,) + tuple(
                (0, value) for value in (None,) + concrete_values
            )
            for rows in product(states, repeat=2):
                with self.subTest(
                    grouped=grouped,
                    rows=rows,
                ):
                    actual = self._symbolic_bag(
                        relation,
                        self._constants(database, rows),
                    )
                    expected = self._decimal_reference_bag("avg", grouped, rows)
                    self.assertEqual(actual, expected)

    def test_split_decimal_avg_weights_unequal_partial_counts(self):
        for grouped, nullable_input in product((False, True), repeat=2):
            rows = (
                ((0, None), (0, 0), (0, 0), (0, 3))
                if nullable_input
                else ((0, 0), (0, 0), (0, 3))
            )
            placements = (
                (False, False, False, True)
                if nullable_input
                else (False, False, True)
            )
            snapshot = aggregate_stage_snapshot(
                "avg",
                grouped,
                True,
                nullable_input=nullable_input,
                input_type="Decimal(2,0)",
            )
            script = smt.Script()
            database = Database(snapshot, len(rows), script)
            router = Router(script)
            with mock.patch.object(
                stage_model,
                "MAX_EXPLICIT_TASK_COPY_ROWS",
                0,
            ):
                relation = StageEvaluator(
                    snapshot,
                    database,
                    ScalarEncoder(script),
                    router,
                ).root().certain()
            constants = self._constants(database, rows)
            for slot, placement in enumerate(placements):
                constants[router.source_task("A", slot).atom] = placement

            with self.subTest(
                grouped=grouped,
                nullable_input=nullable_input,
            ):
                actual = self._symbolic_bag(
                    relation,
                    constants,
                    self._hash_choice,
                )
                expected = self._decimal_reference_bag("avg", grouped, rows)
                self.assertEqual(actual, expected)
                self.assertEqual(
                    actual,
                    Counter({(0, 1) if grouped else (1,): 1}),
                )

    def test_split_decimal_aggregates_match_across_partial_states(self):
        values = (
            None,
            -REFERENCE_DECIMAL_INF,
            -1,
            0,
            1,
            REFERENCE_DECIMAL_INF,
            REFERENCE_DECIMAL_NAN,
        )
        for function, grouped, nullable_input in product(
            ("max", "min", "sum"),
            (False, True),
            (False, True),
        ):
            snapshot = aggregate_stage_snapshot(
                function,
                grouped,
                True,
                nullable_input=nullable_input,
                nullable_key=True,
                input_type="Decimal(2,0)",
            )
            script = smt.Script()
            database = Database(snapshot, 2, script)
            router = Router(script)
            relation = StageEvaluator(
                snapshot,
                database,
                ScalarEncoder(script),
                router,
            ).root().certain()
            input_values = values if nullable_input else values[1:]
            states = (None,) + tuple(product((None, 0, 1), input_values))
            for rows, placements in product(
                product(states, repeat=2),
                product((False, True), repeat=2),
            ):
                with self.subTest(
                    function=function,
                    grouped=grouped,
                    nullable_input=nullable_input,
                    rows=rows,
                    placements=placements,
                ):
                    constants = self._constants(database, rows)
                    for slot, placement in enumerate(placements):
                        constants[
                            router.source_task("A", slot).atom
                        ] = placement
                    actual = self._symbolic_bag(
                        relation,
                        constants,
                        self._hash_choice,
                    )
                    expected = self._decimal_reference_bag(function, grouped, rows)
                    self.assertEqual(actual, expected)

    def test_integral_extrema_match_boundaries_before_and_after_split(self):
        for input_type, function, grouped, nullable_input, staged in product(
            sorted(INTEGER_TYPES),
            ("max", "min"),
            (False, True),
            (False, True),
            (False, True),
        ):
            bounds = integer_bounds(input_type)
            assert bounds is not None
            low, high = bounds[0], bounds[1] - 1
            snapshot = aggregate_stage_snapshot(
                function,
                grouped,
                staged,
                nullable_input=nullable_input,
                input_type=input_type,
            )
            script = smt.Script()
            database = Database(snapshot, 2, script)
            router = Router(script)
            evaluator = (
                StageEvaluator(snapshot, database, ScalarEncoder(script), router)
                if staged
                else RelationEvaluator(snapshot, database, ScalarEncoder(script))
            )
            relation = evaluator.root().certain()
            cases = [
                (None, None),
                ((0, low), None),
                ((0, low), (0, high)),
                ((0, high), (0, low)),
            ]
            if nullable_input:
                cases.append(((0, None), (0, None)))

            placements = product((False, True), repeat=2) if staged else ((False, False),)
            for rows, placement in product(cases, placements):
                with self.subTest(
                    input_type=input_type,
                    function=function,
                    grouped=grouped,
                    nullable_input=nullable_input,
                    staged=staged,
                    rows=rows,
                    placement=placement,
                ):
                    constants = self._constants(database, rows)
                    if staged:
                        for slot, task in enumerate(placement):
                            constants[router.source_task("A", slot).atom] = task
                    self.assertEqual(
                        self._symbolic_bag(
                            relation,
                            constants,
                            self._hash_choice if staged else None,
                        ),
                        self._extrema_reference_bag(function, grouped, rows),
                    )

    def test_three_row_staged_integral_extrema_match_nullable_groups(self):
        rows_cases = tuple(product(
            (None, (0, None), (0, -128), (1, 127)),
            repeat=3,
        ))
        placements = tuple(product((False, True), repeat=3))
        for function in ("max", "min"):
            snapshot = aggregate_stage_snapshot(
                function,
                True,
                True,
                nullable_input=True,
                input_type="Int8",
            )
            script = smt.Script()
            database = Database(snapshot, 3, script)
            router = Router(script)
            relation = StageEvaluator(
                snapshot,
                database,
                ScalarEncoder(script),
                router,
            ).root().certain()
            for rows, placement in product(rows_cases, placements):
                with self.subTest(
                    function=function,
                    rows=rows,
                    placement=placement,
                ):
                    constants = self._constants(database, rows)
                    for slot, task in enumerate(placement):
                        constants[router.source_task("A", slot).atom] = task
                    self.assertEqual(
                        self._symbolic_bag(
                            relation,
                            constants,
                            self._hash_choice,
                        ),
                        self._extrema_reference_bag(function, True, rows),
                    )

    def test_split_decimal_avg_matches_special_state_table(self):
        cases = (
            ((0, REFERENCE_DECIMAL_INF), (0, -REFERENCE_DECIMAL_INF)),
            ((0, REFERENCE_DECIMAL_NAN), (0, 1)),
            ((0, REFERENCE_DECIMAL_INF), (0, -1)),
            ((0, -REFERENCE_DECIMAL_INF), (0, 1)),
            ((0, None), (0, 3)),
            ((0, None), (0, None)),
            (None, (0, 3)),
        )
        for grouped in (False, True):
            snapshot = aggregate_stage_snapshot(
                "avg",
                grouped,
                True,
                nullable_input=True,
                nullable_key=True,
                input_type="Decimal(2,0)",
            )
            script = smt.Script()
            database = Database(snapshot, 2, script)
            router = Router(script)
            relation = StageEvaluator(
                snapshot,
                database,
                ScalarEncoder(script),
                router,
            ).root().certain()

            for rows in cases:
                with self.subTest(grouped=grouped, rows=rows):
                    constants = self._constants(database, rows)
                    constants[router.source_task("A", 0).atom] = False
                    constants[router.source_task("A", 1).atom] = True
                    actual = self._symbolic_bag(
                        relation,
                        constants,
                        self._hash_choice,
                    )
                    expected = self._decimal_reference_bag("avg", grouped, rows)
                    self.assertEqual(actual, expected)

    def test_decimal_extrema_preserve_finite_bound_through_split_state(self):
        for function, staged in product(("max", "min"), (False, True)):
            snapshot = aggregate_stage_snapshot(
                function,
                False,
                staged,
                nullable_input=True,
                input_type="Decimal(3,0)",
            )
            script = smt.Script()
            database = Database(snapshot, 2, script)
            scalar = ScalarEncoder(script)
            evaluator = (
                StageEvaluator(snapshot, database, scalar, Router(script))
                if staged
                else RelationEvaluator(snapshot, database, scalar)
            )
            relation = evaluator.root().certain()
            self.assertEqual(len(relation.rows), 1)
            self.assertEqual(
                relation.rows[0].values["result"].decimal_finite_abs_bound,
                999,
            )

    def test_decimal_sum_fails_closed_when_finite_overflow_can_depend_on_order(self):
        snapshot = aggregate_stage_snapshot(
            "sum",
            False,
            False,
            input_type="Decimal(35,0)",
        )

        one_row_script = smt.Script()
        one_row_database = Database(snapshot, 1, one_row_script)
        RelationEvaluator(
            snapshot,
            one_row_database,
            ScalarEncoder(one_row_script),
        ).root()

        two_row_script = smt.Script()
        two_row_database = Database(snapshot, 2, two_row_script)
        with self.assertRaisesRegex(
            RelationError,
            "non-associative overflow is not modeled",
        ):
            RelationEvaluator(
                snapshot,
                two_row_database,
                ScalarEncoder(two_row_script),
            ).root()

    def test_decimal_avg_fails_closed_when_sum_overflow_can_depend_on_order(self):
        snapshot = aggregate_stage_snapshot(
            "avg",
            False,
            False,
            input_type="Decimal(35,0)",
        )

        one_row_script = smt.Script()
        RelationEvaluator(
            snapshot,
            Database(snapshot, 1, one_row_script),
            ScalarEncoder(one_row_script),
        ).root()

        two_row_script = smt.Script()
        with self.assertRaisesRegex(
            RelationError,
            "Decimal avg sum may overflow",
        ):
            RelationEvaluator(
                snapshot,
                Database(snapshot, 2, two_row_script),
                ScalarEncoder(two_row_script),
            ).root()

    def test_projected_decimal_add_and_sub_supply_sum_headroom(self):
        scalar_type = "Decimal(35,0)"

        def literal(scaled):
            return {
                "kind": "literal",
                "type": scalar_type,
                "value": {"kind": "finite", "scaled": str(scaled)},
            }

        for kind, expected_value in (("add", 5), ("sub", 9)):
            expression = {
                "kind": kind,
                "left": literal(7),
                "right": literal(-2),
                "type": scalar_type,
                "nullable": False,
            }
            snapshot = projected_decimal_sum_snapshot(expression)
            script = smt.Script()
            database = Database(snapshot, 2, script)
            relation = RelationEvaluator(
                snapshot,
                database,
                ScalarEncoder(script),
            ).root().certain()

            with self.subTest(kind=kind, property="bound"):
                self.assertEqual(
                    relation.rows[0].values["result"].decimal_finite_abs_bound,
                    18,
                )
            for rows in product((None, (0, 0)), repeat=2):
                present = sum(row is not None for row in rows)
                expected = None if present == 0 else present * expected_value
                with self.subTest(kind=kind, rows=rows):
                    self.assertEqual(
                        self._symbolic_bag(
                            relation,
                            self._constants(database, rows),
                        ),
                        Counter({(expected,): 1}),
                    )

    def test_projected_decimal_integral_arithmetic_supplies_sum_headroom(self):
        scalar_type = "Decimal(35,0)"

        def decimal_literal(scaled):
            return {
                "kind": "literal",
                "type": scalar_type,
                "value": {"kind": "finite", "scaled": str(scaled)},
            }

        product = {
            "kind": "mul",
            "left": decimal_literal(7),
            "right": {"kind": "column", "column": "a.x"},
            "type": scalar_type,
            "nullable": False,
        }
        quotient = {
            "kind": "div",
            "left": product,
            "right": {"kind": "literal", "type": "Int8", "value": 2},
            "type": scalar_type,
            "nullable": False,
        }
        snapshot = projected_decimal_sum_snapshot(quotient, "Int8")
        script = smt.Script()
        database = Database(snapshot, 2, script)
        relation = RelationEvaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root().certain()

        self.assertEqual(
            relation.rows[0].values["result"].decimal_finite_abs_bound,
            2 * 7 * 128,
        )
        self.assertEqual(
            self._symbolic_bag(
                relation,
                self._constants(database, ((0, -128), (0, 127))),
            ),
            Counter({(-4,): 1}),
        )

        with self.assertRaisesRegex(
            RelationError,
            "non-associative overflow is not modeled",
        ):
            same_decimal_snapshot = projected_decimal_sum_snapshot(
                product,
                scalar_type,
            )
            same_decimal_script = smt.Script()
            RelationEvaluator(
                same_decimal_snapshot,
                Database(same_decimal_snapshot, 2, same_decimal_script),
                ScalarEncoder(same_decimal_script),
            ).root()

    def test_integral_decimal_cast_supplies_sum_headroom(self):
        expression = {
            "kind": "cast_decimal",
            "arg": {"kind": "column", "column": "a.x"},
            "source_type": "Int64",
            "type": "Decimal(35,2)",
            "nullable": False,
        }
        snapshot = projected_decimal_sum_snapshot(expression)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        relation = RelationEvaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root().certain()

        self.assertEqual(
            relation.rows[0].values["result"].decimal_finite_abs_bound,
            2 * (1 << 63) * 100,
        )
        for rows in product((None, (0, -7), (0, 9)), repeat=2):
            present = [row for row in rows if row is not None]
            expected = (
                sum(row[1] * 100 for row in present)
                if present
                else None
            )
            with self.subTest(rows=rows):
                self.assertEqual(
                    self._symbolic_bag(
                        relation,
                        self._constants(database, rows),
                    ),
                    Counter({(expected,): 1}),
                )

    def test_zero_bound_specials_and_null_preserve_sum_semantics(self):
        scalar_type = "Decimal(35,0)"
        cases = (
            ({"kind": "pos_inf"}, REFERENCE_DECIMAL_INF),
            ({"kind": "neg_inf"}, -REFERENCE_DECIMAL_INF),
            ({"kind": "nan"}, REFERENCE_DECIMAL_NAN),
            (None, None),
        )
        for literal, expected_present in cases:
            expression = (
                {"kind": "null", "type": scalar_type}
                if literal is None
                else {
                    "kind": "literal",
                    "type": scalar_type,
                    "value": literal,
                }
            )
            snapshot = projected_decimal_sum_snapshot(expression)
            script = smt.Script()
            database = Database(snapshot, 2, script)
            relation = RelationEvaluator(
                snapshot,
                database,
                ScalarEncoder(script),
            ).root().certain()

            with self.subTest(literal=literal, property="bound"):
                self.assertEqual(
                    relation.rows[0].values["result"].decimal_finite_abs_bound,
                    0,
                )
            for rows in product((None, (0, 0)), repeat=2):
                expected = (
                    expected_present
                    if expected_present is not None and any(rows)
                    else None
                )
                with self.subTest(literal=literal, rows=rows):
                    self.assertEqual(
                        self._symbolic_bag(
                            relation,
                            self._constants(database, rows),
                        ),
                        Counter({(expected,): 1}),
                    )

    def test_full_domain_projected_decimal_bound_still_fails_closed(self):
        scalar_type = "Decimal(35,0)"
        expression = {
            "kind": "add",
            "left": {"kind": "column", "column": "a.x"},
            "right": {
                "kind": "literal",
                "type": scalar_type,
                "value": {"kind": "finite", "scaled": "0"},
            },
            "type": scalar_type,
            "nullable": False,
        }
        snapshot = projected_decimal_sum_snapshot(expression, scalar_type)

        one_row_script = smt.Script()
        one_row = RelationEvaluator(
            snapshot,
            Database(snapshot, 1, one_row_script),
            ScalarEncoder(one_row_script),
        ).root().certain()
        self.assertEqual(
            one_row.rows[0].values["result"].decimal_finite_abs_bound,
            10**35 - 1,
        )

        two_row_script = smt.Script()
        with self.assertRaisesRegex(
            RelationError,
            "non-associative overflow is not modeled",
        ):
            RelationEvaluator(
                snapshot,
                Database(snapshot, 2, two_row_script),
                ScalarEncoder(two_row_script),
            ).root()

    def test_decimal_sum_rejects_the_exact_headroom_limit(self):
        half_limit = 5 * 10**34
        snapshot = projected_decimal_sum_snapshot(
            {
                "kind": "literal",
                "type": "Decimal(35,0)",
                "value": {"kind": "finite", "scaled": str(half_limit)},
            }
        )

        one_row_script = smt.Script()
        one_row = RelationEvaluator(
            snapshot,
            Database(snapshot, 1, one_row_script),
            ScalarEncoder(one_row_script),
        ).root().certain()
        self.assertEqual(
            one_row.rows[0].values["result"].decimal_finite_abs_bound,
            half_limit,
        )

        two_row_script = smt.Script()
        with self.assertRaisesRegex(
            RelationError,
            "non-associative overflow is not modeled",
        ):
            RelationEvaluator(
                snapshot,
                Database(snapshot, 2, two_row_script),
                ScalarEncoder(two_row_script),
            ).root()

    @staticmethod
    def _constants(database, rows):
        result = {}
        for witness, state in zip(database.witness["A"], rows):
            result[witness.present.atom] = state is not None
            key, value = (0, 0) if state is None else state
            for name, concrete in (("k", key), ("x", value)):
                cell = witness.cells[name]
                if cell.is_null.operation == "symbol":
                    result[cell.is_null.atom] = concrete is None
                result[cell.value.atom] = 0 if concrete is None else concrete
        return result

    @staticmethod
    def _symbolic_bag(relation, constants, function_value=None):
        result = Counter()
        for row in relation.rows:
            if not _evaluate_ground_term(row.present, constants, function_value):
                continue
            values = []
            for column in relation.columns:
                value = row.values[column.name]
                values.append(
                    None
                    if _evaluate_ground_term(value.is_null, constants, function_value)
                    else _evaluate_ground_term(value.value, constants, function_value)
                )
            result[tuple(values)] += 1
        return result

    @staticmethod
    def _reference_bag(function, grouped, slots, unsigned_sum):
        rows = [row for row in slots if row is not None]
        groups = {}
        if grouped:
            for key, value in rows:
                groups.setdefault(key, []).append(value)
        else:
            groups[()] = [value for _, value in rows]

        result = Counter()
        for key, values in groups.items():
            non_null = [value for value in values if value is not None]
            if function == "count":
                aggregate = len(non_null)
            elif not non_null:
                aggregate = None
            else:
                total = sum(non_null)
                aggregate = (
                    total % (1 << 64)
                    if unsigned_sum
                    else ((total + (1 << 63)) % (1 << 64)) - (1 << 63)
                )
            prefix = (key,) if grouped else ()
            result[prefix + (aggregate,)] += 1
        return result

    @staticmethod
    def _decimal_reference_bag(function, grouped, slots):
        rows = [row for row in slots if row is not None]
        groups = {}
        if grouped:
            for key, value in rows:
                groups.setdefault(key, []).append(value)
        else:
            groups[()] = [value for _, value in rows]

        result = Counter()
        for key, values in groups.items():
            non_null = [value for value in values if value is not None]
            if not non_null:
                aggregate = None
            elif function == "max":
                # Decimal AggrMax compares the signed in-band codes directly.
                aggregate = max(non_null)
            elif function == "min":
                # Decimal AggrMin uses the same raw signed-code order.
                aggregate = min(non_null)
            elif function == "avg":
                aggregate = AggregateConcreteDifferentialTest._reference_decimal_average(
                    non_null
                )
            elif function != "sum":
                raise AssertionError(f"unsupported Decimal aggregate {function!r}")
            elif REFERENCE_DECIMAL_NAN in non_null:
                aggregate = REFERENCE_DECIMAL_NAN
            elif (
                REFERENCE_DECIMAL_INF in non_null
                and -REFERENCE_DECIMAL_INF in non_null
            ):
                aggregate = REFERENCE_DECIMAL_NAN
            elif REFERENCE_DECIMAL_INF in non_null:
                aggregate = REFERENCE_DECIMAL_INF
            elif -REFERENCE_DECIMAL_INF in non_null:
                aggregate = -REFERENCE_DECIMAL_INF
            else:
                aggregate = sum(non_null)
            prefix = (key,) if grouped else ()
            result[prefix + (aggregate,)] += 1
        return result

    @staticmethod
    def _extrema_reference_bag(function, grouped, slots):
        rows = [row for row in slots if row is not None]
        groups = {}
        if grouped:
            for key, value in rows:
                groups.setdefault(key, []).append(value)
        else:
            groups[()] = [value for _, value in rows]

        result = Counter()
        for key, values in groups.items():
            non_null = [value for value in values if value is not None]
            aggregate = (
                None
                if not non_null
                else (max(non_null) if function == "max" else min(non_null))
            )
            prefix = (key,) if grouped else ()
            result[prefix + (aggregate,)] += 1
        return result

    @staticmethod
    def _reference_decimal_average(values):
        if REFERENCE_DECIMAL_NAN in values:
            return REFERENCE_DECIMAL_NAN
        if (
            REFERENCE_DECIMAL_INF in values
            and -REFERENCE_DECIMAL_INF in values
        ):
            return REFERENCE_DECIMAL_NAN
        if REFERENCE_DECIMAL_INF in values:
            return REFERENCE_DECIMAL_INF
        if -REFERENCE_DECIMAL_INF in values:
            return -REFERENCE_DECIMAL_INF

        numerator = sum(values)
        denominator = len(values)
        sign = -1 if numerator < 0 else 1
        quotient, remainder = divmod(abs(numerator), denominator)
        twice_remainder = 2 * remainder
        if (
            twice_remainder > denominator
            or (
                twice_remainder == denominator
                and quotient % 2 == 1
            )
        ):
            quotient += 1
        return sign * quotient

    @staticmethod
    def _hash_choice(_function, arguments):
        is_null, value = arguments
        return False if is_null else bool(value % 2)

    @classmethod
    def _split_reference_bag(cls, function, grouped, slots, placements):
        source_tasks = [[], []]
        for row, task in zip(slots, placements):
            if row is not None:
                source_tasks[int(task)].append(row)
        partials = [
            cls._concrete_aggregate(function, grouped, rows, "intermediate")
            for rows in source_tasks
        ]

        if grouped:
            final_inputs = [[], []]
            for task_rows in partials:
                for key, value in task_rows:
                    final_inputs[int(False if key is None else bool(key % 2))].append(
                        (key, value)
                    )
        else:
            final_inputs = [[row for task_rows in partials for row in task_rows]]

        outputs = []
        for rows in final_inputs:
            outputs.extend(
                cls._concrete_aggregate(
                    "sum",
                    grouped,
                    rows,
                    "final",
                    coalesce_empty=function == "count",
                )
            )
        return Counter(
            (key, value) if grouped else (value,)
            for key, value in outputs
        )

    @staticmethod
    def _concrete_aggregate(
        function,
        grouped,
        rows,
        phase,
        coalesce_empty=False,
    ):
        groups = {}
        if grouped:
            for key, value in rows:
                groups.setdefault(key, []).append(value)
        elif rows or phase != "intermediate":
            groups[None] = [value for _, value in rows]

        result = []
        for key, values in groups.items():
            non_null = [value for value in values if value is not None]
            if function == "count":
                aggregate = len(non_null)
            elif not non_null:
                aggregate = 0 if coalesce_empty else None
            else:
                total = sum(non_null)
                aggregate = ((total + (1 << 63)) % (1 << 64)) - (1 << 63)
            result.append((key, aggregate))
        return result


class IntegralAverageCertificateLifecycleTest(unittest.TestCase):
    @staticmethod
    def _metadata(family):
        return tuple(
            value.average_metadata
            for outcome in family.outcomes
            for row in outcome.relation.rows
            for value in row.values.values()
            if value.average_metadata is not None
        )

    def _evaluator(self, snapshot, observer=None):
        script = smt.Script()
        return RelationEvaluator(
            snapshot,
            Database(snapshot, 2, script),
            ScalarEncoder(script),
            node_observer=observer,
        )

    def assert_metadata(self, family, expected):
        metadata = self._metadata(family)
        if expected is None:
            self.assertEqual(metadata, ())
        else:
            self.assertTrue(metadata)
            self.assertTrue(all(isinstance(item, expected) for item in metadata))

    def test_certificate_is_observed_once_then_stripped_with_or_without_observer(self):
        snapshot = aggregate_stage_snapshot(
            "avg", False, False, nullable_input=True, input_type="Int64"
        )
        observed = []
        evaluator = self._evaluator(
            snapshot,
            lambda _scope, node, family: observed.append((node, family)),
        )
        aggregate = evaluator.node("aggregate")
        evaluator.node("aggregate")
        aggregate_events = [
            family for node, family in observed if node == "aggregate"
        ]
        self.assertEqual(len(aggregate_events), 1)
        self.assert_metadata(
            aggregate_events[0],
            IntegralAverageCertificate,
        )
        for family in (aggregate, evaluator.root()):
            self.assert_metadata(family, None)
        self.assert_metadata(self._evaluator(snapshot).node("aggregate"), None)

        projected = aggregate_stage_snapshot(
            "avg",
            True,
            False,
            nullable_input=True,
            input_type="Int64",
            project_average_away=True,
        )
        project_events = {}
        result = self._evaluator(
            projected,
            lambda _scope, node, family: project_events.setdefault(node, family),
        ).root()
        self.assert_metadata(
            project_events["aggregate"],
            IntegralAverageCertificate,
        )
        self.assert_metadata(project_events["project"], None)
        self.assert_metadata(result, None)

    def test_all_sort_shapes_and_ordered_limit_receive_plain_values(self):
        snapshot = aggregate_stage_snapshot(
            "avg", False, False, nullable_input=True, input_type="Int64"
        )
        source = self._evaluator(snapshot).node("aggregate")
        key = Column("key", "Int64", False)

        def candidates(count):
            return relation_model.map_family(
                source,
                lambda relation: replace(
                    relation,
                    columns=(key,) + relation.columns,
                    rows=tuple(
                        replace(
                            relation.rows[0],
                            values={
                                "key": Value(
                                    "Int64", smt.FALSE, smt.int_value(index)
                                ),
                                **relation.rows[0].values,
                            },
                        )
                        for index in range(count)
                    ),
                ),
            )

        script = smt.Script()
        order = (SortOrder("key", True, False),)
        tiny = relation_model.sort_family(candidates(2), order, script, "tiny")
        self.assertEqual(len(tiny.outcomes), 2)
        self.assert_metadata(tiny, None)
        limited = relation_model.limit_family(
            tiny,
            Expr(
                kind="literal",
                value=1,
                result_type="Uint64",
                nullable=False,
            ),
            None,
            script,
            "limit",
        )
        self.assert_metadata(limited, None)

        ordinal = relation_model.sort_family(
            candidates(4), order, script, "ordinal"
        )
        self.assert_metadata(ordinal, None)
        self.assertTrue(all(
            outcome.relation.ordinals is not None
            for outcome in ordinal.outcomes
        ))
        network = relation_model._sorting_network_family(
            candidates(4), order, script, "network"
        )
        self.assert_metadata(network, None)
        self.assertTrue(all(
            outcome.relation.present_prefix
            for outcome in network.outcomes
        ))

    def test_split_state_survives_stage_edge_and_finalization(self):
        snapshot = aggregate_stage_snapshot(
            "avg",
            False,
            True,
            nullable_input=True,
            input_type="Int64",
        )
        script = smt.Script()
        observed = {}
        edges = []
        result = StageEvaluator(
            snapshot,
            Database(snapshot, 2, script),
            ScalarEncoder(script),
            Router(script),
            node_observer=lambda _scope, node, family: observed.setdefault(
                node, []
            ).append(family),
            edge_observer=lambda _edge, _task, family: edges.append(family),
        ).root()

        for family in observed["partial"]:
            self.assert_metadata(family, IntegralAverageState)
        for family in edges:
            self.assert_metadata(family, IntegralAverageState)
        self.assert_metadata(
            observed["final"][0],
            IntegralAverageCertificate,
        )
        self.assert_metadata(result, None)


class RestrictedModelSmokeTest(unittest.TestCase):
    def test_domain_constrained_opaque_result_after_symbolic_take_is_constructible(self):
        for result_type in (
            "String",
            "Utf8",
            "Int64",
            "Date",
            "Decimal(5,2)",
        ):
            with self.subTest(result_type=result_type):
                snapshot = unordered_take_opaque_snapshot(result_type)
                problem = build_logical_kernel_problem_for_tests(
                    snapshot,
                    snapshot,
                    6,
                )
                self.assertIn("(forall ", problem.formula())

    def test_date_filter_equivalence_and_boundary_mutation_use_exact_days(self):
        before = date_filtered_snapshot("lt", 1)
        self.assertFalse(
            _restricted_domain_has_model(
                build_logical_kernel_problem_for_tests(before, before, 1).script
            )
        )
        self.assertTrue(
            _restricted_domain_has_model(
                build_logical_kernel_problem_for_tests(
                    before,
                    date_filtered_snapshot("lte", 1),
                    1,
                ).script
            )
        )

    def test_date_year_normalization_propagates_null_exactly(self):
        expression = (
            date_year_snapshot()
            .plan.node_map()["project"]
            .columns[0]
            .expression
        )
        for source_is_null in (False, True):
            with self.subTest(source_is_null=source_is_null):
                result = ScalarEncoder(smt.Script()).evaluate(
                    expression,
                    {
                        "t.shipdate": Value(
                            "Date",
                            smt.bool_value(source_is_null),
                            smt.int_value(123),
                        ),
                    },
                )
                self.assertEqual(result.type, "Uint16")
                self.assertEqual(
                    result.is_null,
                    smt.bool_value(source_is_null),
                )
                if source_is_null:
                    self.assertEqual(result.value, smt.ZERO)
                else:
                    self.assertEqual(result.value.operation, "f_0")
                    self.assertEqual(
                        result.value.arguments,
                        (smt.FALSE, smt.int_value(123)),
                    )

    def test_mixed_width_membership_lowering_has_no_spurious_model(self):
        problem = build_logical_kernel_problem_for_tests(
            mixed_width_membership_snapshot(False),
            mixed_width_membership_snapshot(True),
            1,
        )
        self.assertFalse(_restricted_domain_has_model(problem.script))

    def test_identical_passive_date_and_decimal_columns_have_no_model(self):
        snapshot = parse_snapshot(_snapshot_with_stage_graph(
            {
                "tables": [{
                    "name": "A",
                    "columns": [
                        {"name": "d", "type": "Date", "nullable": True},
                        {"name": "n", "type": "Decimal(5,2)", "nullable": True},
                    ],
                    "unique_keys": [],
                }]
            },
            [{
                "id": "scan",
                "op": "scan",
                "table": "A",
                "columns": [
                    {"source": "d", "output": "d"},
                    {"source": "n", "output": "n"},
                ],
                "pushed_limit": None,
            }],
            "scan",
            ["d", "n"],
        ))
        self.assertFalse(
            _restricted_domain_has_model(
                build_logical_kernel_problem_for_tests(snapshot, snapshot, 1).script
            )
        )

    def test_identical_join_has_no_restricted_model(self):
        snapshot = right_join({"kind": "and", "args": [KEY_EQUALITY, RESIDUAL]})
        self.assertFalse(
            _restricted_domain_has_model(
                build_logical_kernel_problem_for_tests(snapshot, snapshot, 1).script
            )
        )

    def test_dropped_right_join_filter_has_a_restricted_model(self):
        before = right_join({"kind": "and", "args": [KEY_EQUALITY, RESIDUAL]})
        self.assertTrue(
            _restricted_domain_has_model(
                build_logical_kernel_problem_for_tests(
                    before, right_join(KEY_EQUALITY), 1
                ).script
            )
        )

    def test_union_multiplicity_mutation_has_a_restricted_model(self):
        self.assertTrue(
            _restricted_domain_has_model(
                build_logical_kernel_problem_for_tests(
                    union_snapshot(True), union_snapshot(False), 1
                ).script
            )
        )

    def test_null_and_false_filters_have_no_restricted_model(self):
        before = filtered_snapshot({"kind": "null", "type": "Bool"})
        after = filtered_snapshot({"kind": "literal", "type": "Bool", "value": False})
        self.assertFalse(
            _restricted_domain_has_model(
                build_logical_kernel_problem_for_tests(before, after, 1).script
            )
        )

    def test_catalog_key_controls_left_join_elimination(self):
        with_key = build_logical_kernel_problem_for_tests(
            left_join_elimination_snapshot(True, True),
            left_join_elimination_snapshot(False, True),
            2,
        )
        without_key = build_logical_kernel_problem_for_tests(
            left_join_elimination_snapshot(True, False),
            left_join_elimination_snapshot(False, False),
            2,
        )
        self.assertFalse(_restricted_domain_has_model(with_key.script))
        self.assertTrue(_restricted_domain_has_model(without_key.script))


class StageGraphRestrictedModelTest(unittest.TestCase):
    def test_split_distinct_all_matches_logical_deduplication(self):
        logical = distinct_all_stage_snapshot(False)
        staged = distinct_all_stage_snapshot(True)
        self.assertEqual(stage_task_counts(staged), {"source": 2, "root": 2})
        self.assertFalse(
            _restricted_domain_has_model(
                build_problem(logical, staged, 2).script
            )
        )

    def test_nonshuffled_partial_distinct_all_has_a_duplicate_witness(self):
        logical = distinct_all_stage_snapshot(False)
        corrupted = distinct_all_stage_snapshot(True, connection_kind="map")
        self.assertTrue(
            _restricted_domain_has_model(
                build_problem(logical, corrupted, 2).script
            )
        )

    def test_split_aggregates_match_logical_aggregation(self):
        cases = (
            ("count", "Int64"),
            ("sum", "Int64"),
            ("max", "Decimal(2,0)"),
            ("min", "Decimal(2,0)"),
        )
        for (function, input_type), grouped, nullable_input in product(
            cases, (False, True), (False, True)
        ):
            with self.subTest(
                function=function,
                grouped=grouped,
                nullable_input=nullable_input,
            ):
                logical = aggregate_stage_snapshot(
                    function,
                    grouped,
                    False,
                    nullable_input=nullable_input,
                    input_type=input_type,
                )
                staged = aggregate_stage_snapshot(
                    function,
                    grouped,
                    True,
                    nullable_input=nullable_input,
                    input_type=input_type,
                )
                self.assertEqual(
                    stage_task_counts(staged),
                    {"source": 2, "root": 2 if grouped else 1},
                )
                self.assertFalse(
                    _restricted_domain_has_model(
                        build_problem(logical, staged, 1).script
                    )
                )

    def test_integer_sum_uses_exact_64_bit_wrapping(self):
        logical = aggregate_stage_snapshot("sum", False, False)
        staged = aggregate_stage_snapshot("sum", False, True)
        self.assertIn("(mod ", build_problem(logical, staged, 1).formula())

    def test_corrupt_min_final_function_has_a_two_row_witness(self):
        logical = aggregate_stage_snapshot(
            "min",
            False,
            False,
            input_type="Decimal(2,0)",
        )
        corrupted = aggregate_stage_snapshot(
            "min",
            False,
            True,
            final_function="max",
            input_type="Decimal(2,0)",
        )
        self.assertTrue(
            _restricted_domain_has_model(
                build_problem(logical, corrupted, 2).script
            )
        )

    def test_wrong_grouped_aggregate_shuffle_key_has_a_two_row_witness(self):
        for function, input_type in (
            ("count", "Int64"),
            ("sum", "Int64"),
            ("max", "Decimal(2,0)"),
            ("min", "Decimal(2,0)"),
        ):
            with self.subTest(function=function):
                logical = aggregate_stage_snapshot(
                    function,
                    True,
                    False,
                    nullable_input=True,
                    nullable_key=True,
                    input_type=input_type,
                )
                corrupted = aggregate_stage_snapshot(
                    function,
                    True,
                    True,
                    nullable_input=True,
                    nullable_key=True,
                    shuffle_key="_state",
                    input_type=input_type,
                )
                self.assertTrue(
                    _restricted_domain_has_model(
                        build_problem(logical, corrupted, 2).script
                    )
                )

    def test_corrupt_count_final_function_has_a_witness(self):
        logical = aggregate_stage_snapshot("count", False, False)
        corrupted = aggregate_stage_snapshot(
            "count", False, True, final_function="count"
        )
        self.assertTrue(
            _restricted_domain_has_model(build_problem(logical, corrupted, 2).script)
        )

    def test_corrupt_count_final_phase_has_an_empty_input_witness(self):
        logical = aggregate_stage_snapshot("count", False, False)
        corrupted = aggregate_stage_snapshot(
            "count", False, True, final_phase="intermediate"
        )
        self.assertTrue(
            _restricted_domain_has_model(build_problem(logical, corrupted, 0).script)
        )

    def test_dropped_final_count_has_an_empty_input_witness(self):
        logical = aggregate_stage_snapshot("count", False, False)
        self.assertTrue(
            _restricted_domain_has_model(
                build_problem(logical, partial_only_count_snapshot(), 0).script
            )
        )

    def test_unmodeled_aggregate_function_fails_closed(self):
        value = aggregate_stage_snapshot("count", False, False)
        trait = value.plan.nodes[-1].aggregates[0]
        raw = _snapshot_with_stage_graph(
            _stage_schema("A"),
            [
                copy.deepcopy(SCAN_A),
                {
                    "id": "aggregate",
                    "op": "aggregate",
                    "input": "a",
                    "keys": [],
                    "aggregates": [
                        {
                            "input": trait.input,
                            "function": "median",
                            "output": trait.output,
                            "type": trait.output_type,
                            "nullable": trait.output_nullable,
                            "distinct": trait.distinct,
                            "unwrap": trait.unwrap,
                        }
                    ],
                    "phase": "undefined",
                    "distinct_all": False,
                },
            ],
            "aggregate",
            ["result"],
        )
        snapshot = parse_snapshot(raw)
        with self.assertRaisesRegex(VerificationError, "not modeled: median"):
            build_logical_kernel_problem_for_tests(snapshot, snapshot, 1)

    def test_two_task_map_and_hash_shuffle_preserve_rows(self):
        logical = passthrough_stage_snapshot()
        connections = [
            {"kind": "map"},
            {
                "kind": "hash_shuffle",
                "keys": ["a.k"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
        ]
        for connection in connections:
            with self.subTest(kind=connection["kind"]):
                staged = passthrough_stage_snapshot(connection)
                self.assertFalse(
                    _restricted_domain_has_model(build_problem(logical, staged, 2).script)
                )

    def test_serial_and_parallel_union_all_connections_preserve_rows(self):
        logical = passthrough_stage_snapshot()
        for parallel in (False, True):
            with self.subTest(parallel=parallel):
                staged = passthrough_stage_snapshot(
                    {"kind": "union_all", "parallel": parallel}
                )
                self.assertFalse(
                    _restricted_domain_has_model(build_problem(logical, staged, 2).script)
                )

    def test_merge_order_survives_parse_and_requires_an_ordered_producer(self):
        order = [
            {"column": "a.k", "ascending": True, "nulls_first": False},
            {"column": "a.x", "ascending": False, "nulls_first": True},
        ]
        logical = passthrough_stage_snapshot()
        staged = passthrough_stage_snapshot({"kind": "merge", "order": order})

        edge = staged.stage_graph.edges[0]
        self.assertEqual(
            [
                (item.column, item.ascending, item.nulls_first)
                for item in edge.order
            ],
            [
                ("a.k", True, False),
                ("a.x", False, True),
            ],
        )
        with self.assertRaisesRegex(VerificationError, "merge edge .* input is not ordered"):
            build_problem(logical, staged, 2)

    def test_serial_gather_then_parallel_union_stays_single_task(self):
        logical = serial_then_parallel_stage_snapshot(False)
        staged = serial_then_parallel_stage_snapshot(True)

        self.assertEqual(
            stage_task_counts(staged),
            {"source": 2, "gather_stage": 1, "root": 1},
        )
        self.assertFalse(
            _restricted_domain_has_model(build_problem(logical, staged, 2).script)
        )

    def test_matching_shuffle_key_and_hash_make_local_join_complete(self):
        logical = local_join_stage_snapshot()
        staged = local_join_stage_snapshot(
            {
                "kind": "hash_shuffle",
                "keys": ["a.k"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
            {
                "kind": "hash_shuffle",
                "keys": ["b.k"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
        )
        self.assertFalse(
            _restricted_domain_has_model(build_problem(logical, staged, 1).script)
        )

    def test_wrong_shuffle_key_or_hash_has_a_routing_witness(self):
        logical = local_join_stage_snapshot()
        left = {
            "kind": "hash_shuffle",
            "keys": ["a.k"],
            "hash_function": "HashV1",
            "use_spilling": False,
        }
        mutations = [
            {
                "kind": "hash_shuffle",
                "keys": ["b.x"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
            {
                "kind": "hash_shuffle",
                "keys": ["b.k"],
                "hash_function": "HashV2",
                "use_spilling": False,
            },
        ]
        for right in mutations:
            with self.subTest(right=right):
                mutated = local_join_stage_snapshot(left, right)
                self.assertTrue(
                    _restricted_domain_has_model(build_problem(logical, mutated, 1).script)
                )

    def test_broadcast_makes_cross_task_local_join_complete(self):
        logical = local_join_stage_snapshot()
        staged = local_join_stage_snapshot(
            {"kind": "map"},
            {"kind": "broadcast"},
        )
        self.assertFalse(
            _restricted_domain_has_model(build_problem(logical, staged, 2).script)
        )

    def test_replacing_broadcast_with_map_has_a_witness(self):
        logical = local_join_stage_snapshot()
        mutated = local_join_stage_snapshot(
            {"kind": "map"},
            {"kind": "union_all", "parallel": True},
        )
        self.assertTrue(
            _restricted_domain_has_model(build_problem(logical, mutated, 2).script)
        )

    def test_duplicate_edges_use_distinct_occurrences_and_input_ordinals(self):
        staged_value = duplicate_edge_stage_value()
        staged = parse_snapshot(staged_value)
        logical_value = copy.deepcopy(staged_value)
        logical_value["stage_graph"] = None
        logical = parse_snapshot(logical_value)
        self.assertFalse(
            _restricted_domain_has_model(build_problem(logical, staged, 1).script)
        )

        duplicate_occurrence = copy.deepcopy(staged_value)
        duplicate_occurrence["stage_graph"]["edges"][1]["occurrence"] = 0
        with self.assertRaises(SnapshotError):
            parse_snapshot(duplicate_occurrence)

        duplicate_input = copy.deepcopy(staged_value)
        duplicate_input["stage_graph"]["edges"][1]["consumer_input"] = 0
        with self.assertRaises(SnapshotError):
            parse_snapshot(duplicate_input)

        reused_output = copy.deepcopy(staged_value)
        reused_output["stage_graph"]["edges"][1]["producer_output"] = 0
        with self.assertRaises(SnapshotError):
            parse_snapshot(reused_output)

        unused_output = copy.deepcopy(staged_value)
        unused_output["stage_graph"]["stages"][0]["outputs"].append(
            {"index": 2, "node": "a"}
        )
        with self.assertRaises(SnapshotError):
            parse_snapshot(unused_output)

    def test_connection_variants_reject_unknown_and_missing_fields(self):
        def value_with_variant(variant):
            value = duplicate_edge_stage_value()
            edges = value["stage_graph"]["edges"]
            _set_connection(edges[0], variant)
            if variant["kind"] in {"union_all", "merge"}:
                _set_connection(edges[1], variant)
            return value

        valid_variants = [
            {"kind": "map"},
            {"kind": "broadcast"},
            {
                "kind": "hash_shuffle",
                "keys": ["a.k"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
            {"kind": "union_all", "parallel": False},
            {
                "kind": "merge",
                "order": [
                    {"column": "a.k", "ascending": True, "nulls_first": True}
                ],
            },
        ]
        required_settings = {
            "hash_shuffle": ("keys", "hash_function", "use_spilling"),
            "union_all": ("parallel",),
            "merge": ("order",),
        }
        for variant in valid_variants:
            with self.subTest(kind=variant["kind"], case="valid"):
                parse_snapshot(value_with_variant(variant))

            with self.subTest(kind=variant["kind"], case="unknown"):
                invalid = dict(variant, unmodelled=True)
                with self.assertRaisesRegex(SnapshotError, "unknown fields: unmodelled"):
                    parse_snapshot(value_with_variant(invalid))

            for field in required_settings.get(variant["kind"], ()):
                with self.subTest(kind=variant["kind"], case="missing", field=field):
                    invalid = dict(variant)
                    del invalid[field]
                    with self.assertRaisesRegex(SnapshotError, f"missing fields:.*{field}"):
                        parse_snapshot(value_with_variant(invalid))

    def test_stage_output_mapping_must_match_its_node_and_edge_index(self):
        mutations = []

        non_contiguous = duplicate_edge_stage_value()
        non_contiguous["stage_graph"]["stages"][0]["outputs"][0]["index"] = 1
        mutations.append(non_contiguous)

        foreign_node = duplicate_edge_stage_value()
        foreign_node["stage_graph"]["stages"][0]["outputs"][0]["node"] = "union"
        mutations.append(foreign_node)

        missing_output = duplicate_edge_stage_value()
        missing_output["stage_graph"]["edges"][0]["producer_output"] = 1
        mutations.append(missing_output)

        for value in mutations:
            with self.subTest(value=value):
                with self.assertRaises(SnapshotError):
                    parse_snapshot(value)

    def test_consumer_task_counts_match_executor_and_channel_constraints(self):
        broadcast_only = duplicate_edge_stage_value()
        for edge in broadcast_only["stage_graph"]["edges"]:
            _set_connection(edge, {"kind": "broadcast"})
        broadcast_snapshot = parse_snapshot(broadcast_only)
        self.assertEqual(stage_task_counts(broadcast_snapshot)["consumer"], 1)

        serial_with_map = duplicate_edge_stage_value()
        _set_connection(
            serial_with_map["stage_graph"]["edges"][1],
            {"kind": "union_all", "parallel": False},
        )

        two_maps = duplicate_edge_stage_value()
        for edge in two_maps["stage_graph"]["edges"]:
            _set_connection(edge, {"kind": "map"})

        for case, value, message in (
            ("serial_with_map", serial_with_map, "serial UnionAll"),
            ("two_maps", two_maps, "only one Map"),
        ):
            with self.subTest(case=case):
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(value)


@unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3 for solver tests")
class VerificationTest(unittest.TestCase):
    def test_integral_division_is_an_exact_bounded_observable(self):
        original = integral_division_snapshot()
        equivalent = solve(
            build_logical_kernel_problem_for_tests(
                original,
                integral_division_snapshot(),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(equivalent.status, "VERIFIED_BOUNDED")

        reversed_operands = solve(
            build_logical_kernel_problem_for_tests(
                original,
                integral_division_snapshot(reverse_operands=True),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(reversed_operands.status, "COUNTEREXAMPLE")

    def test_split_distinct_all_is_bounded_equivalent(self):
        result = solve(
            build_problem(
                distinct_all_stage_snapshot(False),
                distinct_all_stage_snapshot(True),
                2,
                30_000,
            ),
            SOLVER,
            2,
            30_000,
        )
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_nonshuffled_partial_distinct_all_has_a_solver_counterexample(self):
        result = solve(
            build_problem(
                distinct_all_stage_snapshot(False),
                distinct_all_stage_snapshot(True, connection_kind="map"),
                2,
                10_000,
            ),
            SOLVER,
            2,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_choice_dependent_opaque_domains_prove_self_equivalence(self):
        for result_type in (
            "String",
            "Utf8",
            "Int64",
            "Date",
            "Decimal(5,2)",
        ):
            with self.subTest(result_type=result_type):
                snapshot = unordered_take_opaque_snapshot(result_type)
                result = solve(
                    build_logical_kernel_problem_for_tests(
                        snapshot,
                        snapshot,
                        4,
                        20_000,
                    ),
                    SOLVER,
                    4,
                    20_000,
                )
                self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_date_if_present_semantics_are_exact_and_mutations_are_visible(self):
        original = date_if_present_snapshot()
        identical = solve(
            build_logical_kernel_problem_for_tests(
                original,
                date_if_present_snapshot(),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(identical.status, "VERIFIED_BOUNDED")

        for mutation, changed in (
            ("missing", date_if_present_snapshot(missing=1)),
            ("source", date_if_present_snapshot(source="d.other_day")),
            (
                "present",
                date_if_present_snapshot(
                    present={"kind": "literal", "type": "Date", "value": 0}
                ),
            ),
        ):
            with self.subTest(mutation=mutation):
                result = solve(
                    build_logical_kernel_problem_for_tests(
                        original,
                        changed,
                        1,
                        10_000,
                    ),
                    SOLVER,
                    1,
                    10_000,
                )
                self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_scalar_subplan_and_staged_cross_join_inline_are_bounded_equivalent(self):
        initial = scalar_subplan_inline_snapshot(False)
        final = scalar_subplan_inline_snapshot(True)
        result = solve(
            build_problem(initial, final, 1, 10_000),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_mutated_inlined_scalar_input_has_a_solver_counterexample(self):
        initial = scalar_subplan_inline_snapshot(False)
        mutated = scalar_subplan_inline_snapshot(True, aggregate_input="a.k")
        result = solve(
            build_problem(initial, mutated, 1, 10_000),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(len(result.witness["A"]), 1)
        self.assertEqual(len(result.witness["B"]), 1)
        self.assertNotEqual(
            result.witness["A"][0]["k"],
            result.witness["A"][0]["x"],
        )

    def test_split_aggregates_are_bounded_equivalent(self):
        cases = (
            ("count", "Int64"),
            ("sum", "Int64"),
            ("max", "Decimal(2,0)"),
            ("min", "Decimal(2,0)"),
            ("max", "Int64"),
            ("min", "Int64"),
        )
        for (function, input_type), grouped, nullable_input in product(
            cases, (False, True), (False, True)
        ):
            with self.subTest(
                function=function,
                grouped=grouped,
                nullable_input=nullable_input,
            ):
                logical = aggregate_stage_snapshot(
                    function,
                    grouped,
                    False,
                    nullable_input=nullable_input,
                    input_type=input_type,
                )
                staged = aggregate_stage_snapshot(
                    function,
                    grouped,
                    True,
                    nullable_input=nullable_input,
                    input_type=input_type,
                )
                result = solve(
                    build_problem(logical, staged, 1, 10_000),
                    SOLVER,
                    1,
                    10_000,
                )
                self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_integral_average_is_exact_through_two_non_null_rows(self):
        for grouped in (False, True):
            with self.subTest(grouped=grouped):
                logical = aggregate_stage_snapshot(
                    "avg",
                    grouped,
                    False,
                    nullable_input=True,
                    input_type="Int64",
                )
                staged = aggregate_stage_snapshot(
                    "avg",
                    grouped,
                    True,
                    nullable_input=True,
                    input_type="Int64",
                )
                problem = build_problem(logical, staged, 2, 30_000)
                self.assertIsNotNone(problem.soundness_exclusion)
                self.assertTrue(problem.soundness_exclusions)
                result = solve(problem, SOLVER, 2, 30_000)
                self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_integral_average_three_rows_is_inconclusive_not_counterexample(self):
        logical = aggregate_stage_snapshot(
            "avg",
            False,
            False,
            nullable_input=True,
            input_type="Int64",
        )
        staged = aggregate_stage_snapshot(
            "avg",
            False,
            True,
            nullable_input=True,
            input_type="Int64",
        )
        result = solve(
            build_problem(logical, staged, 3, 30_000),
            SOLVER,
            3,
            30_000,
        )
        self.assertEqual(result.status, "UNKNOWN")
        self.assertIn("greater than two is reachable", result.reason)

    def test_projected_integral_average_still_checks_exactness_region(self):
        logical = aggregate_stage_snapshot(
            "avg",
            True,
            False,
            nullable_input=True,
            input_type="Int64",
            project_average_away=True,
        )
        staged = aggregate_stage_snapshot(
            "avg",
            True,
            True,
            nullable_input=True,
            input_type="Int64",
            project_average_away=True,
        )
        result = solve(
            build_problem(logical, staged, 3, 30_000),
            SOLVER,
            3,
            30_000,
        )
        self.assertEqual(result.status, "UNKNOWN")
        self.assertIn("greater than two is reachable", result.reason)

    def test_integral_average_input_mutation_requires_exact_replay(self):
        logical = aggregate_stage_snapshot(
            "avg",
            True,
            False,
            nullable_input=True,
            nullable_key=True,
            input_type="Int64",
        )
        corrupted = aggregate_stage_snapshot(
            "avg",
            True,
            True,
            nullable_input=True,
            nullable_key=True,
            input_type="Int64",
            aggregate_input="a.k",
        )
        result = solve(
            build_problem(logical, corrupted, 2, 30_000),
            SOLVER,
            2,
            30_000,
        )
        self.assertEqual(result.status, "UNKNOWN")
        self.assertIn("exact binary64 replay is required", result.reason)

    def test_integral_average_carrier_collision_is_never_a_counterexample(self):
        logical = aggregate_stage_snapshot(
            "avg",
            False,
            False,
            nullable_input=True,
            input_type="Int64",
        )
        duplicated = aggregate_stage_snapshot(
            "avg",
            False,
            True,
            nullable_input=True,
            input_type="Int64",
            duplicate_input=True,
        )

        result = solve(
            build_problem(logical, duplicated, 1, 30_000),
            SOLVER,
            1,
            30_000,
        )

        self.assertEqual(result.status, "UNKNOWN")
        self.assertIn("exact binary64 replay is required", result.reason)

    def test_grouped_nullable_sum_is_verified_at_two_rows_and_tasks(self):
        logical = aggregate_stage_snapshot(
            "sum", True, False, nullable_input=True, nullable_key=True
        )
        staged = aggregate_stage_snapshot(
            "sum", True, True, nullable_input=True, nullable_key=True
        )
        result = solve(
            build_problem(logical, staged, 2, 30_000),
            SOLVER,
            2,
            30_000,
        )
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_corrupt_min_final_function_has_a_solver_counterexample(self):
        for input_type in ("Decimal(2,0)", "Int64"):
            with self.subTest(input_type=input_type):
                logical = aggregate_stage_snapshot(
                    "min",
                    False,
                    False,
                    input_type=input_type,
                )
                corrupted = aggregate_stage_snapshot(
                    "min",
                    False,
                    True,
                    final_function="max",
                    input_type=input_type,
                )
                result = solve(
                    build_problem(logical, corrupted, 2, 10_000),
                    SOLVER,
                    2,
                    10_000,
                )
                self.assertEqual(result.status, "COUNTEREXAMPLE")
                self.assertEqual(len(result.witness["A"]), 2)
                self.assertNotEqual(
                    result.witness["A"][0]["x"],
                    result.witness["A"][1]["x"],
                )

    def test_wrong_grouped_aggregate_shuffle_key_has_a_solver_counterexample(self):
        for function, input_type in (
            ("count", "Int64"),
            ("sum", "Int64"),
            ("max", "Decimal(2,0)"),
            ("min", "Decimal(2,0)"),
        ):
            with self.subTest(function=function):
                logical = aggregate_stage_snapshot(
                    function,
                    True,
                    False,
                    nullable_input=True,
                    nullable_key=True,
                    input_type=input_type,
                )
                corrupted = aggregate_stage_snapshot(
                    function,
                    True,
                    True,
                    nullable_input=True,
                    nullable_key=True,
                    shuffle_key="_state",
                    input_type=input_type,
                )
                result = solve(
                    build_problem(logical, corrupted, 2, 10_000),
                    SOLVER,
                    2,
                    10_000,
                )
                self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_corrupt_count_final_function_has_a_solver_counterexample(self):
        logical = aggregate_stage_snapshot("count", False, False)
        corrupted = aggregate_stage_snapshot(
            "count", False, True, final_function="count"
        )
        result = solve(
            build_problem(logical, corrupted, 2, 10_000),
            SOLVER,
            2,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_corrupt_count_final_phase_has_a_solver_counterexample(self):
        logical = aggregate_stage_snapshot("count", False, False)
        corrupted = aggregate_stage_snapshot(
            "count", False, True, final_phase="intermediate"
        )
        result = solve(
            build_problem(logical, corrupted, 0, 10_000),
            SOLVER,
            0,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_dropped_final_count_has_a_solver_counterexample(self):
        logical = aggregate_stage_snapshot("count", False, False)
        result = solve(
            build_problem(logical, partial_only_count_snapshot(), 0, 10_000),
            SOLVER,
            0,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_matching_stage_shuffles_are_bounded_equivalent(self):
        logical = local_join_stage_snapshot()
        staged = local_join_stage_snapshot(
            {
                "kind": "hash_shuffle",
                "keys": ["a.k"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
            {
                "kind": "hash_shuffle",
                "keys": ["b.k"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
        )
        result = solve(build_problem(logical, staged, 1, 10_000), SOLVER, 1, 10_000)
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_wrong_stage_routing_has_a_solver_counterexample(self):
        logical = local_join_stage_snapshot()
        mutations = [
            (
                1,
                local_join_stage_snapshot(
                    {
                        "kind": "hash_shuffle",
                        "keys": ["a.k"],
                        "hash_function": "HashV1",
                        "use_spilling": False,
                    },
                    {
                        "kind": "hash_shuffle",
                        "keys": ["b.k"],
                        "hash_function": "HashV2",
                        "use_spilling": False,
                    },
                ),
            ),
            (
                2,
                local_join_stage_snapshot(
                    {"kind": "map"},
                    {"kind": "union_all", "parallel": True},
                ),
            ),
        ]
        for row_bound, staged in mutations:
            with self.subTest(row_bound=row_bound, staged=staged):
                result = solve(
                    build_problem(logical, staged, row_bound, 10_000),
                    SOLVER,
                    row_bound,
                    10_000,
                )
                self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_identical_plans_are_bounded_equivalent(self):
        snapshot = right_join({"kind": "and", "args": [KEY_EQUALITY, RESIDUAL]})
        result = solve(
            build_logical_kernel_problem_for_tests(snapshot, snapshot, 1, 10_000),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_mixed_width_membership_lowering_is_bounded_equivalent(self):
        result = solve(
            build_logical_kernel_problem_for_tests(
                mixed_width_membership_snapshot(False),
                mixed_width_membership_snapshot(True),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_string_predicate_fingerprint_and_argument_order_are_semantic(self):
        fingerprint = "yql-string-predicate-v1:ends_with"
        original = string_predicate_snapshot(fingerprint)
        identical = solve(
            build_logical_kernel_problem_for_tests(
                original,
                string_predicate_snapshot(fingerprint),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(identical.status, "VERIFIED_BOUNDED")

        for mutation, changed in (
            (
                "fingerprint",
                string_predicate_snapshot(
                    "yql-string-predicate-v1:string_contains"
                ),
            ),
            (
                "argument order",
                string_predicate_snapshot(
                    fingerprint,
                    reverse_arguments=True,
                ),
            ),
        ):
            with self.subTest(mutation=mutation):
                result = solve(
                    build_logical_kernel_problem_for_tests(
                        original,
                        changed,
                        1,
                        10_000,
                    ),
                    SOLVER,
                    1,
                    10_000,
                )
                self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_passive_double_fingerprint_and_arguments_are_semantic(self):
        original = passive_double_snapshot()
        identical = solve(
            build_logical_kernel_problem_for_tests(
                original,
                passive_double_snapshot(),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(identical.status, "VERIFIED_BOUNDED")

        staged = solve(
            build_problem(
                original,
                passive_double_snapshot(staged=True),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(staged.status, "VERIFIED_BOUNDED")

        for mutation, changed in (
            (
                "fingerprint",
                passive_double_snapshot(
                    OPAQUE_DOUBLE_FINGERPRINT_PREFIX + "changed"
                ),
            ),
            (
                "argument",
                passive_double_snapshot(
                    argument_columns=("a.x", "a.y", "a.k")
                ),
            ),
        ):
            with self.subTest(mutation=mutation):
                result = solve(
                    build_logical_kernel_problem_for_tests(
                        original,
                        changed,
                        1,
                        10_000,
                    ),
                    SOLVER,
                    1,
                    10_000,
                )
                self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_passive_double_is_a_sort_merge_passenger(self):
        result = solve(
            build_problem(
                passive_double_snapshot(sort_merge=True),
                passive_double_snapshot(staged=True, sort_merge=True),
                2,
                20_000,
            ),
            SOLVER,
            2,
            20_000,
        )
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_date_year_normalization_fingerprint_and_argument_are_semantic(self):
        original = date_year_snapshot()
        identical = solve(
            build_logical_kernel_problem_for_tests(
                original,
                date_year_snapshot(),
                1,
                10_000,
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(identical.status, "VERIFIED_BOUNDED")

        for mutation, changed in (
            (
                "fingerprint",
                date_year_snapshot("yql-datetime-year-mutated-v1"),
            ),
            (
                "argument",
                date_year_snapshot(
                    opaque_argument={
                        "kind": "literal",
                        "type": "Date",
                        "value": 0,
                    },
                ),
            ),
        ):
            with self.subTest(mutation=mutation):
                result = solve(
                    build_logical_kernel_problem_for_tests(
                        original,
                        changed,
                        1,
                        10_000,
                    ),
                    SOLVER,
                    1,
                    10_000,
                )
                self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_dropped_right_join_filter_has_a_witness(self):
        before = right_join({"kind": "and", "args": [KEY_EQUALITY, RESIDUAL]})
        after = right_join(KEY_EQUALITY)
        result = solve(
            build_logical_kernel_problem_for_tests(before, after, 1, 10_000),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(len(result.witness["A"]), 1)
        self.assertEqual(len(result.witness["B"]), 1)
        self.assertEqual(result.witness["A"][0]["k"], result.witness["B"][0]["k"])

    def test_dropping_union_all_branch_changes_multiplicity(self):
        result = solve(
            build_logical_kernel_problem_for_tests(
                union_snapshot(True), union_snapshot(False), 1, 10_000
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(len(result.witness["A"]), 1)

    def test_null_and_false_filters_are_equivalent(self):
        null_filter = filtered_snapshot({"kind": "null", "type": "Bool"})
        false_filter = filtered_snapshot({"kind": "literal", "type": "Bool", "value": False})
        result = solve(
            build_logical_kernel_problem_for_tests(
                null_filter, false_filter, 1, 10_000
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_unique_key_justifies_left_join_elimination(self):
        before = left_join_elimination_snapshot(True, True)
        after = left_join_elimination_snapshot(False, True)
        result = solve(
            build_logical_kernel_problem_for_tests(before, after, 2, 10_000),
            SOLVER,
            2,
            10_000,
        )
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_left_join_elimination_without_unique_key_has_a_witness(self):
        before = left_join_elimination_snapshot(True, False)
        after = left_join_elimination_snapshot(False, False)
        result = solve(
            build_logical_kernel_problem_for_tests(before, after, 2, 10_000),
            SOLVER,
            2,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(len(result.witness["B"]), 2)

    def test_constant_query_uses_one_empty_source_row(self):
        result = solve(
            build_logical_kernel_problem_for_tests(
                constant_snapshot(1), constant_snapshot(2), 0, 10_000
            ),
            SOLVER,
            0,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(result.witness, {})

    def test_zero_bound_catalog_retains_its_empty_table_witness(self):
        problem = Problem(smt.Script(10_000), {"A": ()})
        result = solve(problem, SOLVER, 0, 10_000)
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(result.witness, {"A": []})

    def test_string_escape_forms_remain_distinct(self):
        result = solve(
            build_logical_kernel_problem_for_tests(
                constant_snapshot("\\n"), constant_snapshot("\n"), 0, 10_000
            ),
            SOLVER,
            0,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_unicode_literal_round_trips_through_z3(self):
        result = solve(
            build_logical_kernel_problem_for_tests(
                constant_snapshot("é"), constant_snapshot("u{e9}"), 0, 10_000
            ),
            SOLVER,
            0,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")


if __name__ == "__main__":
    unittest.main()
