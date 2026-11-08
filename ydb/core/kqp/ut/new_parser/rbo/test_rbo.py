"""Run the existing RBO cases with the new AST frontend enabled.

PRAGMA/DECLARE/named-expression preambles and data-loading setup are retained.
Excluded cases test writes or multiple result-producing statements. Unsupported
frontend constructs still exercise the normal fallback contract; the native
cases below separately require successful direct AST-to-RBO lowering.
"""

import fnmatch
import hashlib
import json
import os
from pathlib import Path

import pytest
import yatest.common


RBO_ROOT = "ydb/core/kqp/ut/rbo"
GROUPS = (
    "", "aggregation", "basic", "explain", "fallback", "join_order", "joins",
    "metadata", "olap", "physical", "reads", "subqueries", "workloads",
)

# Match registered C++ names (including +/- twin and quad suffixes), rather
# than broad words such as "Multi": MultiConsumer is a single SELECT and
# MultiInsert_DistinctOnOff_SameResult only uses multiple ingestion batches.
EXCLUDED = {
    "": {
        "KqpRboCopy::SequencerIsAnAllocationBarrier": "write-side sequence allocation",
        "KqpRboCopy::CopiesEffectBindingsWithoutChangingStorageNames": "effects",
        "KqpRboCopy::ReplicateExpansionDoesNotDuplicateEffects": "effects",
    },
    "basic": {
        "KqpRboYql::MultipleSelects*": "multiple result statements",
        "KqpRboYql::PGInsertUpdate": "effects",
        "KqpRboYql::InsertSelectColumnOrder*": "effects",
        "KqpRboYql::InsertUpdate": "effects",
    },
    "explain": {
        "KqpRboYql::ExplainMultipleSelect": "multiple result statements",
        "KqpRboYql::ExplainAnalyzeMultipleSelect": "multiple result statements",
        "KqpRboYql::ExplainAnalyzeMultipleResultContainers*": "multiple result statements",
        "KqpRboYql::ExplainAnalyzeMultipleSelectTPCDS23": "multiple result statements",
    },
    "fallback": {
        "KqpRboYql::ReqJsonLogsActualOptimizer": "multiple result statements",
        "KqpRboYql::FallbackToYqlEnabled": "includes INSERT",
        "KqpRboYql::FallbackToYqlDisabled": "includes INSERT",
        "KqpRboYql::FallbackToYqlEnabledExecute": "includes INSERT",
        "KqpRboYql::FallbackToYqlDisabledExecute": "includes INSERT",
    },
    "physical": {
        "KqpRboIdLowering::UpsertKeepsDefaultColumnsThroughLowering": "effects",
    },
}


def binary_for(module):
    return yatest.common.binary_path(f"{module}/{module.replace('/', '-')}")


def registered_cases(module):
    output = yatest.common.execute([binary_for(module), "--list-verbose"]).std_out
    names = output.decode().splitlines()
    assert names and all("::" in name for name in names), (module, output)
    assert len(set(names)) == len(names), f"Duplicate C++ test names in {module}"
    return names


def collect_cases():
    cases = []
    for group in GROUPS:
        module = f"{RBO_ROOT}/{group}" if group else RBO_ROOT
        names = registered_cases(module)
        exclusions = EXCLUDED.get(group, {})
        for pattern in exclusions:
            assert any(fnmatch.fnmatchcase(name, pattern) for name in names), (
                f"Stale exclusion in {group}: {pattern}"
            )
        for name in names:
            if not any(fnmatch.fnmatchcase(name, pattern) for pattern in exclusions):
                cases.append(pytest.param(module, name, id=f"{group or 'operators'}/{name}"))

    module = "ydb/core/kqp/ut/new_parser/fallback"
    native = [name for name in registered_cases(module)
              if name.startswith("KqpNewYqlRboLowering::")
              or name == "KqpNewYqlParserFallback::ResolvesNamesUsingLoadedMetadata+NewRbo"]
    assert len(native) >= 9, "Missing direct AST-to-RBO path checks"
    cases.extend(pytest.param(module, name, id=f"native/{name}") for name in native)
    return cases


@pytest.mark.parametrize("module,name", collect_cases())
def test_rbo_with_new_ast(module, name):
    env = os.environ.copy()
    env["YDB_KQP_TEST_NEW_AST"] = "1"
    # Do not inherit an outer C++ runner's selection and accidentally run zero
    # cases. A trace assertion below also validates the precise executed case.
    env.pop("Y_UNITTEST_FILTER_FILE", None)
    suffix = hashlib.sha256(f"{module}/{name}".encode()).hexdigest()[:16]
    trace = yatest.common.output_path(f"rbo-{suffix}.trace")
    result = yatest.common.execute(
        [binary_for(module), "--trace-path", trace, f"+{name}"],
        env=env,
        timeout=1200,
        check_exit_code=False,
        stdout=yatest.common.output_path(f"rbo-{suffix}.out"),
        stderr=yatest.common.output_path(f"rbo-{suffix}.err"),
    )
    assert result.exit_code == 0, f"{module}/{name}: exit {result.exit_code}; see rbo-{suffix}.err"
    events = [json.loads(line) for line in Path(trace).read_text().splitlines()]
    finished = [event["value"] for event in events if event["name"] == "subtest-finished"]
    assert len(finished) == 1, f"Expected exactly one executed case: {finished}"
    case = finished[0]
    assert f"{case['class']}::{case['subtest']}" == name, case
    assert case["status"] == "good", case
