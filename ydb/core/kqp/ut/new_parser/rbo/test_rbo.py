"""Frontend compatibility checks using the existing RBO cases.

Original SQL, data-loading setup and result assertions are retained, including
the complete effects suite. Unsupported frontend constructs exercise the normal
fallback contract; effects runs record that distinction, and native cases below
separately require successful direct lowering and write execution.
"""

import fnmatch
import hashlib
import json
import os
from collections import Counter
from pathlib import Path

import pytest
import yatest.common

RBO_ROOT = "ydb/core/kqp/ut/rbo"
# Enumerate the original targets, then ask each binary for registered names.
# This includes generated twin/quad cases and avoids maintaining a copied list
# of SQL inputs or assertions as the original RBO suite evolves.
GROUPS = (
    "",
    "aggregation",
    "basic",
    "effects",
    "explain",
    "fallback",
    "join_order",
    "joins",
    "metadata",
    "olap",
    "physical",
    "reads",
    "subqueries",
    "workloads",
)

# Match registered C++ names (including +/- twin and quad suffixes), rather
# than broad words such as "Multi": MultiConsumer is a single SELECT and
# MultiInsert_DistinctOnOff_SameResult only uses multiple ingestion batches.
EXCLUDED = {
    "basic": {
        "KqpRboYql::MultipleSelects*": "multiple result statements",
    },
    "explain": {
        "KqpRboYql::ExplainMultipleSelect": "multiple result statements",
        "KqpRboYql::ExplainAnalyzeMultipleSelect": "multiple result statements",
        "KqpRboYql::ExplainAnalyzeMultipleResultContainers*": "multiple result statements",
        "KqpRboYql::ExplainAnalyzeMultipleSelectTPCDS23": "multiple result statements",
    },
    "fallback": {
        "KqpRboYql::ReqJsonLogsActualOptimizer": "multiple result statements",
    },
}

# Cases previously omitted from the frontend driver, outside the effects target.
# Prefix their IDs with effects/ so one filter runs the complete imported set.
EFFECTS = {
    "": (
        "KqpRboCopy::SequencerIsAnAllocationBarrier",
        "KqpRboCopy::CopiesEffectBindingsWithoutChangingStorageNames",
        "KqpRboCopy::ReplicateExpansionDoesNotDuplicateEffects",
    ),
    "basic": ("KqpRboYql::PGInsertUpdate", "KqpRboYql::InsertSelectColumnOrder*", "KqpRboYql::InsertUpdate"),
    "fallback": (
        "KqpRboYql::FallbackToYqlEnabled",
        "KqpRboYql::FallbackToYqlDisabled",
        "KqpRboYql::FallbackToYqlEnabledExecute",
        "KqpRboYql::FallbackToYqlDisabledExecute",
    ),
    "physical": ("KqpRboIdLowering::UpsertKeepsDefaultColumnsThroughLowering",),
}
NATIVE_EFFECTS = {
    "KqpNewYqlParserFallback::NativeValuesWriteModesAndReturning",
    "KqpNewYqlRboLowering::DeclarationsAndParameterTablesLowerDirectly",
    "KqpNewYqlParserFallback::DeclaredParametersAndNativeWritesPreserveRowsAndIndexes",
}
RESTORED_EFFECTS = {
    "KqpConnectionNewRbo::Immediate",
    "KqpConnectionNewRbo::OnPrepare",
    "KqpConnectionNewRbo::OnCommit",
    "KqpEffectsNewRbo::RandomWithIndex+UseSecondaryIndex",
    "KqpEffectsNewRbo::RandomWithIndex-UseSecondaryIndex",
    "KqpEffectsNewRbo::EffectWithSelect",
}


def is_effect_case(module, name):
    if module == f"{RBO_ROOT}/effects" or name in NATIVE_EFFECTS:
        return True
    group = module[len(RBO_ROOT) :].lstrip("/")
    return any(fnmatch.fnmatchcase(name, pattern) for pattern in EFFECTS.get(group, ()))


# ya derives these unit-test executable names from the full module path.
# Use binary_path so execution also works in the isolated test sandbox.
def binary_for(module):
    return yatest.common.binary_path(f"{module}/{module.replace('/', '-')}")


# Discover actual registrations rather than parsing C++ macros. Reject empty
# or malformed output so a broken discovery command cannot silently drop coverage.
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
        if group == "effects":
            assert RESTORED_EFFECTS <= set(names), "Missing restored legacy effects cases"
        exclusions = EXCLUDED.get(group, {})
        # Require every exclusion to match a current case; renamed tests should
        # force a review rather than silently losing cases from a filtered run.
        for pattern in (*exclusions, *EFFECTS.get(group, ())):
            assert any(fnmatch.fnmatchcase(name, pattern) for name in names), f"Stale exclusion in {group}: {pattern}"
        for name in names:
            if not any(fnmatch.fnmatchcase(name, pattern) for pattern in exclusions):
                prefix = "effects/" if group != "effects" and is_effect_case(module, name) else ""
                cases.append(pytest.param(module, name, id=f"{prefix}{group or 'operators'}/{name}"))

    # Include direct importer checks alongside compatibility cases. Most reused
    # SQL tests allow frontend fallback, so those passes alone do not establish
    # that any query reached the new AST-to-RBO importer.
    module = "ydb/core/kqp/ut/new_parser/fallback"
    native = [
        name
        for name in registered_cases(module)
        if name.startswith("KqpNewYqlRboLowering::")
        or name == "KqpNewYqlParserFallback::ResolvesNamesUsingLoadedMetadata+NewRbo"
        or name in NATIVE_EFFECTS
    ]
    assert len(native) >= 9, "Missing direct AST-to-RBO path checks"
    assert NATIVE_EFFECTS <= set(native), "Missing native effects execution/lowering checks"
    cases.extend(
        pytest.param(module, name, id=f"{'effects/' if name in NATIVE_EFFECTS else ''}native/{name}") for name in native
    )
    return cases


@pytest.mark.parametrize("module,name", collect_cases())
def test_rbo_frontend_compatibility(module, name):
    env = os.environ.copy()
    # Imported RBO cases need the override; the native frontend tests already
    # control this flag explicitly, including their parser-off baselines.
    if module.startswith(RBO_ROOT):
        env["YDB_KQP_TEST_NEW_AST"] = "1"
    else:
        env.pop("YDB_KQP_TEST_NEW_AST", None)
    # Do not inherit an outer C++ runner's selection and accidentally run zero
    # cases. A trace assertion below also validates the precise executed case.
    env.pop("Y_UNITTEST_FILTER_FILE", None)
    # Derive unique artifact names from both module and case: registered names
    # can repeat between binaries, and punctuation in names is unsuitable for paths.
    suffix = hashlib.sha256(f"{module}/{name}".encode()).hexdigest()[:16]
    trace = yatest.common.output_path(f"rbo-{suffix}.trace")
    effects = is_effect_case(module, name)
    command = [binary_for(module), "--trace-path", trace]
    if effects:
        command += ["--test-param", "KQP_LOG_KQP_YQL=INFO"]
    result = yatest.common.execute(
        command + [f"+{name}"],
        env=env,
        timeout=1200,
        check_exit_code=False,
        stdout=yatest.common.output_path(f"rbo-{suffix}.out"),
        stderr=yatest.common.output_path(f"rbo-{suffix}.err"),
    )
    if effects:
        log = Path(yatest.common.output_path(f"rbo-{suffix}.err")).read_text(errors="replace")
        reasons = Counter(
            line.split(marker, 1)[1].strip()
            for line in log.splitlines()
            for marker in ("New YQL parser fallback:", "New YQL frontend fallback:")
            if marker in line
        )
        attempts = sum(
            log.count(marker)
            for marker in (
                "New YQL AST construction succeeded",
                "New YQL parser fallback:",
                "New YQL parser reported syntax errors",
            )
        )
        audit = dict(
            module=module,
            case=name,
            frontend_attempts=attempts,
            native_contracts=log.count("New YQL native execution contract built"),
            direct_native_assertions=name in NATIVE_EFFECTS,
            fallback_reasons=dict(reasons),
        )
        Path(yatest.common.output_path(f"rbo-{suffix}.frontend.json")).write_text(json.dumps(audit, indent=2) + "\n")
    assert result.exit_code == 0, f"{module}/{name}: exit {result.exit_code}; see rbo-{suffix}.err"
    # A zero exit code can also mean no selected case ran. Validate the trace
    # identity, cardinality and status so filtering mistakes cannot report a pass.
    events = [json.loads(line) for line in Path(trace).read_text().splitlines()]
    finished = [event["value"] for event in events if event["name"] == "subtest-finished"]
    assert len(finished) == 1, f"Expected exactly one executed case: {finished}"
    case = finished[0]
    assert f"{case['class']}::{case['subtest']}" == name, case
    assert case["status"] == "good", case
    if module == f"{RBO_ROOT}/effects":
        assert attempts > 0, f"Effects case did not exercise the enabled frontend: {audit}"
