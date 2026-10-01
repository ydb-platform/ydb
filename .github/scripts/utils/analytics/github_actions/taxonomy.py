#!/usr/bin/env python3
"""Allowed `source` / `name` values in analytics/ci_metrics."""

from __future__ import annotations

# Written by export_github_job_metrics.py.
EXPORT_SOURCES = ("github_job", "github_step")
GITHUB_JOB_NAMES = ("job", "queue")
# github_step names are the GitHub step names, so they cannot be enumerated.
DYNAMIC_NAME_SOURCES = ("github_step",)

# Written inside a job by test_ya.
YA_PHASE_SOURCE = "ya_phase"
YA_PHASE_NAMES = (
    "init",
    "clean_ya_cache",
    "setup_cache",
    "graph_compare",
    "checkout_head",
    "prepare_ya_make",
    "ya_make_try_N",
    "ya_build",
    "ya_tests",
    "ya_cache_download",
    "ya_cache_upload",
    "postprocess_try",
    "transform_build_results",
    "fail_checker",
    "generate_summary",
    "s3_sync_try",
    "upload_tests_results",
    "runner_info",
)

# Phases derived from ya_evlog.jsonl by ya_evlog_phases.py.
YA_EVLOG_NAMES = ("ya_build", "ya_tests", "ya_cache_download", "ya_cache_upload")

SOURCES = {
    "github_job": GITHUB_JOB_NAMES,
    "github_step": (),
    YA_PHASE_SOURCE: YA_PHASE_NAMES,
}

# `ya_make_try_1`, `ya_make_try_2`, ... are one row per attempt.
ATTEMPT_SUFFIXED_NAMES = ("ya_make_try_N",)


def canonical_name(name: str) -> str:
    if name.startswith("ya_make_try_"):
        return "ya_make_try_N"
    return name
