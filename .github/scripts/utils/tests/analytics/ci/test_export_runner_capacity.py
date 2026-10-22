#!/usr/bin/env python3
"""Unit tests for the runner-capacity snapshot. No Cloud or YDB calls."""

from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "analytics"))

from collector.schema import TTL_MINUTES
from github_actions.export_runner_capacity import (
    IncompleteGitHubJobs,
    build_create_capacity_table_sql,
    build_snapshot_rows,
    bytes_to_gib,
    cloud_list,
    count_assigned_and_queued,
    fetch_quota,
    iam_token,
    github_jobs_incomplete,
    hosts_that_fit,
    index_runner_jobs,
    instance_nrd_gib,
    display_workflow,
    enrich_run_pull,
    is_ghrun,
    is_github_hosted_job,
    job_footprint,
    list_workflow_runs,
    load_label_footprints,
    pull_info,
    queue_jobs,
    quota_headroom,
    snapshot_summary,
    target_branch,
    unique_name,
)


NOW = datetime(2026, 10, 5, 11, 0, tzinfo=timezone.utc)


def limits():
    return [
        {"quota_id": "compute.instanceCores.count", "limit": 8500, "usage": 7500},
        {"quota_id": "compute.instanceMemory.size", "limit": 10 * (1024 ** 3), "usage": 6 * (1024 ** 3)},
        {"quota_id": "compute.instances.count", "limit": 200, "usage": 150},
        {"quota_id": "compute.ssdNonReplicatedDisks.size", "limit": 5000 * (1024 ** 3), "usage": 1000 * (1024 ** 3)},
        {"quota_id": "compute.images.count", "limit": 8, "usage": 1},
    ]


def vm(name, cores, ram_gib, created, labels=None, status="RUNNING", zone=""):
    row = {
        "name": name,
        "status": status,
        "created_at": created,
        "labels": labels or {},
        "resources": {"cores": str(cores), "memory": str(int(ram_gib * (1024 ** 3)))},
    }
    if zone:
        row["zoneId"] = zone
    return row


class FootprintsYamlTest(unittest.TestCase):
    def test_sizes_come_from_runners_footprints_yml(self):
        specs, default = load_label_footprints()
        self.assertEqual(default, {"vcpu": 64.0, "ram_gb": 256.0, "nrd_ssd_gb": 2417.0})
        self.assertEqual(
            specs["build-preset-analytic-node"],
            {"vcpu": 8.0, "ram_gb": 64.0, "nrd_ssd_gb": 186.0},
        )
        self.assertEqual(
            specs["tiny-worker"],
            {"vcpu": 4.0, "ram_gb": 8.0, "nrd_ssd_gb": 186.0},
        )


class DiskSizeTest(unittest.TestCase):
    def test_ghrun_defaults_to_relw_nrd(self):
        self.assertEqual(instance_nrd_gib(vm("ghrun-a", 64, 256, "2026-10-02T14:23:23Z", {"ghrun": "a"})), 2417.0)

    def test_tiny_ghrun_defaults_to_186(self):
        self.assertEqual(instance_nrd_gib(vm("ghrun-tiny", 4, 8, "2026-10-02T14:23:23Z", {"ghrun": "tiny"})), 186.0)

    def test_tiny_worker_and_analytic_node_nrd(self):
        self.assertEqual(instance_nrd_gib(vm("tiny-worker-2026-10-12", 4, 8, "2026-10-12T00:00:00Z")), 186.0)
        self.assertEqual(instance_nrd_gib(vm("analytic-node-2026-10-2", 8, 64, "2026-10-02T00:00:00Z")), 186.0)

    def test_explicit_nrd_disk_wins(self):
        row = vm("ghrun-a", 64, 256, "2026-10-02T14:23:23Z", {"ghrun": "a"})
        row["secondary_disks"] = [{
            "disk_spec": {"type_id": "network-ssd-nonreplicated", "size": str(int(186 * (1024 ** 3)))},
        }]
        self.assertEqual(instance_nrd_gib(row), 186.0)

    def test_other_vm_without_disks_has_no_nrd(self):
        self.assertIsNone(instance_nrd_gib(vm("cachesrv", 32, 96, "2026-10-02T14:23:23Z")))


class QuotaMathTest(unittest.TestCase):
    def test_bytes_and_gib(self):
        self.assertEqual(bytes_to_gib(256), 256)
        self.assertAlmostEqual(bytes_to_gib(256 * (1024 ** 3)), 256)

    def test_headroom_converts_memory_and_ignores_other_quotas(self):
        head = quota_headroom(limits())
        self.assertEqual(set(head), {"cores", "ram_gib", "instances", "nrd_gib"})
        self.assertEqual(head["cores"]["free"], 1000)
        self.assertAlmostEqual(head["ram_gib"]["free"], 4)
        self.assertAlmostEqual(head["nrd_gib"]["free"], 4000)

    def test_headroom_sums_per_zone_quotas(self):
        head = quota_headroom(
            [
                {"quota_id": "compute.instanceCores.count.ru-central1-a", "limit": 100, "usage": 40},
                {"quota_id": "compute.instanceCores.count.ru-central1-d", "limit": 50, "usage": 10},
                {"quota_id": "compute.instanceHighFreqCores.count.ru-central1-a", "limit": 8, "usage": 8},
                {"quota_id": "compute.instanceMemory.size.ru-central1-a", "limit": 8 * (1024 ** 3), "usage": 3 * (1024 ** 3)},
                {"quota_id": "compute.instanceMemory.size.ru-central1-d", "limit": 4 * (1024 ** 3), "usage": 1 * (1024 ** 3)},
                {"quota_id": "compute.instances.count", "limit": 200, "usage": 20},
                {"quota_id": "compute.ssdNonReplicatedDisks.size.ru-central1-a", "limit": 100 * (1024 ** 3), "usage": 10 * (1024 ** 3)},
            ]
        )
        self.assertEqual(head["cores"]["free"], 100)
        self.assertAlmostEqual(head["ram_gib"]["free"], 8)
        self.assertEqual(head["instances"]["free"], 180)
        self.assertAlmostEqual(head["nrd_gib"]["free"], 90)

    def test_fit_is_bound_by_ram(self):
        head = quota_headroom(limits())
        footprint = {"vcpu": 64, "ram_gb": 256, "nrd_ssd_gb": 2417}
        self.assertEqual(hosts_that_fit(head, footprint), 0)
        head["ram_gib"]["free"] = 256 * 3 + 10
        head["nrd_gib"]["free"] = 2417 * 10
        self.assertEqual(hosts_that_fit(head, footprint), 3)

    def test_ttl_is_one_year_on_sampled_at(self):
        self.assertEqual(TTL_MINUTES, 365 * 24 * 60)
        sql = build_create_capacity_table_sql("analytics/ci_capacity")
        self.assertIn('TTL = Interval("PT525600M") ON `sampled_at`', sql)
        self.assertIn("PRIMARY KEY (`sampled_at`, `date`, `kind`, `name`)", sql)
        self.assertIn("analytics/ci_capacity", sql)


class SnapshotRowsTest(unittest.TestCase):
    def test_busy_idle_queue_and_newest(self):
        born = (NOW - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
        old = "2026-10-02T14:23:23Z"
        instances = [
            vm("ghrun-busy", 64, 256, old, {"ghrun": "ghrun-busy"}),
            vm("ghrun-idle", 64, 320, old, {"ghrun": "ghrun-idle"}),
            vm("ghrun-new", 8, 64, born, {"ghrun": "ghrun-new"}),
            vm("cachesrv", 32, 96, old),
            vm("stopped", 64, 256, old, {"ghrun": "stopped"}, status="STOPPED"),
        ]
        run = {"name": "PR-check", "created_at": old}
        pairs = [
            (run, {"status": "in_progress", "runner_name": "ghrun-busy", "name": "Build and test relwithdebinfo", "created_at": old}),
            (run, {"status": "queued", "runner_name": None, "name": "Build and test release-asan", "created_at": (NOW - timedelta(minutes=40)).strftime("%Y-%m-%dT%H:%M:%SZ")}),
            (run, {"status": "queued", "name": "${{ matrix.branch }}", "created_at": old}),
        ]
        rows = build_snapshot_rows(NOW, limits(), instances, pairs, {
            "relwithdebinfo": {"vcpu": 64, "ram_gb": 256, "nrd_ssd_gb": 2417},
            "release-asan": {"vcpu": 64, "ram_gb": 320, "nrd_ssd_gb": 2418},
        }, True)
        by_key = {(row["kind"], row["name"]): row for row in rows}
        self.assertEqual(by_key[("runner", "ghrun-busy")]["state"], "busy")
        self.assertEqual(by_key[("runner", "ghrun-busy")]["nrd_gib"], 2417.0)
        self.assertEqual(by_key[("runner", "ghrun-busy")]["workflow"], "PR-check")
        self.assertEqual(by_key[("runner", "ghrun-idle")]["state"], "idle")
        self.assertEqual(by_key[("runner", "ghrun-new")]["state"], "idle")
        self.assertEqual(by_key[("runner", "cachesrv")]["state"], "other")
        self.assertNotIn(("runner", "stopped"), by_key)
        self.assertEqual(by_key[("summary", "runners.ghrun")]["usage_value"], 3)
        self.assertEqual(by_key[("summary", "runners.ghrun")]["free_value"], 2)
        self.assertEqual(by_key[("summary", "runners.created_24h")]["usage_value"], 1)
        self.assertEqual(by_key[("summary", "runners.newest")]["job_name"], "ghrun-new")
        queued = [row for row in rows if row["kind"] == "queue"]
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0]["workflow"], "PR-check")
        self.assertEqual(queued[0]["job_name"], "Build and test release-asan")
        self.assertEqual(queued[0]["usage_value"], 1)
        self.assertGreater(queued[0]["age_sec"], 0)
        self.assertIn("newest=2026-10-05", snapshot_summary(rows))

    def test_static_prefix_counts_in_the_ghrun_pool(self):
        old = "2026-10-02T14:23:23Z"
        self.assertTrue(is_ghrun(vm("static-relw-1", 64, 256, old)))
        self.assertTrue(is_ghrun(vm("tiny-worker-2026-10-12", 4, 8, old)))
        self.assertTrue(is_ghrun(vm("analytic-node-2026-10-2", 8, 64, old)))
        self.assertFalse(is_ghrun(vm("cachesrv", 32, 96, old)))
        instances = [
            vm("static-relw-1", 64, 256, old),
            vm("tiny-worker-2026-10-12", 4, 8, old),
            vm("analytic-node-2026-10-2", 8, 64, old),
            vm("cachesrv", 32, 96, old),
        ]
        pairs = [
            (
                {"name": "PR-check"},
                {"status": "in_progress", "runner_name": "static-relw-1", "name": "Build and test relwithdebinfo"},
            ),
            (
                {"name": "Collect-analytics-fast-run"},
                {"status": "in_progress", "runner_name": "analytic-node-2026-10-2", "name": "GitHub job metrics"},
            ),
        ]
        rows = build_snapshot_rows(NOW, limits(), instances, pairs, {
            "relwithdebinfo": {"vcpu": 64, "ram_gb": 256, "nrd_ssd_gb": 2417},
        }, True)
        by_key = {(row["kind"], row["name"]): row for row in rows}
        self.assertEqual(by_key[("runner", "static-relw-1")]["state"], "busy")
        self.assertEqual(by_key[("runner", "tiny-worker-2026-10-12")]["state"], "idle")
        self.assertEqual(by_key[("runner", "analytic-node-2026-10-2")]["state"], "busy")
        self.assertEqual(by_key[("runner", "cachesrv")]["state"], "other")
        self.assertEqual(by_key[("summary", "runners.ghrun")]["usage_value"], 3)
        self.assertEqual(by_key[("summary", "runners.ghrun")]["free_value"], 1)

    def test_zone_scoped_quota_and_runners(self):
        old = "2026-10-02T14:23:23Z"
        zonal = [
            {"quota_id": "compute.instanceCores.count.ru-central1-d", "limit": 100, "usage": 40},
            {"quota_id": "compute.instanceCores.count.ru-central1-a", "limit": 50, "usage": 10},
            {"quota_id": "compute.instanceMemory.size.ru-central1-d", "limit": 8 * (1024 ** 3), "usage": 2 * (1024 ** 3)},
            {"quota_id": "compute.instanceMemory.size.ru-central1-a", "limit": 4 * (1024 ** 3), "usage": 1 * (1024 ** 3)},
            {"quota_id": "compute.instances.count", "limit": 200, "usage": 20},
        ]
        instances = [
            vm("ghrun-d", 64, 256, old, {"ghrun": "ghrun-d"}, zone="ru-central1-d"),
            vm("static-a", 64, 256, old, zone="ru-central1-a"),
        ]
        rows = build_snapshot_rows(NOW, zonal, instances, [], {
            "relwithdebinfo": {"vcpu": 64, "ram_gb": 256, "nrd_ssd_gb": 2417},
        }, True)
        by_key = {(row["kind"], row["name"]): row for row in rows}
        self.assertEqual(by_key[("quota", "cores@ru-central1-d")]["free_value"], 60)
        self.assertEqual(by_key[("quota", "cores@ru-central1-a")]["free_value"], 40)
        self.assertEqual(by_key[("runner", "ghrun-d")]["zone"], "ru-central1-d")
        self.assertEqual(by_key[("runner", "static-a")]["zone"], "ru-central1-a")
        self.assertEqual(by_key[("summary", "runners.ghrun")]["usage_value"], 2)
        self.assertEqual(by_key[("summary", "runners.ghrun@ru-central1-d")]["usage_value"], 1)
        self.assertEqual(by_key[("summary", "runners.ghrun@ru-central1-a")]["usage_value"], 1)

    def test_in_progress_beats_queued_on_the_same_runner(self):
        indexed = index_runner_jobs([
            ({"name": "A"}, {"status": "queued", "runner_name": "ghrun-a", "name": "queued job"}),
            ({"name": "B"}, {"status": "in_progress", "runner_name": "ghrun-a", "name": "running job"}),
        ])
        self.assertEqual(indexed["ghrun-a"]["status"], "in_progress")
        self.assertEqual(indexed["ghrun-a"]["workflow"], "B")

    def test_target_branch_is_the_pr_base(self):
        self.assertEqual(
            target_branch({"head_branch": "feature", "pull_requests": [{"base": {"ref": "main"}}]}),
            "main",
        )
        self.assertEqual(target_branch({"head_branch": "stable-25-1"}), "stable-25-1")
        self.assertEqual(
            target_branch({"event": "pull_request_target", "head_branch": "feature"}),
            "",
        )
        indexed = index_runner_jobs([
            (
                {"name": "PR-check", "event": "pull_request", "display_title": "dist-cache miss", "head_branch": "feature", "pull_requests": [{"number": 55140, "base": {"ref": "main"}}]},
                {"status": "in_progress", "runner_name": "ghrun-a", "name": "Build and test", "html_url": "https://github.com/ydb-platform/ydb/actions/runs/1/job/2"},
            ),
        ])
        self.assertEqual(indexed["ghrun-a"]["branch"], "main")
        self.assertEqual(indexed["ghrun-a"]["pr_number"], "55140")
        self.assertEqual(indexed["ghrun-a"]["pr_title"], "dist-cache miss")
        scheduled = index_runner_jobs([
            (
                {"name": "Nightly-Build", "event": "schedule", "pull_requests": [{"number": 10, "base": {"ref": "main"}}]},
                {"status": "in_progress", "runner_name": "ghrun-b", "name": "Build and test"},
            ),
        ])
        self.assertIsNone(scheduled["ghrun-b"]["pr_number"])
        self.assertEqual(indexed["ghrun-a"]["run_url"], "https://github.com/ydb-platform/ydb/actions/runs/1/job/2")

    def test_queue_demand_is_the_sum_of_runner_sizes(self):
        old = "2026-10-05T10:00:00Z"
        pairs = [
            (
                {"id": 1, "name": "PR-check", "html_url": "https://github.com/ydb-platform/ydb/actions/runs/1"},
                {"id": 11, "status": "queued", "name": "Build and test release-asan", "labels": ["build-preset-release-asan"], "created_at": old, "html_url": "https://github.com/ydb-platform/ydb/actions/runs/1/job/11"},
            ),
            (
                {"id": 2, "name": "PR-check", "html_url": "https://github.com/ydb-platform/ydb/actions/runs/2"},
                {"id": 22, "status": "queued", "name": "Build and test relwithdebinfo", "labels": ["build-preset-relwithdebinfo"], "created_at": old, "html_url": "https://github.com/ydb-platform/ydb/actions/runs/2/job/22"},
            ),
        ]
        label_specs = {
            "build-preset-relwithdebinfo": {"vcpu": 64, "ram_gb": 256, "nrd_ssd_gb": 2417},
            "build-preset-release-asan": {"vcpu": 64, "ram_gb": 320, "nrd_ssd_gb": 2418},
        }
        rows = build_snapshot_rows(
            NOW,
            limits(),
            [],
            pairs,
            {
                "relwithdebinfo": {"vcpu": 64, "ram_gb": 256, "nrd_ssd_gb": 2417},
                "release-asan": {"vcpu": 64, "ram_gb": 320, "nrd_ssd_gb": 2418},
            },
            True,
            label_specs=label_specs,
            label_default={"vcpu": 64, "ram_gb": 256, "nrd_ssd_gb": 2417},
        )
        queued = [row for row in rows if row["kind"] == "queue"]
        self.assertEqual(len(queued), 2)
        self.assertEqual(sum(row["usage_value"] or 0 for row in queued), 2)
        self.assertEqual(sum(row["cores"] or 0 for row in queued), 128)
        self.assertEqual(sum(row["ram_gib"] or 0 for row in queued), 576)
        by_name = {row["name"]: row for row in queued}
        self.assertEqual(by_name["q.1.11"]["run_url"], "https://github.com/ydb-platform/ydb/actions/runs/1/job/11")
        self.assertEqual(by_name["q.2.22"]["job_name"], "Build and test relwithdebinfo")

    def test_queue_skips_github_hosted_ubuntu(self):
        specs, default = load_label_footprints()
        pairs = [
            (
                {"id": 1, "name": "Postcommits gate"},
                {"id": 11, "status": "queued", "name": "SwitchGate", "labels": ["ubuntu-latest"]},
            ),
            (
                {"id": 2, "name": "PR-check"},
                {
                    "id": 22,
                    "status": "queued",
                    "name": "Build and test relwithdebinfo",
                    "labels": ["self-hosted", "auto-provisioned", "build-preset-relwithdebinfo"],
                },
            ),
        ]
        rows = queue_jobs(pairs, NOW, specs, default)
        self.assertEqual([row["job_name"] for row in rows], ["Build and test relwithdebinfo"])
        self.assertTrue(is_github_hosted_job({"labels": ["ubuntu-latest"]}))
        self.assertFalse(is_github_hosted_job({"labels": ["self-hosted", "ubuntu-latest"]}))
        self.assertEqual(count_assigned_and_queued(pairs), (0, 1))

    def test_analytic_node_is_not_a_tsan_host(self):
        specs, default = load_label_footprints()
        job = {
            "id": 1,
            "status": "queued",
            "name": "update-muted-tests (main, release-tsan, .github/config/muted_ya_tsan.txt)",
            "labels": ["self-hosted", "auto-provisioned", "build-preset-analytic-node"],
        }
        size = job_footprint(job, specs, default)
        self.assertEqual(size, {"vcpu": 8.0, "ram_gb": 64.0, "nrd_ssd_gb": 186.0})
        rows = queue_jobs([({"name": "Update Muted tests"}, job)], NOW, specs, default)
        self.assertEqual(rows[0]["cores"], 8)
        self.assertEqual(rows[0]["ram"], 64)
        self.assertEqual(rows[0]["nrd"], 186)

    def test_tiny_worker_queue_uses_tiny_size(self):
        specs, default = load_label_footprints()
        job = {
            "id": 2,
            "status": "queued",
            "name": "PR-check",
            "labels": ["self-hosted", "tiny-worker"],
        }
        self.assertEqual(job_footprint(job, specs, default), {"vcpu": 4.0, "ram_gb": 8.0, "nrd_ssd_gb": 186.0})

    def test_enrich_keeps_pr_and_target_branch(self):
        run = {
            "name": "PR-check",
            "event": "pull_request_target",
            "head_branch": "feature",
            "display_title": "fix it",
            "pull_requests": [{"number": 42, "title": "fix it", "base": {"ref": "main"}}],
        }
        out = enrich_run_pull("ydb-platform", "ydb", run)
        self.assertEqual(pull_info(out), ("42", "fix it"))
        self.assertEqual(target_branch(out), "main")
        rows = queue_jobs([(out, {"id": 1, "status": "queued", "name": "Build and test relwithdebinfo"})], NOW)
        self.assertEqual(rows[0]["pr_number"], "42")
        self.assertEqual(rows[0]["branch"], "main")
        self.assertEqual(rows[0]["workflow"], "PR-check")

    def test_postcommit_is_not_grouped_as_pr_check(self):
        self.assertEqual(
            display_workflow(
                {"name": "PR-check", "event": "push"},
                {"name": "Postcommit · Build and test relwithdebinfo"},
            ),
            "Postcommit",
        )
        self.assertEqual(
            display_workflow(
                {"name": "PR-check", "event": "pull_request_target"},
                {"name": "Build and test relwithdebinfo"},
            ),
            "PR-check",
        )
        rows = queue_jobs([
            (
                {"id": 1, "name": "PR-check", "event": "push", "head_branch": "main"},
                {"id": 11, "status": "queued", "name": "Postcommit · Build and test relwithdebinfo"},
            ),
            (
                {
                    "id": 2,
                    "name": "PR-check",
                    "event": "pull_request_target",
                    "head_branch": "feature",
                    "display_title": "fix it",
                    "pull_requests": [{"number": 1, "title": "fix it", "base": {"ref": "main"}}],
                },
                {"id": 22, "status": "queued", "name": "Build and test relwithdebinfo"},
            ),
        ], NOW)
        by_job = {row["job_name"]: row for row in rows}
        self.assertEqual(by_job["Postcommit · Build and test relwithdebinfo"]["workflow"], "Postcommit")
        self.assertIsNone(by_job["Postcommit · Build and test relwithdebinfo"]["pr_number"])
        self.assertEqual(by_job["Build and test relwithdebinfo"]["workflow"], "PR-check")
        self.assertEqual(by_job["Build and test relwithdebinfo"]["branch"], "main")

    def test_enrich_strips_pulls_on_push(self):
        run = {
            "event": "push",
            "head_branch": "main",
            "pull_requests": [{"number": 42, "title": "fix it", "base": {"ref": "main"}}],
        }
        out = enrich_run_pull("ydb-platform", "ydb", run)
        self.assertEqual(out["pull_requests"], [])
        self.assertEqual(pull_info(out), (None, None))
        self.assertEqual(target_branch(out), "main")

    def test_short_in_progress_page_plus_long_queue_is_incomplete(self):
        self.assertTrue(github_jobs_incomplete(124, 24, 96))
        self.assertTrue(github_jobs_incomplete(117, 5, 9))
        self.assertFalse(github_jobs_incomplete(124, 70, 2))
        self.assertFalse(github_jobs_incomplete(124, 108, 93))
        self.assertFalse(github_jobs_incomplete(3, 0, 50))

    def test_count_assigned_and_queued(self):
        pairs = [
            ({}, {"status": "in_progress", "runner_name": "ghrun-a"}),
            ({}, {"status": "in_progress", "runner_name": "ghrun-a"}),
            ({}, {"status": "in_progress", "runner_name": ""}),
            ({}, {"status": "queued", "name": "waiting"}),
            ({}, {"status": "queued", "name": "also waiting"}),
            ({}, {"status": "completed", "runner_name": "ghrun-b"}),
        ]
        self.assertEqual(count_assigned_and_queued(pairs), (1, 2))

    def test_workflow_run_pages_keep_going_after_a_short_page(self):
        pages = {
            1: {"workflow_runs": [{"id": 1}, {"id": 2}]},
            2: {"workflow_runs": []},
            3: {"workflow_runs": [{"id": 3}]},
            4: {"workflow_runs": []},
            5: {"workflow_runs": []},
        }

        def fetcher(_url, params=None):
            return pages[params["page"]]

        runs = list_workflow_runs("ydb-platform", "ydb", "in_progress", per_page=50, fetcher=fetcher)
        self.assertEqual([run["id"] for run in runs], [1, 2, 3])

    def test_workflow_run_pages_raise_when_the_list_does_not_end(self):
        def fetcher(_url, params=None):
            return {"workflow_runs": [{"id": params["page"]}]}

        with self.assertRaises(IncompleteGitHubJobs):
            list_workflow_runs("ydb-platform", "ydb", "queued", per_page=1, max_pages=2, fetcher=fetcher)

    def test_github_failure_marks_runners_unknown_and_skips_queue(self):
        instances = [vm("ghrun-a", 64, 256, "2026-10-02T14:23:23Z", {"ghrun": "ghrun-a"})]
        rows = build_snapshot_rows(NOW, limits(), instances, None, {
            "relwithdebinfo": {"vcpu": 64, "ram_gb": 256, "nrd_ssd_gb": 2417},
        }, False)
        states = [row["state"] for row in rows if row["kind"] == "runner"]
        self.assertEqual(states, ["unknown"])
        self.assertFalse(any(row["kind"] == "queue" for row in rows))

    def test_queue_survives_empty_footprint(self):
        rows = queue_jobs(
            [({"id": 1, "name": "PR-check"}, {"id": 11, "status": "queued", "name": "Build"})],
            NOW,
            specs={},
            default={},
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["cores"], 0.0)
        self.assertEqual(rows[0]["ram"], 0.0)
        self.assertEqual(rows[0]["nrd"], 0.0)

    def test_unique_name_does_not_collide(self):
        used = set()
        first = unique_name({"name": "unnamed", "_folder_id": "f1"}, used)
        second = unique_name({"name": "unnamed", "_folder_id": "f1"}, used)
        third = unique_name({"name": "unnamed", "_folder_id": "f1"}, used)
        self.assertEqual(first, "unnamed")
        self.assertEqual(second, "unnamed@f1")
        self.assertEqual(third, "unnamed@f1.3")
        self.assertEqual(len({first, second, third}), 3)


class CloudApiTest(unittest.TestCase):
    def test_iam_token_uses_ci_ydb_key(self):
        with mock.patch.dict(os.environ, {"CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS": "/tmp/sa.json"}, clear=False):
            with mock.patch("github_actions.export_runner_capacity.os.path.isfile", return_value=True):
                with mock.patch(
                    "github_actions.export_runner_capacity.iam_token_from_key_file",
                    return_value="t-from-sa",
                ) as mint:
                    self.assertEqual(iam_token(), "t-from-sa")
                    mint.assert_called_once_with("/tmp/sa.json")

    def test_quota_accepts_camel_case_id(self):
        headroom = quota_headroom([
            {"quotaId": "compute.instanceCores.count", "limit": 10, "usage": 3},
        ])
        self.assertEqual(headroom["cores"]["free"], 7)

    def test_fetch_quota_normalizes_rest_rows(self):
        with mock.patch("github_actions.export_runner_capacity.cloud_list", return_value=[
            {"quotaId": "compute.instanceCores.count", "limit": "10", "usage": "3"},
        ]):
            rows = fetch_quota("cloud", token="t")
        self.assertEqual(rows, [{"quota_id": "compute.instanceCores.count", "limit": "10", "usage": "3"}])

    def test_cloud_list_follows_page_token(self):
        pages = [
            {"folders": [{"id": "1"}], "nextPageToken": "p2"},
            {"folders": [{"id": "2"}]},
        ]

        def getter(_url, _token, params):
            return pages[0 if "pageToken" not in params else 1]

        with mock.patch("github_actions.export_runner_capacity.cloud_get", side_effect=getter):
            self.assertEqual(cloud_list("https://example", "t", {"cloudId": "c"}, "folders"), [
                {"id": "1"}, {"id": "2"},
            ])

    def test_snapshot_reads_created_at_camel_case(self):
        instance = vm("ghrun-a", 64, 256, None, {"ghrun": "ghrun-a"})
        instance.pop("created_at", None)
        instance["createdAt"] = "2026-10-02T14:23:23Z"
        rows = build_snapshot_rows(NOW, limits(), [instance], [], {
            "relwithdebinfo": {"vcpu": 64, "ram_gb": 256, "nrd_ssd_gb": 2417},
        }, True)
        runner = next(row for row in rows if row["kind"] == "runner")
        self.assertEqual(runner["created_at"], datetime(2026, 10, 2, 14, 23, 23, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
