"""Pinned browser acceptance. Missing registered resources are a hard failure."""
import asyncio
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import stat
import subprocess
import unittest
import uuid

import yatest.common

from ydb.apps.bridge_skipper.acceptance.lib.harness import Environment
from ydb.apps.bridge_skipper.ydbd_mock.lib.control_ui import PAGE as MOCK_CONTROL_PAGE


SOURCE = "ydb/apps/bridge_skipper/acceptance/browser"


def require_bundle(condition, message):
    if not condition:
        raise AssertionError("Invalid pinned browser bundle: " + message)


def file_sha256(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def bundle_relative(value):
    require_bundle(isinstance(value, str) and bool(value), "empty/non-string manifest path")
    path = PurePosixPath(value)
    require_bundle(not path.is_absolute() and ".." not in path.parts and "\\" not in value
                   and str(path) == value and value != ".", "noncanonical/traversing manifest path " + value)
    return path


def verify_prepared_bundle(bundle, registered, name):
    require_bundle(not bundle.is_symlink() and stat.S_ISDIR(bundle.lstat().st_mode), "bundle root is not a regular directory")
    manifest_path = bundle / "bundle.json"
    require_bundle(stat.S_ISREG(manifest_path.lstat().st_mode), "bundle.json must be a regular file")
    manifest_bytes = manifest_path.read_bytes()
    trusted = registered.get("bundle_manifest_sha256")
    require_bundle(isinstance(trusted, str) and re.fullmatch(r"[0-9a-f]{64}", trusted) is not None,
                   "missing trusted bundle_manifest_sha256")
    require_bundle(hashlib.sha256(manifest_bytes).hexdigest() == trusted, "bundle manifest digest differs from registration pin")
    manifest = json.loads(manifest_bytes)
    require_bundle(manifest.get("schema") == 1 and manifest.get("platform") == name, "manifest schema/platform mismatch")
    require_bundle(manifest.get("playwright_version") == "1.64.1" and
                   manifest.get("chromium_headless_shell_revision") == "1248", "manifest version/revision mismatch")
    files = manifest.get("files")
    require_bundle(isinstance(files, dict), "manifest files map required")
    expected_files = {"bundle.json"}
    expected_directories = set()
    for relative, checksum in files.items():
        path = bundle_relative(relative)
        require_bundle(relative != "bundle.json", "manifest cannot list itself")
        require_bundle(isinstance(checksum, str) and re.fullmatch(r"[0-9a-f]{64}", checksum) is not None,
                       "invalid checksum for " + relative)
        expected_files.add(relative)
        expected_directories.update(str(parent) for parent in path.parents if str(parent) != ".")
    actual_files = set()
    for path in bundle.rglob("*"):
        relative = str(path.relative_to(bundle))
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            require_bundle(relative in expected_directories, "unlisted directory " + relative)
        else:
            require_bundle(stat.S_ISREG(mode), "symlink/nonregular node " + relative)
            require_bundle(relative in expected_files, "unlisted file " + relative)
            actual_files.add(relative)
    require_bundle(actual_files == expected_files, "missing manifest-listed file")
    for relative, checksum in files.items():
        require_bundle(file_sha256(bundle / relative) == checksum, "file digest mismatch for " + relative)
    package = json.loads((bundle / "node_modules/playwright-core/package.json").read_text())
    require_bundle(package.get("name") == "playwright-core" and package.get("version") == "1.64.1", "Playwright package mismatch")
    browser_relative = str(bundle_relative(registered["browser_relative_path"]))
    require_bundle(browser_relative in files, "browser executable is not anchored by the manifest")
    return bundle / browser_relative


def resource_paths():
    system = platform.system().lower()
    machine = platform.machine().lower()
    name = {("linux", "x86_64"): "linux-x86_64", ("darwin", "arm64"): "darwin-arm64"}.get((system, machine))
    if name is None:
        raise AssertionError("Skipper browser tests support Linux x86_64 and macOS arm64; got %s/%s" % (system, machine))
    registration = json.loads(Path(yatest.common.source_path(SOURCE + "/resources/registration.json")).read_text())
    registered = registration["platforms"][name]
    resources = yatest.common.global_resources()
    # A pre-upload verification run may supply a prepared bundle, never an
    # installed browser. Its manifest, package versions and file hashes still
    # undergo the same checks. CI uses the registered resource global only.
    bundle_value = resources.get("SKIPPER_BROWSER_RESOURCE_GLOBAL") or os.environ.get("SKIPPER_PREPARED_BROWSER_BUNDLE")
    if not bundle_value:
        raise AssertionError("Missing pinned Skipper browser resource for %s: playwright-core 1.64.1, "
                             "Chromium headless-shell r1248, fonts and native closure. "
                             "Prepare bundles, obtain repository-owner approval, register bundle_uri and wire DATA; "
                             "registration=%r. No browser download or local-browser fallback is allowed." % (name, registered))
    bundle = Path(bundle_value)
    browser = verify_prepared_bundle(bundle, registered, name)
    bundle = bundle.resolve()
    browser = browser.resolve()
    node_root = resources.get("NODEJS_24_22_1_RESOURCE_GLOBAL")
    if not node_root:
        raise AssertionError("Missing pinned Node 24.22.1 resource from build/platform/nodejs/24.22.1; globals=%r" % sorted(resources))
    node = Path(node_root) / "node"
    require_bundle(node.is_file() and browser.is_file(), "Node/browser executable missing")
    return node, bundle, browser


class BrowserTest(unittest.IsolatedAsyncioTestCase):
    async def make(self, names=("a", "b"), logging_level=None):
        # Resolve before starting a server so a missing resource fails promptly.
        self.node, self.bundle, self.browser = resource_paths()
        root = Path(yatest.common.output_path("browser_" + uuid.uuid4().hex))
        root.mkdir(mode=0o700)
        environment = Environment(yatest.common.binary_path("ydb/apps/bridge_skipper/bridge_skipper"), root, mode="observe", names=names)
        if logging_level is not None:
            environment.controller.config["logging"] = {"level": logging_level}
            environment.controller.config_path.write_text(json.dumps(environment.controller.config))
        self.addAsyncCleanup(environment.close)
        await environment.start()
        self.environment = environment
        return environment

    async def run_browser(self, scenario):
        env = dict(os.environ)
        env.update({"PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD": "1", "PLAYWRIGHT_BROWSERS_PATH": "0",
                    "SKIPPER_BROWSER_BUNDLE": str(self.bundle), "SKIPPER_BROWSER_EXECUTABLE": str(self.browser),
                    "SKIPPER_URL": self.environment.controller.url,
                    "SKIPPER_ARTIFACTS": str(self.environment.root), "SKIPPER_SCENARIO": scenario})
        if platform.system() == "Linux":
            env["LD_LIBRARY_PATH"] = str(self.bundle / "native/lib")
            env["FONTCONFIG_FILE"] = str(self.bundle / "fonts.conf")
            env["FONTCONFIG_PATH"] = str(self.bundle)
        command = [str(self.node), yatest.common.source_path(SOURCE + "/browser.cjs")]
        # Blocking subprocess.run belongs to a Python worker; keep mock gRPC and
        # peer listeners on the asyncio thread alive for the real-server cases.
        result = await asyncio.to_thread(subprocess.run, command, env=env, text=True,
                                         capture_output=True, timeout=70)
        (self.environment.root / (scenario + ".stdout")).write_text(result.stdout)
        (self.environment.root / (scenario + ".stderr")).write_text(result.stderr)
        self.assertEqual(result.returncode, 0, result.stderr + "\n" + result.stdout)

    async def test_real_server_manual_review_executes_exactly_once(self):
        e = await self.make()
        generation = await e.generation()
        await e.blame("b")
        await e.controller.until(lambda state: state["decision"]["kind"] == "READY", "Manual browser failover Ready")
        await self.run_browser("manual")
        await e.mutation(generation, {"b": "DISCONNECTED"})
        await e.quiet(1)
        self.assertEqual(len(e.mutations()), 1)

    async def test_real_server_auto_and_off_prevent_future_dispatch(self):
        e = await self.make()
        browser_task = asyncio.create_task(self.run_browser("auto_off"))
        await e.controller.until(lambda state: state["mode"] == "observe" and int(state["mode_revision"]) >= 3,
                                 "browser Auto then Off acknowledgement")
        await browser_task
        await e.blame("b")
        await e.controller.until(lambda state: state["decision"]["kind"] == "READY", "Off still evaluates policy")
        await e.quiet(1)
        self.assertEqual(e.mutations(), [])

    async def test_auto_off_retains_ambiguous_request_and_observed_roles(self):
        e = await self.make()
        generation = await e.generation()
        preferred = min(e.cluster.topo.pile("a").nodes, key=lambda node: "grpc://" + node.endpoint)
        await e.control("a", "/rpc-override", label=preferred.label, method="UpdateClusterState",
                        response={"operation": {"ready": True, "status": "INTERNAL_ERROR"}}, once=True)
        await e.blame("b")
        await e.controller.until(lambda state: state["decision"]["kind"] == "READY", "Auto review with Ready failover")
        await self.run_browser("pending_off")
        await e.mutation(generation, {"b": "DISCONNECTED"})
        state = await e.controller.state()
        self.assertEqual(state["mode"], "observe")
        self.assertEqual(state["last_mutation"]["status"], "INTERNAL_ERROR")
        self.assertEqual(state["last_mutation"]["outcome"], "AMBIGUOUS")
        self.assertEqual(state["pending_mutation"]["update"], {"b": "DISCONNECTED"})
        self.assertEqual({pile["name"]: pile["state"] for pile in state["view"]["piles"]},
                         {"a": "PRIMARY", "b": "SYNCHRONIZED"})
        await e.quiet(6)
        self.assertEqual(len(e.mutations()), 1)

    async def test_viewports_themes_many_piles_focus_and_logs(self):
        await self.make()
        await self.run_browser("presentation")

    async def test_mock_control_poll_preserves_keyboard_focus(self):
        environment = await self.make()
        (environment.root / "mock-control.html").write_text(MOCK_CONTROL_PAGE)
        await self.run_browser("mock_control_focus")

    async def test_incident_failover_uses_canonical_disabled_reason(self):
        await self.make()
        await self.run_browser("disabled_failover")

    async def test_pending_presentation_matches_no_js_and_hydrated_document(self):
        e = await self.make()
        await e.controller.set_mode("manual")
        preferred = min(e.cluster.topo.pile("a").nodes, key=lambda node: "grpc://" + node.endpoint)
        await e.control("a", "/rpc-override", label=preferred.label, method="UpdateClusterState",
                        response={"operation": {"ready": True, "status": "INTERNAL_ERROR"}}, once=True)
        await e.blame("b")
        await e.controller.until(lambda state: state["decision"]["kind"] == "READY", "presentation failover Ready")
        await e.controller.execute(await e.controller.plan("failover"))
        await e.controller.until(lambda state: state["last_mutation"] is not None and
                                 state["last_mutation"]["outcome"] == "AMBIGUOUS", "real ambiguous request for presentation")
        await self.run_browser("canonical_presentation")
        self.assertEqual(len(e.mutations()), 1)

    async def test_hydration_uses_supplied_canonical_text_and_log_summary(self):
        await self.make()
        await self.run_browser("canonical_labels")

    async def test_cancelled_plan_reply_cannot_replace_new_review(self):
        await self.make()
        await self.run_browser("review_race")

    async def test_manual_mode_review_ignores_health_snapshot_changes(self):
        await self.make()
        await self.run_browser("manual_review_scope")

    async def test_failed_mode_activation_keeps_review_visible_until_explicit_refresh(self):
        await self.make()
        await self.run_browser("mode_review_failure")

    async def test_auto_mode_review_refreshes_snapshot_and_restores_opener_focus(self):
        await self.make()
        await self.run_browser("auto_review_refresh")

    async def test_state_poll_failure_preserves_command_handoff_status(self):
        await self.make()
        await self.run_browser("poll_handoff_status")

    async def test_forced_theme_sets_native_control_color_scheme(self):
        await self.make()
        await self.run_browser("forced_theme_scheme")

    async def test_pending_operation_blocks_refresh_but_allows_off_once(self):
        await self.make()
        await self.run_browser("pending_operation_controls")

    async def test_incident_evidence_opens_affected_second_pile(self):
        await self.make()
        await self.run_browser("incident_evidence_target")

    async def test_timestamp_formatter_construction_is_bounded_per_zone(self):
        await self.make()
        await self.run_browser("cached_time_formatting")

    async def test_unchanged_expanded_history_preserves_row_nodes(self):
        await self.make()
        await self.run_browser("stable_expanded_history")

    async def test_fresh_evidence_health_expires_on_local_clock(self):
        await self.make()
        await self.run_browser("fresh_evidence_expiry")

    async def test_stage1_exclusion_expires_on_local_clock(self):
        await self.make()
        await self.run_browser("stage1_exclusion_expiry")

    async def test_manual_actions_expire_with_state_proof(self):
        await self.make()
        await self.run_browser("manual_actions_state_expiry")

    async def test_log_filter_change_discards_stale_query_cursor(self):
        await self.make()
        await self.run_browser("log_filter_race")

    async def test_log_retention_is_bounded_while_text_is_selected(self):
        await self.make()
        await self.run_browser("log_selection_retention")

    async def test_log_pile_and_search_match_retained_api_and_export(self):
        await self.make(names=("cursor", "b"))
        await self.run_browser("log_predicates")

    async def test_debug_capture_defaults_to_info_ssr_and_hydration(self):
        await self.make(logging_level="debug")
        await self.run_browser("default_info_debug")

    async def test_local_zones_dst_and_expiry_wording(self):
        await self.make()
        await self.run_browser("time")

    async def test_rejoin_ignores_expired_retained_target_health(self):
        e = await self.make()
        await e.controller.set_mode("manual")
        generation = await e.generation()
        plan = await e.controller.plan("take_offline", "b")
        await e.controller.execute(plan)
        await e.mutation(generation, {"b": "SUSPENDED"})

        def eligible_after_target_expiry(state):
            target = next(card for card in state["cards"] if card["name"] == "b")
            return target["state"] == "DISCONNECTED" and target["evidence"]["expired"] and target["actions"]["rejoin"]["available"]

        await e.controller.until(eligible_after_target_expiry, "Rejoin available with expired retained target health")
        generation = await e.generation()
        marker = max(event["ts"] for event in e.mutations()) + 0.000001
        await self.run_browser("rejoin_retained_evidence")
        await e.mutation(generation, {"b": "NOT_SYNCHRONIZED"}, after=marker)
        await e.quiet(1)
        self.assertEqual(len(e.mutations()), 2)

    async def test_review_alternates_follow_the_effective_primary(self):
        e = await self.make()
        await e.controller.set_mode("manual")
        await self.run_browser("review_alternates")
        self.assertEqual(e.mutations(), [])
