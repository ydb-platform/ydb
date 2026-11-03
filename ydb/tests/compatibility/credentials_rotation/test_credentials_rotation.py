from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib
from pathlib import Path
import subprocess

import pytest

from ydb.tests.compatibility.credentials_rotation.placement import (
    actual_node_id, controller_location, move_controller, tablet_action, wait_adoption,
    wait_service_auth_error, wait_worker,
)

from ydb.tests.library.compatibility.fixtures import (
    current_binary_path, current_name, inter_stable_binary_path, inter_stable_name,
    prepare_feature_flags,
)
from ydb.tests.library.replication_credentials import (
    ADMIN, PHASE_TIMEOUT, P1, P2, RPC_TIMEOUT, USER, SecretKind, cleanup_objects, create_replication, database, recover, rotate_to_error, write_secret, ydb, ydb_status_codes_pb2,
)


def binary_manifest(path, name):
    resolved = Path(path).resolve()
    digest = hashlib.sha256()
    with resolved.open("rb") as executable:
        for chunk in iter(lambda: executable.read(8 * 1024**2), b""):
            digest.update(chunk)
    version = subprocess.run([str(resolved), "-V"], capture_output=True, text=True, timeout=30, check=True)
    return {"name": name, "path": str(resolved), "sha256": digest.hexdigest(),
            "version": version.stdout + version.stderr}


def source_auth_state(source, phase):
    rows = source.query(f"SELECT IsEnabled, IsLockedOut, FailedAttemptCount FROM `/Root/.sys/auth_users` WHERE Sid = '{USER}';")[0].rows
    assert len(rows) == 1
    state = {name: rows[0][name] for name in ("IsEnabled", "IsLockedOut", "FailedAttemptCount")}
    source.record(phase, auth_state=state)
    return state


def restart_on(destination, binary):
    storage = destination.storage_identity()
    destination.stop_nodes()
    for node in destination.processes():
        node.binary_path = binary
    destination.start_nodes()
    assert destination.storage_identity() == storage


class TestCredentialStateCompatibility:
    def test_error_downgrade_paused_upgrade(self):
        # Producing Error from fresh bad credentials is part of this fix.
        # The old binary still has to read Error and produce durable Paused.
        if current_name != "current":
            pytest.skip("Error-producing regression requires -DYDB_COMPAT_TARGET_REF=current")
        binaries = [binary_manifest(inter_stable_binary_path, inter_stable_name),
                    binary_manifest(current_binary_path, current_name)]
        flags, disabled = prepare_feature_flags([], [])
        with ExitStack() as cleanup:
            source = cleanup.enter_context(database(
                "source", binary_paths=[inter_stable_binary_path], extra_flags=flags, disabled_flags=disabled))
            destination = cleanup.enter_context(database(
                "destination", binary_paths=[current_binary_path], extra_flags=flags, disabled_flags=disabled))
            cleanup.callback(cleanup_objects, source, destination, SecretKind.LEGACY)
            destination.record("physical-binaries", binaries=binaries, branch_target=current_name == "current",
                               extra_flags=flags, disabled_flags=disabled)
            identities = create_replication(source, destination, SecretKind.LEGACY)
            rotate_to_error(source, destination, identities)
            restart_on(destination, inter_stable_binary_path)
            result = destination.wait_state("error")
            assert result.error.issues, "Old process lost persisted authentication issue"
            assert destination.identities() == identities
            destination.assert_no_secrets(result.error.issues, source.tokens)
            destination.record("old-reads-new-error", identities=identities)
            write_secret(destination, SecretKind.LEGACY, P2)
            destination.query('ALTER ASYNC REPLICATION r SET (STATE="PAUSED");')
            destination.wait_state("paused")
            assert destination.identities() == identities
            destination.record("old-completes-paused", identities=identities)
            restart_on(destination, current_binary_path)
            destination.wait_state("paused")
            assert destination.identities() == identities
            destination.record("new-reads-old-paused", identities=identities)
            recover(source, destination, SecretKind.LEGACY, identities)


def stop_processes(destination, processes):
    destination.saved_logs.update(destination.log_offsets())
    destination.disconnect()
    for process in processes:
        process.stop()


def start_process(destination, process, binary):
    destination.restart_sequence += 1
    process.binary_path = binary
    process.set_log_file_prefix(f"physical_{destination.restart_sequence}_")
    process.start()


def roll_process(destination, process, binary):
    storage = destination.storage_identity()
    stop_processes(destination, [process])
    start_process(destination, process, binary)
    destination.connect()
    assert destination.storage_identity() == storage


def pause_across_controller_upgrade(destination, tablet, controller, survivor):
    before = controller_location(destination, tablet)
    assert before and before["node"] == actual_node_id(destination, controller)
    path = f"{destination.database_path}/r"
    assert destination.describe(path)["Self"]["PathState"] == "EPathStateNoChanges"
    driver = ydb.Driver(ydb.DriverConfig(
        endpoint=survivor.endpoint, database=destination.database_path,
        credentials=ydb.AccessTokenCredentials(ADMIN), disable_discovery=True))
    executor = ThreadPoolExecutor(max_workers=1)
    session = None
    future = None
    stopped = False
    try:
        driver.wait(timeout=30)
        session = ydb.QuerySession(driver).create(settings=ydb.BaseRequestSettings().with_timeout(RPC_TIMEOUT))
        assert session.node_id == actual_node_id(destination, survivor), "Pause SQL session is on the process being rolled"
        destination.record("pause-client-placement", session_node=session.node_id, controller=before,
                           controller_binary=controller.binary_path)
        tablet_action(destination, tablet, "StopTablet")
        stopped = True
        destination.poll("controller stopped by Hive", lambda timeout: controller_location(destination, tablet, timeout) is None)
        future = executor.submit(lambda: list(session.execute(
            'ALTER ASYNC REPLICATION r SET (STATE="PAUSED");',
            settings=ydb.BaseRequestSettings().with_timeout(PHASE_TIMEOUT))))
        def accepted(timeout):
            if future.done():
                future.result()  # Preserve an immediate SQL error instead of hiding it behind a poll.
                raise AssertionError("Pause completed before the stopped-controller transition")
            return destination.describe(path, timeout)["Self"]["PathState"] == "EPathStateAlter"
        destination.poll("persisted accepted pause awaiting controller", accepted)
        destination.record("pause-persisted-before-upgrade", controller=before,
                           path_state="EPathStateAlter", sql_pending=not future.done())
        roll_process(destination, controller, current_binary_path)
        assert not future.done(), "Pause completed while controller was persistently stopped"
        tablet_action(destination, tablet, "ResumeTablet")
        stopped = False
        future.result(timeout=PHASE_TIMEOUT)
        destination.wait_state("paused")
        after = destination.poll("resumed controller location", lambda timeout: controller_location(destination, tablet, timeout))
        destination.record("pause-completed-after-upgrade", before=before, after=after,
                           processes=destination.node_manifest())
    finally:
        try:
            if stopped:
                tablet_action(destination, tablet, "ResumeTablet")
        finally:
            # The independent SQL RPC is bounded. Closing its driver cancels a
            # pending stream on failure; always join it before fixture teardown.
            if future is not None and not future.done():
                driver.stop(timeout=10)
            executor.shutdown(wait=True, cancel_futures=True)
            if session is not None:
                try:
                    session.delete(settings=ydb.BaseRequestSettings().with_timeout(RPC_TIMEOUT))
                except ydb.Error:
                    pass
            driver.stop(timeout=10)


class TestCredentialRolesCompatibility:
    def test_mixed_roles_rolling_upgrade_and_downgrade(self):
        if current_name != "current":
            pytest.skip("New-service auth regression requires -DYDB_COMPAT_TARGET_REF=current")
        binaries = [binary_manifest(inter_stable_binary_path, inter_stable_name),
                    binary_manifest(current_binary_path, current_name)]
        flags, disabled = prepare_feature_flags([], [])
        with ExitStack() as cleanup:
            source = cleanup.enter_context(database(
                "source", binary_paths=[inter_stable_binary_path], extra_flags=flags, disabled_flags=disabled,
                account_lockout_attempt_threshold=0))
            destination = cleanup.enter_context(database(
                "destination", binary_paths=[inter_stable_binary_path], tenant_slots=1,
                extra_flags=flags, disabled_flags=disabled, replication_log_level=8))
            cleanup.callback(cleanup_objects, source, destination, SecretKind.LEGACY)
            destination.record("physical-binaries", binaries=binaries, branch_target=True,
                               extra_flags=flags, disabled_flags=disabled)
            # Multiple real bad-password readers/controllers are intentional.
            # Disable account lockout, preserving actual password enforcement.
            source.record("source-lockout-policy", attempt_threshold=0)
            identities = create_replication(source, destination, SecretKind.LEGACY)
            tablet = identities["controller"]
            first = next(iter(destination.cluster.slots.values()))
            old_worker = wait_worker(destination, first, {})
            second = destination.cluster.register_and_start_slots(destination.database_path, 1)[0]
            destination.record("all-old-initial", identities=identities)

            # Existing old worker stays alive while the second process and the
            # controller upgrade. Endpoint selection alone is never placement.
            roll_process(destination, second, current_binary_path)
            offsets = destination.log_offsets()
            location = move_controller(destination, tablet, second)
            wait_adoption(destination, second, first, old_worker, tablet, location, offsets)
            extras = [(3001, "new-controller-old-service")]
            source.query('UPSERT INTO t (id, value) VALUES (3001u, "new-controller-old-service");')
            destination.wait_rows([(1, "initial"), (2, "remove-me"), *extras])
            destination.record("new-controller-old-service-delivery", worker=old_worker, controller=location)

            # Establish current worker persistence before the offline cold boot.
            offsets = destination.log_offsets()
            roll_process(destination, first, current_binary_path)
            destination.poll("current worker source data", lambda timeout: destination.rows(timeout) ==
                             [(1, "initial"), (2, "remove-me"), *extras])
            roll_process(destination, destination.cluster.nodes[1], current_binary_path)
            destination.restart()
            destination.wait_state("running")
            source.query('UPSERT INTO t (id, value) VALUES (3002u, "all-current-before-rotation");')
            extras.append((3002, "all-current-before-rotation"))
            destination.wait_rows([(1, "initial"), (2, "remove-me"), *extras])
            destination.record("all-current-persisted-worker", identities=identities)

            # Both tenant services stop first. Root storage stays available and
            # publishes a different service board from /Root/tenant.
            stop_processes(destination, [first, second])
            source.query(f"ALTER USER {USER} PASSWORD '{P2}';")
            source.login(P2, ydb_status_codes_pb2.StatusIds.SUCCESS)
            source.login(P1, ydb_status_codes_pb2.StatusIds.UNAUTHORIZED)
            source.stop_nodes()
            start_process(destination, first, current_binary_path)
            destination.connect()
            cold = wait_worker(destination, first, {}, expected=old_worker["logical"])
            assert all(not process.is_alive() for process in source.processes())
            assert destination.identities() == identities
            start_process(destination, second, inter_stable_binary_path)
            offsets = destination.log_offsets()
            location = move_controller(destination, tablet, second)
            wait_adoption(destination, second, first, cold, tablet, location, offsets)
            assert all(not process.is_alive() for process in source.processes())
            assert controller_location(destination, tablet) == location
            destination.record("qualified-old-controller-cold-new-service", worker=cold, controller=location)

            # Release the real external fault exactly once after all placement
            # prerequisites. Wrong behavior after this point is never retried.
            offsets = destination.log_offsets()
            source.start_nodes()
            source.query('UPSERT INTO t (id, value) VALUES (1002u, "during-error");')
            wait_service_auth_error(destination, second, first, cold, offsets)
            result = destination.wait_state("error")
            assert result.error.issues
            assert controller_location(destination, tablet)["node"] == location["node"]
            assert destination.identities() == identities
            destination.assert_no_secrets(result.error.issues, source.tokens)
            source_auth_state(source, "mixed-source-after-auth-error")
            write_secret(destination, SecretKind.LEGACY, P2)
            pause_across_controller_upgrade(destination, tablet, second, first)
            destination.record("mixed-error-pause-completed", identities=identities)
            source_auth_state(source, "mixed-source-after-pause")

            # Record this intentional recovery role transition. Full current
            # recovery and durable GLOBAL readback run on unchanged storage.
            restart_on(destination, current_binary_path)
            auth_state = source_auth_state(source, "mixed-source-before-recovery")
            assert not auth_state["IsLockedOut"], "Source account locked before destination credential recovery"
            source.login(P2, ydb_status_codes_pb2.StatusIds.SUCCESS)
            source.record("source-current-password-valid-before-recovery")
            recover(source, destination, SecretKind.LEGACY, identities, extras)
            expected = sorted([(1, "updated"), (1002, "during-error"), (1004, "after-recovery"),
                               (1005, "after-restart"), *extras])
            for index, process in enumerate(destination.processes()):
                roll_process(destination, process, inter_stable_binary_path)
                row = (5000 + index, f"rollback-{index}")
                source.query(f'UPSERT INTO t (id, value) VALUES ({row[0]}u, "{row[1]}");')
                expected = sorted([*expected, row])
                destination.wait_rows(expected)
                assert destination.identities() == identities
                destination.record("rolling-downgrade-delivery", rows=expected, identities=identities)
            destination.wait_state("running")
            destination.assert_no_secrets(extra_tokens=source.tokens)
            source.assert_no_secrets()
            destination.record("all-old-valid-rollback", rows=expected, identities=identities)

