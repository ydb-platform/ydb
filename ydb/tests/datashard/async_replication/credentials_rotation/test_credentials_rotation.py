from contextlib import ExitStack
import time

import grpc
import pytest

from ydb.tests.library.replication_credentials import (
    DATABASE, P1, P2, USER, SecretKind, cleanup_objects, create_replication, database,
    recover, rejecting_source_endpoint, rotate_to_error, secret_name, secret_option, write_secret, ydb, ydb_auth_pb2, ydb_auth_v1_pb2_grpc, ydb_status_codes_pb2,
)


class TestCredentialsRotation:
    @pytest.mark.parametrize("kind", list(SecretKind), ids=lambda kind: kind.value)
    def test_restart_error_and_secret_recovery(self, kind):
        with ExitStack() as cleanup:
            source = cleanup.enter_context(database("source"))
            destination = cleanup.enter_context(database("destination"))
            cleanup.callback(cleanup_objects, source, destination, kind)
            identities = create_replication(source, destination, kind)
            try:
                destination.query(f'ALTER ASYNC REPLICATION r SET ({secret_option(kind)}="{secret_name(kind, "alternatepw")}");')
            except ydb.Error as error:
                diagnostic = destination.redact(error)
                assert all(word in diagnostic for word in ("Running", "STANDBY", "PAUSED")), diagnostic
            else:
                pytest.fail("Running replication accepted a settings change")
            assert destination.identities() == identities
            rotate_to_error(source, destination, identities)
            recover(source, destination, kind, identities)

    def test_invalid_credentials_after_pause_are_error(self):
        with ExitStack() as cleanup:
            source = cleanup.enter_context(database("source"))
            destination = cleanup.enter_context(database("destination", replication_log_level=8))
            cleanup.callback(cleanup_objects, source, destination, SecretKind.LEGACY)
            identities = create_replication(source, destination, SecretKind.LEGACY)
            destination.query('ALTER ASYNC REPLICATION r SET (STATE="PAUSED");')
            destination.wait_state("paused")
            offsets = destination.log_offsets()
            source.query(f"ALTER USER {USER} PASSWORD '{P2}';")
            source.login(P2, ydb_status_codes_pb2.StatusIds.SUCCESS)

            def source_unlocked(phase):
                rows = source.query(f"SELECT IsLockedOut, FailedAttemptCount FROM `/Root/.sys/auth_users` WHERE Sid = '{USER}';")[0].rows
                assert len(rows) == 1
                state = {name: rows[0][name] for name in ("IsLockedOut", "FailedAttemptCount")}
                source.record(phase, auth_state=state)
                assert not state["IsLockedOut"], "Source account lockout invalidates the wrong-secret prerequisite"
            source_unlocked("registrar-source-before-resume")
            destination.query('ALTER ASYNC REPLICATION r SET (STATE="STANDBY");')

            def registrar_rejected(timeout):
                lines = list(destination.log_lines_after(offsets))
                assert not any("Run worker" in line and "actorClassName=TReplicationService" in line
                               for _, line in lines), "Worker started before the registrar authentication failure"
                for path, line in lines:
                    if "TableWorkerRegistar" in line and "CLIENT_UNAUTHENTICATED" in line:
                        destination.record("registrar-authentication-rejected", log=path, event=destination.redact(line.strip()))
                        return True
                return False
            destination.poll("actual registrar authentication rejection", registrar_rejected)
            source_unlocked("registrar-source-after-rejection")
            result = destination.wait_state("error")
            issue = str(result.error.issues)
            assert "auth" in issue.lower() and "registration" in issue.lower()
            assert destination.identities() == identities
            destination.assert_no_secrets(issue, source.tokens)
            destination.record("registrar-public-error", identities=identities, issue=issue)
            destination.restart()
            result = destination.wait_state("error")
            assert result.error.issues and destination.identities() == identities
            destination.record("registrar-persisted-error", identities=identities)
            write_secret(destination, SecretKind.LEGACY, P2)
            destination.query('ALTER ASYNC REPLICATION r SET (STATE="PAUSED");')
            destination.wait_state("paused")
            source.query('UPSERT INTO t (id, value) VALUES (6001u, "after-registrar-error");')
            destination.query('ALTER ASYNC REPLICATION r SET (STATE="STANDBY");')
            destination.wait_state("running")
            destination.wait_rows([(1, "initial"), (2, "remove-me"), (6001, "after-registrar-error")])
            assert destination.identities() == identities
            destination.assert_no_secrets(extra_tokens=source.tokens)
            source.assert_no_secrets()
            destination.record("registrar-recovered", identities=identities, rows=destination.last_rows)

    def test_temporary_source_outage_recovers(self):
        with ExitStack() as cleanup:
            source = cleanup.enter_context(database("source", enable_audit_log=True))
            destination = cleanup.enter_context(database("destination"))
            cleanup.callback(cleanup_objects, source, destination, SecretKind.LEGACY)
            identities = create_replication(source, destination, SecretKind.LEGACY)
            source.stop_nodes()
            storage = destination.storage_identity()
            destination.stop_nodes()
            with grpc.insecure_channel(source.endpoint) as channel:
                with pytest.raises(grpc.RpcError) as failure:
                    ydb_auth_v1_pb2_grpc.AuthServiceStub(channel).Login(
                        ydb_auth_pb2.LoginRequest(user=USER, password=P1),
                        metadata=(("x-ydb-database", DATABASE),), timeout=5,
                    )
                assert failure.value.code() == grpc.StatusCode.UNAVAILABLE
            # Both processes are stopped before arming the listener. The only
            # source client during this interval is the restarted destination;
            # the independent Login probe above cannot satisfy this barrier.
            with rejecting_source_endpoint(source) as connection_probe:
                offsets = destination.log_offsets()
                destination.start_nodes()
                assert destination.storage_identity() == storage
                destination.wait_cold_source_attempt(offsets, connection_probe)
                assert destination.replication().WhichOneof("state") != "error"
                destination.record("cold-source-outage", identities=identities)
            # Listener is closed and joined before the real source reclaims its port.
            source.start_nodes()
            source.query('UPSERT INTO t (id, value) VALUES (3u, "after-outage");')
            destination.wait_rows([(1, "initial"), (2, "remove-me"), (3, "after-outage")])
            destination.wait_state("running")
            # A bounded healthy idle control; no production no-data deadline exists.
            until = time.monotonic() + 2
            while time.monotonic() < until:
                assert destination.replication().WhichOneof("state") != "error"
                time.sleep(0.25)
            source.query('UPSERT INTO t (id, value) VALUES (4u, "after-idle");')
            destination.wait_rows([(1, "initial"), (2, "remove-me"), (3, "after-outage"), (4, "after-idle")])
            assert destination.identities() == identities
            destination.assert_no_secrets(extra_tokens=source.tokens)
            source.assert_no_secrets()
