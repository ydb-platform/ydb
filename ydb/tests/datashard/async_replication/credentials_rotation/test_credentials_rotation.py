from contextlib import ExitStack
import time

import grpc
import pytest

from ydb.tests.library.replication_credentials import (
    DATABASE, P1, USER, SecretKind, cleanup_objects, create_replication, database,
    recover, rejecting_source_endpoint, rotate_to_error, secret_name, secret_option, ydb, ydb_auth_pb2, ydb_auth_v1_pb2_grpc,
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

    def test_temporary_source_outage_recovers(self):
        with ExitStack() as cleanup:
            source = cleanup.enter_context(database("source"))
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
