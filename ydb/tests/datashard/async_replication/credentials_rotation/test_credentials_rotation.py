from contextlib import ExitStack

import pytest

from ydb.tests.library.replication_credentials import (
    SecretKind, cleanup_objects, create_replication, database, recover,
    rejecting_source_endpoint, rotate_to_error, secret_option, ydb,
)


class TestCredentialsRotation:
    @pytest.mark.parametrize("kind", list(SecretKind), ids=lambda kind: kind.value)
    def test_restart_error_and_secret_recovery(self, kind):
        with ExitStack() as cleanup:
            source = cleanup.enter_context(database("source"))
            destination = cleanup.enter_context(database("destination", replication_log_level=8))
            cleanup.callback(cleanup_objects, source, destination, kind)
            identities = create_replication(source, destination, kind)
            try:
                # A settings change is rejected while Running; recovery below
                # changes only the value of the original secret.
                destination.query(f'ALTER ASYNC REPLICATION r SET ({secret_option(kind)}="unused-secret");')
            except ydb.Error as error:
                diagnostic = destination.redact(error)
                assert all(word in diagnostic for word in ("Running", "STANDBY", "PAUSED")), diagnostic
            else:
                pytest.fail("Running replication accepted a settings change")
            assert destination.identities() == identities
            # Legacy cold boot exercises the service; completed schema pause
            # exercises registrar rejection before a fresh worker can start.
            rotate_to_error(source, destination, identities, paused=kind == SecretKind.SCHEMA)
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
            # No other client contacts this port while the source is down.
            # The destination must actually attempt a connection before restore.
            with rejecting_source_endpoint(source) as connected:
                destination.start_nodes()
                assert destination.storage_identity() == storage
                destination.poll("destination source connection while down", lambda timeout: connected.is_set())
                assert destination.replication().WhichOneof("state") != "error"
            source.start_nodes()
            source.query('UPSERT INTO t (id, value) VALUES (3u, "after-outage");')
            destination.wait_rows([(1, "initial"), (2, "remove-me"), (3, "after-outage")])
            destination.wait_state("running")
            source.query('UPSERT INTO t (id, value) VALUES (4u, "later-row");')
            destination.wait_rows([(1, "initial"), (2, "remove-me"), (3, "after-outage"), (4, "later-row")])
            assert destination.identities() == identities
            destination.assert_no_secrets(extra_tokens=source.tokens)
            source.assert_no_secrets()
