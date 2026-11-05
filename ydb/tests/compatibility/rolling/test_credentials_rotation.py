from contextlib import ExitStack

import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.compatibility.fixtures import (
    RollingUpgradeAndDowngradeFixture, current_binary_path, current_name,
    inter_stable_binary_path, prepare_feature_flags,
)
from ydb.tests.library.replication_credentials import (
    ADMIN, Database, P2, SecretKind, cleanup_objects, create_replication, database,
    recover, rotate_to_error, write_secret,
)


class CredentialCluster(RollingUpgradeAndDowngradeFixture):
    nodes = 2
    reverse_versions = False

    @pytest.fixture(autouse=True)
    def base_setup(self):
        if current_name != "current":
            pytest.skip("Credential regression requires -DYDB_COMPAT_TARGET_REF=current")
        self.all_binary_paths = [inter_stable_binary_path, current_binary_path]
        if self.reverse_versions:
            self.all_binary_paths.reverse()

    @pytest.fixture(autouse=True)
    def setup(self, base_setup):
        yield from self.setup_cluster(
            nodes=self.nodes, erasure=Erasure.NONE, static_pdisk_size=16 * 1024**3,
            extra_grpc_services=["replication"], default_clusteradmin=ADMIN,
            enforce_user_token_requirement=True,
            disabled_feature_flags=["disable_old_secret_creation", "disable_old_secrets"],
        )

    def create_driver(self):
        # Adapt the public workload to the maintained rolling fixture. It owns
        # node swaps/readiness; Database owns authenticated RPCs and assertions.
        if not hasattr(self, "destination"):
            self.destination = Database("destination", cluster=self.cluster, config=self.config)
        self.destination.connect()
        return self.destination.driver

    def stop_driver(self):
        self.destination.saved_logs.update(self.destination.log_offsets())
        self.destination.disconnect()
        self.driver = None

    def source(self):
        flags, disabled = prepare_feature_flags([], [])
        return database("source", binary_paths=[inter_stable_binary_path],
                        extra_flags=flags, disabled_flags=disabled, account_lockout_attempt_threshold=0)


class TestCredentialRollingCompatibility(CredentialCluster):
    def test_upgrade_and_downgrade(self):
        with ExitStack() as cleanup:
            source = cleanup.enter_context(self.source())
            destination = self.destination
            cleanup.callback(cleanup_objects, source, destination, SecretKind.LEGACY)
            identities = create_replication(source, destination, SecretKind.LEGACY)
            storage = destination.storage_identity()
            expected = [(1, "initial"), (2, "remove-me")]
            for index, _ in enumerate(self.roll()):
                row = (5000 + index, f"version-step-{index}")
                source.query(f'UPSERT INTO t (id, value) VALUES ({row[0]}u, "{row[1]}");')
                expected.append(row)
                destination.wait_rows(expected)
                destination.wait_state("running")
                assert destination.identities() == identities
                assert destination.storage_identity() == storage
            assert all(node.binary_path == inter_stable_binary_path for node in destination.processes())
            destination.assert_no_secrets(extra_tokens=source.tokens)
            source.assert_no_secrets()


class TestCredentialStateCompatibility(CredentialCluster):
    nodes = 1
    reverse_versions = True

    def test_error_downgrade_paused_upgrade(self):
        with ExitStack() as cleanup:
            source = cleanup.enter_context(self.source())
            destination = self.destination
            cleanup.callback(cleanup_objects, source, destination, SecretKind.LEGACY)
            identities = create_replication(source, destination, SecretKind.LEGACY)
            storage = destination.storage_identity()
            rotate_to_error(source, destination, identities)
            transitions = self.roll()
            next(transitions)  # Initial current process.
            next(transitions)  # Old process on the same disks.
            result = destination.wait_state("error")
            assert result.error.issues, "Old process lost persisted authentication issue"
            assert destination.identities() == identities
            destination.assert_no_secrets(result.error.issues, source.tokens)
            write_secret(destination, SecretKind.LEGACY, P2)
            destination.query('ALTER ASYNC REPLICATION r SET (STATE="PAUSED");')
            destination.wait_state("paused")
            next(transitions)  # Current process reads old durable PAUSED.
            destination.wait_state("paused")
            assert destination.identities() == identities
            assert destination.storage_identity() == storage
            assert list(transitions) == []
            recover(source, destination, SecretKind.LEGACY, identities)
