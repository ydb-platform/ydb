from types import SimpleNamespace

import pytest

from ydb.tests.library.common.msgbus_types import EDriveStatus
from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import use_cluster
from ydb.tests.stability.nemesis.internal.nemesis.runners import cluster_disk
from ydb.tests.stability.nemesis.internal.nemesis.runners.cluster_disk import (
    ClusterSafelyBreakDiskNemesis,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import (
    ChaosTarget,
)


class _Client:
    def __init__(self, fail_break_after_mutation=False):
        self.states = {
            "/active": EDriveStatus.ACTIVE,
            "/preexisting-broken": EDriveStatus.BROKEN,
        }
        self.updates = []
        self.fail_break_after_mutation = fail_break_after_mutation

    def read_drive_status(self, host, ic_port):
        del host, ic_port
        drives = [
            SimpleNamespace(Path=path, Status=status)
            for path, status in self.states.items()
        ]
        return SimpleNamespace(
            BlobStorageConfigResponse=SimpleNamespace(
                Status=[SimpleNamespace(DriveStatus=drives)],
            )
        )

    def update_drive_status(self, host, ic_port, path, status):
        del host, ic_port
        self.states[path] = status
        self.updates.append((path, status))
        success = not (
            self.fail_break_after_mutation
            and status == EDriveStatus.BROKEN
        )
        return SimpleNamespace(
            BlobStorageConfigResponse=SimpleNamespace(
                Success=success,
                Status=[],
            )
        )

    def query_base_config(self):
        return SimpleNamespace(
            BaseConfig=SimpleNamespace(
                PDisk=[
                    SimpleNamespace(NodeId=1, PDiskId=1, Path="/active"),
                    SimpleNamespace(
                        NodeId=1,
                        PDiskId=2,
                        Path="/preexisting-broken",
                    ),
                ],
                VSlot=[],
            )
        )


def test_local_disk_runner_restores_only_the_drive_it_changed(monkeypatch):
    client = _Client()
    cluster = SimpleNamespace(
        client=client,
        config=SimpleNamespace(
            pdisks_info=[
                {
                    "node_id": 1,
                    "pdisk_path": "/active",
                    "pdisk_user_kind": 1,
                },
            ],
        ),
        nodes={
            node_id: SimpleNamespace(host="localhost", ic_port=19000 + node_id)
            for node_id in range(1, 9)
        },
    )
    runner = ClusterSafelyBreakDiskNemesis()
    runner._state_ready = lambda cluster: True
    runner.on_success_inject_fault = lambda: None
    runner.on_success_extract_fault = lambda: None

    def reject_host_command(*args, **kwargs):
        del args, kwargs
        raise AssertionError("disk status chaos invoked a host command")

    monkeypatch.setattr(
        cluster_disk.subprocess,
        "run",
        reject_host_command,
    )
    target = ChaosTarget.for_disk("node-1", node_id=1)
    payload = {"chaos_target": target.to_dict()}

    with use_cluster(cluster):
        runner.inject_fault(payload)
        assert client.states == {
            "/active": EDriveStatus.BROKEN,
            "/preexisting-broken": EDriveStatus.BROKEN,
        }
        assert runner.changed_drives() == {
            target.identity_key(): ((1, "/active"),),
        }

        runner.extract_fault(payload)

    assert client.states == {
        "/active": EDriveStatus.ACTIVE,
        "/preexisting-broken": EDriveStatus.BROKEN,
    }
    assert client.updates == [
        ("/active", EDriveStatus.BROKEN),
        ("/active", EDriveStatus.ACTIVE),
    ]
    assert runner.changed_drives() == {}


def test_local_disk_runner_compensates_for_partial_bsc_failure():
    client = _Client(fail_break_after_mutation=True)
    cluster = SimpleNamespace(
        client=client,
        config=SimpleNamespace(
            pdisks_info=[
                {
                    "node_id": 1,
                    "pdisk_path": "/active",
                    "pdisk_user_kind": 1,
                },
            ],
        ),
        nodes={
            node_id: SimpleNamespace(host="localhost", ic_port=19000 + node_id)
            for node_id in range(1, 9)
        },
    )
    runner = ClusterSafelyBreakDiskNemesis()
    runner._state_ready = lambda cluster: True
    runner.on_success_inject_fault = lambda: None
    target = ChaosTarget.for_disk("node-1", node_id=1)
    payload = {"chaos_target": target.to_dict()}

    with use_cluster(cluster):
        with pytest.raises(RuntimeError, match="compensating_restore=succeeded"):
            runner.inject_fault(payload)

    assert client.states == {
        "/active": EDriveStatus.ACTIVE,
        "/preexisting-broken": EDriveStatus.BROKEN,
    }
    assert client.updates == [
        ("/active", EDriveStatus.BROKEN),
        ("/active", EDriveStatus.ACTIVE),
    ]
    assert runner.changed_drives() == {}
