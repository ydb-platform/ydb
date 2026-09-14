# -*- coding: utf-8 -*-
"""Tests for the standalone consistency checker.

Each stale-restore scenario from the plan gets a test that asserts *which*
invariants fire, so a regression in a check is visible as a changed finding set
rather than as a silently passing run.
"""

from __future__ import annotations

import calendar
import hashlib
import glob
import json
import os
import time

import pytest

from ydb.tests.stress.system_tablet_backup.consistency import Severity
from ydb.tests.stress.system_tablet_backup.consistency import doctor
from ydb.tests.stress.system_tablet_backup.consistency.model import (
    LiveCluster,
    LiveHive,
    LivePath,
)
from ydb.tests.stress.system_tablet_backup.consistency.registry import (
    required_tables,
    run_checks,
    select_checks,
)
from ydb.tests.stress.system_tablet_backup.consistency.sources import BackupError, load_ledger, load_state
from ydb.tests.stress.system_tablet_backup.consistency.tests import fake_backup as fb


def build_cluster(tmp_path, hive=None, schemeshard=None, bsc=None):
    root = str(tmp_path / "backups")
    os.makedirs(root, exist_ok=True)
    for builder in (
        hive if hive is not None else fb.hive_backup(),
        schemeshard if schemeshard is not None else fb.schemeshard_backup(),
        bsc if bsc is not None else fb.bsc_backup(),
    ):
        builder.write(root)
    return root


def check_ids(root, ledger_path=None, **kwargs):
    """Run every check and return {check_id: [findings above INFO]}."""
    specs = select_checks()
    needed = {k: v for k, v in required_tables(specs).items() if k != "ledger"}
    state, _ = load_state(root=root, needed_tables=needed, **kwargs)
    if ledger_path:
        state.ledger = load_ledger(ledger_path)

    outcomes = run_checks(state, specs)
    return {
        outcome.spec.id: [f for f in outcome.findings if f.severity > Severity.INFO]
        for outcome in outcomes
    }, outcomes


def failing(results):
    return sorted(check_id for check_id, findings in results.items() if findings)


def test_consistent_cluster_is_clean(tmp_path):
    root = build_cluster(tmp_path)
    results, outcomes = check_ids(root)

    assert failing(results) == [], "a consistent cluster must produce no findings"
    assert all(o.failed_reason is None for o in outcomes), [o.failed_reason for o in outcomes]

    # Only the checks needing a ledger or a live cluster may be skipped.
    skipped = {o.spec.id for o in outcomes if o.skipped_reason}
    assert skipped == {"I8", "I12", "I14", "I15", "I20"}


def test_stale_hive_loses_shards_and_reuses_ids(tmp_path):
    """S1: Hive restored from before the second shard was created."""
    hive = fb.hive_backup(
        tablets=[
            {
                "ID": fb.SHARDS[0]["TabletId"],
                "Owner": [fb.SS_TABLET_ID, 1],
                "State": 200,
                "TabletType": 2,
                "KnownGeneration": 1,
            }
        ],
        # Rolled back below the uniq part SchemeShard still references.
        next_tablet_id=fb.uniq(fb.SHARDS[1]["TabletId"]),
        channel_groups=[
            {
                "Tablet": fb.SHARDS[0]["TabletId"],
                "Channel": 0,
                "Generation": 1,
                "Group": fb.GROUPS[0],
                "DeletedAtGeneration": 0,
            }
        ],
    )
    root = build_cluster(tmp_path, hive=hive)
    results, _ = check_ids(root)

    assert "I1" in failing(results), "the forgotten shard must be reported"
    assert "I4" in failing(results), "tablet id reuse must be reported"

    lost = results["I1"][0]
    assert lost.severity is Severity.ERROR
    assert str(fb.SHARDS[1]["TabletId"]) in lost.message

    reuse = results["I4"][0]
    assert reuse.severity is Severity.CRITICAL
    assert reuse.details["tablet_id"] == fb.SHARDS[1]["TabletId"]


def test_stale_schemeshard_orphans_tablets_and_reuses_shard_idx(tmp_path):
    """S2: SchemeShard restored from before the second shard was created."""
    schemeshard = fb.schemeshard_backup(
        shards=[fb.SHARDS[0]],
        next_shard_idx=2,
        next_path_id=3,
        paths=[
            {"Id": 1, "ParentId": 0, "Name": "Root", "PathType": 1, "StepDropped": 0},
            {"Id": 2, "ParentId": 1, "Name": "t_1", "PathType": 2, "StepDropped": 0},
        ],
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)
    results, _ = check_ids(root)

    assert "I2" in failing(results), "the orphaned tablet must be reported"
    assert "I6" in failing(results), "shard index reuse must be reported"

    orphan = results["I2"][0]
    assert orphan.details["tablet_id"] == fb.SHARDS[1]["TabletId"]

    reuse = results["I6"][0]
    assert reuse.severity is Severity.CRITICAL
    assert reuse.details["shard_idx"] == 2


def test_stale_bscontroller_loses_groups_and_reuses_ids(tmp_path):
    """S3: BSController restored from before the second group was allocated."""
    bsc = fb.bsc_backup(groups=[fb.GROUPS[0]], next_group_id=fb.GROUPS[1])
    root = build_cluster(tmp_path, bsc=bsc)
    results, _ = check_ids(root)

    assert "I3" in failing(results), "the dangling channel must be reported"
    assert "I5" in failing(results), "group id reuse must be reported"

    dangling = results["I3"][0]
    assert dangling.severity is Severity.CRITICAL
    assert dangling.details["group"] == fb.GROUPS[1]

    reuse = results["I5"][0]
    assert reuse.details["group_id"] == fb.GROUPS[1]


def test_owner_mismatch_is_critical(tmp_path):
    """The same tablet id claimed by two different owners."""
    tablets = [
        {
            "ID": fb.SHARDS[0]["TabletId"],
            "Owner": [fb.SS_TABLET_ID, 99],  # SchemeShard says shard idx 1
            "State": 200,
            "TabletType": 2,
            "KnownGeneration": 1,
        },
        {
            "ID": fb.SHARDS[1]["TabletId"],
            "Owner": [fb.SS_TABLET_ID, 2],
            "State": 200,
            "TabletType": 2,
            "KnownGeneration": 1,
        },
    ]
    root = build_cluster(tmp_path, hive=fb.hive_backup(tablets=tablets))
    results, _ = check_ids(root)

    assert "I1" in failing(results)
    mismatch = [f for f in results["I1"] if f.severity is Severity.CRITICAL]
    assert mismatch, "conflicting ownership must be critical"
    assert mismatch[0].details["tablet_id"] == fb.SHARDS[0]["TabletId"]


def test_deleting_tablets_are_not_orphans(tmp_path):
    """A tablet already being deleted is expected to outlive its shard row."""
    tablets = [
        {
            "ID": fb.SHARDS[0]["TabletId"],
            "Owner": [fb.SS_TABLET_ID, 1],
            "State": 200,
            "TabletType": 2,
            "KnownGeneration": 1,
        },
        {
            "ID": fb.SHARDS[1]["TabletId"],
            "Owner": [fb.SS_TABLET_ID, 2],
            "State": 202,  # Deleting
            "TabletType": 2,
            "KnownGeneration": 1,
        },
    ]
    schemeshard = fb.schemeshard_backup(shards=[fb.SHARDS[0]], next_shard_idx=3)
    root = build_cluster(tmp_path, hive=fb.hive_backup(tablets=tablets), schemeshard=schemeshard)
    results, _ = check_ids(root)

    assert "I2" not in failing(results), "a Deleting tablet must not be reported as an orphan"


def test_changelog_is_applied_on_top_of_snapshot(tmp_path):
    """A shard created after the snapshot lives only in the changelog."""
    hive = fb.hive_backup(
        tablets=[
            {
                "ID": fb.SHARDS[0]["TabletId"],
                "Owner": [fb.SS_TABLET_ID, 1],
                "State": 200,
                "TabletType": 2,
                "KnownGeneration": 1,
            }
        ],
        channel_groups=[
            {
                "Tablet": fb.SHARDS[0]["TabletId"],
                "Channel": 0,
                "Generation": 1,
                "Group": fb.GROUPS[0],
                "DeletedAtGeneration": 0,
            }
        ],
    )
    hive.commit(
        2,
        [
            {
                "table": "Tablet",
                "op": "upsert",
                "ID": fb.SHARDS[1]["TabletId"],
                "Owner": [fb.SS_TABLET_ID, 2],
                "State": 200,
                "TabletType": 2,
                "KnownGeneration": 1,
            },
            {
                "table": "TabletChannelGen",
                "op": "upsert",
                "Tablet": fb.SHARDS[1]["TabletId"],
                "Channel": 0,
                "Generation": 1,
                "Group": fb.GROUPS[1],
                "DeletedAtGeneration": 0,
            },
        ],
    )
    root = build_cluster(tmp_path, hive=hive)

    replayed, _ = check_ids(root)
    assert failing(replayed) == [], "replaying the changelog must heal the snapshot gap"

    snapshot_only, _ = check_ids(root, apply_changelog=False)
    assert "I1" in failing(snapshot_only), "without the changelog the shard looks lost"


def test_changelog_erase_removes_rows(tmp_path):
    hive = fb.hive_backup()
    hive.commit(2, [{"table": "Tablet", "op": "erase", "ID": fb.SHARDS[1]["TabletId"]}])
    root = build_cluster(tmp_path, hive=hive)
    results, _ = check_ids(root)

    assert "I1" in failing(results), "an erased tablet must be reported as missing"


def test_checksum_mismatch_is_detected(tmp_path):
    root = build_cluster(tmp_path)
    tablet_json = os.path.join(
        root, "hive", str(fb.HIVE_TABLET_ID), fb.hive_backup().dir_name, "snapshot", "Tablet.json"
    )
    with open(tablet_json, "a") as handle:
        handle.write(json.dumps({"ID": 1, "Owner": [0, 0]}) + "\n")

    specs = select_checks()
    needed = {k: v for k, v in required_tables(specs).items() if k != "ledger"}

    state, notes = load_state(root=root, needed_tables=needed)
    assert not state.by_type("hive"), "a tampered backup must not be loaded"
    assert any("checksum mismatch" in note for note in notes)

    # ...and is loadable once validation is waived, which is the documented
    # path for a deliberately hand-edited backup.
    state, _ = load_state(root=root, needed_tables=needed, verify_checksums=False)
    assert state.by_type("hive")


def test_incomplete_snapshot_is_skipped(tmp_path):
    root = build_cluster(tmp_path)
    unfinished = os.path.join(
        root, "hive", str(fb.HIVE_TABLET_ID), "backup_20260818130000Z_g2_s99", "snapshot.tmp"
    )
    os.makedirs(unfinished)

    specs = select_checks()
    needed = {k: v for k, v in required_tables(specs).items() if k != "ledger"}
    state, _ = load_state(root=root, needed_tables=needed)

    hive = state.one("hive")
    assert hive is not None and hive.generation == 1, "must fall back to the last complete backup"


def test_ledger_checks_run_when_ledger_is_present(tmp_path):
    """I8/I12 are skipped without a ledger and active with one."""
    schemeshard = fb.schemeshard_backup(
        shards=[fb.SHARDS[0]],
        next_shard_idx=2,
        next_path_id=3,
        paths=[
            {"Id": 1, "ParentId": 0, "Name": "Root", "PathType": 1, "StepDropped": 0},
            {"Id": 2, "ParentId": 1, "Name": "t_1", "PathType": 2, "StepDropped": 0},
        ],
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)

    ledger_path = str(tmp_path / "ledger.jsonl")
    with open(ledger_path, "w") as handle:
        for index, shard in enumerate(fb.SHARDS):
            handle.write(
                json.dumps(
                    {
                        "ts": 1000.0 + index,
                        "op": "create",
                        "status": "ok",
                        "path": "/Root/t_%d" % (index + 1),
                        "path_id": shard["PathId"],
                        "shards": [
                            {"shard_idx": shard["ShardIdx"], "tablet_id": shard["TabletId"]}
                        ],
                    }
                )
                + "\n"
            )

    results, outcomes = check_ids(root, ledger_path=ledger_path)

    assert not any(o.skipped_reason for o in outcomes if o.spec.id in ("I8", "I12"))
    assert "I8" in failing(results), "the object lost by SchemeShard must be reported"
    assert results["I8"][0].details["path"] == "/Root/t_2"


def test_ledger_drop_is_not_reported_as_loss(tmp_path):
    schemeshard = fb.schemeshard_backup(
        shards=[fb.SHARDS[0]],
        next_shard_idx=3,
        next_path_id=4,
        paths=[
            {"Id": 1, "ParentId": 0, "Name": "Root", "PathType": 1, "StepDropped": 0},
            {"Id": 2, "ParentId": 1, "Name": "t_1", "PathType": 2, "StepDropped": 0},
            {"Id": 3, "ParentId": 1, "Name": "t_2", "PathType": 2, "StepDropped": 7},
        ],
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)

    ledger_path = str(tmp_path / "ledger.jsonl")
    with open(ledger_path, "w") as handle:
        handle.write(json.dumps({
            "ts": 1.0, "op": "create", "status": "ok", "path": "/Root/t_2", "path_id": 3,
            "shards": [{"shard_idx": 2, "tablet_id": fb.SHARDS[1]["TabletId"]}],
        }) + "\n")
        handle.write(json.dumps({"ts": 2.0, "op": "drop", "status": "ok", "path": "/Root/t_2"}) + "\n")

    results, _ = check_ids(root, ledger_path=ledger_path)
    assert "I8" not in failing(results), "an intentionally dropped object is not a loss"


def test_overlapping_hive_sequences_are_critical(tmp_path):
    hive = fb.hive_backup(
        sequences=[
            {"OwnerId": 0, "OwnerIdx": 0, "Begin": 100, "End": 200, "Next": 150},
            {"OwnerId": 0, "OwnerIdx": 1, "Begin": 180, "End": 300, "Next": 190},
        ]
    )
    root = build_cluster(tmp_path, hive=hive)
    results, _ = check_ids(root)

    assert "I9" in failing(results)
    assert results["I9"][0].severity is Severity.CRITICAL


def test_pdisk_slot_reuse_is_critical(tmp_path):
    bsc = fb.bsc_backup(pdisks=[{"NodeID": 1, "PDiskID": 1, "Guid": 111, "NextVSlotId": 1}])
    root = build_cluster(tmp_path, bsc=bsc)
    results, _ = check_ids(root)

    assert "I7" in failing(results)
    assert any("NextVSlotId" in f.message for f in results["I7"])


def _touch_changelog(root, tablet_type, tablet_id, when):
    """Create/date a changelog file, which is what freshness is measured from."""
    backup_dir = glob.glob(os.path.join(root, tablet_type, str(tablet_id), "backup_*"))[0]
    path = os.path.join(backup_dir, "changelog.json")
    if not os.path.exists(path):
        with open(path, "w"):
            pass
    os.utime(path, (when, when))
    return path


def test_directory_name_is_not_used_as_a_freshness_measure(tmp_path):
    """The name records when the snapshot *started*.

    Backups are not taken on a timer: the changelog keeps receiving commits
    afterwards, so an old directory name can hold current state.  Comparing
    names across tablets must not produce a staleness warning.
    """
    root = build_cluster(
        tmp_path,
        hive=fb.hive_backup(timestamp="20260818110000Z"),
        schemeshard=fb.schemeshard_backup(timestamp="20260818120000Z"),
        bsc=fb.bsc_backup(timestamp="20260818120000Z"),
    )
    # Every tablet last wrote its changelog at the same moment: despite an hour
    # between the snapshot names, the states are equally fresh.
    same_moment = 1786974000.0
    for tablet_type, tablet_id in (
        ("hive", fb.HIVE_TABLET_ID),
        ("scheme_shard", fb.SS_TABLET_ID),
        ("bscontroller", fb.BSC_TABLET_ID),
    ):
        _touch_changelog(root, tablet_type, tablet_id, same_moment)

    results, outcomes = check_ids(root)

    assert "I11" not in failing(results), "snapshot names must not drive the staleness verdict"
    i11 = [o for o in outcomes if o.spec.id == "I11"][0]
    assert i11.findings and i11.findings[0].severity is Severity.INFO


def test_changelog_mtime_drives_the_freshness_verdict(tmp_path):
    root = build_cluster(tmp_path)
    base = 1786974000.0
    _touch_changelog(root, "hive", fb.HIVE_TABLET_ID, base - 3600)
    _touch_changelog(root, "scheme_shard", fb.SS_TABLET_ID, base)
    _touch_changelog(root, "bscontroller", fb.BSC_TABLET_ID, base)

    results, _ = check_ids(root)

    assert "I11" in failing(results), "an hour of real staleness must be surfaced"
    finding = results["I11"][0]
    assert finding.severity is Severity.WARNING
    assert "changelog mtime" in finding.message
    assert finding.details["hive"]["lag_seconds"] == 3600
    assert finding.details["scheme_shard"]["lag_seconds"] == 0


def test_small_spread_carries_the_transfer_caveat(tmp_path):
    """A healthy cluster and a timestamp-dropping copy look identical.

    Snapshots cut at different times with every changelog current is the normal
    picture, so the verdict must state the limitation rather than guess that the
    files were copied badly.
    """
    root = build_cluster(
        tmp_path,
        hive=fb.hive_backup(timestamp="20260818110000Z"),
        schemeshard=fb.schemeshard_backup(timestamp="20260818110000Z"),
        bsc=fb.bsc_backup(timestamp="20260818110000Z"),
    )
    # Snapshot names say 11:00; every changelog claims to have been written now.
    copied_at = calendar.timegm(time.strptime("20260818140000Z", "%Y%m%d%H%M%SZ"))
    for tablet_type, tablet_id in (
        ("hive", fb.HIVE_TABLET_ID),
        ("scheme_shard", fb.SS_TABLET_ID),
        ("bscontroller", fb.BSC_TABLET_ID),
    ):
        _touch_changelog(root, tablet_type, tablet_id, copied_at)

    results, outcomes = check_ids(root)
    i11 = [o for o in outcomes if o.spec.id == "I11"][0]

    assert "I11" not in failing(results), "equal freshness is not a warning"
    assert i11.findings, "the checker must say something rather than stay silent"
    assert "relies on preserved mtimes" in i11.findings[0].message


def test_ledger_dates_staleness_independently_of_file_timestamps(tmp_path):
    """I14: the ledger dates each tablet's state, immune to how it was copied."""
    hive = fb.hive_backup(
        tablets=[{"ID": fb.SHARDS[0]["TabletId"], "Owner": [fb.SS_TABLET_ID, 1],
                  "State": 200, "TabletType": 2, "KnownGeneration": 1}],
        channel_groups=[{"Tablet": fb.SHARDS[0]["TabletId"], "Channel": 0, "Generation": 1,
                         "Group": fb.GROUPS[0], "DeletedAtGeneration": 0}],
    )
    root = build_cluster(tmp_path, hive=hive)

    ledger_path = str(tmp_path / "ledger.jsonl")
    with open(ledger_path, "w") as handle:
        # t_1 at T, t_2 an hour later; Hive only knows t_1.
        for index, shard in enumerate(fb.SHARDS):
            handle.write(json.dumps({
                "ts": 1786974000.0 + index * 3600,
                "op": "create", "status": "ok",
                "path": "/Root/t_%d" % (index + 1),
                "path_id": shard["PathId"],
                "shards": [{"tablet_id": shard["TabletId"]}],
            }) + "\n")

    results, outcomes = check_ids(root, ledger_path=ledger_path)
    i14 = [o for o in outcomes if o.spec.id == "I14"][0]

    assert not i14.skipped_reason
    assert "I14" in failing(results), "Hive is an hour behind by ledger dating"
    assert i14.findings[0].details["hive"]["lag_seconds"] == 3600.0
    assert i14.findings[0].details["scheme_shard"]["lag_seconds"] == 0.0


def test_missing_backup_root_is_an_error(tmp_path):
    with pytest.raises(BackupError):
        load_state(root=str(tmp_path / "nope"))


# --------------------------------------------------------------------------
# Doctor mode
# --------------------------------------------------------------------------


def doctor_run(root, ledger_path=None, **kwargs):
    """Load with the tables repairs need, run the checks, build a repair plan."""
    specs = select_checks()
    needed = {k: set(v) for k, v in required_tables(specs).items() if k != "ledger"}
    for slice_, tables in doctor.REQUIRED_TABLES.items():
        if slice_ in needed:
            needed[slice_] |= set(tables)

    state, _ = load_state(root=root, needed_tables=needed, **kwargs)
    if ledger_path:
        state.ledger = load_ledger(ledger_path)
    outcomes = run_checks(state, specs)
    return state, outcomes, doctor.plan(state, outcomes)


def stale_hive():
    """Hive rolled back before the second shard: the id it will re-issue is one
    only SchemeShard remembers, which is what Hive's own self-heal cannot cover."""
    return fb.hive_backup(
        tablets=[{"ID": fb.SHARDS[0]["TabletId"], "Owner": [fb.SS_TABLET_ID, 1],
                  "State": 200, "TabletType": 2, "KnownGeneration": 1}],
        next_tablet_id=fb.uniq(fb.SHARDS[1]["TabletId"]),
        channel_groups=[{"Tablet": fb.SHARDS[0]["TabletId"], "Channel": 0, "Generation": 1,
                         "Group": fb.GROUPS[0], "DeletedAtGeneration": 0}],
    )


def test_doctor_leaves_a_healthy_backup_alone(tmp_path):
    root = build_cluster(tmp_path)
    _, _, repair_plan = doctor_run(root)

    assert repair_plan.empty, "nothing to repair: %s" % [e.describe() for e in repair_plan.edits]
    assert not repair_plan.unrepairable


def test_doctor_repairs_tablet_id_reuse(tmp_path):
    """S1: doctor must clear I4 without inventing the lost shard back."""
    root = build_cluster(tmp_path, hive=stale_hive())

    state, outcomes, repair_plan = doctor_run(root)
    assert "I4" in failing({o.spec.id: [f for f in o.findings if f.severity > Severity.INFO]
                            for o in outcomes})

    edits = [e for e in repair_plan.edits if e.check_id == "I4"]
    assert len(edits) == 1, [e.describe() for e in repair_plan.edits]
    assert edits[0].tablet_type == "hive" and edits[0].table == "State"
    assert edits[0].values["Value"] > fb.uniq(fb.SHARDS[1]["TabletId"])
    # The repair must stay inside the 44-bit allocator space, not jump to a
    # composed tablet id.
    assert edits[0].values["Value"] < fb.SHARDS[1]["TabletId"]

    # The referential loss is honestly reported as out of reach.
    assert "I1" in repair_plan.unrepairable
    assert "TabletID" in doctor.GUIDANCE["I1"]

    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)

    fixed, _ = check_ids(out)
    assert "I4" not in failing(fixed), "doctor must clear the reuse risk"
    assert "I1" in failing(fixed), "and must not pretend the lost shard is back"


def test_doctor_ignores_unassigned_tablet_id_in_allocator_repair(tmp_path):
    schemeshard = fb.schemeshard_backup(
        shards=fb.SHARDS + [{"ShardIdx": 3, "TabletId": (1 << 64) - 1, "PathId": 4}],
        next_shard_idx=4,
        next_path_id=5,
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)
    _, _, repair_plan = doctor_run(root)
    assert all(
        edit.values.get("Value", 0) < (1 << 44)
        for edit in repair_plan.edits
        if edit.table == "State"
    )


def test_doctor_erases_hive_tablets_dropped_by_schemeshard(tmp_path):
    """A stale Hive restore must not resurrect a shard absent from SchemeShard."""
    schemeshard = fb.schemeshard_backup(
        shards=[fb.SHARDS[0]], next_shard_idx=3, next_path_id=3
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)

    live = LiveCluster(hives={
        fb.HIVE_TABLET_ID: LiveHive(
            hive_id=fb.HIVE_TABLET_ID,
            tablet_ids={fb.SHARDS[0]["TabletId"]},
        )
    })
    state, _, repair_plan = doctor_run_with_live(root, live)
    edits = [e for e in repair_plan.edits if e.check_id == "I2"]
    assert len(edits) == 1
    assert edits[0].table == "Tablet"
    assert edits[0].key == {"ID": fb.SHARDS[1]["TabletId"]}
    assert edits[0].op == "erase"

    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)
    fixed, _ = check_ids(out)
    assert "I2" not in failing(fixed)


@pytest.mark.parametrize("root_status", ["absent", "unreachable", "offline"])
def test_doctor_preserves_orphan_when_failed_root_hive_is_unavailable(tmp_path, root_status):
    schemeshard = fb.schemeshard_backup(
        shards=[fb.SHARDS[0]], next_shard_idx=3, next_path_id=3
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)
    live = LiveCluster(hives={
        fb.TENANT_HIVE_TABLET_ID: LiveHive(hive_id=fb.TENANT_HIVE_TABLET_ID)
    })
    if root_status == "unreachable":
        live.hives[fb.HIVE_TABLET_ID] = LiveHive(hive_id=fb.HIVE_TABLET_ID, reachable=False)
    elif root_status == "offline":
        live = None
    _, outcomes, repair_plan = doctor_run_with_live(root, live)
    assert any(o.spec.id == "I2" and o.findings for o in outcomes)
    assert not [e for e in repair_plan.edits if e.check_id in ("I2", "I20")]
    assert "I2" in repair_plan.unrepairable


def test_doctor_does_not_erase_orphan_which_is_running_live(tmp_path):
    schemeshard = fb.schemeshard_backup(
        shards=[fb.SHARDS[0]], next_shard_idx=3, next_path_id=3
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)
    live = LiveCluster(hives={
        fb.HIVE_TABLET_ID: LiveHive(
            hive_id=fb.HIVE_TABLET_ID,
            tablet_ids={s["TabletId"] for s in fb.SHARDS},
            tablet_states={s["TabletId"]: 4 for s in fb.SHARDS},
        )
    })
    _, _, repair_plan = doctor_run_with_live(root, live)
    assert not [e for e in repair_plan.edits if e.check_id == "I2"]


@pytest.mark.parametrize("live", [None, LiveCluster(), LiveCluster(hives={
    fb.HIVE_TABLET_ID: LiveHive(hive_id=fb.HIVE_TABLET_ID, reachable=False)
})])
def test_hive_recovery_trusts_current_schemeshard_without_live_root(tmp_path, live):
    root = build_cluster(tmp_path, schemeshard=fb.schemeshard_backup(
        shards=[fb.SHARDS[0]], next_shard_idx=3, next_path_id=3
    ))
    state, outcomes, _ = doctor_run_with_live(root, live)
    state.authoritative_schemeshard = True
    plan = doctor.plan(state, outcomes)
    assert [(e.op, e.key) for e in plan.edits if e.check_id == "I2"] == [
        ("erase", {"ID": fb.SHARDS[1]["TabletId"]})
    ]
    out = str(tmp_path / "repaired")
    doctor.apply(state, plan, out_dir=out)
    fixed, _ = check_ids(out)
    assert "I2" not in failing(fixed)
    original, _ = check_ids(root)
    assert "I2" in failing(original), "original backup must remain unchanged"


@pytest.mark.parametrize("missing_tablet", [False, True])
def test_hive_recovery_repairs_orphan_and_blocks_unresolved_loss(tmp_path, monkeypatch, missing_tablet):
    from ydb.tests.stress.system_tablet_backup.consistency import hive_recovery
    from ydb.tests.stress.system_tablet_backup.consistency.sources import live as live_source

    shards = [fb.SHARDS[0]]
    if missing_tablet:
        shards.append(dict(fb.SHARDS[1], TabletId=fb.SHARDS[1]["TabletId"] + 100))
    root = build_cluster(tmp_path, schemeshard=fb.schemeshard_backup(
        shards=shards, next_shard_idx=3, next_path_id=4,
    ))

    def peers(endpoint, ids, paths, **kwargs):
        assert fb.HIVE_TABLET_ID not in ids
        return LiveCluster(source=endpoint)

    def unexpected_network(*args, **kwargs):
        pytest.fail("doctor must not request live root Hive or orphan confirmation")

    monkeypatch.setattr(hive_recovery, "read_live", peers)
    monkeypatch.setattr(live_source.urllib.request, "urlopen", unexpected_network)
    out = tmp_path / "doctor"
    rc = hive_recovery.main([
        "--backup-root", root, "--hive-id", str(fb.HIVE_TABLET_ID),
        "--mon-endpoint", "http://unused", "--out", str(out),
    ])
    gate = json.loads((out / "restore-gate.json").read_text())
    if missing_tablet:
        assert rc == 3
        assert not gate["allowed"]
        assert any(b.startswith("I1:") for b in gate["blockers"])
    else:
        assert rc == 0, gate
        assert gate["allowed"]
    assert json.loads((out / "plan.json").read_text())["authoritative_schemeshard"]


def test_i1_ignores_invalid_tablet_id_while_creation_is_in_flight(tmp_path):
    root = build_cluster(tmp_path, schemeshard=fb.schemeshard_backup(
        shards=fb.SHARDS + [{"ShardIdx": 3, "TabletId": (1 << 64) - 1, "PathId": 4}],
        next_shard_idx=4, next_path_id=5,
    ))
    results, _ = check_ids(root)
    assert "I1" not in failing(results)


def test_doctor_erases_restarting_live_orphan(tmp_path):
    schemeshard = fb.schemeshard_backup(
        shards=[fb.SHARDS[0]], next_shard_idx=3, next_path_id=3
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)
    orphan_id = fb.SHARDS[1]["TabletId"]
    live = LiveCluster(hives={
        fb.HIVE_TABLET_ID: LiveHive(
            hive_id=fb.HIVE_TABLET_ID,
            tablet_ids={s["TabletId"] for s in fb.SHARDS},
            tablet_states={s["TabletId"]: 4 for s in fb.SHARDS},
            tablet_restarts={orphan_id: 128},
        )
    })
    _, _, repair_plan = doctor_run_with_live(root, live)
    edits = [e for e in repair_plan.edits if e.check_id == "I2"]
    assert [e.key for e in edits] == [{"ID": orphan_id}]


def test_doctor_raises_hive_known_generation_to_live(tmp_path):
    root = build_cluster(tmp_path)
    tablet_id = fb.SHARDS[0]["TabletId"]
    live = LiveCluster(hives={
        fb.HIVE_TABLET_ID: LiveHive(
            hive_id=fb.HIVE_TABLET_ID,
            tablet_ids={tablet_id},
            tablet_generations={tablet_id: 17},
        )
    })
    state, outcomes, repair_plan = doctor_run_with_live(root, live)
    failing_ids = {
        outcome.spec.id
        for outcome in outcomes
        if any(f.severity > Severity.INFO for f in outcome.findings)
    }
    assert "I20" in failing_ids
    edits = [e for e in repair_plan.edits if e.check_id == "I20"]
    assert [(e.key, e.values) for e in edits] == [
        ({"ID": tablet_id}, {"KnownGeneration": 17})
    ]

    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)
    fixed, _, _ = run_with_live(out, live)
    assert "I20" not in failing(fixed)


def test_doctor_keeps_snapshot_checksums_valid(tmp_path):
    """Repairs land in the changelog, so the restore needs no --skip-checksum.

    Editing the snapshot instead would both break its sha256 and be overwritten
    by the changelog replay.
    """
    root = build_cluster(tmp_path, hive=stale_hive())

    state, _, repair_plan = doctor_run(root)
    assert repair_plan.edits

    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)

    # verify_checksums defaults to True: this would raise if a snapshot changed.
    reloaded, notes = load_state(root=out, needed_tables={"hive": {"State"}})
    assert reloaded.by_type("hive"), notes
    assert not any("checksum" in note for note in notes)

    hive_dump = reloaded.one("hive")
    assert hive_dump.changelog_commits == 1
    state_row = [r for r in hive_dump.rows("State") if r.get("Key") == 0][0]
    assert state_row["Value"] > 1, "the appended commit must win over the snapshot"


def test_doctor_advances_hive_sequences(tmp_path):
    """With delegated ranges present, bumping NextTabletId alone is not enough."""
    risky = fb.uniq(fb.SHARDS[1]["TabletId"])
    hive = fb.hive_backup(
        tablets=[{"ID": fb.SHARDS[0]["TabletId"], "Owner": [fb.SS_TABLET_ID, 1],
                  "State": 200, "TabletType": 2, "KnownGeneration": 1}],
        next_tablet_id=risky,
        channel_groups=[{"Tablet": fb.SHARDS[0]["TabletId"], "Channel": 0, "Generation": 1,
                         "Group": fb.GROUPS[0], "DeletedAtGeneration": 0}],
        sequences=[{"OwnerId": 0, "OwnerIdx": 0, "Begin": risky - 10,
                    "End": risky + 1000, "Next": risky - 5}],
    )
    root = build_cluster(tmp_path, hive=hive)

    _, _, repair_plan = doctor_run(root)
    sequence_edits = [e for e in repair_plan.edits if e.table == "Sequences"]

    assert sequence_edits, "the delegated range still hands out an id in use"
    assert sequence_edits[0].values["Next"] > risky


def test_doctor_repairs_group_id_reuse(tmp_path):
    bsc = fb.bsc_backup(groups=[fb.GROUPS[0]], next_group_id=fb.GROUPS[1])
    root = build_cluster(tmp_path, bsc=bsc)

    state, _, repair_plan = doctor_run(root)
    edits = [e for e in repair_plan.edits if e.check_id == "I5"]

    assert len(edits) == 1
    assert edits[0].tablet_type == "bscontroller"
    assert edits[0].values["NextGroupID"] > fb.GROUPS[1]

    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)

    fixed, _ = check_ids(out)
    assert "I5" not in failing(fixed)
    assert "I3" in failing(fixed), "the missing group itself cannot be repaired offline"


def test_doctor_repairs_shard_idx_reuse_as_a_string(tmp_path):
    """SysParams.Value is Utf8, so the repair has to write a decimal string."""
    schemeshard = fb.schemeshard_backup(
        shards=[fb.SHARDS[0]], next_shard_idx=2, next_path_id=3,
        paths=[{"Id": 1, "ParentId": 0, "Name": "Root", "PathType": 1, "StepDropped": 0},
               {"Id": 2, "ParentId": 1, "Name": "t_1", "PathType": 2, "StepDropped": 0}],
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)

    state, _, repair_plan = doctor_run(root)
    edits = [e for e in repair_plan.edits if e.check_id == "I6"]

    assert len(edits) == 1
    assert isinstance(edits[0].values["Value"], str), "Utf8 column must stay a string"
    assert int(edits[0].values["Value"]) > 2

    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)

    fixed, _ = check_ids(out)
    assert "I6" not in failing(fixed)


def _changelog_of(root, tablet_type, tablet_id):
    path = glob.glob(os.path.join(root, tablet_type, str(tablet_id), "backup_*", "changelog.json"))[0]
    with open(path, "rb") as handle:
        return path, handle.read()


def test_doctor_continues_the_changelog_hash_chain(tmp_path):
    """Every changelog line carries prev_sha256 and the restore verifies it.

    Learned the hard way: a Dry Run on a real cluster rejected doctor's commit
    with "Changelog line is missing 'prev_sha256' field".  The checker had not
    noticed because it ignores unknown fields.
    """
    hive = stale_hive()
    hive.commit(2, [{"table": "Tablet", "op": "upsert", "ID": 999, "Owner": [fb.SS_TABLET_ID, 9]}])
    root = build_cluster(tmp_path, hive=hive)

    state, _, repair_plan = doctor_run(root)
    assert repair_plan.edits

    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)

    _, body = _changelog_of(out, "hive", fb.HIVE_TABLET_ID)
    lines = [line for line in body.split(b"\n") if line]

    prefix = b""
    for line in lines:
        record = json.loads(line.decode("utf-8", errors="surrogateescape"))
        assert "prev_sha256" in record, "restore rejects a line without the chain field"
        assert record["prev_sha256"] == hashlib.sha256(prefix).hexdigest(), \
            "the chain must stay verifiable through the appended commit"
        prefix += line + b"\n"


def test_doctor_drops_an_unparsable_tail_before_appending(tmp_path):
    """A commit appended after a torn record would never be replayed.

    Replay -- the checker's and TTxUploadChangelog's alike -- stops at the first
    line it cannot parse, so the repair has to become the new last line.
    """
    hive = stale_hive()
    hive.commit(2, [{"table": "Tablet", "op": "upsert", "ID": 999, "Owner": [fb.SS_TABLET_ID, 9]}])
    root = build_cluster(tmp_path, hive=hive)

    # Tear the last record the way a crash mid-write would.
    path, body = _changelog_of(root, "hive", fb.HIVE_TABLET_ID)
    with open(path, "wb") as handle:
        handle.write(body[: len(body) - 30])

    state, _, repair_plan = doctor_run(root)
    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)

    assert repair_plan.discarded_tail.get("hive") == 1

    # The repaired copy replays cleanly and the repair actually took effect.
    fixed, outcomes, _ = run_with_live(out, None)
    assert "I4" not in failing(fixed)
    dump = [o for o in outcomes if o.spec.id == "I13"][0]
    assert not dump.findings, "the torn record is gone, so nothing is truncated any more"


def test_doctor_refuses_to_overwrite_an_existing_output(tmp_path):
    root = build_cluster(tmp_path, hive=stale_hive())
    state, _, repair_plan = doctor_run(root)

    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)

    with pytest.raises(ValueError):
        doctor.apply(state, repair_plan, out_dir=out)


def test_doctor_in_place_modifies_the_original(tmp_path):
    root = build_cluster(tmp_path, hive=stale_hive())
    state, _, repair_plan = doctor_run(root)

    doctor.apply(state, repair_plan, in_place=True)

    fixed, _ = check_ids(root)
    assert "I4" not in failing(fixed)


# --------------------------------------------------------------------------
# Tenant tablets, which have no backups at all
# --------------------------------------------------------------------------


def with_tenant(**kwargs):
    """Root SchemeShard that owns one database (SchemeShard/Hive/coordinator/mediator)."""
    return fb.schemeshard_backup(
        shards=fb.SHARDS + fb.TENANT_SHARDS,
        next_shard_idx=14,
        next_path_id=11,
        paths=[
            {"Id": 1, "ParentId": 0, "Name": "Root", "PathType": 1, "StepDropped": 0},
            {"Id": 2, "ParentId": 1, "Name": "t_1", "PathType": 2, "StepDropped": 0},
            {"Id": 3, "ParentId": 1, "Name": "t_2", "PathType": 2, "StepDropped": 0},
            {"Id": fb.TENANT_PATH_ID, "ParentId": 1, "Name": fb.TENANT_NAME,
             "PathType": 3, "StepDropped": 0},
        ],
        subdomains=[{"PathId": fb.TENANT_PATH_ID, "SharedHiveId": 0}],
        subdomain_shards=[{"PathId": fb.TENANT_PATH_ID, "ShardIdx": sh["ShardIdx"]}
                          for sh in fb.TENANT_SHARDS],
        **kwargs
    )


def hive_with_tenant(next_tablet_id=None, **kwargs):
    """Root Hive that owns the root shards plus the database's tablets."""
    tablets = [
        {"ID": sh["TabletId"], "Owner": [fb.SS_TABLET_ID, sh["ShardIdx"]], "State": 200,
         "TabletType": sh["TabletType"], "KnownGeneration": 1}
        for sh in fb.SHARDS + fb.TENANT_SHARDS
    ]
    channels = [
        {"Tablet": sh["TabletId"], "Channel": 0, "Generation": 1,
         "Group": fb.GROUPS[0], "DeletedAtGeneration": 0}
        for sh in fb.SHARDS + fb.TENANT_SHARDS
    ]
    if next_tablet_id is None:
        next_tablet_id = fb.uniq(fb.TENANT_SHARDS[-1]["TabletId"]) + 1
    return fb.hive_backup(tablets=tablets, channel_groups=channels,
                          next_tablet_id=next_tablet_id, **kwargs)


def live_tenant_hive(tablet_ids):
    """A live reading of the tenant Hive, standing in for /viewer/hiveinfo."""
    return LiveCluster(
        hives={fb.TENANT_HIVE_TABLET_ID: LiveHive(
            hive_id=fb.TENANT_HIVE_TABLET_ID, tablet_ids=set(tablet_ids))},
        source="test",
    )


def run_with_live(root, live, **kwargs):
    specs = select_checks()
    needed = {k: v for k, v in required_tables(specs).items() if k not in ("ledger", "live")}
    state, _ = load_state(root=root, needed_tables=needed, **kwargs)
    state.live = live
    outcomes = run_checks(state, specs)
    return {
        o.spec.id: [f for f in o.findings if f.severity > Severity.INFO] for o in outcomes
    }, outcomes, state


def test_tenant_topology_is_read_from_the_root_schemeshard(tmp_path):
    """The root SchemeShard backup alone names every database tablet."""
    root = build_cluster(tmp_path, schemeshard=with_tenant(), hive=hive_with_tenant())
    _, outcomes, state = run_with_live(root, None)

    from ydb.tests.stress.system_tablet_backup.consistency.views import schemeshard_views
    subdomains = schemeshard_views(state)[0].subdomains()

    assert len(subdomains) == 1
    db = subdomains[0]
    assert db.name == fb.TENANT_NAME
    assert db.scheme_shard_id == fb.TENANT_SS_TABLET_ID
    assert db.hive_id == fb.TENANT_HIVE_TABLET_ID
    assert len(db.coordinators) == 1 and len(db.mediators) == 1


def test_root_hive_reusing_a_tenant_tablet_id_is_critical(tmp_path):
    """The worst case: the range was delegated to a tenant Hive and forgotten.

    The tenant's tablets never appear in the root Hive's table, so nothing
    offline can see the collision -- only the live reading can.
    """
    tenant_owned = 72075186224038000  # minted by the tenant Hive from its range
    hive = hive_with_tenant(next_tablet_id=fb.uniq(tenant_owned))
    root = build_cluster(tmp_path, schemeshard=with_tenant(), hive=hive)

    without_live, _, _ = run_with_live(root, None)
    assert "I15" not in failing(without_live), "cannot be seen without reading the tenant"

    results, outcomes, _ = run_with_live(root, live_tenant_hive([tenant_owned]))
    i15 = [o for o in outcomes if o.spec.id == "I15"][0]

    assert not i15.skipped_reason
    assert "I15" in failing(results)
    finding = results["I15"][0]
    assert finding.severity is Severity.CRITICAL
    assert finding.details["tablet_id"] == tenant_owned
    assert finding.details["tenant_hive"] == fb.TENANT_HIVE_TABLET_ID


def test_tenant_tablet_ids_below_the_allocator_are_fine(tmp_path):
    tenant_owned = 72075186224037950
    hive = hive_with_tenant(next_tablet_id=fb.uniq(tenant_owned) + 1000)
    root = build_cluster(tmp_path, schemeshard=with_tenant(), hive=hive)

    results, _, _ = run_with_live(root, live_tenant_hive([tenant_owned]))
    assert "I15" not in failing(results)


def test_unreachable_tenant_hive_is_reported_not_ignored(tmp_path):
    root = build_cluster(tmp_path, schemeshard=with_tenant(), hive=hive_with_tenant())
    live = LiveCluster(hives={fb.TENANT_HIVE_TABLET_ID: LiveHive(
        hive_id=fb.TENANT_HIVE_TABLET_ID, reachable=False, error="connection refused")})

    results, _, _ = run_with_live(root, live)
    assert "I15" in failing(results), "an unread Hive is a gap in coverage, not a pass"
    assert any("could not be read" in f.message for f in results["I15"])


def test_database_losing_its_hive_cannot_boot(tmp_path):
    """Stale root Hive: it forgot the tablet that *is* the database's Hive.

    Measured against a live cluster: a database's SchemeShard, coordinators and
    mediators run under the database's own Hive, so the root Hive holds exactly
    one of its tablets -- that Hive.  Losing it is what makes the database
    unstartable; losing the others is not something the root Hive can even know.
    """
    kept = [sh for sh in fb.SHARDS + fb.TENANT_SHARDS
            if sh["TabletId"] != fb.TENANT_HIVE_TABLET_ID]
    hive = fb.hive_backup(
        tablets=[{"ID": sh["TabletId"], "Owner": [fb.SS_TABLET_ID, sh["ShardIdx"]],
                  "State": 200, "TabletType": sh["TabletType"], "KnownGeneration": 1}
                 for sh in kept],
        channel_groups=[{"Tablet": sh["TabletId"], "Channel": 0, "Generation": 1,
                         "Group": fb.GROUPS[0], "DeletedAtGeneration": 0} for sh in kept],
        next_tablet_id=fb.uniq(fb.TENANT_SHARDS[-1]["TabletId"]) + 1,
    )
    root = build_cluster(tmp_path, schemeshard=with_tenant(), hive=hive)

    results, _, _ = run_with_live(root, None)
    assert "I16" in failing(results)
    finding = results["I16"][0]
    assert finding.severity is Severity.CRITICAL
    assert "nothing can start the database" in finding.message
    assert finding.details["tenant_hive"] == fb.TENANT_HIVE_TABLET_ID


def test_tenant_shards_absent_from_the_root_hive_are_not_losses(tmp_path):
    """Regression from the live cluster: I1 used to report them as lost.

    Only the database's own Hive is a root-Hive tablet; its SchemeShard,
    coordinators and mediators are not, and never were.
    """
    only_tenant_hive = [sh for sh in fb.SHARDS
                        if True] + [fb.TENANT_SHARDS[1]]  # SHARDS + the tenant Hive
    hive = fb.hive_backup(
        tablets=[{"ID": sh["TabletId"], "Owner": [fb.SS_TABLET_ID, sh["ShardIdx"]],
                  "State": 200, "TabletType": sh["TabletType"], "KnownGeneration": 1}
                 for sh in only_tenant_hive],
        channel_groups=[{"Tablet": sh["TabletId"], "Channel": 0, "Generation": 1,
                         "Group": fb.GROUPS[0], "DeletedAtGeneration": 0}
                        for sh in only_tenant_hive],
        next_tablet_id=fb.uniq(fb.TENANT_SHARDS[-1]["TabletId"]) + 1,
    )
    root = build_cluster(tmp_path, schemeshard=with_tenant(), hive=hive)

    # Without a live reading their placement is unknown: a database's system
    # tablets may sit in the root Hive or in the database's own Hive.  That is
    # a coverage gap, not a loss.
    results, outcomes, _ = run_with_live(root, None)
    i1 = [o for o in outcomes if o.spec.id == "I1"][0]

    assert not any(f.severity >= Severity.ERROR for f in i1.findings), \
        "absence from the root Hive alone is not proof of loss"
    assert any("could not be verified" in f.message for f in i1.findings)

    # With the tenant Hive read and holding them, nothing is reported at all.
    tenant_tablets = [sh["TabletId"] for sh in fb.TENANT_SHARDS
                      if sh["TabletId"] != fb.TENANT_HIVE_TABLET_ID]
    results, outcomes, _ = run_with_live(root, live_tenant_hive(tenant_tablets))
    i1 = [o for o in outcomes if o.spec.id == "I1"][0]

    assert "I1" not in failing(results)
    assert any("present in a live tenant Hive" in f.message for f in i1.findings)


def test_tenant_shard_missing_everywhere_is_a_real_loss(tmp_path):
    """With live data proving no Hive has it, absence becomes a finding."""
    only_tenant_hive = list(fb.SHARDS) + [fb.TENANT_SHARDS[1]]
    hive = fb.hive_backup(
        tablets=[{"ID": sh["TabletId"], "Owner": [fb.SS_TABLET_ID, sh["ShardIdx"]],
                  "State": 200, "TabletType": sh["TabletType"], "KnownGeneration": 1}
                 for sh in only_tenant_hive],
        channel_groups=[{"Tablet": sh["TabletId"], "Channel": 0, "Generation": 1,
                         "Group": fb.GROUPS[0], "DeletedAtGeneration": 0}
                        for sh in only_tenant_hive],
        next_tablet_id=fb.uniq(fb.TENANT_SHARDS[-1]["TabletId"]) + 1,
    )
    root = build_cluster(tmp_path, schemeshard=with_tenant(), hive=hive)

    # The tenant Hive is up but holds only one of the database's tablets.
    results, _, _ = run_with_live(root, live_tenant_hive([fb.TENANT_SS_TABLET_ID]))

    assert "I1" in failing(results), "no Hive has them, so they are lost"


def test_database_forgotten_by_the_root_schemeshard_is_orphaned(tmp_path):
    """Stale root SchemeShard: the database keeps running, unreferenced."""
    root = build_cluster(
        tmp_path,
        schemeshard=fb.schemeshard_backup(next_shard_idx=14),  # no subdomain at all
        hive=hive_with_tenant(),
    )
    results, _, _ = run_with_live(root, None)

    assert "I17" in failing(results)
    orphaned = {f.details.get("tablet_id") for f in results["I17"]}
    assert fb.TENANT_SS_TABLET_ID in orphaned
    assert fb.TENANT_HIVE_TABLET_ID in orphaned


def test_missing_live_reading_is_flagged(tmp_path):
    """I18 must say the tenant side was not observed at all."""
    root = build_cluster(tmp_path, schemeshard=with_tenant(), hive=hive_with_tenant())
    results, _, _ = run_with_live(root, None)

    assert "I18" in failing(results)
    assert "no backups" in results["I18"][0].message


def test_doctor_gap_covers_live_tenant_tablet_ids(tmp_path):
    """The repair must clear tenant ids too, not just what the backups show."""
    tenant_owned = 72075186224038000
    hive = hive_with_tenant(next_tablet_id=fb.uniq(tenant_owned))
    root = build_cluster(tmp_path, schemeshard=with_tenant(), hive=hive)

    specs = select_checks()
    needed = {k: set(v) for k, v in required_tables(specs).items()
              if k not in ("ledger", "live")}
    for slice_, tables in doctor.REQUIRED_TABLES.items():
        if slice_ in needed:
            needed[slice_] |= set(tables)
    state, _ = load_state(root=root, needed_tables=needed)
    state.live = live_tenant_hive([tenant_owned])
    outcomes = run_checks(state, specs)

    repair_plan = doctor.plan(state, outcomes)
    edits = [e for e in repair_plan.edits if e.table == "State" and e.tablet_type == "hive"]

    assert edits, "doctor must raise the allocator"
    assert edits[0].values["Value"] > fb.uniq(tenant_owned)

    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)

    fixed, _, _ = run_with_live(out, live_tenant_hive([tenant_owned]))
    assert "I15" not in failing(fixed)
    assert "I4" not in failing(fixed)


# --------------------------------------------------------------------------
# I19: what the restored SchemeShard does next
# --------------------------------------------------------------------------

VOLUME_PATH_ID = 4
VOLUME_PATH = "/Root/vol-42"

# A BlockStore volume hanging off the root, next to the two tables.
PATHS_WITH_VOLUME = [
    {"Id": 1, "ParentId": 0, "Name": "Root", "PathType": 1, "StepDropped": 0},
    {"Id": 2, "ParentId": 1, "Name": "t_1", "PathType": 2, "StepDropped": 0},
    {"Id": 3, "ParentId": 1, "Name": "t_2", "PathType": 2, "StepDropped": 0},
    {"Id": VOLUME_PATH_ID, "ParentId": 1, "Name": "vol-42", "PathType": 8, "StepDropped": 0},
]


def with_volume(version, alter_version=None, **kwargs):
    alters = []
    if alter_version is not None:
        alters = [{"PathId": VOLUME_PATH_ID, "AlterVersion": alter_version, "PartitionCount": 1}]
    return fb.schemeshard_backup(
        paths=PATHS_WITH_VOLUME,
        volumes=[{"PathId": VOLUME_PATH_ID, "AlterVersion": version}],
        volume_alters=alters,
        **kwargs
    )


def live_volume(version, path=VOLUME_PATH, kind="blockstore", **kwargs):
    return LiveCluster(
        paths={path: LivePath(path=path, kind=kind, version=version, **kwargs)},
        source="test",
    )


def test_operation_surviving_the_replay_names_its_shards(tmp_path):
    """Bug #9: the backup caught TxCreateTable at ConfigureParts.

    The datashards finished that operation long ago and erased the txid, so the
    resumed proposal trips Y_ENSURE(!GetUserTables().contains(tableId)) and the
    shards restart forever.  The finding has to name them.
    """
    schemeshard = fb.schemeshard_backup(
        txs_in_flight=[
            {"TxId": 281474976721557, "TxPartId": 0, "TxType": 2, "State": 3,
             "TargetPathId": 2, "MinStep": 100, "PlanStep": 101}
        ],
        tx_shards=[
            {"TxId": 281474976721557, "TxPartId": 0, "ShardIdx": 1, "Operation": 0},
            {"TxId": 281474976721557, "TxPartId": 0, "ShardIdx": 2, "Operation": 0},
        ],
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)
    results, _ = check_ids(root)

    assert "I19" in failing(results)
    finding = results["I19"][0]
    assert finding.severity == Severity.CRITICAL
    assert finding.details["tx_type"] == "TxCreateTable"
    assert finding.details["state"] == "ConfigureParts"
    assert finding.details["path"] == "/Root/t_1"
    assert finding.details["tablet_ids"] == [s["TabletId"] for s in fb.SHARDS]


def test_finished_operation_row_is_not_a_replay(tmp_path):
    """A Done row is a leak, not a crash loop: nothing is re-sent from it."""
    schemeshard = fb.schemeshard_backup(
        txs_in_flight=[
            {"TxId": 281474976721558, "TxPartId": 0, "TxType": 5, "State": 240,
             "TargetPathId": 2}
        ],
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)
    results, _ = check_ids(root)

    assert "I19" in failing(results)
    assert [f.severity for f in results["I19"]] == [Severity.WARNING]


def test_volume_operation_warns_about_the_node_abort(tmp_path):
    """TxAlterBlockStoreVolume at ConfigureParts takes down the node, not a shard."""
    schemeshard = with_volume(
        5,
        txs_in_flight=[
            {"TxId": 281474976721559, "TxPartId": 0, "TxType": 17, "State": 3,
             "TargetPathId": VOLUME_PATH_ID}
        ],
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)
    results, _ = check_ids(root)

    resumed = [f for f in results["I19"] if f.details.get("tx") == "281474976721559:0"]
    assert resumed and resumed[0].severity == Severity.CRITICAL
    assert "ERROR_BAD_VERSION" in resumed[0].message
    assert "aborts the SchemeShard node" in resumed[0].message


def test_rolled_back_volume_version_is_critical(tmp_path):
    """The volume refuses a config older than the one it has applied.

    SchemeShard asserts on that refusal instead of handling it, so the restore
    turns into a SchemeShard restart loop.
    """
    root = build_cluster(tmp_path, schemeshard=with_volume(5))
    results, _, _ = run_with_live(root, live_volume(7))

    assert "I19" in failing(results)
    finding = results["I19"][0]
    assert finding.severity == Severity.CRITICAL
    assert finding.details == {
        "path": VOLUME_PATH,
        "path_id": VOLUME_PATH_ID,
        "kind": "blockstore",
        "backup_version": 5,
        "backup_pending_version": None,
        "live_version": 7,
    }


def test_pending_alter_version_is_the_one_compared(tmp_path):
    """An alter in flight is what ConfigureParts will send, so judge that."""
    root = build_cluster(tmp_path, schemeshard=with_volume(5, alter_version=6))

    behind, _, _ = run_with_live(root, live_volume(7))
    assert "I19" in failing(behind)
    assert behind["I19"][0].details["backup_pending_version"] == 6

    # The same backup against a volume that never got the alter is fine: 6 is
    # exactly the next version the volume expects.
    ahead, _, _ = run_with_live(root, live_volume(5))
    assert [f for f in ahead["I19"] if f.severity >= Severity.CRITICAL] == []


def test_volume_at_the_same_version_is_clean(tmp_path):
    """Equality is safe: the volume answers OK to a config it already has."""
    root = build_cluster(tmp_path, schemeshard=with_volume(5))
    results, _, _ = run_with_live(root, live_volume(5))

    assert "I19" not in failing(results)


def test_unverified_volume_is_reported_as_a_gap(tmp_path):
    """Without --mon-endpoint the rollback cannot be seen, and must be said so."""
    root = build_cluster(tmp_path, schemeshard=with_volume(5))
    results, _ = check_ids(root)

    assert "I19" in failing(results)
    assert [f.severity for f in results["I19"]] == [Severity.WARNING]
    assert "--mon-endpoint" in results["I19"][0].message


def test_dropped_volume_is_not_compared(tmp_path):
    """A dropped path has nothing live behind it to be judged against."""
    paths = [dict(p) for p in PATHS_WITH_VOLUME]
    paths[-1]["StepDropped"] = 42
    schemeshard = fb.schemeshard_backup(
        paths=paths, volumes=[{"PathId": VOLUME_PATH_ID, "AlterVersion": 5}]
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)
    results, _ = check_ids(root)

    assert "I19" not in failing(results)


# --------------------------------------------------------------------------
# I19 repairs
# --------------------------------------------------------------------------

CREATE_TX = 281474976721557


def doctor_run_with_live(root, live):
    """doctor_run, but against a live cluster: the I19 repairs need one."""
    specs = select_checks()
    needed = {k: set(v) for k, v in required_tables(specs).items()
              if k not in ("ledger", "live")}
    for slice_, tables in doctor.REQUIRED_TABLES.items():
        if slice_ in needed:
            needed[slice_] |= set(tables)

    state, _ = load_state(root=root, needed_tables=needed)
    state.live = live
    outcomes = run_checks(state, specs)
    return state, outcomes, doctor.plan(state, outcomes)


def live_with(*paths):
    return LiveCluster(paths={p.path: p for p in paths}, source="test")


def live_created(path, path_id, tx_id, owner_id=fb.SS_TABLET_ID):
    """A live path the given transaction created and finished creating."""
    return LivePath(path=path, kind="path", owner_id=owner_id, path_id=path_id,
                    create_tx_id=tx_id, create_finished=True)


def creating_table(state=3, parts=((0, 2),), shards=(1, 2)):
    """A backup that caught TxCreateTable mid-flight, as bug #9 did."""
    return fb.schemeshard_backup(
        txs_in_flight=[
            {"TxId": CREATE_TX, "TxPartId": part, "TxType": 2, "State": state,
             "TargetPathId": path_id, "MinStep": 100, "PlanStep": 101}
            for part, path_id in parts
        ],
        tx_shards=[
            {"TxId": CREATE_TX, "TxPartId": parts[0][0], "ShardIdx": idx, "Operation": 0}
            for idx in shards
        ],
    )


def test_doctor_raises_a_rolled_back_version_to_the_live_one(tmp_path):
    """Bug #10: the target is the live version exactly, never one above it.

    An equal version is acknowledged without being applied, so the operation
    completes and the stale body in the backup never reaches the volume.
    """
    root = build_cluster(tmp_path, schemeshard=with_volume(5))
    state, _, repair_plan = doctor_run_with_live(root, live_volume(7))

    edits = [e for e in repair_plan.edits if e.check_id == "I19"]
    assert len(edits) == 1, [e.describe() for e in repair_plan.edits]
    assert edits[0].table == "BlockStoreVolumes"
    assert edits[0].key == {"PathId": VOLUME_PATH_ID}
    assert edits[0].values == {"AlterVersion": 7}
    assert edits[0].op == "upsert"

    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)

    fixed, _, _ = run_with_live(out, live_volume(7))
    assert "I19" not in failing(fixed)


def test_doctor_keeps_the_pending_alter_one_above_the_committed_version(tmp_path):
    """FinishAlter does ++Version and then asserts it equals the alter's.

    So the pair has to move together: raising only the alter row would abort the
    node on the very transaction the repair is meant to let through.
    """
    root = build_cluster(tmp_path, schemeshard=with_volume(2, alter_version=3))
    state, _, repair_plan = doctor_run_with_live(root, live_volume(7))

    by_table = {e.table: e for e in repair_plan.edits if e.check_id == "I19"}
    assert set(by_table) == {"BlockStoreVolumes", "BlockStoreVolumeAlters"}
    assert by_table["BlockStoreVolumeAlters"].values == {"AlterVersion": 7}
    assert by_table["BlockStoreVolumes"].values == {"AlterVersion": 6}

    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)

    fixed, _, _ = run_with_live(out, live_volume(7))
    assert "I19" not in failing(fixed)


def test_doctor_drops_an_operation_the_live_cluster_has_finished(tmp_path):
    """Bug #9: the live path names the tx that created it, and says it is done.

    That is the whole proof -- the shards cannot still be waiting for an
    operation whose result is already serving traffic -- so the operation is
    removed the way SchemeShard removes its own: row plus every shard row.
    """
    root = build_cluster(tmp_path, schemeshard=creating_table())
    live = live_with(live_created("/Root/t_1", 2, CREATE_TX))
    state, _, repair_plan = doctor_run_with_live(root, live)

    edits = [e for e in repair_plan.edits if e.check_id == "I19"]
    assert all(e.op == "erase" for e in edits), [e.describe() for e in edits]
    assert sorted(e.table for e in edits) == ["TxInFlightV2", "TxShardsV2", "TxShardsV2"]

    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)

    fixed, _, state_after = run_with_live(out, live)
    assert "I19" not in failing(fixed)
    # A shard row outliving its operation aborts SchemeShard on boot, so the
    # erase has to be complete rather than merely enough to silence the check.
    ss_dump = state_after.one("scheme_shard")
    assert ss_dump.rows("TxShardsV2") == []


def test_doctor_leaves_an_operation_the_live_cluster_cannot_vouch_for(tmp_path):
    """A path recreated by a later transaction proves nothing about this one.

    The shards behind it are different shards; dropping the operation would be a
    guess, so doctor says what it cannot do instead.
    """
    root = build_cluster(tmp_path, schemeshard=creating_table())
    live = live_with(live_created("/Root/t_1", 2, CREATE_TX + 100))
    _, _, repair_plan = doctor_run_with_live(root, live)

    assert [e for e in repair_plan.edits if e.check_id == "I19"] == []
    assert repair_plan.unrepairable.get("I19")
    assert "TolerateOrphanedPaths" in doctor.GUIDANCE["I19"]


def test_doctor_drops_every_part_of_an_operation_or_none(tmp_path):
    """Boot asserts the parts of an operation are numbered from zero, no gaps.

    Y_ABORT_UNLESS(subTxId == operation->Parts.size()) in schemeshard__init.cpp,
    so a half-erased operation is worse than the one it replaced.
    """
    schemeshard = creating_table(parts=((0, 2), (1, 3)))
    root = build_cluster(tmp_path, schemeshard=schemeshard)

    # Only the first part's path can be vouched for: nothing may be erased.
    partial = live_with(live_created("/Root/t_1", 2, CREATE_TX))
    _, _, plan_partial = doctor_run_with_live(root, partial)
    assert [e for e in plan_partial.edits if e.check_id == "I19"] == []

    both = live_with(
        live_created("/Root/t_1", 2, CREATE_TX),
        live_created("/Root/t_2", 3, CREATE_TX),
    )
    state, _, plan_both = doctor_run_with_live(root, both)
    erased = [e.key for e in plan_both.edits if e.table == "TxInFlightV2"]
    assert erased == [{"TxId": CREATE_TX, "TxPartId": 0}, {"TxId": CREATE_TX, "TxPartId": 1}]

    out = str(tmp_path / "repaired")
    doctor.apply(state, plan_both, out_dir=out)
    fixed, _, _ = run_with_live(out, both)
    assert "I19" not in failing(fixed)


def test_doctor_leaves_a_versioned_operation_to_the_version_repair(tmp_path):
    """A volume alter is fixed by realigning the version, not by dropping it.

    Dropping it would strand the alter row, and SchemeShard refuses a new alter
    while one is pending -- the volume would never be alterable again.
    """
    schemeshard = with_volume(
        5,
        txs_in_flight=[
            {"TxId": CREATE_TX, "TxPartId": 0, "TxType": 17, "State": 3,
             "TargetPathId": VOLUME_PATH_ID}
        ],
    )
    root = build_cluster(tmp_path, schemeshard=schemeshard)
    live = live_with(
        LivePath(path=VOLUME_PATH, kind="blockstore", version=7, owner_id=fb.SS_TABLET_ID,
                 path_id=VOLUME_PATH_ID, create_tx_id=CREATE_TX, create_finished=True)
    )
    state, _, repair_plan = doctor_run_with_live(root, live)

    edits = [e for e in repair_plan.edits if e.check_id == "I19"]
    assert [e.op for e in edits] == ["upsert"]
    assert edits[0].values == {"AlterVersion": 7}

    out = str(tmp_path / "repaired")
    doctor.apply(state, repair_plan, out_dir=out)
    reloaded, _, _ = run_with_live(out, live)
    # The operation stays and now completes on its own; only the crash is gone.
    assert [f.details.get("tx") for f in reloaded["I19"]] == ["%d:0" % CREATE_TX]
    assert all(f.severity == Severity.CRITICAL for f in reloaded["I19"])


def test_doctor_offers_guidance_for_i19_without_a_live_cluster(tmp_path):
    """Nothing offline can tell whether the shards are already done."""
    root = build_cluster(tmp_path, schemeshard=creating_table())
    _, _, repair_plan = doctor_run(root)

    assert [e for e in repair_plan.edits if e.check_id == "I19"] == []
    assert repair_plan.unrepairable.get("I19")
    assert "--mon-endpoint" in doctor.GUIDANCE["I19"]


def test_live_reader_parses_a_viewer_hiveinfo_response(tmp_path):
    """Exercises the real HTTP path and the viewer's field names.

    /viewer/json/hiveinfo renders TEvResponseHiveInfo, where a tablet carries
    TabletID and TabletOwner{Owner, OwnerIdx}; with ui64=false the ids arrive as
    strings, which is why the reader parses everything through int().
    """
    import http.server
    import threading

    from ydb.tests.stress.system_tablet_backup.consistency.sources import live as live_source

    body = json.dumps({
        "Tablets": [
            {"TabletID": "72075186224038000", "TabletType": "DataShard",
             "TabletOwner": {"Owner": "72075186224037900", "OwnerIdx": "5"}},
            {"TabletID": "72075186224038001",
             "TabletOwner": {"Owner": "72075186224037900", "OwnerIdx": "6"}},
            {"TabletType": "DataShard"},  # no id: must be skipped, not crash
        ]
    }).encode()

    seen = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            seen["path"] = self.path
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        endpoint = "http://127.0.0.1:%d" % server.server_address[1]
        hive = live_source.read_hive(endpoint, 72075186224037901, timeout=10)
    finally:
        server.shutdown()
        server.server_close()

    assert hive.reachable, hive.error
    assert hive.tablet_ids == {72075186224038000, 72075186224038001}
    assert (72075186224037900, 5) in hive.owners
    assert "hive_id=72075186224037901" in seen["path"]
    assert "/viewer/json/hiveinfo" in seen["path"]


def test_live_reader_parses_a_viewer_describe_response(tmp_path):
    """The version comes out of the same field SchemeShard sends to the tablet.

    /viewer/json/describe renders TEvDescribeSchemeResult, where a volume
    carries PathDescription.BlockStoreVolumeDescription.AlterVersion.
    """
    import http.server
    import threading

    from ydb.tests.stress.system_tablet_backup.consistency.sources import live as live_source

    body = json.dumps({
        "Status": "StatusSuccess",
        "Path": "/Root/vol-42",
        "PathDescription": {
            "Self": {"Name": "vol-42", "PathId": "4"},
            "BlockStoreVolumeDescription": {"Name": "vol-42", "AlterVersion": "7"},
        },
    }).encode()

    seen = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            seen["path"] = self.path
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        endpoint = "http://127.0.0.1:%d" % server.server_address[1]
        volume = live_source.read_path(endpoint, "/Root/vol-42", "blockstore", timeout=10)
    finally:
        server.shutdown()
        server.server_close()

    assert volume.reachable and volume.exists, volume.error
    assert volume.version == 7
    assert "/viewer/json/describe" in seen["path"]
    assert "path=%2FRoot%2Fvol-42" in seen["path"]


def test_live_reader_reports_a_path_that_is_gone(tmp_path):
    """A missing path is answered, not raised: the caller decides what it means."""
    import http.server
    import threading

    from ydb.tests.stress.system_tablet_backup.consistency.sources import live as live_source

    body = json.dumps({
        "Status": "StatusPathDoesNotExist",
        "Reason": "Path not found",
    }).encode()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        endpoint = "http://127.0.0.1:%d" % server.server_address[1]
        volume = live_source.read_path(endpoint, "/Root/vol-42", "blockstore", timeout=10)
    finally:
        server.shutdown()
        server.server_close()

    assert volume.reachable
    assert not volume.exists
    assert "StatusPathDoesNotExist" in volume.error


def test_live_reader_records_an_unreachable_hive(tmp_path):
    """A closed port must produce a recorded gap, never an exception."""
    from ydb.tests.stress.system_tablet_backup.consistency.sources import live as live_source

    # Port 1 on loopback is reliably closed.
    hive = live_source.read_hive("http://127.0.0.1:1", 42, timeout=2)

    assert not hive.reachable
    assert hive.error
    assert hive.tablet_ids == set()


def test_cli_clean_report_lists_sources_and_short_summary(tmp_path, monkeypatch, capsys):
    from ydb.tests.stress.system_tablet_backup.consistency import __main__ as cli

    root = build_cluster(tmp_path)
    endpoint = "http://cluster:8765"
    monkeypatch.setattr(cli, "read_live", lambda *args, **kwargs: LiveCluster(
        source=endpoint, hives={
            fb.HIVE_TABLET_ID: LiveHive(hive_id=fb.HIVE_TABLET_ID),
            fb.TENANT_HIVE_TABLET_ID: LiveHive(hive_id=fb.TENANT_HIVE_TABLET_ID),
        }, paths={"/Root/table": LivePath(path="/Root/table", kind="path")},
    ))
    report_path = str(tmp_path / "report.json")
    assert cli.main(["--backup-root", root, "--mon-endpoint", endpoint, "--json", report_path]) == 0

    output = capsys.readouterr().out
    with open(report_path) as stream:
        report = json.load(stream)
    names = {"hive": "Hive", "scheme_shard": "SchemeShard", "bscontroller": "BSController"}
    expected = [
        "%s data successfully loaded from %s" % (names[dump["tablet_type"]], dump["source"])
        for dump in report["state"]["tablets"]
    ]
    expected += [
        "Live data successfully loaded from %s" % endpoint,
        "", "20 checks: 0 critical, 0 error, 0 warning",
    ]
    assert output.splitlines() == expected
    # Optional checks stay visible in the machine-readable report.
    assert report["summary"]["skipped"] == 3
    assert any("read 1 live Hive(s)" in note for note in report["notes"])
    assert {c["id"] for c in report["checks"] if c["skipped_reason"]} == {"I8", "I12", "I14"}


@pytest.mark.parametrize("partial", [False, True])
def test_compact_report_keeps_problems_and_failed_sources_visible(partial):
    from ydb.tests.stress.system_tablet_backup.consistency.model import ClusterState, critical, warning
    from ydb.tests.stress.system_tablet_backup.consistency.registry import CheckOutcome, CheckSpec
    from ydb.tests.stress.system_tablet_backup.consistency.report import render_text

    state = ClusterState(live=LiveCluster(source="http://cluster:8765", hives={
        42: LiveHive(hive_id=42, reachable=False, error="connection refused"),
    }, paths={
        "/Root/table": LivePath(path="/Root/table", kind="path", reachable=False, error="timeout"),
    }))
    if partial:
        state.live.hives[43] = LiveHive(hive_id=43)
    outcomes = [
        CheckOutcome(CheckSpec("I4", "ID reuse", lambda state: ()), findings=[critical("ID 123 will be reused")]),
        CheckOutcome(CheckSpec("I11", "Freshness", lambda state: ()), findings=[warning("stale backup")]),
        CheckOutcome(CheckSpec("I3", "Groups", lambda state: ()), failed_reason="ValueError: bad group"),
    ]
    text = render_text(state, outcomes, notes=["/backup/hive: checksum mismatch"])
    assert "successfully loaded" not in text
    live_lines = [line for line in text.splitlines() if line.startswith("Live data")]
    status = "partially loaded" if partial else "could not be loaded"
    assert live_lines == ["Live data %s from http://cluster:8765" % status]
    for problem in ("connection refused", "timeout", "checksum mismatch", "ID 123 will be reused",
                    "stale backup", "BROKE", "ValueError: bad group"):
        assert problem in text
    assert text.endswith("3 checks: 1 critical, 0 error, 1 warning")


def test_compact_report_shows_missing_required_backups(tmp_path):
    from ydb.tests.stress.system_tablet_backup.consistency.report import render_text

    root = build_cluster(tmp_path)
    state, _ = load_state(root=root, needed_tables={"hive": {"Tablet"}})
    outcomes = run_checks(state, select_checks(only=["I1"]))
    text = render_text(state, outcomes)
    assert "SKIP  I1 -- no state for: scheme_shard" in text


def test_verbose_report_keeps_check_details(tmp_path):
    from ydb.tests.stress.system_tablet_backup.consistency.report import render_text

    state, notes = load_state(root=build_cluster(tmp_path))
    text = render_text(state, run_checks(state), notes, verbose=True)
    assert "state:" in text
    assert "SKIP  I8" in text
    assert "changelog commits" in text
    assert "5 skipped, 0 broken" in text


@pytest.mark.parametrize("header", ["Authorization", "Cookie"])
def test_monitoring_credentials_are_sent_to_hives_and_paths(tmp_path, monkeypatch, header):
    import http.server
    import threading
    from ydb.tests.stress.system_tablet_backup.consistency.sources import live as live_source
    from ydb.tests.stress.system_tablet_backup.consistency.sources.http_auth import load_credentials

    monkeypatch.setenv("http_proxy", "http://127.0.0.1:1")
    monkeypatch.setenv("no_proxy", "")
    secret = "Bearer private-test-token" if header == "Authorization" else "ydb_session_id=private-test-token"
    seen = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append((self.path, self.headers.get(header)))
            if self.headers.get(header) != secret:
                self.send_error(401)
                return
            body = {"Tablets": [{"TabletID": "42"}]} if "hiveinfo" in self.path else {
                "PathDescription": {"Self": {"Name": "table", "PathId": "2"}}
            }
            payload = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = "http://127.0.0.1:%d" % server.server_port
    path = tmp_path / "credentials.json"
    path.write_text(json.dumps({header: secret}))
    path.chmod(0o600)
    try:
        credentials = load_credentials(str(path), endpoint)
        state = live_source.read_live(endpoint, [42], {"/Root/table": "path"}, credentials=credentials)
        assert state.hives[42].reachable and state.paths["/Root/table"].reachable
        assert state.hives[42].tablet_ids == {42}
        assert len(seen) == 2 and all(value == secret for _, value in seen)
        assert secret not in repr(credentials) and secret not in repr(state)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_monitoring_credentials_do_not_follow_cross_origin_redirect(tmp_path):
    import http.server
    import threading
    from ydb.tests.stress.system_tablet_backup.consistency.sources import live as live_source
    from ydb.tests.stress.system_tablet_backup.consistency.sources.http_auth import load_credentials

    leaked = []
    secret = "Bearer never-forward-this"

    class Sink(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            leaked.append(self.headers.get("Authorization"))
            self.send_error(401)

        def log_message(self, *args):
            pass

    sink = http.server.HTTPServer(("127.0.0.1", 0), Sink)

    class Redirect(Sink):
        def do_GET(self):
            self.send_response(302)
            self.send_header("Location", "http://127.0.0.1:%d/?echo=%s" % (sink.server_port, "never-forward-this"))
            self.end_headers()

    server = http.server.HTTPServer(("127.0.0.1", 0), Redirect)
    threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in (server, sink)]
    for thread in threads:
        thread.start()
    path = tmp_path / "credentials.json"
    path.write_text(json.dumps({"Authorization": secret}))
    path.chmod(0o600)
    endpoint = "http://127.0.0.1:%d" % server.server_port
    try:
        credentials = load_credentials(str(path), endpoint)
        hive = live_source.read_hive(endpoint, 42, credentials=credentials)
        assert not hive.reachable
        assert not leaked
        assert "never-forward-this" not in hive.error
    finally:
        for s in (server, sink):
            s.shutdown()
            s.server_close()
        for thread in threads:
            thread.join()


@pytest.mark.parametrize("headers,mode,endpoint,insecure", [
    ({"Authorization": "Bearer test"}, 0o644, "https://example.test", False),
    ({"Authorization": "Bearer test\r\nX-Injected: yes"}, 0o600, "https://example.test", False),
    ({"Authorization": "Bearer test"}, 0o600, "http://example.test", False),
    ({"Authorization": "Bearer test"}, 0o600, "https://example.test", True),
])
def test_monitoring_credentials_reject_unsafe_inputs(tmp_path, headers, mode, endpoint, insecure):
    from ydb.tests.stress.system_tablet_backup.consistency.sources.http_auth import load_credentials
    path = tmp_path / "credentials.json"
    path.write_text(json.dumps(headers))
    path.chmod(mode)
    with pytest.raises(ValueError) as error:
        load_credentials(str(path), endpoint, insecure)
    assert headers["Authorization"] not in str(error.value)


@pytest.mark.parametrize("failure,expected", [
    ("timeout", "network operation timed out (socket timeout: 5s)"),
    ("dns", "DNS resolution failed"),
    ("refused", "TCP connection refused"),
    ("certificate", "TLS certificate verification failed"),
    ("http", "HTTP 401"),
    ("unknown", "HTTP/TLS request failed"),
    ("json", "invalid JSON response or request"),
])
def test_authenticated_live_failures_and_progress_are_safe(monkeypatch, failure, expected):
    import errno
    import socket
    import ssl
    import urllib.error
    from ydb.tests.stress.system_tablet_backup.consistency.sources import live as live_source
    from ydb.tests.stress.system_tablet_backup.consistency.sources.http_auth import HttpCredentials

    secret = "secret-must-not-appear"
    errors = {
        "timeout": urllib.error.URLError(socket.timeout(secret)),
        "dns": urllib.error.URLError(socket.gaierror(socket.EAI_NONAME, secret)),
        "refused": urllib.error.URLError(ConnectionRefusedError(errno.ECONNREFUSED, secret)),
        "certificate": urllib.error.URLError(ssl.SSLCertVerificationError(1, secret)),
        "http": urllib.error.HTTPError("https://example.test/" + secret, 401, secret, {}, None),
        "unknown": urllib.error.URLError(secret),
        "json": ValueError(secret),
    }
    credentials = HttpCredentials({"Cookie": secret})
    progress = []

    def fail(url, timeout):
        assert timeout == 5
        assert "timeout=5000" in url
        # Progress must be visible before entering a potentially blocked call.
        assert "reading" in progress[-1]
        raise errors[failure]

    monkeypatch.setattr(credentials, "open", fail)
    state = live_source.read_live("https://example.test", [42], {"/Root/table": "path"},
                                  timeout=5, credentials=credentials, progress=progress.append)
    assert not state.hives[42].reachable and not state.paths["/Root/table"].reachable
    assert expected in state.hives[42].error and expected in state.paths["/Root/table"].error
    assert len(progress) == 4
    assert "[1/2]: reading Hive 42" in progress[0]
    assert "[2/2]: reading path '/Root/table'" in progress[2]
    assert secret not in repr(progress) and secret not in repr(state)


@pytest.mark.parametrize("seconds", ["0", "-1"])
def test_cli_rejects_nonpositive_monitoring_timeout(seconds):
    from ydb.tests.stress.system_tablet_backup.consistency import __main__ as cli
    with pytest.raises(SystemExit) as error:
        cli.main(["--mon-timeout", seconds])
    assert error.value.code == 2
