# F1 — User-level main scenarios

Priority **P0**. Levels: local pytest against `NbsTestBase`. No faults.

Objective: every user-visible operation that exists today works, and the
operations that abort or no-op (`ZeroBlocks`, `DeletePartition` wipe) have
expected-fail placeholders so they cannot be forgotten.

Existing coverage is noted per case. Helpers:
`create_disk` / `delete_disk` / `write` / `read`,
`VhostUserBlkClient`, `run_nbs_load_test`.

Geometry to keep in mind when picking offsets:

* stripe **512 KiB**
* vchunk **32 MiB** on in-memory PDisks (the default harness), **128 MiB**
  otherwise
* region **4 GiB**
* default disk **1 048 576 × 4 KiB = 4 GiB** (exactly one region)
* supported block sizes and max volume (2³¹ blocks at each size):

| Block size | Maximum disk size |
| --- | --- |
| 4 KiB | 8 TiB |
| 8 KiB | 16 TiB |
| 16 KiB | 32 TiB |
| 32 KiB | 64 TiB |
| 64 KiB | 128 TiB |
| 128 KiB | 256 TiB |

---

## Disk lifecycle

| # | Case | Assertion | Existing |
| --- | --- | --- | --- |
| F1.1 | Create disk, write block 0, read it back | byte-exact read-after-write | `test_nbs_disk_creation` |
| F1.2 | Create the same `disk_id` twice | first `SUCCESS` + `tabletId`; second `ALREADY_EXISTS` with the same `tabletId` | `test_nbs_disk_creation_idempotent` |
| F1.3 | Second create with a different `blocks_count` | `GENERIC_ERROR`, not SUCCESS / ALREADY_EXISTS | `test_nbs_disk_creation_conflict` |
| F1.4 | Write so PBuffers hold LSNs, then delete | delete `SUCCESS`; second delete `NOT_FOUND`; DBG mon cleared; tablet id gone from PBuffer mon | `test_nbs_disk_deletion` |
| F1.5 | Delete a disk that does not exist | `NOT_FOUND`, no `diskId` in the response | `test_nbs_disk_deletion_nonexistent` |
| F1.6 | Disk id containing `%` (and similar reserved characters) | create + IO succeed | `test_nbs_disk_creation_name_with_symbols` |
| F1.7 | Delete while load-actor IO is in flight | delete completes; in-flight IO fails retriably or is cancelled; no leftover tablet on PBuffer mon | none |
| F1.8 | Re-create the same `disk_id` after a successful delete | new `tabletId`; never-written ranges read as zeroes; previous payload is gone | none |

---

## IO correctness

| # | Case | Assertion | Existing |
| --- | --- | --- | --- |
| F1.9 | Write crossing a 512 KiB stripe boundary | reassembled read is exact | none |
| F1.10 | Write crossing a vchunk boundary (32 MiB on this harness) | both vchunks hold the correct slice; read is exact | none |
| F1.11 | Write crossing a 4 GiB region boundary (needs a disk > 4 GiB) | both regions hold the correct slice; read is exact | none |
| F1.12 | Read of a never-written range | zeroes, not garbage, not an error | none |
| F1.13 | Block sizes 4 / 8 / 16 / 32 / 64 / 128 KiB (create 512 GiB with `--block-size`) | create succeeds at every size; first / middle / last block read-after-write exact | `F1_13_block_sizes.py` |
| F1.14 | Unaligned vhost writes at byte offsets 1024 and 5120, then unaligned read | `VIRTIO_BLK_S_OK`; payload match | `test_nbs_vhost_unaligned_write` |
| F1.15 | Vhost after partition tablet kill + recovery | same vhost session keeps working across the restart; generation increases; previously written data readable | `F1_15_vhost_after_tablet_restart.py` |
| F1.16 | Two disks, independent IO | no cross-talk | `test_nbs_multiple_disks_creation` |
| F1.17 | Noisy neighbour: load-actor on disk A while doing verified IO on disk B | disk B read-after-write holds; disk A `RequestsFailed == 0` | none |
| F1.18 | 500 GiB disk, first / middle / last block of first / middle / last 32 MiB chunk (in-memory PDisk chunk size) | each 4 KiB read matches | `test_nbs_500gb_disk_read_write` |
| F1.25 | Max disk size at every supported block size (`2³¹` blocks) | create, first / middle / last block read-after-write exact, write past the last block rejected. Only 4 KiB runs today; larger sizes are `known_bug` xfail with `run=False` (eager vchunk metadata at 2³¹ blocks) | `F1_25_max_disk_size.py` (known_bug xfail, not run, per size >4 KiB) |

Extend F1.18 with a self-describing payload (LBA + sequence) so a misplaced
block is distinguishable from a torn one. Today's test writes the same
random string to every location.

---

## Load actor

These check liveness, not data. Pair them with a self-describing verifier
once it exists.

| # | Case | Assertion | Existing |
| --- | --- | --- | --- |
| F1.19 | 100% write, 10 s, iodepth 32 | `Iops > 0`, `RequestsFailed == 0`, `BlocksWritten > 0` | `test_nbs_load_actor_write` |
| F1.20 | Seed blocks, then 100% read | `BlocksRead > 0`, no writes | `test_nbs_load_actor_read` |
| F1.21 | 50/50 mixed after seed | both `BlocksRead` and `BlocksWritten` | `test_nbs_load_actor_mixed` |
| F1.22 | Write-only then read-only on the same disk | write phase has no reads; read phase has no writes | `test_nbs_load_actor_write_then_read` |

Add a follow-up (F1.22b) that **verifies** the written image after the
write phase by reading a sample of blocks through `dstool nbs partition io`.
The current tests never check contents after a load-actor run.

---

## Expected-fail placeholders

| # | Case | Assertion | Status |
| --- | --- | --- | --- |
| F1.23 | `ZeroBlocks` / discard from vhost or load-actor | currently `Y_ABORT_UNLESS` in `TFastPathService::ZeroBlocksLocal` — mark xfail until implemented; then: range reads as zeroes, other ranges unchanged | unimplemented |
| F1.24 | `DeletePartition` actually wipes | `delete_disk` replies OK today without a confirmed wipe of DDisk chunks. After the stub in `delete_partition.cpp` is filled: chunks released, PBuffer records gone (already asserted in F1.4), a re-create with the same id (F1.8) cannot read old data | stub |

Do not skip these. An xfail that starts passing is the signal that the
product changed.

---

## Suggested pytest layout

Keep `TestNbs` / `TestNbsLoadActor` for the existing cases. Add:

* `test_nbs_lifecycle.py` — F1.7, F1.8
* `test_nbs_io_geometry.py` — F1.9–F1.13
* `test_nbs_vhost.py` — move F1.14, add F1.15
* `test_nbs_placeholders.py` — F1.23, F1.24 marked xfail
