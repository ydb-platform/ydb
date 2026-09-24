# Functional test matrix

Update this file when a case lands. Legend:

* **Pri** — P0 / P1 / P2 / P3
* **Fault** — primitive from [README.md](README.md) §4, or `—`
* **Config** — override required beyond the default `NbsTestBase` cluster
* **Now** — `yes` existing pytest, `ready` can be written today, `prereq`
  waits on README §6, `blocked` waits on a named hook
* **Hook** — blocking harness or product gap

`nbs_cfg` = per-test `nbs_config` override. `disk_pdisks` =
`use_in_memory_pdisks=False`.

---

## F1 — User scenarios

| ID | Title | Pri | Fault | Config | Now | Hook |
| --- | --- | --- | --- | --- | --- | --- |
| F1.1 | Create + write/read | P0 | — | — | yes | `test_nbs_disk_creation` |
| F1.2 | Idempotent create | P0 | — | — | yes | `test_nbs_disk_creation_idempotent` |
| F1.3 | Conflicting create | P0 | — | — | yes | `test_nbs_disk_creation_conflict` |
| F1.4 | Delete + PBuffer wipe | P0 | — | — | yes | `test_nbs_disk_deletion` |
| F1.5 | Delete nonexistent | P0 | — | — | yes | `test_nbs_disk_deletion_nonexistent` |
| F1.6 | Disk id with symbols | P0 | — | — | yes | `test_nbs_disk_creation_name_with_symbols` |
| F1.7 | Delete during IO | P0 | — | — | yes | `F1_07_delete_during_io.py` |
| F1.8 | Re-create same id after delete | P0 | — | — | yes | `F1_08_recreate_after_delete.py` |
| F1.9 | Write across stripe | P0 | — | — | yes | `F1_09_write_across_stripe.py` |
| F1.10 | Write across vchunk | P0 | — | — | yes | `F1_10_write_across_vchunk.py` |
| F1.11 | Write across region | P0 | — | disk > 4 GiB | yes | `F1_11_write_across_region.py` |
| F1.12 | Never-written reads zero | P0 | — | — | yes | `F1_12_never_written_reads_zero.py` |
| F1.13 | Block sizes 4K / 8K / 16K / 32K / 64K / 128K, 512 GiB each | P0 | — | block size | yes | `F1_13_block_sizes.py` (known_bug xfail per size >4 KiB: VChunk hardcodes 4 KiB) |
| F1.14 | Vhost unaligned write | P0 | — | — | yes | `test_nbs_vhost_unaligned_write` |
| F1.15 | Vhost after tablet restart | P0 | `tablet_kill` | — | yes | `F1_vhost/F1_15_vhost_after_tablet_restart.py` |
| F1.16 | Two disks isolated | P0 | — | — | yes | `test_nbs_multiple_disks_creation` |
| F1.17 | Noisy neighbour | P1 | — | — | yes | `F1_17_noisy_neighbour.py` |
| F1.18 | 500 GiB nine locations | P0 | — | 500 GiB | yes | `test_nbs_500gb_disk_read_write` |
| F1.19 | Load-actor write | P1 | — | — | yes | `test_nbs_load_actor_write` |
| F1.20 | Load-actor read | P1 | — | — | yes | `test_nbs_load_actor_read` |
| F1.21 | Load-actor mixed | P1 | — | — | yes | `test_nbs_load_actor_mixed` |
| F1.22 | Load-actor write then read | P1 | — | — | yes | `test_nbs_load_actor_write_then_read` |
| F1.23 | ZeroBlocks | P0 | — | — | yes | `F1_vhost/F1_23_zero_blocks.py` (known_bug xfail) |
| F1.24 | DeletePartition wipe | P0 | — | — | yes | `F1_24_delete_partition_wipe.py` |
| F1.25 | Max disk size per block size, 2³¹ blocks each | P0 | — | 2³¹ blocks | yes | `F1_25_max_disk_size.py` (known_bug xfail, not run, per size >4 KiB) |

---

## F2 — Faults and slowdown

| ID | Title | Pri | Fault | Config | Now | Hook |
| --- | --- | --- | --- | --- | --- | --- |
| F2.1 | Tablet kill under load | P0 | `tablet_kill` | — | yes | `F2_01_tablet_kill_under_load.py` |
| F2.2 | Slot stop + start | P0 | slot stop/start | — | yes | `F2_02_slot_stop_start.py` |
| F2.3 | One DBG host down | P1 | `node.stop` | file PDisks | yes | `F2_03_one_dbg_host_down.py` |
| F2.4 | Two DBG hosts down | P1 | `node.stop` ×2 | file PDisks | yes | `F2_04_two_dbg_hosts_down.py` |
| F2.5 | Three DBG hosts down | P1 | `node.stop` ×3 PBuffer nodes | file PDisks | yes | `F2_05_three_dbg_hosts_down.py` (known_bug: IO does not recover within 120s after three PBuffer nodes across 3 DCs restart) |
| F2.6 | PDisk BROKEN | P1 | `pdisk set BROKEN` | file PDisks | yes | `F2_06_pdisk_broken.py` (CMS BROKEN is metadata only; writes still succeed) |
| F2.7 | PDisk stop + restart | P1 | `pdisk stop/restart` | file PDisks | yes | `F2_07_pdisk_stop_restart.py` |
| F2.8 | SIGSTOP slow node | P1 | `SIGSTOP` | — | yes | `F2_08_sigstop_slow_node.py` |
| F2.9 | Kill mid-flush | P0 | `tablet_kill` | — | yes | `F2_09_kill_mid_flush.py` |
| F2.10 | Kill mid-erase | P0 | `tablet_kill` | — | yes | `F2_10_kill_mid_erase.py` |
| F2.11 | Kill mid-restore | P0 | `tablet_kill` ×2 | — | yes | `F2_11_kill_mid_restore.py` |
| F2.12 | Kill mid-AddHost | P1 | `tablet_kill` | nbs_cfg (Offline) | prereq | nbs_cfg |
| F2.13 | Repeated host bounce | P1 | `node.stop`/`start` | disk_pdisks recommended | prereq | disk_pdisks for data-preserving bounce |
| F2.14 | Combined faults | P1 | kill + SIGSTOP + stop | file PDisks | yes | `F2_14_combined_faults.py` (known_bug: IO does not recover within 120s after tablet kill + host stop + host freeze) |
| F2.15 | Per-request IO error | P1 | — | — | blocked | DDisk/PBuffer error hook or ICB |
| F2.16 | Per-request latency | P1 | — | — | blocked | same hook, delay mode |
| F2.17 | Torn write / power loss | P0 | — | — | blocked | precise-point kill; checksums unimplemented |
| F2.18 | Interconnect partition | P0 | — | — | blocked | netem/iptables (not in sandbox) |
| F2.19 | On-disk block corruption | P0 | — | disk_pdisks | blocked | corruption tool; checksums unimplemented |

---

## F3 — Node down and data copy

| ID | Title | Pri | Fault | Config | Now | Hook |
| --- | --- | --- | --- | --- | --- | --- |
| F3.1 | TemporaryOffline only, no promote | P1 | `SIGSTOP` or stop | nbs_cfg durations 2s/20s | prereq | nbs_cfg |
| F3.2 | TemporaryOffline recovery | P1 | then SIGCONT / start | nbs_cfg; disk_pdisks if process stopped | prereq | nbs_cfg, disk_pdisks |
| F3.3 | Offline promotes HandOff, watermark 0 | P1 | `node.stop` | file PDisks | yes | `F3_03_offline_promotes_handoff.py` (known_bug: NBS slot SIGSEGV on Primary DDisk loss) |
| F3.4 | Offline QueryAddHost | P1 | `node.stop` | file PDisks | yes | `F3_04_offline_query_addhost.py` (known_bug: NBS slot SIGSEGV on Primary DDisk loss) |
| F3.5 | OnDDiskBroken stays Broken | P1 | `pdisk BROKEN` | file PDisks | yes | `F3_05_ondiskbroken_stays_broken.py` (known_bug: `TReplyStatus::BROKEN` never produced; see `oracle_ut`) |
| F3.6 | Copier starts after promotion | P1 | follows F3.3 | file PDisks | yes | `F3_06_copier_starts_after_promotion.py` (known_bug: NBS slot SIGSEGV on Primary DDisk loss) |
| F3.7 | Serial 1 MiB ranges | P1 | follows F3.3 | file PDisks | yes | `F3_07_serial_1mib_ranges.py` (known_bug: NBS slot SIGSEGV on Primary DDisk loss) |
| F3.8 | Fresh DDisk not readable above watermark | P1 | follows F3.3 | file PDisks | yes | `F3_08_fresh_ddisk_not_readable_above_watermark.py` (known_bug: NBS slot SIGSEGV on Primary DDisk loss) |
| F3.9 | Copy complete, watermark nullopt | P1 | follows F3.3 | file PDisks | yes | `F3_09_copy_complete_watermark_unset.py` (known_bug: NBS slot SIGSEGV on Primary DDisk loss) |
| F3.10 | Original host returns during copy | P1 | stop then start mid-copy | disk_pdisks | prereq | disk_pdisks |
| F3.11 | BehindField drains after return | P1 | short disable then enable | nbs_cfg; disk_pdisks | prereq | nbs_cfg, disk_pdisks |
| F3.12 | User writes during copy | P1 | follows F3.3 | file PDisks | yes | `F3_12_user_writes_during_copy.py` (known_bug: NBS slot SIGSEGV on Primary DDisk loss) |
| F3.13 | Copier retriable backoff | P1 | bounce dest during copy | disk_pdisks or F2.15 | prereq | disk_pdisks or F2.15 |
| F3.14 | Copier never-retriable stop | P1 | — | — | blocked | F2.15 |
| F3.15 | Copy progress survives tablet kill | P1 | `tablet_kill` mid-copy | file PDisks | yes | `F3_15_copy_does_not_resume_incrementally.py` (asserts NBS-7656 watermark persist; known_bug: NBS slot SIGSEGV on Primary DDisk loss) |
| F3.16 | DoStart does not start copiers | P1 | follows F3.15 | file PDisks | yes | `F3_16_dostart_does_not_start_copiers.py` (known_bug: NBS slot SIGSEGV on Primary DDisk loss) |
| F3.17 | Read fallback to disabled replica | P0 | disable last replica | nbs_cfg | prereq | nbs_cfg |
| F3.18 | Ahead not readable, still copied | P1 | write during copy | file PDisks | yes | `F3_18_ahead_not_readable_still_copied.py` (known_bug: NBS slot SIGSEGV on Primary DDisk loss) |
| F3.19 | Failed host stays listed; demote after quorum | P1 | follows F3.3+F3.4 | file PDisks | yes | `F3_19_no_demote_no_rebalance.py` (known_bug: NBS slot SIGSEGV on Primary DDisk loss) |

---

## F4 — Throttling and limits

| ID | Title | Pri | Fault | Config | Now | Hook |
| --- | --- | --- | --- | --- | --- | --- |
| F4.1 | CopyRangeBandwidthMbs 20 vs 200 | P2 | Offline to start copy | nbs_cfg bandwidth | prereq | nbs_cfg |
| F4.2 | CopyRangeBandwidthMbs 0 | P2 | same | nbs_cfg = 0 | prereq | nbs_cfg |
| F4.3 | Shared volume copy budget | P2 | two copiers | nbs_cfg | prereq | nbs_cfg |
| F4.4 | User IO during copy | P2 | same | nbs_cfg | prereq | nbs_cfg |
| F4.5 | SyncRequestsBatchSize 100 vs 1 | P2 | — | nbs_cfg batch | prereq | nbs_cfg; F5.3 |
| F4.6 | CleaningUp force-flush in 1 s | P2 | — | — | yes | `F4_06_cleaningup_force_flush.py` |
| F4.7 | Flush cooldown on errors | P2 | `SIGSTOP` | — | yes | `F4_07_flush_cooldown_on_errors.py` |
| F4.8 | OVERFILL PerTabletStorageLimit | P2 | stop flush targets | nbs_cfg lower limit | prereq | nbs_cfg |
| F4.9 | OVERLOADED pending queue | P2 | burst | — | yes | `F4_09_overloaded_pending_queue.py` |
| F4.10 | Write > 512 KiB | P2 | — | — | yes | `F4_10_write_over_512kib.py` (skip: dstool ARG_MAX / stripe split) |
| F4.11 | Hedge delay vs PB reply timeout | P2 | `SIGSTOP` | nbs_cfg delays | prereq | nbs_cfg |
| F4.12 | No user IOPS throttle | P2 | — | — | yes | `F4_12_no_user_iops_throttle.py` |
| F4.13 | Copy-rate counter | P2 | — | — | blocked | leaky-bucket / copy MB/s sensor |

---

## F5 — Observability

| ID | Title | Pri | Fault | Config | Now | Hook |
| --- | --- | --- | --- | --- | --- | --- |
| F5.1 | Parse DBG host table | P3 | — | — | yes | `F5_01_parse_dbg_host_table.py` |
| F5.2 | Parse PBuffer occupancy / LSNs | P3 | — | — | yes | `F5_02_parse_pbuffer_occupancy.py` |
| F5.3 | VChunk Pending / MinLsn | P3 | — | — | yes | `F5_03_vchunk_pending_minlsn.py` (best-effort) |
| F5.4 | Volume request counters | P3 | — | — | yes | `F5_04_volume_request_counters.py` (best-effort) |
| F5.5 | DDisk DirectIO / PB pending | P3 | — | — | yes | `F5_05_ddisk_directio.py` |
| F5.6 | wait_until convention | P3 | — | — | yes | coding rule, not a test |
| F5.7 | Sensor inventory spike | P3 | — | — | yes | documented in F5_observability_and_oracles.md §5 |

---

## Counts

| Now | Count | Meaning |
| --- | --- | --- |
| yes | 61 | automated in `ydb/tests/functional/nbs/` (parent suite + F1–F5 group suites) plus the F5.7 inventory note |
| ready | 0 | first slice landed |
| prereq | 15 | need `nbs_config` override and/or on-disk PDisks |
| blocked | 7 | need a new hook (F2.15–F2.19, F3.14, F4.13) |

Total: 83. The first implementation slice (F5 parsers, F1 leftovers, F2.1–F2.11/F2.14, F3 ready, F4 ready) has landed as one-file-per-case suites under `ydb/tests/functional/nbs/F{1..5}_*/`. Next: `nbs_config` override + `use_in_memory_pdisks=False` to unlock the 15 prereq rows.
