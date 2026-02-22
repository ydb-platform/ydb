# Mute v4 — Task List for Implementation

**Design doc:** [MUTE_V4_DESIGN.md](MUTE_V4_DESIGN.md)

**Branch:** mute_version_4

---

## Phase 1: Mute Files per Build Type

- [x] **1.1** Create empty mute files for sanitizers: `muted_ya_asan.txt`, `muted_ya_tsan.txt`, `muted_ya_msan.txt`
- [x] **1.2** Add helper to get mute file path by build_type: inline case in action.yml
- [x] **1.3** Update `test_ya/action.yml` to use correct mute file based on `BUILD_PRESET`
- [x] **1.4** Update `mute_utils.py convert_muted_txt_to_yaml` to accept file path parameter
- [ ] **1.5** Update `update_muted_ya.yml` to process per build_type (matrix or loop)
- [x] **1.6** Update `create_new_muted_ya.py` to accept `--muted_ya_file` and `--output_file` per build_type

---

## Phase 2: Quarantine Infrastructure

- [x] **2.1** Create `quarantine.txt` (empty initially)
- [x] **2.2** Reuse YaMuteCheck for quarantine loading
- [x] **2.3** Implement `apply_quarantine` in mute_utils: effective_muted = muted - quarantine
- [x] **2.4** Update test_ya action: use effective_muted for convert and transform_build_results
- [x] **2.5** Exclude quarantine tests from `to_mute` in `create_new_muted_ya.py`

---

## Phase 3: Manual vs Auto Unmute Detection

- [x] **3.1** Add cache save/restore to `update_muted_ya.yml`: `mute_state` with previous_base.txt, our_to_unmute.txt
- [x] **3.2** Implement detect_manual_unmutes.py: `removed = previous_base - current_base`
- [x] **3.3** For each removed test: if not in `our_to_unmute` → add to quarantine.txt
- [x] **3.4** Persist quarantine.txt changes in PR (when creating update PR)
- [x] **3.5** Handle first run (no cache): skip detection when mute_state missing

---

## Phase 4: Quarantine Graduation

- [x] **4.1** Add aggregation for 1-day window (aggregate_test_data period_days=1)
- [x] **4.2** Implement get_quarantine_graduation: 4+ runs in 1 day AND 1+ pass
- [x] **4.3** For tests passing graduation: remove from quarantine, exclude from muted_ya output
- [x] **4.4** Integrate graduation into create_new_muted_ya mute_worker

---

## Phase 5: Integration & Workflow Updates

- [x] **5.1** Update `update_muted_ya.yml` full flow: cache restore → detection → quarantine load → create_new_muted_ya (with quarantine exclusion) → graduation → cache save
- [x] **5.2** Update `collect_analytics.yml` if it uses muted_ya (per build_type) — N/A: data marts read from tests_monitor, not muted_ya
- [ ] **5.3** Update `create_issues_for_muted_tests.yml` for multi-file if needed
- [ ] **5.4** Update `get_muted_tests.py upload_muted_tests` for per build_type files
- [x] **5.5** Update `mute_rules.md` documentation

---

## Phase 5b: Pattern Rules

- [x] **5b.1** Create pattern_rules.yaml with mute/unmute/delete/graduation rules
- [x] **5b.2** Create pattern_rules_loader.py
- [x] **5b.3** Refactor create_new_muted_ya to use rule params
- [x] **5b.4** Add --build_type and --rules_file to create_new_muted_ya

---

## Phase 5d: Mute Decisions (YDB)

- [x] **5d.1** Add mute_decisions table to ydb_qa_config.json
- [x] **5d.2** Create mute_decisions.py (create table, write_mute_decisions)
- [x] **5d.3** Integrate write_mute_decisions into create_new_muted_ya.py
- [x] **5d.4** Document mute_decisions and architectural risks in MUTE_V4_DESIGN.md

---

## Phase 5c: PR-check rules & error_type

- [ ] **5c.1** evaluate_pr_check_rules.py — скрипт для правил scope: pr_check (читает test_results)
- [ ] **5c.2** Реализовать pattern: floating_across_days
- [ ] **5c.3** Реализовать pattern: retry_recovered
- [ ] **5c.4** Добавить error_type в flaky_tests_window или отдельную агрегацию
- [ ] **5c.5** Реализовать error_filter в is_mute_candidate
- [ ] **5c.6** Реализовать muted_test_different_error (reaction: alert)

---

## Phase 6: Testing & Validation

- [ ] **6.1** Test manual unmute flow: remove test, merge, verify quarantine
- [ ] **6.2** Test quarantine graduation: 4 runs / 1 day with 1 pass
- [ ] **6.3** Test that quarantine tests don't get re-muted
- [ ] **6.4** Test per build_type mute file selection in CI

---

## Progress Log

| Date | Task | Status |
|------|------|--------|
| — | Design doc created | ✅ |
| — | Task list created | ✅ |
| — | Phase 1.1-1.3: Mute files per build type, test_ya action | ✅ |
| — | Phase 2: Quarantine (apply_quarantine, exclude from to_mute) | ✅ |
| — | Phase 3: Manual vs auto detection (cache, detect_manual_unmutes) | ✅ |
| — | Phase 4: Quarantine graduation (4 runs/1 day, 1+ pass) | ✅ |
| — | Phase 5b: Pattern rules (pattern_rules.yaml, rule loader) | ✅ |
| — | Phase 5d: Mute decisions table (mute_decisions, traceability) | ✅ |
