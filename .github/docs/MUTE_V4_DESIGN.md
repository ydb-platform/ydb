# Mute System v4 — Design Document

## Overview

Mute v4 introduces:
1. **Separate mute files per build type** — relwithdebinfo vs sanitizers
2. **Quarantine for manually unmuted tests** — protects from immediate re-mute
3. **Automatic detection of manual vs auto unmute** — no manual file editing needed
4. **Quarantine graduation rule** — fast path to full unmute (4 runs / 1 day, 1+ pass)

---

## 1. Mute Files per Build Type

### File Structure

```
.github/config/
  muted_ya.txt          # relwithdebinfo (unchanged name for backward compat)
  muted_ya_asan.txt     # release-asan
  muted_ya_tsan.txt     # release-tsan
  muted_ya_msan.txt     # release-msan
  quarantine.txt        # manually unmuted tests (per build_type or shared — TBD)
```

### Rationale

- Different build types have different flakiness patterns (ASAN slower, TSAN concurrency)
- A test can be flaky in ASAN but stable in relwithdebinfo
- Clean separation, no mixing

### CI Usage

When running tests, select mute file by `BUILD_PRESET`:
- `relwithdebinfo` → `muted_ya.txt`
- `release-asan` → `muted_ya_asan.txt`
- `release-tsan` → `muted_ya_tsan.txt`
- `release-msan` → `muted_ya_msan.txt`

---

## 2. Quarantine for Manually Unmuted Tests

### Problem

Developer removes test from muted_ya.txt (manual unmute). Next `update_muted_ya` run may add it back to `to_mute` if test still has 2+ failures in 4 days (old data). Manual unmute gets undone.

### Solution: Quarantine

- **quarantine.txt** — tests that were manually unmuted, under observation
- **Effective mute** = `muted_ya \ quarantine` (tests in quarantine run, not muted)
- Tests in quarantine are **excluded from to_mute** — protected from re-mute
- When quarantine graduation rule passes → remove from quarantine (and muted_ya if present)

### Developer Workflow

Developer simply **removes test from muted_ya.txt** and merges PR. No need to add to quarantine manually — we detect it automatically.

---

## 3. Manual vs Auto Unmute Detection

### How We Distinguish

- **Auto unmute** — test was removed by our bot's "Update muted_ya.txt" PR (was in our `to_unmute`)
- **Manual unmute** — test was removed by a developer's PR (not in our `to_unmute`)

### Implementation

1. **Persist state** at end of each `update_muted_ya` run (GitHub Actions cache):
   - `previous_base_tests` — tests that were in muted_ya at run start
   - `our_to_unmute` — tests we recommended for unmute
   - `our_new_muted_ya` — our generated output (or hash for comparison)

2. **At next run**:
   - `current_base` = muted_ya from main
   - `removed` = previous_base_tests - current_base_tests
   - For each test in `removed`:
     - If `test in our_to_unmute` → **auto** (we did it)
     - Else → **manual** → add to quarantine.txt

3. **Edge cases**:
   - Our PR didn't merge: current_base == previous_base, removed = ∅
   - Both our PR and developer PR merged: check each removed test against our_to_unmute

---

## 4. Quarantine Graduation Rule

### Rule

Remove from quarantine when:
- **4+ runs** in **1 day**
- **1+ pass** (at least one successful run)

### Effect

- Test is removed from quarantine.txt
- Test is removed from muted_ya.txt if still present
- Test becomes a normal test again (can be re-muted if 2+ failures in 4 days)

---

## 5. Update Flow (update_muted_ya)

```
1. Restore cache: previous_base, our_to_unmute, our_new_muted_ya
2. Get current_base from main
3. Detect manual unmutes: removed = previous_base - current_base
   For each in removed, if not in our_to_unmute → add to quarantine
4. Load quarantine.txt
5. Run create_new_muted_ya with:
   - Exclude quarantine tests from to_mute
   - Effective all_muted_ya = (muted from file) - quarantine
6. Check quarantine graduation: for each test in quarantine,
   if 4+ runs in 1 day and 1+ pass → remove from quarantine (and muted_ya)
7. Generate new_muted_ya
8. Save to cache: current_base, our_to_unmute, our_new_muted_ya
```

---

## 6. Data Flow

```
muted_ya.txt (from main)
       +
quarantine.txt (tests to temporarily unmute)
       ↓
effective_muted = muted_ya - quarantine  (for CI test runs)
       +
to_mute (excluding quarantine)
       -
to_unmute
       -
to_delete
       ↓
new_muted_ya.txt
```

---

## 7. Future Extensions (Not in v4 Scope)

- **pattern_rules.yaml** — configurable rules (mute, alert, log) per pattern
- **error_type** in mute — timeout vs non-timeout, detect error change
- **PR-check patterns** — floating timeout detection, log/alert
- **Fast unmute** — 1-day window for auto unmute

---

## 8. Backward Compatibility

- `muted_ya.txt` stays for relwithdebinfo
- If quarantine.txt doesn't exist → empty set, no effect
- If muted_ya_asan.txt etc. don't exist → create empty or skip (sanitizers may not have update_muted_ya initially)
