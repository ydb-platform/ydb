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

## 6. Data Storage: где хранятся статусы тестов

### Текущая схема

| Таблица | Содержимое | Ключ |
|---------|------------|------|
| **test_results/test_runs_column** | Сырые запуски (каждый run) | run_timestamp, build_type, branch, full_name, job_name, status, error_type |
| **flaky_tests_window** | Агрегат по дням (только regression/nightly/postcommit) | full_name, date_window, build_type, branch |
| **tests_monitor** | Статусы на каждый день по веткам и build_type | test_name, suite_folder, full_name, **date_window**, **build_type**, **branch** |

**tests_monitor** — основное хранилище статусов: pass_count, fail_count, mute_count, skip_count, state (Flaky, Muted Flaky, Passed, etc.), is_muted. Одна строка = один тест на одну дату для одной ветки и build_type.

### История запусков

- Полная история — в **test_results** (все job_name, включая PR-check)
- Для mute-логики — **tests_monitor** (агрегат из flaky_tests_window, только regression/nightly/postcommit)

### Ограничение

В flaky_tests_window и tests_monitor нет **error_type** (timeout vs non-timeout). error_type есть только в test_results. Для правил с error_filter потребуется расширить агрегацию.

### Исключение manual runs

Запуски по `workflow_dispatch` (ручной запуск) получают `run_name` с суффиксом `_manual` (test_ya/action.yml). Такие запуски **исключаются** из аналитики (mute, patterns, flaky history): в запросах к test_results добавляется `AND (pull IS NULL OR pull NOT LIKE '%_manual%')`.

---

## 7. Data Flow (mute pipeline)

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

## 8. Pattern Rules (pattern_rules.yaml)

Правила задают логику mute/unmute/delete через конфиг:

- **pattern**: test_flaky, test_stable, test_no_runs, quarantine_graduation
- **scope**: regression | pr_check
- **build_types**: для каких build type применяется
- **params**: пороги (window_days, min_failures_high, min_runs, и т.д.)
- **reaction**: mute | unmute | delete | remove_from_quarantine

create_new_muted_ya загружает правила и использует params для порогов. Добавление нового правила не требует изменений кода.

**Правила с scope: pr_check** (floating_across_days, retry_recovered) требуют отдельного скрипта — данные PR-check не попадают в tests_monitor. Нужен evaluate_pr_check_rules.py, который читает test_results с job_name=PR-check.

**Правила с scope: regression и reaction: alert**:
- **duration_increased** — выросла продолжительность теста (сравнение median baseline vs recent). Данные из test_results (duration), job_name = regression/nightly/postcommit.

## 9. Mute Decisions (mute_decisions)

История всех событий правил: mute/unmute/delete/graduation/alert/log.

| Поле | Описание |
|------|----------|
| timestamp | Время события |
| full_name | suite/test_name (для suite-level: suite_folder) |
| build_type, branch | Контекст |
| action | mute \| unmute \| delete \| quarantine_graduation \| alert:rule_id \| log:rule_id |
| rule_id | ID правила из pattern_rules.yaml |
| reason | Краткое описание (debug-строка) |
| previous_state, new_state | muted \| unmuted \| quarantine (null для alert/log) |
| match_details | Json — полный контекст срабатывания (pattern, growth_ratio, …) |
| behavior_start_date | Дата первого появления поведения |
| behavior_start_commit | Коммит при первом появлении |
| behavior_start_pr | PR при первом появлении |

Таблица: `test_results/analytics/mute_decisions`. Пишется при `create_new_muted_ya` — mute/unmute/delete/graduation; при `evaluate_pr_check_rules` — alert/log.

**find_behavior_start** — опция в params правила: когда true, ищем первое появление (commit, PR, date) в данных.

**Миграция:** если таблица создана до добавления match_details, behavior_start_* — выполнить:
```sql
ALTER TABLE `test_results/analytics/mute_decisions` ADD COLUMN match_details Json;
ALTER TABLE `test_results/analytics/mute_decisions` ADD COLUMN behavior_start_date Date;
ALTER TABLE `test_results/analytics/mute_decisions` ADD COLUMN behavior_start_commit Utf8;
ALTER TABLE `test_results/analytics/mute_decisions` ADD COLUMN behavior_start_pr Utf8;
```

---

## 10. Архитектурные риски

| Риск | Описание | Митигация |
|------|----------|-----------|
| **Cache eviction** | GitHub Actions cache может быть удалён. Теряем previous_base, our_to_unmute → детекция manual unmute не сработает | При cache miss — skip detect_manual_unmutes. Следующий run восстановит state |
| **Quarantine scope** | Один quarantine.txt на все build_type. Размьют в relwithdebinfo → тест размьючен и в asan | Пока приемлемо (dev починил — тест должен работать везде). При необходимости — quarantine per build_type |
| **Graduation gap** | Тесты только в PR-check не попадают в tests_monitor → никогда не graduate | Добавить fallback: time-based graduation (N дней в quarantine без новых падений) |
| **update_muted_ya только relwithdebinfo** | Workflow обновляет только muted_ya.txt, не muted_ya_asan.txt | Расширить matrix по build_type когда понадобится |
| **Concurrency** | Параллельные runs по веткам — разные cache keys. OK | — |

---

## 11. Parallel Testing (Legacy vs Current vs v4_direct)

Для проверки новой системы параллельно со старой:

1. **Legacy** — create_new_muted_ya --legacy: tests_monitor, без quarantine.
2. **Current** — create_new_muted_ya: tests_monitor, с quarantine.
3. **v4_direct** — create_new_muted_ya_v4: **test_results напрямую**, без flaky_tests_window и tests_monitor.
4. **compare_mute_systems.py** — запускает все три режима и строит diff.
5. **Workflow compare_mute_systems.yml** — еженедельно (воскресенье) или вручную.

```bash
# Локально (legacy/current требуют flaky_tests_history, tests_monitor):
python3 .github/scripts/analytics/flaky_tests_history.py --branch=main
python3 .github/scripts/analytics/tests_monitor.py --branch=main
python3 .github/scripts/tests/compare_mute_systems.py --branch main

# Только v4_direct (без flaky/monitor):
python3 .github/scripts/tests/compare_mute_systems.py --skip_legacy --skip_current
```

Артефакт `mute-comparison-{branch}` содержит comparison_report.md и папки legacy/, current/, v4_direct/.

**v4_direct** — целевая новая версия: отказ от flaky_tests_window и tests_monitor, чтение только из test_results.

---

## 12. Parallel Production Run & Switch

После мержа в main обе системы работают **параллельно** при каждом update_muted_ya:

1. **run_parallel_mute_update.py** запускает legacy и v4_direct
2. **Legacy** пишет в `test_results/analytics/mute_decisions` (action: mute, unmute, delete)
3. **v4** пишет в `test_results/mute/v4_decisions` — наглядно отдельно
4. **Активная** система определяет содержимое PR (muted_ya.txt, quarantine.txt)
5. По v4_decisions можно симулировать: «как бы повлияла v4 за этот период»

### Переключение на v4

Изменить **одно** из:

- **Workflow** `.github/workflows/update_muted_ya.yml`: `ACTIVE_SYSTEM: v4_direct`
- **Config** `.github/config/active_mute_system.txt`: одна строка `v4_direct`
- **Env** при запуске: `ACTIVE_SYSTEM=v4_direct`

Приоритет: `--active_system` arg > env > config file > default (legacy).

---

## 13. Backward Compatibility

- `muted_ya.txt` stays for relwithdebinfo
- Default ACTIVE_SYSTEM=legacy — поведение не меняется до переключения
- If quarantine.txt doesn't exist → empty set, no effect
- If muted_ya_asan.txt etc. don't exist → create empty or skip (sanitizers may not have update_muted_ya initially)
