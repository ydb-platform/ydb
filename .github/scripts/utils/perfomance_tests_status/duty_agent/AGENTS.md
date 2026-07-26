# Agent instructions — performance duty investigator

Toolkit: `.github/scripts/utils/perfomance_tests_status/duty_agent`  
Architecture: [`REDESIGN.md`](REDESIGN.md) — **Python = facts + validate; you think.**

Input: frozen **perf-duty-context/v1** JSON (OLAP/TPC-C Now `Save` / `Copy context`).  
Outputs: `analysis.md` (plain language) + `result.json` (`perf-duty-result/v1`).

## Goal

For each problem, answer **all** of:

1. **What** broke (symptom vs root)  
2. **Why / mechanism** — behavior of the system that led here (not just an error string)  
3. **Who / what change** — PR or long-standing behavior, tied to **tested sha** codebase  
4. **Since when** — how long this could have been showing (priors / first-fail / sticky metrics)  
5. **Verify** hypothesis against logs + code; if falsified → new H (≤3)  
6. If unclear → `investigate_further` / unknown. **Do not invent confidence.**

## Setup

```bash
cd .github/scripts/utils/perfomance_tests_status/duty_agent
eval "$(python3 dutyctl.py init-token --shell)"   # if SANDBOX_TOKEN missing
OUT=./runs/my-case
```

## CLI

| Command | Role |
|---------|------|
| `prepare -c CONTEXT -o $OUT` | facts: detect + Allure focus(+fatal) + priors + metrics |
| `dig-runs -c CONTEXT -o $OUT` | Mart history (~35d): neighbors (other run_type/suite + peer clusters, same branch) |
| `dig-prs -o $OUT [--base-sha … --head-sha …]` | product PRs + hot areas in jump window |
| `bisect -o $OUT [-c CONTEXT] [--path …]` | code window on **tested** sha vs prev + focus PR files |
| `validate -o $OUT` | **quality gate** — must exit 0 (fix ≤5) |
| `write-result -c CONTEXT -o $OUT` | final `result.json` only after validate OK |

Extra digs: `gh search` / MCP `user-ydb-qa` / browse code at tested sha.

## Pipeline (mandatory quality loop)

```text
1) dutyctl prepare -c CONTEXT.json -o $OUT
2) DIG RUNS FROM DB before writing analysis (mandatory for tpcc + olap):
     Pack suite_history is short — do NOT stop there.
     dutyctl dig-runs -c CONTEXT -o $OUT [--days-before 35|60|90]
     MCP user-ydb-qa → ydb_query(<sql>) → save JSON
     dutyctl dig-runs -c CONTEXT -o $OUT --from-json raw.json
     Default neighbors (same branch):
       TPC-C — all ydb_cli_* run_type + all clusters; jump on focus suite; cross_run_type + peer jumps
       OLAP  — related suite families + all DbAlias; fail/ydb jumps; cross_suite + peer_dbs
     If summary.window_edge_hint → widen --days-before and re-query
3) DIG LOGS (OLAP fail — never skip):
     focus.json → kikimr__stderr + kikimr__logs (+ Stderr)
4) DIG CODE IN THE JUMP INTERVAL (not only PR of the alert commit):
     dutyctl dig-prs -o $OUT   # uses largest lat step / history window
     dutyctl bisect …          # crash path or forced --path
     read hot PR diffs @ tested sha
5) Form hypothesis H → verify against dig_runs + dig_prs + logs
     falsified → new H (≤3)
6) Self-check — if fail, dig more (do NOT jump to wait_next_wave early)
7) Write analysis.md + problems.json
8) dutyctl validate -o $OUT
9) dutyctl write-result …
```

**`wait_next_wave` only after** dig-runs + dig-prs (or explicit skip reason) and no remaining dig that could pin mechanism.

**Final validation:** do not call `write-result` with `completed` until `validate` exits 0.

## OLAP: logs are not optional

For `olap_fail` / any Allure focus:

| Attachment | Role |
|------------|------|
| `kikimr__stderr` | **execution / process** — VERIFY, AFL_VERIFY, SIGABRT, stacks |
| `kikimr__logs` | **cluster** — connection lost, node down/restart, tablet, IC |
| `Stderr` | query SQL / iteration `statusMessage` surface |

`prepare` already fetches these into `focus.json`. You **must read them** (via `focus.fatal` + per-case `attach_analysis`).  
Stopping at Allure `statusMessage` / code 2005 **without** stderr+logs dig = failed investigation.

If attachments empty: say so explicitly (`stderr empty` / `logs empty`) and lower confidence.

## Root cause from tested codebase

Focus sha = version under test (`selection.focus_run.sha`).

Required:

1. Name the **mechanism** (e.g. abort in `OnReadResult` when `Groups` miss range → SIGABRT → peers see 2005).  
2. Point to **source path/symbol** in that tree (`ydb/core/…`).  
3. `dutyctl bisect` on that path: introduced in window vs long-standing.  
4. If blaming a PR: files ∩ path/symbol; else **candidate** or **unknown**.  
5. Prefer reading code at focus sha:
   `gh api repos/ydb-platform/ydb/contents/<path>?ref=<focus_sha>`  
   or local checkout at that commit.

## Since when (давность)

Use `priors.json` + `history.json` + sticky/suite metrics:

- first-fail sha / label vs sticky-prev-green (metric only — suite may already be red)  
- same surface / same fatal class in prior Allures?  
- last touch of crash path (`bisect` → `last_touch`)  
- known GitHub issue age if matched  

Put a clear line in the report: **Давность:** …

## Analysis types

| Type | Evidence |
|------|----------|
| `olap_fail` | Allure + **stderr + cluster logs** + **dig-runs** (when suite went red / peers) + bisect |
| `olap_slow` | **dig-runs** + metrics_delta + dig-prs/bisect |
| `tpcc_tpmc` / `tpcc_lat` | **dig-runs** (`perfomance/tpcc`) + **dig-prs** on jump window + metrics + DataLens |
| `mixed` | split problems |

### Harness (read for yourself — filter candidates; do not dump into report)

**TPC-C**
- Suites / upload name: [`ydb/tests/olap/load/lib/tpcc.py`](https://github.com/ydb-platform/ydb/blob/main/ydb/tests/olap/load/lib/tpcc.py) — `TestTpccW*Snapshot` / `*Serializable`, `TPCC_RUN_TYPE` → mart `ydb_cli_{tx}_{run_type}`
- Entry: [`ydb/tests/olap/load/test_tpcc.py`](https://github.com/ydb-platform/ydb/blob/main/ydb/tests/olap/load/test_tpcc.py)
- CLI wrapper: [`ydb/tests/olap/lib/ydb_cli.py`](https://github.com/ydb-platform/ydb/blob/main/ydb/tests/olap/lib/ydb_cli.py) (`TxMode`, `create_tpcc_executions` → `--tx-mode`)
- CLI parse: [`ydb/public/lib/ydb_cli/commands/ydb_workload_tpcc.cpp`](https://github.com/ydb-platform/ydb/blob/main/ydb/public/lib/ydb_cli/commands/ydb_workload_tpcc.cpp)
- Workload txs: [`ydb/library/workload/tpcc/`](https://github.com/ydb-platform/ydb/tree/main/ydb/library/workload/tpcc) (`transaction_neworder.cpp`, `common_queries.cpp`)

**OLAP load**
- Base / Allure / upload: [`ydb/tests/olap/load/lib/conftest.py`](https://github.com/ydb-platform/ydb/blob/main/ydb/tests/olap/load/lib/conftest.py) (`LoadSuiteBase`)
- Upload TPC-H: [`ydb/tests/olap/load/lib/upload.py`](https://github.com/ydb-platform/ydb/blob/main/ydb/tests/olap/load/lib/upload.py) (`TestUploadTpch*`)
- TPC-H queries: [`ydb/tests/olap/load/lib/tpch.py`](https://github.com/ydb-platform/ydb/blob/main/ydb/tests/olap/load/lib/tpch.py)
- TPC-DS: [`ydb/tests/olap/load/lib/tpcds.py`](https://github.com/ydb-platform/ydb/blob/main/ydb/tests/olap/load/lib/tpcds.py)
- Clickbench: [`ydb/tests/olap/load/lib/clickbench.py`](https://github.com/ydb-platform/ydb/blob/main/ydb/tests/olap/load/lib/clickbench.py)
- Workload manager: [`ydb/tests/olap/load/lib/workload_manager.py`](https://github.com/ydb-platform/ydb/blob/main/ydb/tests/olap/load/lib/workload_manager.py)
- Pytest entrypoints: `ydb/tests/olap/load/test_*.py`

### TPC-C playbook (mandatory)

1. `prepare` → lat/tpmC vs prev-7 in pack (short).  
2. `dig-runs` (default ~35d, neighbors) → all `ydb_cli_*` on **all clusters**, same branch. Use `largest_lat_step` on focus suite + `cross_run_type` + `peer_clusters_latest` (often jump ≠ alert commit). Widen window if edged.  
3. Use harness knowledge to filter PRs (SnapshotRW vs StrictSerializable; WH scale).  
4. `dig-prs` on the jump window.  
5. Only then: `wait_next_wave` / `investigate_further` / candidate.  
**Forbidden:** stop at «no Allure» / pack metrics only / blame alert-commit PR without dig-runs/dig-prs.  
**Report:** no harness dump unless one sentence narrows the product cause. Cross-suite = search filter, not root cause.

### OLAP playbook (mandatory dig-runs)

1. `prepare` → Allure focus + fatal + pack history.  
2. `dig-runs` (~35d+) → related suites (e.g. UploadTpch↔Tpch) + **peer DbAlias**, same branch. See when FailCount / YdbSumMeans jumped; whether peers/other suites correlate.  
3. Logs (fail) / metrics (slow) + bisect/dig-prs as needed.  
4. Use harness paths above when mechanism depends on upload vs query suite.

## Evidence bar (culprit)

1. Path/symbol **∩** PR files, or  
2. Mechanism + bisect shows path **changed** in prev…first-fail, or  
3. Metrics-only → **candidate** if weak.

Focus-wave PR alone forbidden. Unchanged crash path → do not blame that PR.

## Self-check (before validate)

For each problem, you can answer yes:

- [ ] TPC-C / OLAP: `dig_runs.json` from mart (neighbors + ≥~month if needed), not only pack metrics  
- [ ] TPC-C: `dig_prs.json` on the **jump** window (not only PR of alert commit)  
- [ ] Correlations checked: other run_type/suite and peer cluster on same branch  
- [ ] Read cluster logs **and** execution stderr (OLAP fail), or documented empty  
- [ ] Mechanism stated (not only fingerprint)  
- [ ] Tied to code at **tested sha**  
- [ ] Давность stated with dates/labels  
- [ ] Hypothesis verified or explicitly `partial`/`no`  
- [ ] Culprit only if evidence bar met  

If any checkbox fails → dig again (loop), do not polish a hollow report.

## Report: `analysis.md`

Пиши **по-русски**, для дежурного. Английский — только устоявшиеся термины и идентификаторы:
`VERIFY`, `SIGABRT`, `AFL_VERIFY`, имена символов/файлов, `code: 2005`, имена query, токены решения (`update_known`).

**Язык отчёта = для человека в тикете**, не для агента.

Запрещено в `analysis.md` (и в подсказках validate):  
`фокус` / `фокусный` / `focus`, `волна` / `wave`, `RC`, `root cause` (пиши «корневая причина»),  
`last touch` / «касание path», `Crash path`, `priors`, `sticky`, `prev-green`,  
`seed`, `attach`, `rollup`, `mixed`, `reclassified`,  
`dig-runs` / `dig-prs` / `cross-suite` / `high-load` / `hot areas` / `peer` как ярлыки агента,  
`bisect` как ярлык (пиши «проверка/откат кандидатов» или «сужение окна кода»),  
«раньше» / «предыдущий» без даты или label прогона.

Пиши так:  
«разбираемый прогон `2026-07-25_f88e100`», «прогон `2026-07-24_99ac2c7`»,  
«когда файл меняли последний раз»,  
«в UI отмечен зелёным по метрике, по Allure уже с падениями».

**В `analysis.md` только проблемы продукта/кластера.**  
Ложный seed молча отбросить. Баги harness и «как устроен тест» в отчёт не писать — кроме случая, когда это даёт понимание проблемы (например: suite бьёт только SnapshotRW → PR про StrictSerializable слаб).

**Не писать «отпадает / отброшено / не виноват»** про PR из commit алерта или прочие ложные следы — просто не включай их в отчёт.  
Исключение: явный запрос «проверь этого кандидата» или сравнение с предыдущим отчётом, где кандидат уже фигурировал.

**Ссылки обязательны** (markdown), не голый `#123` / короткий sha:
- issue → `https://github.com/ydb-platform/ydb/issues/N`
- PR → `https://github.com/ydb-platform/ydb/pull/N`
- commit → `https://github.com/ydb-platform/ydb/commit/<sha>`
- файл на tested sha → `https://github.com/ydb-platform/ydb/blob/<full_sha>/path#L…`
- sandbox → полный URL отчёта

Пример: `[#29944](https://github.com/ydb-platform/ydb/issues/29944)`,
[`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100).

**Кандидаты PR в отчёте — не список ссылок.** Таблица (или строки) с полями:
`дата влития (UTC)` · `[#N](url)` · `title` · `@author` · кратко почему.
Бери `merged_at` / `pr_title` / `author_login` из `dig_prs.json` (после `dutyctl dig-prs`).

```markdown
# Perf duty — {suite} @ {db} — {focus_label}

## Заключение
- **Итог:** …
- **Решение:** токен + коротко по-русски:
  - `investigate_further` — продолжить разбор (ещё не хватает доказательств на PR/механику)
  - `open_ticket` — завести/обновить issue
  - `update_known` — это уже известный тикет
  - `wait_next_wave` — ждать следующий прогон (только после dig, когда копать больше нечего)
  - `no_action` — действий не нужно
- **Виновник:** unknown | @{login} / [PR #N](url) — …
- **Уверенность:** высокая | средняя | низкая
- **Давность:** … (когда могло проявиться; ссылки на issue/PR/коммиты)
- **Механика:** … (поведение системы → поломка)

## Проблемы
### P1 — …
- Тип: olap_fail | …
- Что сломалось: …
- Почему / механика: …
- Логи: kikimr__stderr …; kikimr__logs … (или явно пусто)
- Код ([sha](commit-url)): [path](blob-url) / symbol …
- Кто (если есть): … + доказательство
- Давность: …
- Гипотеза проверена: yes | no | partial
- Связанный issue: [ссылка] или нет

## Что дальше
1. …

## Материалы для issue
Обязательный блок в конце — одним куском в GitHub issue / комментарий.
- Окружение: suite, db, branch, label, время, CI, commit (ссылка)
- Таблица отчётов: разбираемый + соседние по дате/label (полные URL sandbox)
- Код: место падения (blob@commit), менялся ли файл, когда меняли последний раз, issue/PR
- Короткая цитата из stderr/logs
- 3–5 пунктов «что важно для формулировки issue»
```

Also update `$OUT/problems.json`.

## Rules

- Prefer crash stack + mechanism over surface 2005.  
- Prefer **unknown** over a confident lie.  
- No mute without human. Secrets out of reports.  
- `validate` is the quality gate — treat failures as missing investigation, not as “tweak wording only” when logs/bisect were skipped.

## Tests

```bash
python3 test_duty_agent.py
```
