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
| `bisect -o $OUT [-c CONTEXT] [--path …]` | code window on **tested** sha vs prev + focus PR files |
| `validate -o $OUT` | **quality gate** — must exit 0 (fix ≤5) |
| `write-result -c CONTEXT -o $OUT` | final `result.json` only after validate OK |

Extra digs: `gh search` / `gh api` / browse code at focus sha.

## Pipeline (mandatory quality loop)

```text
1) dutyctl prepare -c CONTEXT.json -o $OUT
2) DIG LOGS (OLAP — never skip):
     focus.json → allure.cases[].attach_analysis
     - kikimr__stderr  (crashes / VERIFY / signal)
     - kikimr__logs    (cluster: disconnect, restart, tablet, IC)
     - Stderr          (query / iteration errors)
3) Form hypothesis H (mechanism in one sentence)
4) dutyctl bisect …  (+ read code at focus sha via gh/git)
5) Verify H against logs + code + priors
     - falsified → new H, goto 3 (≤3)
6) Self-check checklist (below) — if fail, dig more, do NOT write a fake report
7) Write analysis.md + problems.json
8) dutyctl validate -o $OUT     # must pass; fix report/evidence ≤5 times
9) dutyctl write-result …
```

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
| `olap_fail` | Allure + **stderr + cluster logs** + bisect |
| `olap_slow` | metrics_delta + optional stall logs |
| `tpcc_tpmc` / `tpcc_lat` | metrics + DataLens + sha window |
| `mixed` | split problems |

## Evidence bar (culprit)

1. Path/symbol **∩** PR files, or  
2. Mechanism + bisect shows path **changed** in prev…first-fail, or  
3. Metrics-only → **candidate** if weak.

Focus-wave PR alone forbidden. Unchanged crash path → do not blame that PR.

## Self-check (before validate)

For each problem, you can answer yes:

- [ ] Read cluster logs **and** execution stderr (OLAP), or documented empty  
- [ ] Mechanism stated (not only fingerprint)  
- [ ] Tied to code path at **focus sha**  
- [ ] Bisect / priors → **давность** stated  
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
`seed`, `attach`, `rollup`, `mixed`, `reclassified`, `bisect` как ярлык без смысла,  
«раньше» / «предыдущий» без даты или label прогона.

Пиши так:  
«разбираемый прогон `2026-07-25_f88e100`», «прогон `2026-07-24_99ac2c7`»,  
«когда файл меняли последний раз», «PR в том же commit — не причина»,  
«в UI отмечен зелёным по метрике, по Allure уже с падениями».

**В `analysis.md` только проблемы продукта/кластера.**  
Ложный seed молча отбросить. Баги harness в отчёт не писать.

**Ссылки обязательны** (markdown), не голый `#123` / короткий sha:
- issue → `https://github.com/ydb-platform/ydb/issues/N`
- PR → `https://github.com/ydb-platform/ydb/pull/N`
- commit → `https://github.com/ydb-platform/ydb/commit/<sha>`
- файл на tested sha → `https://github.com/ydb-platform/ydb/blob/<full_sha>/path#L…`
- sandbox → полный URL отчёта

Пример: `[#29944](https://github.com/ydb-platform/ydb/issues/29944)`,
[`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100).

```markdown
# Perf duty — {suite} @ {db} — {focus_label}

## Заключение
- **Итог:** …
- **Решение:** update_known | open_ticket | wait_next_wave | investigate_further | no_action
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
