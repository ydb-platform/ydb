# Agent instructions — performance duty investigator

Toolkit: `.github/scripts/utils/perfomance_tests_status/duty_agent`  
Model: **Python = facts + validate; agent = thinking.**

Input: frozen **perf-duty-context/v1** JSON (OLAP/TPC-C Now `Save` / `Copy context`).  
Outputs: `analysis.md` (plain language) + `result.json` (`perf-duty-result/v1`).

## Priority from Save pack

Read these fields **before** digging covered tickets:

1. **`ticket_coverage`** / `queries[].ticket_coverage` — `uncovered` and `wrong_branch` = проблемы **без** открытого issue на label ветки. Их разбирать **первыми**. `covered` → обычно `update_known` (расширить `affected`), не плодить дубликат.  
   Если fail уже `covered`, а **последующие** query в том же suite/db — `uncovered` **nodata** после abort/cut-off suite — это **тот же** issue: допиши nodata-query в `affected` (`annotate-issue`), не заводи new issues и не оставляй дыры uncovered.  

2. **`hints.investigate_uncovered_first`** / React `new` — человек явно смотрел «new issues».  
3. **`compare.active`** — в heatmap выбран cmp. Это **обязательный** второй прогон, не сноска:
   - `prepare` пишет `compare_focus.json` (Allure `compare.run.report` + fail/slow query из `compare.queries`);
   - разобрать gaps на **`compare.run`** (stderr/logs/coredump как для fail) **и** на `selection.focus_run` (now);
   - в `analysis.md` явно: прогон сравнения `YYYY-MM-DD_sha` vs разбираемый now; дельта cmp→now;
   - `validate` падает, если cmp не упомянут / нет `compare_focus.json` при active fail.
   Нельзя закрыть разбор одним now (даже если now = nodata/ok).

## Goal

For each problem, answer **all** of:

1. **What** broke (detection vs origin — see [`RCA.md`](RCA.md))  
2. **Why / mechanism** — behavior that led here (not just an error string / stack frame)  
3. **Causes** — changes / sequence / what exposed the defect (not «path unchanged»)  
4. **How to fix** — fix direction or decisive experiment  
5. **Who** — PR/login only if evidence bar; else `unknown` (causes still required)  
6. **Since when** — priors / first-fail / sticky metrics  
7. **Verify** H against logs + code; falsified → new H (≤3)  
8. If unclear → `investigate_further` / unknown culprit. **Do not invent confidence.**

## Setup

```bash
cd .github/scripts/utils/perfomance_tests_status/duty_agent
eval "$(python3 dutyctl.py init-token --shell)"   # SANDBOX + YDB SA + AWS S3 keys (YAV)
OUT=./runs/my-case
```

`init-token` loads from [`token_config.json`](token_config.json): sandbox OAuth, SA JSON
(`CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS` → path under `.cache/`), and
`AWS_KEY_ID` / `AWS_KEY_VALUE` for duty-report upload to bucket **`workload-log`**.
Mart access goes through [`../common/ydb_client.py`](../common/ydb_client.py) (YDBWrapper) —
**do not use MCP for YDB**.

## CLI

| Command | Role |
|---------|------|
| `prepare -c CONTEXT -o $OUT` | facts: detect + Allure focus(+fatal) + priors + metrics |
| `dig-runs -c CONTEXT -o $OUT` | Mart history (~35d): execute + summarize; auto `baseline_focus.json` for slow/lat |
| `dig-baseline -o $OUT [-c CONTEXT]` | re-fetch / override baseline Allure plans+logs |
| `dig-prs -o $OUT [--base-sha … --head-sha …]` | product PRs; default window = mart `pr_window` (suite-stable streak end→focus / ydb|lat jump), not pack prev-green / not nearest FailCount=0 |
| `bisect -o $OUT [-c CONTEXT] [--path …]` | code window on **tested** sha vs prev + focus PR files |
| `inject-trace -o $OUT` | rebuild `action_tree.json` + inject `<details>` tree into `analysis.md` |
| `trace-note -o $OUT "…"` | append hypothesis/dig/decision node to the action tree |
| `known-issues --keys … -o $OUT` | open hits (`update_known`) + **`related_closed`** (recently closed, same keys → link on `open_ticket`) |
| `validate -o $OUT` | **quality gate** (+ refreshes action tree under the cut) |
| `write-result -c CONTEXT -o $OUT` | final `result.json` only after validate OK |
| `upload-report -o $OUT [--issue N]` | put report to S3 (immutable stamp dir) + upsert **[полный отчёт]** into issue body; issue# from `--issue` or `Тикет: #N` in analysis. For `wait_next_wave`: `--no-issue` (publishes `duty_decision` index for dashboard **wait next** badge) |

Extra digs: `gh search` / browse code at tested sha. Offline mart: `dig-runs --from-json`. SQL only: `--sql-only`.

## Pipeline (mandatory quality loop)

```text
1) dutyctl prepare -c CONTEXT.json -o $OUT
   → focus.json (now) + **compare_focus.json** when compare.active
2) DIG LOGS / PLANS FIRST when Allure focus exists (never skip):
     fail → kikimr__stderr + kikimr__logs (+ Stderr); crash → coredump playbook
     slow → plan_dig (Stats + Final plan × iterations) + logs; then baseline Allure plan
     If compare.active: same dig on **compare_focus** (fail/slow on compare.run) before
       concluding from now-only nodata/ok.
     Do not block log/plan reading on mart fetch.
3) DIG RUNS FROM DB before writing analysis (mandatory for tpcc + olap):
     Pack suite_history is short — do NOT stop there.
     dutyctl dig-runs -c CONTEXT -o $OUT [--days-before 35|60|90]
       → ping + scan via common/ydb_client → dig_runs_raw.json + dig_runs.json
       → summary.baseline_candidate (good Ydb/lat90 + Report) + baseline_focus.json
         (Allure plans/logs for slow queries + plan_compare vs focus)
     Default neighbors (same branch):
       TPC-C — all ydb_cli_* run_type + all clusters; jump on focus suite; cross_run_type + peer jumps
       OLAP  — related suite families + all DbAlias; fail/ydb jumps; cross_suite + peer_dbs
     If summary.window_edge_hint → widen --days-before and re-run dig-runs
     If baseline missing Report → dig-baseline --report-url … or widen window
4) DIG CODE IN THE MART WINDOW (not only pack prev-green / alert commit):
     dutyctl dig-prs -o $OUT   # после dig-runs; **без** --base-sha из пака
       → окно = dig_runs.summary.pr_window:
         OLAP fail: конец серии suite-ok (≥3 подряд FailCount=0 + Ydb≈median greens)
           → focus; **не** ближайший одиночный FailCount=0 (fluke в красной полосе)
         OLAP slow: largest_ydb_step; TPC-C: largest_lat_step
       Pack prev-green — только fallback, если mart window нет.
       Если сам нашёл более ранний стабильный в mart — используй его (или
       `dig-prs --base-sha <stable> --head-sha <focus>`), не окно алерта.
     dutyctl bisect …          # тот же интервал / crash path
     read hot PR diffs @ tested sha — filter by plan hints (join/scan/kqp/cs)
     В analysis «Кандидаты PR» укажи source окна (stable_streak_end / ydb_step / …).
5) RCA (обязательно) — follow [`RCA.md`](RCA.md) after logs/plans + dig-runs/prs:
     dig (issues search, one winning hypothesis) → в analysis/S3 детали;
     **человеку и в Заключении только** три строки:
     **Проблема / Из‑за чего / Чинить** (см. RCA.md). Не вываливать H1/стеки.
     Culprit only if evidence bar met. Slow: same, axis = plan_compare.
6) Self-check — if fail, dig more (do NOT jump to wait_next_wave early)
7) Write analysis.md + problems.json
   Along the way: dutyctl trace-note -o $OUT --kind hypothesis -- "H1: …"
8) dutyctl inject-trace -o $OUT   # or rely on validate — дерево под <details>
9) dutyctl validate -o $OUT
10) dutyctl write-result …
```

Note: `metrics_delta.json` is produced for slow/tpcc (or `prepare --metrics`); pure `olap_fail` may omit it.

**`wait_next_wave` only after** dig-runs + dig-prs (or explicit skip reason) and no remaining dig that could pin mechanism.

После `wait_next_wave` **обязательно** залить разбор (иначе validate упадёт и на дашборде останется `no ticket`):

```text
dutyctl upload-report -o $OUT --no-issue
dutyctl validate -o $OUT
dutyctl write-result …
```

`upload-report` кладёт `analysis.md` в S3 и пишет публичный `duty_decisions` pointer/index → generate рисует бейдж **wait next** со ссылкой на отчёт.

**Final validation:** do not call `write-result` with `completed` until `validate` exits 0.

### Disconnect / IC cascade ≠ корневая причина (частая ошибка)

Если `kikimr__stderr` **пустой**, а в `kikimr__logs` только `DeadPeer` / `connection closed by peer` / `YDBE-02001` / `detected disconnected node`:

| Понятно | Непонятно |
|--|--|
| почему query красные (каскад после обрыва IC / node lost) | **почему** peer закрыл сессию / умерла нода |

- Пиши в **Итог** явно: «каскад понятен; причина close/peer — нет».
- Гипотеза: `partial` (не `yes`).
- Решение по умолчанию: **`wait_next_wave`** (нужен повтор с abort/core на стороне peer).
- **`no_action` запрещён**, пока нет abort/VERIFY/fline **или** явного infra-вердикта с доказательством (не «просто IC»).

### Mart «зелёный позже»

Строка mart / Allure с FailCount=0 **после** красного на том же Version — валидный повтор (не отбрасывай из‑за `report.window` HTML).  
Но один зелёный **не** превращает IC/disconnect без abort в `no_action`: каскад известен, причина close peer — нет → всё ещё `wait_next_wave` / копай peer.

## OLAP: logs are not optional

For `olap_fail` / any Allure focus:

| Attachment | Role |
|------------|------|
| `kikimr__stderr` | **execution / process** — VERIFY, AFL_VERIFY, SIGABRT, **SIGSEGV / `Received signal 11`**, stacks |
| `kikimr__logs` | **cluster** — connection lost, node down/restart, tablet, IC |
| `Stderr` | query SQL / iteration `statusMessage` surface |
| `descriptionHtml` / `host_dig` | coredumps.yandex-team.ru links + shell recipes (`parallel-ssh` / `unified_agent select` / `journalctl`) |

`prepare` already fetches these into `focus.json`. You **must read them** (via `focus.fatal` + per-case `attach_analysis`).  
Stopping at Allure `statusMessage` / code 2005 **without** stderr+logs dig = failed investigation.

If attachments empty: say so explicitly (`stderr empty` / `logs empty`) and lower confidence.

### Host journals + coredumps (mandatory when crash / node death)

`descriptionHtml` «запросы к журналу» — это **shell-рецепты**, не YDB SQL. Не пытайся гонять их через mart/MCP.

**Порядок (после чтения вложений):**

1. **Сначала вложения** — `kikimr__stderr` / `kikimr__logs` уже содержат результат `unified_agent select` / stderr слота. Ищи `Received signal 11|6`, `SIGSEGV`/`SIGABRT`, `VERIFY`, `Backtrace:`, `Registered as <nodeId>` (рестарт после abort).  
2. **Coredump** — если в `attach_analysis.host_dig.coredump_urls` / description есть `coredumps.yandex-team.ru`, открой UUID-ссылку. На хосте падения (из `Node NNN@host` / `host_dig.hosts`):
   ```bash
   ssh <host> 'ls -la /place/coredumps/*$(date -d @<approx_ts> +%s 2>/dev/null || true)* 2>/dev/null; ls /place/coredumps/backtrace_kikimr_* /place/coredumps/sended_kikimr_*.json | tail -20'
   # конкретный dump около времени падения:
   ssh <host> 'cat /place/coredumps/sended_kikimr_<slot>_<unix_ts>.json'   # → url_v3 / traceback_fingerprint_v3
   ssh <host> 'grep -nE "Program terminated|BufferReader|VERIFY|TWorker|#0 " /place/coredumps/backtrace_kikimr_<slot>_<unix_ts>.dmp | head'
   ```
   В секцию P\* отчёта: URL coredump + 1–2 ключевые frame.  
   В **Materials / GitHub issue Body** (`#### Детали ошибки`): **полный** backtrace из `kikimr__stderr` (`#0`…последний кадр, без «…») **и** кликабельный `coredumps.yandex-team.ru/v3/cores…` из `focus.fatal.coredump_urls` / `host_dig` — не плейсхолдер «filter URL в descriptionHtml».
3. **Повтор рецептов с хоста** — только если вложений мало / нет abort, а node down остаётся загадкой:
   ```bash
   # окно времени — из descriptionHtml (MSK/UTC как в рецепте)
   ssh <host> 'sudo journalctl -k -S "YYYY-MM-DD HH:MM:SS" -U "…" --grep ydb --no-pager'
   ssh <host> 'ulimit -n 100500; unified_agent select -S "…+03:00" -U "…+03:00" -s kikimr' | grep -Ei 'signal|VERIFY|Fatal|Received signal'
   ```
   Пустой `journalctl -k --grep ydb` **нормален** при user-space SIGSEGV — не останавливай разбор на «kernel empty».  
4. **Не путать** abort A (`workers_pool` VERIFY / signal 6) с abort B (Arrow `BufferReader` / signal 11) и с «просто 2005» на соседнем query (следствие disconnect).

`focus.fatal` после `prepare` несёт `coredump_urls` / `journal_cmds` / `signals` — используй как чеклист, не как ответ.

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
| `olap_slow` | **plans per iteration** + server logs + **baseline Allure plan** + dig-runs + metrics_delta + dig-prs |
| `olap_nodata` | Pack `query_counts` / nodata samples / incomplete `SuccessCount`. **First:** Allure/report for those queries — then branch (lag vs real gap). |
| `tpcc_tpmc` / `tpcc_lat` | Allure focus (+ stderr/logs when URL present) + **dig-runs** (`perfomance/tpcc`, `Report` from `tests_results`) + **dig-prs** + metrics + DataLens |
| `mixed` | split problems |

### OLAP nodata (mandatory when seeded)

`detect_type` seeds `olap_nodata` when Now has query gaps or incomplete SuccessCount.  
**Forbidden:** ignore nodata because `suite_now.issue=ok`, or jump straight to IC/crash logs, or chase only a sibling-suite fail on the same Allure.

**Playbook (order matters):**

1. **Список дыр** — какие query в Now `nodata` / какой `SuccessCount` (из pack `query_counts` / samples).  
2. **Открыть Allure/report** того же прогона (`focus_run.report`) и проверить **именно эти** query:
   - **В отчёте они ok / passed** → вывод: **в базу (mart / daily) данные ещё не доехали** (лаг выгрузки/агрегации). Не копать stderr/кластер как продуктовую аварию. Решение обычно `wait_next_wave` / `no_action` + перепроверить mart на следующем refresh.  
   - **В отчёте тоже нет / skipped / failed / нет кейса** → это уже реальный gap прогона: копать `kikimr__stderr` + `kikimr__logs`, при необходимости журналы на кластере (хосты из логов). Дальше — как `olap_fail` / инфраструктура.  
3. **Следствие abort (обязательно при `update_known` / `open_ticket`):** если suite оборвался на fail/crash (SIGSEGV, VERIFY, abort) и nodata — **хвост того же прогона** (кейсы после cut-off / SuccessCount не дописан), это **не** отдельный баг и **не** `no_action` «забыть в Now».  
   - В `problems.json` / P\* nodata можно пометить как следствие P1.  
   - **Источник истины для списка дыр** (не только sample в `queries[]`):  
     `ticket_coverage.uncovered_queries` ∪ `selection.focus_run.uncovered_queries` ∪ pack `queries` kind=nodata.  
     Сверь с `suite_now.n_nodata` / `query_counts.nodata` — если `n_nodata=43`, а в `queries[]` меньше имён, **не** ограничивайся sample; бери полный `uncovered_queries` (в т.ч. `Infrastructure error` и хвост QueryNN).  
   - **Обязательно** все эти имена попадут в `perf-duty-match` → `affected` **того же** issue:  
     `dutyctl annotate-issue --issue N --suite … --db … --queries Query17 Query18 … --no-comment`  
   - И в Materials Body `queries: […]` тоже перечисли fail **и** полный nodata-хвост.  
   - Перед `write-result`: пересохрани/проверь Now — `ticket_coverage.status` должен стать `covered` (нет хвоста в `uncovered_queries`).  
   Иначе generate/Now покажет nodata как uncovered / new issues, хотя корень уже в тикете.  
4. **dig-runs** — сверить Now vs mart `SuccessCount` / `YdbSumMeans` на том же Version/Report (подтверждает lag или устойчивую дыру).  
5. В `analysis.md` явно написать ветку: «отчёт ok → не доехали» **или** «в отчёте тоже нет → логи/кластер» (+ при abort: «nodata = следствие, в affected #N»).

`validate` fails if nodata is seeded but report-check branch is missing, **or** if fail+nodata (report gap) + ticket resolution but Materials `affected` omits names from pack nodata **or** `ticket_coverage.uncovered_queries` / focus-run uncovered.

### OLAP slow / duration growth (mandatory when seeded)

`olap_slow` / soft regression — **не** сводить к `ydb_pct` и списку PR. Нужен план и сравнение с нормальным прогоном.

**Playbook (order matters):**

1. **Какие query** — из `detect_type` / pack `queries` (`kind=slow|both|soft`) + `metrics_delta.queries`.  
2. **`prepare` уже тянет** эти кейсы из Allure (даже если `passed`) и кладёт `attach_analysis.plan_dig`:
   - `Stats` / Mean  
   - `Plan table` (Explain)  
   - `Final plan table|json|stats` **по Iteration 0..N**  
   - плюс `kikimr__stderr` / `kikimr__logs` (не только для fail)  
3. **На медленном прогоне:**
   - сравнить планы **между итерациями** (`plan_dig.plan_changed_across_iterations`, hints: Lookup/GraceJoin/FullScan/…);  
   - если план стабилен, а duration прыгает — смотреть логи сервера (CPU steal, spilling, tablet, IC) в окне query;  
   - если план меняется между итерациями — это уже сигнал нестабильности оптимизатора/статистики.  
4. **Baseline (нормальная продолжительность):** `dig-runs` сам выбирает `summary.baseline_candidate`
   (предпочтительно точка `largest_ydb_step.from` / latest better + `Report`) и пишет `baseline_focus.json`
   + `plan_compare` (hints focus vs baseline). Читай это; при пустом Report — `dutyctl dig-baseline` / pack `suite_history.reports`.  
   В отчёте: Version/label baseline + «план совпал / разъехался» (`plan_compare.verdict`).  
5. Если `plan_same` — копай логи baseline **и** focus (`kikimr__logs` в обоих) на runtime/infra.  
6. **Код в окне jump (обязательно, как для fail/TPC-C):**
   - `dutyctl dig-prs -o $OUT` (окно из mart `pr_window` / suite-stable streak / ydb jump) — не PR алерт-коммита и не pack prev-green / не ближайший FailCount=0.  
   - Отфильтруй hot PR по `plan_compare` / hints:
     - `plan_regressed` → kqp / optimizer / statistics / columnshard reader / join  
     - `plan_same` → runtime / CS execute / conveyor / memory / IC (или infra)  
     - `unstable_across_iterations` → stats/cache/planner nondeterminism  
   - `dutyctl bisect --path …` на подозрительный файл/директорию из diff.  
7. **Hypothesis loop (≤3 в работе → 1 в отчёте):** H1 из plan_compare → проверить diff@tested sha + логи + GH issues search; falsified → следующая; в analysis оставить только победителя.  
   Виновник = PR только при evidence bar (files ∩ path **или** path changed in window). Иначе `unknown` / candidate.  
8. В `analysis.md`: механика — `plan_regressed` | `plan_same_runtime_regressed` | `unstable_across_iterations` | `infra/logs`;  
   кандидаты PR таблицей из `dig_prs.json`; **Гипотеза проверена:** yes|partial|no.

**Forbidden:** `wait_next_wave` / blame PR только по `ydb_pct` без plan dig, baseline и dig-prs.  
`validate` fails without plan/iteration/baseline **and** without `dig_prs.json` (or explicit skip).

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

1. `prepare` → lat/tpmC vs prev-7 in pack (short). If `focus_run.report` is set (Allure from Now join) — **read** `focus.json` attachments (`kikimr__stderr`, `kikimr__logs`, Stderr) like OLAP fail.  
2. `dig-runs` (default ~35d, neighbors) → all `ydb_cli_*` on **all clusters**, same branch; slice_runs include `Report` when join hits. Use `largest_lat_step` on focus suite + `cross_run_type` + `peer_clusters_latest` (often jump ≠ alert commit). Widen window if edged.  
3. Use harness knowledge to filter PRs (SnapshotRW vs StrictSerializable; WH scale).  
4. `dig-prs -o $OUT` after dig-runs (mart `pr_window` suite-stable streak / lat jump — not pack prev-green / not nearest FailCount=0).  
5. Only then: `wait_next_wave` / `investigate_further` / candidate.  
**Forbidden:** skip Allure when URL is in the pack / pack metrics only / blame alert-commit PR without dig-runs/dig-prs.  
**Report:** no harness dump unless one sentence narrows the product cause. Cross-suite = search filter, not root cause.  
**Allure source:** mart `perfomance/tpcc` has no URL; Now + dig-runs join `perfomance/olap/tests_results` (`TpccW{WH}T0Snapshot|Serializable`, `oltp-perf-N` ↔ `perfN`).

### OLAP playbook (mandatory dig-runs)

1. `prepare` → Allure focus + fatal + pack history (+ **`compare_focus.json`** если `compare.active`). **Read `detect_type.json` seeds first** — включая `seed_compare_*` / `compare_fail_seeded`.  
1b. If `compare.active`: разобрать **прогон сравнения** (`compare.run` / `compare_focus`) как отдельный P\* при fail/slow/nodata на cmp; затем now. Нельзя `no_action` только по now, пока cmp fail не закрыт.  
2. If `olap_nodata` (or `n_nodata>0`) **на now**: **сначала отчёт** по этим query (см. playbook nodata выше). Логи/кластер — только если в отчёте тоже дыра. Не подменять now-nodata разбором fail с **другого** прогона (cmp) и наоборот — это разные P\*.  
3. If `olap_slow`: follow **OLAP slow / duration growth** end-to-end  
   (plans × iterations → baseline_focus/plan_compare → logs if plan_same → **dig-prs + bisect + H-loop**).  
4. **Read logs** from `focus.json` when `olap_fail` **or** nodata branch = «в отчёте тоже нет» **or** slow path needs server-side evidence (`kikimr__stderr` + `kikimr__logs`).  
5. If signals include `segfault` / `abort` / `verify`, or status is 2005+node-down: follow **Host journals + coredumps** (coredump URL / `/place/coredumps` / optional journalctl). Prefer stack over surface 2005.  
6. `dig-runs` (~35d+, ydb_client) → related suites + **peer DbAlias**. Для nodata — Now vs mart SuccessCount/YdbSumMeans. Для slow — auto `baseline_focus` + ydb jump window for dig-prs.  
7. Metrics + **dig-prs/bisect обязательны для slow/fail** (для «не доехали» — нет). dig-prs без `--base-sha` из пака — окно из mart.  
   Для fail: dig-prs / `git log` расширяй на пути из **гипотез происхождения** (producers), не только fline из стека.  
8. Use harness paths above when mechanism depends on upload vs query suite.  
9. **RCA.md** — гипотезы + причины + как починить (см. step 5). Запрещено закрыть причины фразой «файл из трейса не менялся».

## Evidence bar (culprit)

1. Path/symbol **∩** PR files, or  
2. Mechanism + bisect shows path **changed** in prev…first-fail, or  
3. Metrics-only → **candidate** if weak.

Focus-wave PR alone forbidden. Unchanged crash path → do not blame that PR.

## Self-check (before validate)

For each problem, you can answer yes:

- [ ] TPC-C / OLAP: `dig_runs.json` from mart (neighbors + ≥~month if needed), not only pack metrics  
- [ ] TPC-C / OLAP fail|slow: `dig_prs.json` on mart `pr_window` / jump (suite-stable streak→focus or ydb|lat step), not nearest FailCount=0 / not pack prev-green alone  
- [ ] Если `compare.active`: `compare_focus.json` + в отчёте прогон сравнения (label/sha) + gaps; fail на cmp → stderr/coredump; дельта cmp→now  
- [ ] OLAP nodata: checked Allure/report for those queries; wrote branch «не доехали» **or** «в отчёте тоже нет → логи/кластер»  
- [ ] OLAP nodata после abort/cut-off: **все** имена из `ticket_coverage.uncovered_queries` (+ pack nodata; в т.ч. `Infrastructure error`) в `affected` того же issue; после annotate Now `ticket_coverage` = covered  


- [ ] OLAP slow: plans × iterations + `baseline_focus` / plan_compare + dig-prs on ydb jump + H-loop (≤3) + culprit only with evidence  
- [ ] OLAP slow: if plan_same — server logs focus+baseline; if plan_regressed — code/bisect on planner/CS path  
- [ ] Correlations checked: other run_type/suite and peer cluster on same branch  
- [ ] Read cluster logs **and** execution stderr (OLAP fail), or documented empty  
- [ ] On segfault/abort/VERIFY: coredump URL or `/place/coredumps` dig (or explicit skip why)  
- [ ] При `open_ticket` + signal/Backtrace: в Materials полный стек `#0…#N` из stderr **и** кликабельный `coredumps.yandex-team.ru` (не «filter URL в descriptionHtml»)
- [ ] После заведения тикета: `Тикет: [#N](url)` в analysis → `upload-report -o $OUT` → **[полный отчёт]** в body
- [ ] Mechanism stated (not only fingerprint)  
- [ ] Tied to code at **tested sha**  
- [ ] RCA: в Заключении **только** статус **Проблема / Из‑за чего / Чинить** (по-русски; см. RCA.md)  
- [ ] RCA: dig сделан (GH issues search + одна гипотеза в analysis/S3); человеку длинный разбор не дублировать  
- [ ] RCA: **не** «path не менялся» как единственная причина  
- [ ] Давность stated with dates/labels  
- [ ] Hypothesis verified or explicitly `partial`/`no`  
- [ ] Culprit only if evidence bar met (гипотеза/причина ≠ автоматический Виновник)  
- [ ] Если только IC/disconnect и stderr empty → в Итоге разделены каскад vs причина; решение **не** `no_action` без причины close peer  
- [ ] При IC DeadPeer: копнут **peer**-сторону (хост/node из YDBE/DeadPeer), не только клиентский disconnect  


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

**Читаемость (обязательно):**
- В **Заключении** каждый пункт — 1–2 коротких предложения; не сваливать sha + PR + давность + гонку в один абзац.
- **Давность** раздели: (1) что подтверждено на разбираемом прогоне; (2) с какого label suite/метрика красные; (3) с какого PR строка в коде. Если fline раньше не смотрели — напиши явно.
- При **Виновник: unknown** не тегай авторов истории файла как виновников; если упоминаешь PR введения строки — «история кода, не виновник».
- Числа вроде «доля падений 0.17 vs ~0.05» — одна фраза, что это значит для читателя.
- **Один P\* = одна корневая причина.** Abort/crash ноды и соседние query с `code: 2005` / node down / connection lost на том же прогоне — **один** пункт P1 (причина + следствие в одном блоке). Не плодить P2 «Query0N упал с 2005» как будто это отдельная проблема.
- Второй P\* только если есть **другая** корневая причина (другой fline/сигнал/механизм) или отдельный класс (например nodata = лаг mart vs реальная дыра). Иначе — не заводить P2.
- Следствия того же abort — не отдельный issue; в P1 перечисли затронутые query одной строкой.

**Issue = copy-paste + findable:** при `open_ticket` / `update_known` обязательны `### Title` и `### Body`.  
Body (порядок): `#### Фактура` → `#### Что сломалось` → `#### К чему приводит` → `#### Из‑за чего` → `#### Чинить` → `#### Детали ошибки` → `#### Код` → `<!-- perf-duty-match -->`.  
«Из‑за чего» / «Чинить» — тот же смысл, что статус в чате; **обязательны** при `open_ticket` / `update_known`.  
**Не** писать в Body/отчёте: «Отчёты (соседи)», Search keys / `gh search` в первом экране,  
и **запрещены** фразы «это не #N», «не путать с…», «не смешивать с…» — антипаттерн (fingerprint уже разделяет).  
Keys — только в match-блоке. Для людей: **node down / connection lost**, не «просто 2005».  
Label **не нужен**. Шаблон: [`REPORT_TEMPLATE.md`](REPORT_TEMPLATE.md).

**Перед `open_ticket`:**  
1. Ключи из fingerprint (`file.cpp:NN`, `AFL_VERIFY(…)`, symbol) — **не** suite alone.  
2. `dutyctl known-issues --keys 'read.cpp:59' 'range.Offset' -o $OUT` (или `gh` + parse блоков).  
   Пишет `known_issues.json`: **`open_hits`** + **`related_closed`** (recently closed, тот же keys overlap).  
3. Если **open** hit → `update_known`: `dutyctl annotate-issue --issue N --suite … --db … --queries …` (расширяет `affected` в body; **по умолчанию без коммента**). Хватает match-блока + `upload-report` (Duty report в Фактуре). Если нужен timeline-коммент — **после** dig стека/coredump и `upload-report`:
   `dutyctl annotate-issue … --sighting-from $OUT` («Повтор»: branch / commit / Allure / Duty report / coredump + **полный** backtrace `#0…#N` из `host_dig/*crash_stack*` или `stderr/`).  
   **Запрещены** голый `also seen` и таблица без стека при segfault/VERIFY.  
   **Чеклист `annotate-issue` (один вызов или два):**  
   1) fail/crash query(и) с доказательством;  
   2) **полный** nodata/uncovered хвост того же suite/db после abort (не лаг mart):  
      все из `ticket_coverage.uncovered_queries` / `focus_run.uncovered_queries`, не только первые N из `queries[]`.  
   Пример: `--queries Query03 Query17 … Query42 'Infrastructure error' --no-comment`.  
   Не закрывай разбор, пока `ticket_coverage.uncovered_queries` не пуст (status `covered`) тем же `#N`.
4. Если open нет, но есть **`related_closed`** → `open_ticket` **ок** (post-close / uncovered), но **обязательно** связать:
   - в Фактуре строка `| Related closed | [#N](url)… |`;
   - в **Чинить** / Заключении — «здесь [#new](…); заодно закрытый [#N](…)»;
   - в `Issues (поиск):` — явно перечислить closed hits.  
   Не оставлять «no open matches» без упоминания closed того же fingerprint.
5. Context `known_tickets` + `ticket_coverage` из Save — стартовые кандидаты; **uncovered** в pack = кандидат на новый issue (после search).  
6. Если `compare.active` — сверь симптом на `compare.run` и на `focus_run` (появилось на now / уже было на cmp).

**После `open_ticket` / `update_known` — полный отчёт в S3 (обязательно):**  
1. Заведи / обнови issue вручную (Materials Title+Body) — отдельный шаг по команде человека.  
2. В analysis напиши `Тикет: [#N](https://github.com/ydb-platform/ydb/issues/N)`.  
3. `dutyctl upload-report -o $OUT` — один шаг:
   - заливает в `s3://workload-log/perfomance_tests_status/duty_artifacts/{run_id}/{utc_stamp}/`  
     (новый stamp каждый раз → без перезаписи);
   - нужен `boto3` (`pip install boto3` или `.cache/venv-s3`);
   - для **нового** issue / пустой Фактуры пишет в body:  
     `| Duty report | [полный отчёт](…) · [result](…) · [problems](…) |`;
   - если в issue уже есть Duty report **другого** `run_id` — **не** перезаписывает  
     первичную Фактуру (это opening report). Ссылки текущего прогона — в комментарии  
     `annotate-issue --sighting-from $OUT`.  
   Явный override: `--issue N`. Только upload: `--no-issue`.  
4. `dutyctl validate` после upload — если тикет уже указан, без `s3_report.json` / без «полный отчёт» будет error.  
Не клади весь `analysis.md` в body.

**Как следующий агент ищет:** overlap `keys` в open issues с блоком `perf-duty-match`. Suite/query только в `affected` (растёт при новых проявлениях).

**Backfill:** уже открытые [#47862](https://github.com/ydb-platform/ydb/issues/47862) / [#47870](https://github.com/ydb-platform/ydb/issues/47870) / [#47871](https://github.com/ydb-platform/ydb/issues/47871) — один раз `dutyctl annotate-issue` (блок в body), иначе generate их не покажет.

```markdown
# Perf duty — {suite} @ {db} — {focus_label}

## Заключение
**Сначала — статус для человека (обязательно, см. RCA.md):**
- **Проблема:** …
- **Из‑за чего:** …
- **Чинить:** …

Затем служебное (токены duty; человеку в чате не повторять вместо статуса):
- **Решение:** `investigate_further` | `open_ticket` | `update_known` | `wait_next_wave` | `no_action`
- **Виновник:** unknown | @{login} / [PR #N](url)
- **Уверенность:** высокая | средняя | низкая
- **Критичность:** HIGH | MEDIUM | LOW
- **Давность:** …

## Проблемы
### P1 — …
- … + Тикет: …

## Гипотезы происхождения / Причины / Как починить
Только в analysis (S3); одна H; см. [`RCA.md`](RCA.md). **Не** копировать в чат человеку.

## Что дальше
Опционально 1–3 пункта для дежурного. **Запрещено:** инструкции агенту (`gh search`, «dutyctl …»).

## Материалы для issue
### Title
{fingerprint + suite — одна строка}

### Body
#### Фактура

| | |
|--|--|
| Suite / DB | … / … |
| Branch · Version | main · [`sha`](…) |
| Run | label · ts UTC |
| Allure | https://proxy.sandbox… |
| Duty report | [полный отчёт](https://storage.yandexcloud.net/workload-log/…/analysis.md) · [result](…) · [problems](…) |
| Failed | Query… |
| Related closed | [#N](…) — тот же fingerprint (если `known-issues` → `related_closed`); иначе строку не пиши |

**Обязательно** строки `| | |` и `|--|--|` перед данными — без них GitHub **не** рисует таблицу. То же для `#### Код`.

#### Что сломалось
…

#### К чему приводит
- …

#### Из‑за чего
…

#### Чинить
уже [#N](…) / здесь; …

#### Детали ошибки
Host: `…`  
Coredump: https://coredumps.yandex-team.ru/v3/cores?filter=…   ← из focus.fatal / host_dig
```
Received signal 11
Backtrace:
#0 …
…
#N …   ← полный стек из kikimr__stderr, без «…» между кадрами
```

#### Код

| | |
|--|--|
| Место падения | path @ sha |
| Связанный issue | нет / #N |

<!-- perf-duty-match
kind: olap
fingerprint: file.cpp:NN
keys:
  - file.cpp:NN
  - AFL_VERIFY(…)
affected:
  - suite: {suite}
    db: {db}
    queries: [QueryFail, QueryNodataTail…]  # fail + полный uncovered_queries (не sample)
-->
```

Also update `$OUT/problems.json`. При `update_known` — расширь `affected` в GitHub issue (`annotate-issue`), не только analysis.md. Nodata-хвост после abort — в том же `queries` списке (= весь `ticket_coverage.uncovered_queries`, включая `Infrastructure error` при suite wipe).

## Ход разбора (дерево под кат)

Пайплайн пишет `action_tree.json`. Перед validate / через `inject-trace` в `analysis.md`
появляется секция **«Ход разбора»** с GitHub `<details>` — ASCII-дерево для человека:
что сделали → что нашли (без Python-repr и без дублей «Сводка по артефактам»).

- CLI stages логируются сами (Подготовка / mart / PR / путь в коде).  
- Агент **обязан** добавлять смысловые узлы по-русски (итог dig, гипотеза, решение):
  ```bash
  dutyctl trace-note -o $OUT --kind hypothesis -- "H1: plan_regressed → kqp join"
  dutyctl trace-note -o $OUT --kind dig --detail "stderr: нет VERIFY" -- "читал kikimr__stderr"
  dutyctl trace-note -o $OUT --kind decision -- "виновник unknown; wait_next_wave"
  ```
- Не дублируй дерево в Заключение — только под катом.  
- `validate` сам обновляет кат (одна актуальная сводка артефактов); ручной `inject-trace` — если правишь analysis без validate.

## Rules

- Prefer crash stack + mechanism over surface 2005.  
- Prefer coredump / `Received signal N` stack over «kernel journal empty».  
- Prefer **unknown** over a confident lie.  
- No mute without human. Secrets out of reports.  
- `validate` is the quality gate — treat failures as missing investigation, not as “tweak wording only” when logs/bisect were skipped.

## Tests

```bash
python3 tests/test_duty_agent.py
# fixtures: tests/fixtures/*.json
```
