# Perf duty — {suite} @ {db} — {label}

## Заключение
Каждый пункт — **1–2 коротких предложения**. Не сваливать sha/PR/давность/гонку в один абзац.

- **Итог:** <симптом → механизм одной фразой>
- **Решение:** `open_ticket` | `update_known` | `investigate_further` | `wait_next_wave` | `no_action` — <что сделать>
  - при `wait_next_wave`: после analysis → `dutyctl upload-report -o $OUT --no-issue` (S3 + dashboard badge **wait next**), затем `validate`
- **Виновник:** `unknown` | `@{login}` / [PR #N](https://github.com/ydb-platform/ydb/pull/N) — <доказательство>
  - При `unknown` авторов истории файла **не** пиши как виновников; если упоминаешь PR введения строки — пометь «история кода, не виновник»
- **Уверенность:** высокая | средняя | низкая — <на чём высокая / где только следствие>
- **Давность:** раздели явно:
  - подтверждено на разбираемом прогоне `…`;
  - suite/метрика красные с `…` (если fline раньше не смотрели — так и напиши);
  - строка в коде с [PR](url) / даты
- **Механика:** <поведение системы → поломка> без лишних sha

## Проблемы

Один P\* = одна корневая причина. Crash/abort + соседние `code: 2005` на том же прогоне — **один** P1 (следствие query перечисли строкой). P2 только при другом механизме / отдельном классе (nodata lag и т.п.).

### P1 — <короткое имя>
- Тип: `olap_fail` | `olap_slow` | `tpcc_tpmc` | `tpcc_lat`
- Что сломалось: … (включая следствия: Query0N/0M → 2005 после abort)
- Почему / механика: … (для slow: `plan_regressed` | `plan_same_runtime_regressed` | `unstable_across_iterations` | …)
- Логи: `kikimr__stderr` …; `kikimr__logs` … (или явно пусто); при crash — coredump URL / `/place/coredumps`
- План (если `olap_slow`): Explain / Final plan по итерациям; сравнение с baseline Allure
- Код ([`{sha}`](https://github.com/ydb-platform/ydb/commit/{sha})): [файл](blob-url) / функция …
- Кто (если есть): … + доказательство (или `unknown`)
- Давность: …
- Гипотеза проверена: `yes` | `no` | `partial`
- Связанный issue: [#{N}](https://github.com/ydb-platform/ydb/issues/{N}) или нет
- Тикет: …

## Кандидаты PR (если есть)
Окно = mart `pr_window` (конец suite-stable streak FailCount=0+Ydb≈median / ydb|lat jump → разбираемый sha), не ближайший одиночный FailCount=0 и не prev-green из пака. В заголовке укажи окно **и** `source` из `dig_prs.json` / `dig_runs.summary.pr_window`.

Не список голых ссылок. Таблица:

| Влито (UTC) | PR | Title | Автор | Почему |
|--|--|--|--|--|
| 2026-07-22 11:11 | [#46638](https://github.com/ydb-platform/ydb/pull/46638) | … | @login | … |

Поля из `dig_prs.json`: `merged_at`, `pr_url`, `pr_title`, `author_login`, `window_source`.

## Что дальше
Только для дежурного (не чеклист агента). Пример:
1. Тикет: [#N](url) — комментарий при повторе
2. Coredump на `<host>` около `<ts>`, если нужно докопать стек

Запрещено: `gh search`, «Перед заведением», «скопировать Title/Body», команды `dutyctl`,  
списки «это не #N / не смешивать с #M» (fingerprint в Title / match-keys уже разделяет).

## Ход разбора

Под катом — дерево действий (`dutyctl inject-trace` / `validate` обновляет само).  
Сюда же попадают `trace-note` (гипотезы / dig / решение).

<!-- duty-action-tree:start -->
<details>
<summary>Дерево разбора (от начала до конца)</summary>

```
(будет заполнено inject-trace / validate)
```

</details>
<!-- duty-action-tree:end -->

## Материалы для issue

При `open_ticket` / `update_known` блок **Title + Body** копируется в GitHub **целиком**.  
Цель Body: **сначала человек**, потом машина. Dashboard / следующий агент находят issue по скрытому `<!-- perf-duty-match -->` (`keys` + `affected`). Label **не нужен**.

Title: fingerprint + suite (для списка issues и поиска). Не сокращай уникальный `file.cpp:NN` / symbol.

### Title
```
{OLAP|TPC-C}: {fingerprint/symbol} ({file.cpp:NN}) on {suite}
```

### Body

Порядок секций **строгий** (всё без `<details>`, кроме опционального хвоста).  
**Не** писать: «Отчёты (соседи)», `Search keys` / `gh search` в первом экране,  
и **никогда** абзацы/буллеты «это не #N», «не путать с #M», «не смешивать с #K» — антипаттерн.  
Fingerprint в Title + `keys` в match-блоке достаточно. В «Код → Связанный issue» — только реально связанный тикет или «нет».  
В тексте для людей: **node down / connection lost**, не «просто `code: 2005`».

#### Фактура
Короткая таблица **первая** в Body (из context).

**GFM header обязателен:** первая строка `| | |`, вторая `|--|--|`, потом данные.  
Без separator GitHub **не** рендерит таблицу (частая поломка markdown в issues).

| | |
|--|--|
| Suite / DB | `{suite}` / `{db}` |
| Branch · Version | `{branch}` · [`{sha}`](https://github.com/ydb-platform/ydb/commit/{sha}) |
| Run | `{label}` · `{ts}` UTC |
| Allure | https://proxy.sandbox.yandex-team.ru/{id}/index.html |
| Duty report | [полный отчёт](https://storage.yandexcloud.net/workload-log/perfomance_tests_status/duty_artifacts/{run_id}/{stamp}/analysis.md) · [result](…) · [problems](…) |
| Failed | Query… (кратко: VERIFY / node down) |
| Related closed | [#N](url) — тот же fingerprint из `known-issues` → `related_closed` (если есть) |

`Duty report` — после `dutyctl upload-report -o $OUT` (нужен `Тикет: #N` или `--issue N`).  
Bucket `workload-log`, путь с `{stamp}` (immutable). Labels: **полный отчёт** / result / problems.  
Не весь отчёт в body.  
Без Kind / CI version / Fingerprint / Search keys в этой таблице — они в Title и в `perf-duty-match`.  
`Related closed` — только если `dutyctl known-issues` вернул recently-closed с overlap keys; при `open_ticket` их **нужно** упомянуть (новый тикет ок, но связать). Строку без hits не добавляй.

#### Что сломалось
1–3 предложения: симптом → где упало (функция/путь). Без списка «не путать с …».

#### К чему приводит
Буллеты impact: crash/abort ноды; какие query fail; что видит Allure (**node down / connection lost**); suite FailCount.

#### Из‑за чего
По-русски: кто реально ломает. Если кадр стека — только место падения, так и скажи; корень ещё не найден — напиши прямо. **Не** «файл не менялся».

#### Чинить
Уже [#N](url) / [PR#M](url) — **или** здесь (этот issue); **заодно** закрытые/связанные [#A](url)… из `related_closed`. Одной–двумя фразами что делать.

#### Детали ошибки
Цитата VERIFY / `Received signal N` / backtrace **открытым** code-блоком (не под катом).

При crash (`Received signal` / `Backtrace:`):
- **полный** стек из `kikimr__stderr` — от `#0` до последнего кадра, **без** «…» / вырезанных `#7 … #16`;
- **кликабельный** URL `https://coredumps.yandex-team.ru/v3/cores…` из `focus.fatal.coredump_urls` / case `host_dig` (filter-query ок; UUID лучше);
- **запрещено:** «Coredump: filter URL в descriptionHtml» и другие плейсхолдеры без ссылки;
- если URL реально нет — явно `coredump skipped` + почему (нет UUID / нет signal в stderr этого кейса).

Host / node — строкой над или под code-блоком.

#### Код

Та же GFM-шапка `| | |` / `|--|--|` (иначе таблица не рендерится):

| | |
|--|--|
| Место падения | [path:line @ sha](blob-url) |
| Связанный issue | нет / [#N](url) только если это **тот же** баг (`update_known`) |

Подробности разбора — в Duty report (`analysis.md`). См. `RCA.md`.

<!-- perf-duty-match
kind: olap
fingerprint: workers_pool.cpp:117
keys:
  - workers_pool.cpp:117
  - AFL_VERIFY(found)
  - PutTaskResults
affected:
  - suite: UploadTpch1000
    db: sas_big_column
    queries: [Query03, Query01, Query05]
-->

**Обязательный** скрытый блок `<!-- perf-duty-match … -->` в конце Body:

- `keys` — стабильные токены ошибки (**без** обязательного suite); сюда же то, что раньше клали в Search keys.
- `affected` — `suite` / `db` / `queries`; при `update_known` **дописывай** (`dutyctl annotate-issue`), не плоди issue.
- Если suite оборвался и в Now **nodata** на последующих query (следствие abort, не лаг mart) — **все** эти query тоже в `queries` того же `affected` / того же issue. Иначе dashboard покажет их как uncovered.
- Generate ищет open issues с `perf-duty-match`, матчит inbox по `affected`, показывает `#N · title`.

---

Язык: обычный русский. Английский — термины (`VERIFY`, имена файлов, SIGSEGV).  
Не писать: фокус, волна, RC, last touch, priors, sticky, prev-green, jargon агента, `gh search` в Body.  
Issue / PR / commit / sandbox — markdown-ссылками.  
Quality: self-check → `dutyctl validate` → `write-result`.  
При `open_ticket` / `update_known`: создать issue → `Тикет: #N` → `dutyctl upload-report -o $OUT` → validate. См. `AGENTS.md`.
