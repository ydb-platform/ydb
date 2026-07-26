# Perf duty — {suite} @ {db} — {label}

## Заключение
Каждый пункт — **1–2 коротких предложения**. Не сваливать sha/PR/давность/гонку в один абзац.

- **Итог:** <симптом → корневая причина одной фразой; если не смешивать с известным тикетом — скажи явно>
- **Решение:** `open_ticket` | `update_known` | `investigate_further` | `wait_next_wave` | `no_action` — <что сделать>
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
Окно = mart `pr_window` (последний стабильный FailCount=0 / ydb|lat jump → разбираемый sha), не «просто» prev-green из пака. В заголовке укажи окно **и** `source` из `dig_prs.json` / `dig_runs.summary.pr_window`.

Не список голых ссылок. Таблица:

| Влито (UTC) | PR | Title | Автор | Почему |
|--|--|--|--|--|
| 2026-07-22 11:11 | [#46638](https://github.com/ydb-platform/ydb/pull/46638) | … | @login | … |

Поля из `dig_prs.json`: `merged_at`, `pr_url`, `pr_title`, `author_login`, `window_source`.

## Что дальше
1. … (для `open_ticket`: «вставить Title+Body из блока ниже»)
2. Перед заведением: `gh search issues "<search keys>" --repo ydb-platform/ydb`

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
Цель Body: человек понимает баг; **следующий duty-агент находит issue** по стабильным ключам (`gh search issues "fline|file.cpp:NN|symbol"`).

Title: коротко + уникальный fingerprint (`file.cpp:NN` / symbol / suite).

### Title
```
{OLAP|TPC-C}: {fingerprint/symbol} ({file.cpp:NN}) on {suite}
```

### Body

#### Фактура
Обязательная таблица — первая в Body. Значения копируй из context (`selection.*`, `suite_now.*`).

| Поле | Значение |
|--|--|
| Kind | `olap` \| `tpcc` |
| Branch | `{branch}` (напр. `main`) |
| Version | `main.{sha}` (как в mart / Allure Version) |
| CI version | `{ci_version}` (напр. `trunk.r20455406`) |
| Suite | `{suite}` |
| DB / cluster | `{db}` |
| Run label | `{label}` |
| Run time (UTC) | `{ts}` |
| Commit | [`{sha}`](https://github.com/ydb-platform/ydb/commit/{sha}) |
| Allure / Sandbox | https://proxy.sandbox.yandex-team.ru/{id}/index.html |
| Failed tests | `Suite.Query…` |
| Fingerprint | `verification=found` / `fline=workers_pool.cpp:117` / иной стабильный токен из stderr |
| Symbol / path | `TWorkersPool::PutTaskResults` · `ydb/core/…/workers_pool.cpp:117` |
| Search keys | `` `workers_pool.cpp:117` `` `` `AFL_VERIFY(found)` `` `` `PutTaskResults` `` `` `UploadTpch1000` `` `` `sas_big_column` `` |

`Search keys` — точные строки для `gh search issues "…" --repo ydb-platform/ydb`. Без них следующий агент тикет не найдёт.

#### Кратко
1–3 предложения: что сломалось, корневая причина, с чем не смешивать.

#### Отчёты (соседи)
| Дата / label | Commit | Отчёт | FailCount / заметка |
|--|--|--|--|
| **{label}** (разбираем) | … | https://proxy.sandbox… | … |
| соседний на том же sha / prior | … | https://proxy.sandbox… | … |

#### Код
| | |
|--|--|
| Место падения | [path:line @ sha](blob-url) |
| Менялся ли файл в окне | да/нет |
| Когда файл меняли последний раз | commit / PR |
| Связанный issue | нет / не [#N](url) |

#### Доказательства из логов
```
<цитата VERIFY / Received signal N / fatal — целиком ключевые строки, без «…» посередине fingerprint>
```
Coredump (если был): `https://coredumps.yandex-team.ru/v3/cores/<uuid>` + 1–2 frame.

#### Важно
1. Корневая причина — …, не «просто 2005».
2. Воспроизведение: branch / Version / suite @ db / Allure URL (см. Фактура).
3. Не тот же баг, что [#N](url) (если применимо).
4. Один тикет на корневую причину; следствия — в этом же issue.
5. Нестабильность на том же Version (если есть зелёный соседний прогон).

---

Язык: обычный русский. Английский — только термины (`VERIFY`, имена файлов, `code: 2005`).  
Не писать: фокус, волна, RC, last touch, priors, sticky, prev-green, jargon агента.  
Issue / PR / commit / sandbox — markdown-ссылками.  
Quality: self-check → `dutyctl validate` → `write-result`. См. `AGENTS.md`.
