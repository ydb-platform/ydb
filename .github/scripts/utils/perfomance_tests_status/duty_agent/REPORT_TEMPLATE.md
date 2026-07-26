# Perf duty — {suite} @ {db} — {label}

## Заключение
- **Итог:** <что случилось; по-русски>
- **Решение:** `investigate_further` — продолжить разбор (…); или `open_ticket` / `update_known` / `wait_next_wave` / `no_action` + пояснение по-русски
- **Виновник:** `unknown` | `@{login}` / [PR #N](https://github.com/ydb-platform/ydb/pull/N) — <доказательство>
- **Уверенность:** высокая | средняя | низкая
- **Давность:** <с каких пор могло проявляться; со ссылками>
- **Механика:** <как система дошла до поломки>

## Проблемы

### P1 — <короткое имя>
- Тип: `olap_fail` | `olap_slow` | `tpcc_tpmc` | `tpcc_lat`
- Что сломалось: …
- Почему / механика: …
- Логи: `kikimr__stderr` …; `kikimr__logs` … (или явно пусто)
- Код ([`{sha}`](https://github.com/ydb-platform/ydb/commit/{sha})): [файл](blob-url) / функция …
- Кто (если есть): … + доказательство
- Давность: …
- Гипотеза проверена: `yes` | `no` | `partial`
- Связанный issue: [#{N}](https://github.com/ydb-platform/ydb/issues/{N}) или нет

## Кандидаты PR (если есть)
Не список голых ссылок. Таблица:

| Влито (UTC) | PR | Title | Автор | Почему |
|--|--|--|--|--|
| 2026-07-22 11:11 | [#46638](https://github.com/ydb-platform/ydb/pull/46638) | Decouple WorkloadManager and Kqp | @zverevgeny | … |

Поля из `dig_prs.json`: `merged_at`, `pr_url`, `pr_title`, `author_login`.

## Что дальше
1. …

## Материалы для issue
### Окружение
| | |
|--|--|
| Suite / DB | … |
| Разбираемый прогон | label · время · CI |
| Commit | [sha](https://github.com/ydb-platform/ydb/commit/…) |

### Отчёты Sandbox / Allure
| Дата / label | Commit | Отчёт | Упавшие тесты |
|--|--|--|--|
| … (разбираем) | … | https://proxy.sandbox… | … |
| … | … | https://proxy.sandbox… | … |

### Код и тикеты
- Место падения (ссылка на файл@commit)
- Менялся ли файл между соседними commit
- Когда файл меняли последний раз
- Связанные issue / PR

### Доказательства из логов
```
<короткая цитата VERIFY / fatal>
```

### Что важно для формулировки issue
1. …

---

Язык отчёта: обычный русский. Английский — только термины продукта (`VERIFY`, имена файлов, `code: 2005`).  
Не писать: фокус, волна, RC, last touch, path, priors, sticky, prev-green, «метрика зелёный» без пояснения.  
Не писать блоки «отпадает / отброшено» про PR алерта и прочие ложные следы (молча не включать), кроме явного «проверь кандидата» / сравнения с прошлым отчётом.  
Не писать механику/пути тестов, если это не сужает продуктовую причину.  
Вместо этого: «разбираемый прогон», «прогон `YYYY-MM-DD_sha`», «когда файл меняли последний раз», «корневая причина».  
Issue / PR / commit / sandbox — markdown-ссылками.  
Кандидаты PR: дата влития + ссылка + title + автор (не голый список #N).  
Quality: self-check → `dutyctl validate` → `write-result`. См. `AGENTS.md`.
