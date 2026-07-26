# Perf duty — {suite} @ {db} — {label}

## Заключение
- **Итог:** <что случилось; по-русски>
- **Решение:** `update_known` | `open_ticket` | `wait_next_wave` | `investigate_further` | `no_action`
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
Вместо этого: «разбираемый прогон», «прогон `YYYY-MM-DD_sha`», «когда файл меняли последний раз», «корневая причина», «PR в том же commit — не причина».  
Issue / PR / commit / sandbox — markdown-ссылками.  
Quality: self-check → `dutyctl validate` → `write-result`. См. `AGENTS.md`.
