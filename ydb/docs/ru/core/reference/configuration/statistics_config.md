# statistics_config

Секция `statistics_config` настраивает сбор статистики столбцов для [стоимостного оптимизатора](../../concepts/query_execution/optimizer.md#statistics).

Чтобы собрать статистику вручную, выполните [ANALYZE](../../yql/reference/syntax/analyze.md). Для сбора многоколоночной статистики укажите нужные столбцы в конструкции [STATISTICS](../../yql/reference/syntax/create_table/statistics.md) при создании или изменении таблицы.

## Параметры конфигурации

| Параметр | Тип | По умолчанию | Описание |
|:---------|:----|:-------------|:---------|
| `enable_background_column_stats_collection` | bool | `false` | Собирать статистику столбцов в фоне |
| `background_analyze_change_ratio_threshold_percent` | uint32 | `20` | Порог изменений для досрочного сбора, в процентах |
| `analyze_collect_primary_key_histogram` | bool | `false` | Строить гистограмму по первичному ключу |
| `analyze_column_table_whole_table_scan_max_bytes` | uint64 | `10737418240` (10 ГиБ) | Максимальный размер для чтения колоночной таблицы целиком |
| `analyze_row_table_whole_table_scan_max_bytes` | uint64 | `10737418240` (10 ГиБ) | Максимальный размер для чтения строковой таблицы целиком |

## Фоновый сбор {#enable-background-column-stats-collection}

`enable_background_column_stats_collection: true` включает автоматический запуск `ANALYZE` для пользовательских таблиц. По умолчанию фоновый сбор выключен.

Повторный сбор планируется через 24 часа после начала предыдущего полного `ANALYZE`. Этот интервал фиксирован и не настраивается. При достижении [порога изменений](#background-analyze-change-ratio-threshold-percent) сбор может начаться раньше. Если уже выполняется сбор статистики для другой таблицы, новый запуск ждёт его завершения.

Сбор по выборке (`ANALYZE SAMPLE` с долей меньше `1`) не меняет расписание.

### Порог изменений {#background-analyze-change-ratio-threshold-percent}

`background_analyze_change_ratio_threshold_percent` задаёт порог для досрочного сбора. Обновления и удаления строк с последнего полного `ANALYZE` сравниваются с текущим числом строк в таблице.

Например, если сейчас в таблице 1000 строк, порог 20% соответствует 200 обновлениям или удалениям. Повторные изменения одной строки учитываются отдельно.

## Гистограмма первичного ключа {#analyze-collect-primary-key-histogram}

`analyze_collect_primary_key_histogram: true` включает построение гистограммы равной высоты по первичному ключу при `ANALYZE`.

В `ANALYZE` должны входить все столбцы первичного ключа. Если ключ пустой или содержит столбец неподдерживаемого типа, гистограмма не строится.

На гистограммы, заданные через [STATISTICS](../../yql/reference/syntax/create_table/statistics.md), этот параметр не влияет: `ANALYZE` собирает их независимо от его значения.

## Чтение таблиц

Пороги размера задаются в байтах и применяются только к полному `ANALYZE`. Для сбора по выборке способ чтения описан в разделе [{#T}](../../yql/reference/syntax/analyze.md#vyborka).

### Колоночные таблицы {#analyze-column-table-whole-table-scan-max-bytes}

Если размер колоночной таблицы известен и не превышает порог, `ANALYZE` сканирует её целиком, без разбиения по шардам. Более крупные таблицы и таблицы неизвестного размера сканируются по шардам.

При значении `0` сканирование всегда выполняется по шардам.

### Строковые таблицы {#analyze-row-table-whole-table-scan-max-bytes}

Если размер строковой таблицы известен и не превышает порог, `ANALYZE` сканирует её целиком, без разбиения на диапазоны первичного ключа. Более крупные таблицы и таблицы неизвестного размера сканируются по диапазонам первичного ключа.

Значение `0` включает чтение по диапазонам. Если первичный ключ не позволяет разбить таблицу на диапазоны, она читается целиком.

## Пример конфигурации

Включить фоновый сбор статистики с порогом изменений 20%:

```yaml
statistics_config:
  enable_background_column_stats_collection: true
  background_analyze_change_ratio_threshold_percent: 20
```

## См. также

- [{#T}](../../concepts/query_execution/optimizer.md#statistics)
- [{#T}](../../yql/reference/syntax/analyze.md)
- [{#T}](../../yql/reference/syntax/create_table/statistics.md)
