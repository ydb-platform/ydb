# statistics_config

Секция `statistics_config` настраивает сбор статистики столбцов для [стоимостного оптимизатора](../../concepts/query_execution/optimizer.md#statistics).

## Параметры конфигурации

| Параметр | Тип | По умолчанию | Описание |
|:---------|:----|:-------------|:---------|
| [`enable_background_column_stats_collection`](#enable-background-column-stats-collection) | bool | `false` | Включение фонового сбора статистики. По умолчанию выключен. |
| [`background_analyze_change_ratio_threshold_percent`](#background-analyze-change-ratio-threshold-percent) | uint32 | `20` | Порог изменений для повторного сбора, в процентах |
| [`analyze_collect_primary_key_histogram`](#analyze-collect-primary-key-histogram) | bool | `false` | Построение гистограммы по первичному ключу |
| [`analyze_column_table_whole_table_scan_max_bytes`](#analyze-column-table-whole-table-scan-max-bytes) | uint64 | `10737418240` (10 ГиБ) | Порог размера для выбора между чтением колоночной таблицы целиком и по шардам |
| [`analyze_row_table_whole_table_scan_max_bytes`](#analyze-row-table-whole-table-scan-max-bytes) | uint64 | `10737418240` (10 ГиБ) | Порог размера для выбора между чтением строковой таблицы целиком и по диапазонам первичного ключа |

## Фоновый сбор {#enable-background-column-stats-collection}

`enable_background_column_stats_collection: true` включает автоматический запуск `ANALYZE` для пользовательских таблиц. По умолчанию фоновый сбор выключен.

Повторный сбор планируется через 24 часа после начала предыдущего полного `ANALYZE`. Этот интервал фиксирован и не настраивается. При достижении [порога изменений](#background-analyze-change-ratio-threshold-percent) сбор может начаться раньше. Если уже выполняется сбор статистики для другой таблицы, новый запуск ждёт его завершения.

Сбор по выборке (`ANALYZE SAMPLE` с долей меньше `1`) не меняет расписание.

### Порог изменений {#background-analyze-change-ratio-threshold-percent}

`background_analyze_change_ratio_threshold_percent` позволяет обновить статистику после изменения данных, не дожидаясь планового интервала 24 часа. Параметр действует при включённом фоновом сборе и задаёт порог в процентах от текущего числа строк в таблице. Учитываются обновления и удаления строк с последнего полного `ANALYZE`.

Например, если сейчас в таблице 1000 строк, порог 20% соответствует 200 обновлениям или удалениям. Повторные изменения одной строки учитываются отдельно.

## Гистограмма первичного ключа {#analyze-collect-primary-key-histogram}

При создании многоколоночной статистики через [CREATE TABLE ... STATISTICS](../../yql/reference/syntax/create_table/statistics.md) укажите столбцы, по которым она будет собираться.

Чтобы не перечислять столбцы первичного ключа в `STATISTICS`, установите `analyze_collect_primary_key_histogram: true`. Это включает построение гистограммы равной высоты по первичному ключу при `ANALYZE`.

## Чтение таблиц

Полный `ANALYZE` читает все строки таблицы. Пороги ниже определяют, разбивается ли чтение по шардам или диапазонам первичного ключа. Пороги задаются в байтах.

### Колоночные таблицы {#analyze-column-table-whole-table-scan-max-bytes}

`analyze_column_table_whole_table_scan_max_bytes` определяет способ чтения колоночной таблицы: целиком или по отдельным шардам. Если размер таблицы известен и не превышает порог, `ANALYZE` читает её целиком. Более крупные таблицы и таблицы неизвестного размера сканируются по шардам.

При значении `0` сканирование всегда выполняется по шардам.

### Строковые таблицы {#analyze-row-table-whole-table-scan-max-bytes}

`analyze_row_table_whole_table_scan_max_bytes` определяет способ чтения строковой таблицы: целиком или по диапазонам первичного ключа. Если размер таблицы известен и не превышает порог, `ANALYZE` читает её целиком. Более крупные таблицы и таблицы неизвестного размера сканируются по диапазонам первичного ключа.

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
- [CREATE TABLE ... STATISTICS](../../yql/reference/syntax/create_table/statistics.md)
- [ALTER TABLE ... STATISTICS](../../yql/reference/syntax/alter_table/statistics.md)
