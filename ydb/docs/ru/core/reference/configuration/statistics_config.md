# statistics_config

Секция `statistics_config` задаёт параметры сбора статистики столбцов для [стоимостного оптимизатора](../../concepts/query_execution/optimizer.md#statistics). Число строк и размер таблицы берутся из схемной статистики и в этой секции не настраиваются.

Статистику вручную можно собрать оператором [ANALYZE](../../yql/reference/syntax/analyze.md). Многоколоночную статистику объявляют с помощью [STATISTICS](../../yql/reference/syntax/create_table/statistics.md).

## Параметры конфигурации

| Параметр | Тип | По умолчанию | Описание |
|:---------|:----|:-------------|:---------|
| `enable_background_column_stats_collection` | bool | `false` | Собирать статистику столбцов в фоне |
| `background_analyze_change_ratio_threshold_percent` | uint32 | `20` | Доля строк, обновлённых или удалённых с последнего полного `ANALYZE`, после которой статистика столбцов считается устаревшей |
| `analyze_collect_primary_key_histogram` | bool | `false` | Строить гистограмму равной высоты по первичному ключу при полном `ANALYZE` |
| `analyze_column_table_whole_table_scan_max_bytes` | uint64 | `10737418240` (10 GiB) | Максимальный размер колоночной таблицы, которую `ANALYZE` читает одним запросом |
| `analyze_row_table_whole_table_scan_max_bytes` | uint64 | `10737418240` (10 GiB) | Максимальный размер строковой таблицы, которую `ANALYZE` читает одним запросом |

### enable_background_column_stats_collection {#enable-background-column-stats-collection}

При значении `true` {{ ydb-short-name }} автоматически запускает `ANALYZE` для пользовательских таблиц. Обычно каждая таблица сканируется примерно раз в сутки; порог [`background_analyze_change_ratio_threshold_percent`](#background-analyze-change-ratio-threshold-percent) может запустить сбор раньше. `ANALYZE SAMPLE` не сбрасывает расписание фонового сбора. Внутренняя таблица `.metadata/statistics_v2` исключена из этого расписания.

### background_analyze_change_ratio_threshold_percent {#background-analyze-change-ratio-threshold-percent}

Порог задаётся в процентах от текущего числа строк. Статистика столбцов считается устаревшей, когда доля строк, обновлённых или удалённых с последнего полного `ANALYZE`, достигает этого значения:

`(число обновлений строк + число удалений строк с последнего полного ANALYZE) / число строк × 100%`

По умолчанию порог равен 20%.

### analyze_collect_primary_key_histogram {#analyze-collect-primary-key-histogram}

При значении `true` полный `ANALYZE` строит гистограмму равной высоты по первичному ключу. Она пропускается, если в списке столбцов нет столбца ключа, а также если ключ пустой или неподдерживаемый. По умолчанию параметр выключен. Гистограмма, объявленная как `STATISTICS ... WITH (EQ_HEIGHT_HISTOGRAM)`, собирается независимо от значения этого параметра.

### analyze_column_table_whole_table_scan_max_bytes {#analyze-column-table-whole-table-scan-max-bytes}

Если размер колоночной таблицы известен и не превышает этот порог, `ANALYZE` читает её целиком одним запросом. Более крупные таблицы и таблицы неизвестного размера сканируются по шардам. При значении `0` сканирование всегда выполняется по шардам. По умолчанию порог равен 10 ГиБ (`10737418240`).

`ANALYZE SAMPLE` с долей меньше `1` всегда читает только часть шардов. Порог размера применяется при полном `ANALYZE`.

### analyze_row_table_whole_table_scan_max_bytes {#analyze-row-table-whole-table-scan-max-bytes}

Если размер строковой таблицы известен и не превышает этот порог, `ANALYZE` читает её целиком одним запросом. Более крупные таблицы и таблицы неизвестного размера сканируются по диапазонам первичного ключа. При значении `0` чтение идёт по диапазонам первичного ключа, если ключ это позволяет. Иначе читается вся таблица. По умолчанию порог равен 10 ГиБ (`10737418240`).

`ANALYZE SAMPLE` с долей меньше `1` всегда выполняется одним проходом. Порог размера применяется при полном `ANALYZE`.

## Пример конфигурации

```yaml
statistics_config:
  enable_background_column_stats_collection: true
  background_analyze_change_ratio_threshold_percent: 20
  analyze_collect_primary_key_histogram: false
```

## См. также

- [{#T}](../../concepts/query_execution/optimizer.md#statistics)
- [{#T}](../../yql/reference/syntax/analyze.md)
- [{#T}](../../yql/reference/syntax/create_table/statistics.md)
