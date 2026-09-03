# Тесты CTAS Write Node Affinity

## Обзор

Файл [`kqp_write_affinity_ut.cpp`](kqp_write_affinity_ut.cpp) содержит 12 TWIN тестов (24 запуска) для проверки оптимизации ColumnShard Write Node Affinity для CTAS (CREATE TABLE AS SELECT).

Каждый TWIN тест запускается дважды: `EnableCsWriteAffinity=true` и `EnableCsWriteAffinity=false`.

## Настройка EnableCsWriteAffinity

Настройка `EnableCsWriteAffinity` задаётся на уровне сервера через `TKikimrSettings`:

```cpp
static TVector<NKikimrKqp::TKqpSetting> BuildKqpSettingsWithCsWriteAffinity(bool enableCsWriteAffinity) {
    NKikimrKqp::TKqpSetting setting;
    setting.SetName("EnableCsWriteAffinity");
    setting.SetValue(enableCsWriteAffinity ? "true" : "false");
    return {setting};
}
```

Затем применяется к настройкам тестового сервера:

```cpp
auto settings = TKikimrSettings().SetWithSampleTables(false);
settings.SetKqpSettings(BuildKqpSettingsWithCsWriteAffinity(EnableCsWriteAffinity));
TKikimrRunner kikimr(settings);
```

**Важно**: PRAGMA `ydb.EnableCsWriteAffinity` **не используется** в текущих тестах. Настройка передаётся через серверные KQP-настройки.

## Архитектура проверок

Все тесты используют унифицированный набор проверок через `VerifyCtasPlanFull()`:

### 1. Проверка количества стадий (`VerifyCtasPlanWithAffinity`)
- **С affinity (table source)**: 4 стадии (table creation + transform + hashshuffle + sink)
- **Без affinity (table source)**: 3 стадии (table creation + transform с inlined sink)
- **Generated data / pure literals**: 3 стадии с affinity, 2 без (всегда с HashShuffle при affinity)

### 2. Проверка структуры плана (только с affinity=true)
1. HashShuffle connection существует
2. HashFunc = ColumnShardHashV1
3. PlanNodeType = Connection
4. Sink stage существует
5. Ровно 1 HashShuffle
6. Нет Broadcast connection
7. Inner compute stage существует

### 3. Проверка KeyColumns (`VerifyHashShuffleKeyColumns`)
- С affinity=true: HashShuffle.KeyColumns == ожидаемым значениям
- Без affinity=false: HashShuffle отсутствует

### 4. Выполнение CTAS и точное сравнение данных
- Source таблица заполняется `kRowCount=80` строками (> кол-ва шардов=8)
- CTAS выполняется всегда (при обоих значениях `EnableCsWriteAffinity`)
- SELECT читает данные из destination таблицы
- Результаты сравниваются через `CompareYson()` с ожидаемым YSON

## Поведение без affinity (разное количество шардов)

При `EnableCsWriteAffinity=false` и CTAS из table source с разным количеством шардов (source=8, destination=2) оптимизатор строит `TDqCnMap` connection с inlined sink. В рантайме (`CountComputeTasks` в [`kqp_tasks_graph.cpp`](../executer_actor/kqp_tasks_graph.cpp)) sink-стадия наследует количество задач source-стадии через Map connection, и каждая задача пишет строки в любые шарды назначения (стандартное поведение CTAS). Per-shard задачи и `TargetShardIds` создаются только при наличии HashShuffle-входа у sink-стадии (план с affinity). Поэтому:

- **План проверяется всегда** (и при true, и при false)
- **Выполнение CTAS и сравнение данных** -- всегда (обе ветки), включая тесты с table source

## Перечень тестов

| # | Тест | Источник данных | Ожидаемые KeyColumns | Стадии | HashShuffle | Выполнение |
|---|------|----------------|---------------------|--------|-------------|------------|
| 1 | `CtasTableSourcePkMatchesPartitionBy` | Table source (80 строк), PK=PartitionBy | `["Col1"]` | 4/3 | Только при true | Всегда |
| 2 | `CtasTableSourceMultipleShardingColumns` | Table source, 1 и 2 sharding колонки | `["Col1"]`, `["Col1","Col2"]` | 4/3 | Только при true | Всегда |
| 3 | `CtasTableSourceNoPartitionByUsesPrimaryKey` | Table source, нет PARTITION BY | `["Col1"]` (из PK) | 4/3 | Только при true | Всегда |
| 4 | `CtasTableSourcePartitionBySubsetOfPrimaryKey` | Table source, PartitionBy⊂PK | `["Col2"]` (из PartitionBy) | 4/3 | Только при true | Всегда |
| 5 | `CtasGeneratedDataWithPartitionBy` | `AS_TABLE($data)`, 100 строк, PARTITION BY HASH(Col1) | `["Col1"]` | 3/2 | Только при true | Всегда |
| 6 | `CtasPureLiteralWithPartitionBy` | Чистый литерал (1u, 42), PARTITION BY HASH(Col1) | `["Col1"]` | 3/2 | Только при true | Всегда |
| 7 | `CtasGeneratedDataWithoutPartitionBy` | `AS_TABLE($data)`, 80 строк, без PARTITION BY (fallback к PK) | `["Col1"]` | 3/2 | Только при true | Всегда |
| 8 | `CtasGeneratedDataPartitionBySubsetOfPrimaryKey` | `AS_TABLE($data)`, PartitionBy⊂PK | `["Col2"]` | 3/2 | Только при true | Всегда |
| 9 | `CtasTableSourceSelectWithAliases` | Table source, SELECT с алиасами | `["A"]` (алиас) | 4/3 | Только при true | Всегда |
| 10 | `CtasTableSourceWithWhereFilter` | Table source, WHERE Col1 > 40 | `["Col1"]` | 4/3 | Только при true | Всегда |
| 11 | `CtasTableSourceVerifyAffinityFlagTogglesHashShuffle` | Table source, проверка переключения HashShuffle | `["Col1"]` | 4/3 | Только при true | Всегда |
| 12 | `CtasPureLiteralVerifyAffinityFlagTogglesHashShuffle` | Чистый литерал, проверка переключения HashShuffle | `["Col1"]` | 3/2 | Только при true | Всегда |

**Итого: 12 TWIN тестов = 24 запуска** (каждый тест запускается с `EnableCsWriteAffinity=true` и `EnableCsWriteAffinity=false`).

## Helper функции

### `BuildKqpSettingsWithCsWriteAffinity(bool enableCsWriteAffinity)`
Создаёт KQP-настройки с включённым/выключенным EnableCsWriteAffinity:
```cpp
static TVector<NKikimrKqp::TKqpSetting> BuildKqpSettingsWithCsWriteAffinity(bool enableCsWriteAffinity) {
    NKikimrKqp::TKqpSetting setting;
    setting.SetName("EnableCsWriteAffinity");
    setting.SetValue(enableCsWriteAffinity ? "true" : "false");
    return {setting};
}
```

### `VerifyCtasPlanFull()`
Полная проверка плана CTAS:
- Количество стадий
- Структура плана (HashShuffle, Sink, etc.)
- KeyColumns в HashShuffle

### `ExplainQuery()`
Выполняет EXPLAIN запроса и возвращает распарсенный JSON плана.

## Ключевые файлы

| Файл | Роль |
|------|------|
| [`kqp_opt_effects.cpp`](../opt/kqp_opt_effects.cpp) | Оптимизатор: `BuildFillTableEffect()` -- создаёт план CTAS. Использует `node.CtasShardingColumns().IsValid()` как индикатор affinity. |
| [`kqp_statement_rewrite.cpp`](../host/kqp_statement_rewrite.cpp) | Rewrite фаза: `RewriteCreateTableAs()` -- читает `EnableCsWriteAffinity` из config и устанавливает `CtasShardingColumns` в insert settings. |
| [`kqp_tasks_graph.cpp`](../executer_actor/kqp_tasks_graph.cpp) | Рантайм: `CountComputeTasks()` / `BuildInternalSinks()` -- per-shard задачи и `TargetShardIds` создаются только для планов с HashShuffle-входом у sink-стадии (`HasHashShuffleInput()`). |
| [`yql_kikimr_settings.cpp`](../provider/yql_kikimr_settings.cpp) | Регистрация настройки `EnableCsWriteAffinity` и метод `GetEnableCsWriteAffinity()`. |

## Итого

- **12 TWIN тестов** = **24 запуска**
- **24/24 проходят успешно** (стабильный результат)
- Все тесты проверяют план и выполняют CTAS при обоих значениях настройки
- Все тесты делают точное сравнение данных через `CompareYson()`
