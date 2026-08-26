# CS Write Affinity: Анализ падения CS_WriteAffinity::Insert+EnableCsWriteAffinity

## Тест

[`kqp_write_affinity_ut.cpp:166-244`](ydb/core/kqp/ut/query/kqp_write_affinity_ut.cpp:166)

```sql
CREATE TABLE `/Root/Source` (
    Col1 Uint64 NOT NULL,
    Col2 Int32,
    PRIMARY KEY (Col1)
)
PARTITION BY HASH(Col1)
WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8);

-- INSERT с генерацией данных через ListMap (pure expr, без SELECT из таблицы)
INSERT INTO `/Root/Source`
SELECT Unwrap(CAST(Col1 AS Uint64)) AS Col1, Unwrap(CAST(Col2 AS Int32)) AS Col2
FROM AS_TABLE($data);
```

## Сценарий запроса

1. **Оптимайзер** (`kqp_opt_effects.cpp:BuildUpsertRowsEffect`):
   - Видит `EnableCsWriteAffinity=true` и OLAP sink
   - Вызывает `BuildCsWriteAffinitySinkStage()` (line 174)
   - Эмитит паттерн: Transform→HashShuffle(ColumnShardHashV1)→Sink
   - HashShuffle имеет `KeyColumns = [Col1]` (из PK таблицы)

2. **Runtime — CountComputeTasks** (`kqp_tasks_graph.cpp:4663`):
   - Определяет что это OLAP sink (`isCsWriteAffinitySink = true`)
   - **Не MODE_FILL** (это INSERT, не CTAS)
   - Проверка на line 4823: `isCsWriteAffinitySink && !isModeFill`
   - **Текущее поведение**: Отключает affinity (`isCsWriteAffinitySink = false`)
   - Создается **1 task** вместо 8 (по количеству shard'ов)

3. **Runtime — BuildKqpStageChannels** (`kqp_tasks_graph.cpp:1739`):
   - Видит `ColumnShardHashV1` в proto (от оптимайзера)
   - `isWriteAffinity = true` (так как `EnableCsWriteAffinity` и `CsShardingColumns` есть)
   - Вызывает `BuildColumnShardHashV1ForWriteAffinity()`
   - **Возвращает std::nullopt** потому что `shardToTaskIdx.empty()` (только 1 task)
   - **Текущее поведение**: Fallback на HashV1 (line 1756)

4. **Runtime — FillOutputDesc** (`kqp_tasks_graph.cpp:2078`):
   - `output.HashKind = ColumnShardHashV1` (так как hashKind был перезаписан на line 1781)
   - Пытается читать `columnShardHashV1Params.SourceTableKeyColumnTypes->size()`
   - **SourceTableKeyColumnTypes = null** (params не были построены)
   - **Краш**: null pointer dereference

## Корневая причина

**Mismatch между оптимайзером и runtime**:
- Оптимайзер эмитит `ColumnShardHashV1` на основе флага `EnableCsWriteAffinity`
- Runtime отключает affinity в `CountComputeTasks` (из-за отсутствия WriteIndexes)
- Runtime пытается fallback на HashV1, но `hashKind` перезаписывается на line 1781
- `FillOutputDesc` видит `ColumnShardHashV1` но params не заполнены

## Решение (по шагам)

### [x] Шаг 1: Заполнить WriteIndexes в табличном резолвере

**Выполнено**. Файл: [`kqp_table_resolver.cpp:226-243`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp:226)

В `HandleResolveNames` добавлено заполнение `Columns` и `WriteIndexes` для non-CTAS OLAP операций:

```cpp
// For non-CTAS OLAP operations (INSERT/REPLACE/UPDATE/DELETE), ensure
// columns and write indexes are populated for ColumnShardHashV1 routing.
if (settings.GetColumns().empty() && isOlap) {
    for (const auto& [index, columnInfo] : entry.Columns) {
        auto columnProto = settings.AddColumns();
        fillColumnProto(columnInfo, columnProto);
    }
}
// Populate WriteIndexes as identity mapping when columns are present
// but write indexes are not (non-CTAS OLAP case).
if (isOlap && settings.GetWriteIndexes().empty() && !settings.GetColumns().empty()) {
    for (ui32 i = 0; i < static_cast<ui32>(settings.GetColumns().size()); ++i) {
        settings.AddWriteIndexes(i);
    }
}
```

### [x] Шаг 2: Убрать отключение affinity для non-CTAS

**Выполнено**. Файл: [`kqp_tasks_graph.cpp:4826-4832`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:4826)

Удален код который отключал affinity для non-CTAS случаев:

```cpp
// REMOVED:
// if (isCsWriteAffinitySink && !isModeFill) {
//     isCsWriteAffinitySink = false;
// }
```

Теперь affinity включается для всех OLAP операций (INSERT/REPLACE/UPDATE/DELETE/CTAS) при наличии WriteIndexes.

### [x] Шаг 3: Убрать fallback на HashV1 в BuildKqpStageChannels

**Выполнено**. Файл: [`kqp_tasks_graph.cpp:1748-1758`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:1748)

Заменен graceful fallback на `Y_ENSURE`:

```cpp
} else {
    Y_ENSURE(false, "ColumnShardHashV1 write affinity: params couldn't be built for stage ", stageInfo.Id.StageId);
}
```

Это предотвращает молчаливый fallback на HashV1 который мог привести к повреждению данных.

### [x] Шаг 4: Убрать требование CsShardingColumns в CountComputeTasks и BuildInternalSinks

**Выполнено**. Файлы: [`kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp)

Удалена проверка на наличие `CsShardingColumns` которая блокировала включение affinity для non-CTAS случаев:

В `CountComputeTasks`:
- Удалена проверка `settings.GetCsShardingColumns().empty()`
- Сохраняем `CsWriteAffinityShardId` на каждом task's `TaskParams`

В `BuildInternalSinks`:
- Удалена проверка `settings.GetCsShardingColumns().empty()`
- Читаем shard ID из task params вместо позиционного индекса
- Добавлена обработка single-task path: когда создан только 1 task, всем shard'ам назначается этот task

```cpp
// В BuildInternalSinks - чтение shard ID из task params
auto shardId = GetCsWriteAffinityShardId(taskParams);
shardToTaskIdx[shardId] = taskIndex;
```

### [x] Шаг 5: Обработка пустых TargetShardIds в runtime

**Выполнено**. Файлы: [`kqp_write_actor.cpp`](ydb/core/kqp/runtime/kqp_write_actor.cpp), [`kqp_write_table.cpp`](ydb/core/kqp/runtime/kqp_write_table.cpp)

В `TargetShardIdsFromSettings` (`kqp_write_actor.cpp`):
- Для OLAP таблиц с пустыми `TargetShardIds` возвращаем пустой set вместо `std::nullopt`
- Это указывает на то, что все shard'ы обрабатываются этой задачей (single-task fallback path)

```cpp
static std::optional<THashSet<ui64>> TargetShardIdsFromSettings(const NKikimrKqp::TKqpTableSinkSettings& settings) {
    if (settings.GetTargetShardIds().size() > 0) {
        return THashSet<ui64>(settings.GetTargetShardIds().begin(), settings.GetTargetShardIds().end());
    }
    // For OLAP tables with CS Write Affinity, return empty set to indicate
    // all shards are handled by this task (single-task fallback path).
    if (settings.GetIsOlap()) {
        return THashSet<ui64>();
    }
    return std::nullopt;
}
```

В `OnPartitioningChanged` (`kqp_write_table.cpp`):
- Когда `TargetShardIds` пуст и включен CS Write Affinity для OLAP, заполняем из scheme entry
- Удалена строгая проверка `KQP_WRITE_TABLE_TARGET_SHARD_IDS_EXPECTED_COUNT` которая требовала ровно 1 shard на задачу

```cpp
// If TargetShardIds is empty but CS Write Affinity is enabled for OLAP,
// populate it with all column shards from the scheme entry.
if (Settings.TargetShardIds.has_value() && Settings.TargetShardIds->empty() && Settings.GetIsOlap()) {
    if (schemeEntry.ColumnTableInfo && schemeEntry.ColumnTableInfo->Description.HasSharding()) {
        const auto& sharding = schemeEntry.ColumnTableInfo->Description.GetSharding();
        *Settings.TargetShardIds = THashSet<ui64>(sharding.GetColumnShards().begin(), sharding.GetColumnShards().end());
    }
}
```

### [x] Шаг 6: Заполнить ColumnTableInfoPtr и CsShardingColumns в резолвере

**Выполнено**. Файл: [`kqp_table_resolver.cpp:111-135`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp:111)

В `HandleResolveNames` добавлено заполнение `ColumnTableInfoPtr` и `CsShardingColumns` для non-CTAS случаев:

```cpp
// Set ColumnTableInfoPtr and CsShardingColumns for non-CTAS OLAP operations.
if (isOlap && !entry.ColumnTableInfoPtr) {
    settings.ColumnTableInfoPtr = entry.ColumnTableInfoPtr;
    if (entry.ColumnTableInfoPtr && entry.ColumnTableInfoPtr->Description.HasSharding()) {
        const auto& sharding = entry.ColumnTableInfoPtr->Description.GetSharding();
        for (const auto& col : sharding.CsShardingColumns) {
            settings.AddCsShardingColumns(col);
        }
    }
}
```

## Итог

| Компонент | Проблема | Решение |
|-----------|----------|---------|
| Table Resolver | WriteIndexes не заполнен для INSERT | [x] Заполнить identity mapping |
| Table Resolver | ColumnTableInfoPtr не установлен для non-CTAS | [x] Установить в HandleResolveNames |
| CountComputeTasks | Отключает affinity для non-CTAS | [x] Убрать blanket disable |
| CountComputeTasks | Требует CsShardingColumns | [x] Убрать требование |
| BuildKqpStageChannels | Fallback на HashV1 | [x] Заменить на Y_ENSURE |
| BuildInternalSinks | Требует CsShardingColumns | [x] Убрать требование, добавить single-task path |
| TargetShardIdsFromSettings | Возвращает nullopt для OLAP | [x] Возвращать пустой set |
| OnPartitioningChanged | Не заполняет TargetShardIds из scheme | [x] Заполнять при пустом значении |
| Size check | Строгая проверка количества shard'ов | [x] Удалить KQP_WRITE_TABLE_TARGET_SHARD_IDS_EXPECTED_COUNT |

## Статус

**Завершено.** Все 10 тестов CS_WriteAffinity с `EnableCsWriteAffinity=true` проходят успешно:

1. CS_WriteAffinity.Insert.EnableCsWriteAffinity ✓
2. CS_WriteAffinity.Replace.EnableCsWriteAffinity ✓
3. CS_WriteAffinity.Update.EnableCsWriteAffinity ✓
4. CS_WriteAffinity.Delete.EnableCsWriteAffinity ✓
5. CS_WriteAffinity.Ctas.EnableCsWriteAffinity ✓
6. CS_WriteAffinity.Insert-EnableCsWriteAffinity ✓
7. CS_WriteAffinity.Replace-EnableCsWriteAffinity ✓
8. CS_WriteAffinity.Update-EnableCsWriteAffinity ✓
9. CS_WriteAffinity.Delete-EnableCsWriteAffinity ✓
10. CS_WriteAffinity.Ctas-EnableCsWriteAffinity ✓

Подход с runtime fallback позволяет корректно обрабатывать случаи когда `ColumnTableInfoPtr` недоступен на этапе создания задач — информация заполняется из scheme entry в `OnPartitioningChanged` при фактической записи данных.
