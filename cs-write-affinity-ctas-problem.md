# CS Write Affinity: Анализ падения CTAS теста с EnableCsWriteAffinity

## Тест

```
CS_WriteAffinity::Ctas+EnableCsWriteAffinity
```

Файл: [`kqp_write_affinity_ut.cpp:395-494`](ydb/core/kqp/ut/query/kqp_write_affinity_ut.cpp:395)

## Шаги теста

1. **CREATE TABLE Source** (8 shard'ов) — ✓ проходит
2. **REPLACE INTO Source** (80 строк) — ✓ проходит
3. **CREATE TABLE Destination AS SELECT * FROM Source** (2 shard'а) — ✗ **краш**

## Ошибка

```
VERIFY failed (2026-08-26T01:51:49.176750Z): verification=TargetShardIds.has_value();fline=kqp_write_table.cpp:457;
```

Краш в конструкторе `TColumnShardPayloadSerializer` при проверке `AFL_VERIFY(TargetShardIds.has_value())`.

## Стек вызовов

```
TColumnShardPayloadSerializer::TColumnShardPayloadSerializer()  [kqp_write_table.cpp:457]
  <- CreateColumnShardPayloadSerializer()  [kqp_write_table.cpp:1112]
    <- TShardedWriteController::OnPartitioningChanged()  [kqp_write_table.cpp:1877]
      <- TKqpTableWriteActor::Prepare()  [kqp_write_actor.cpp:1601]
```

## Корневая причина

**Цель ветки**: Каждый WriteActor должен получать данные только одного ColumnShard.

Для этого в [`kqp_write_table.cpp:457`](ydb/core/kqp/runtime/kqp_write_table.cpp:457) стоит проверка:
```cpp
#ifdef KQP_WRITE_TABLE_TARGET_SHARD_IDS_CHECK
    AFL_VERIFY(TargetShardIds.has_value());
#endif
```

`KQP_WRITE_TABLE_TARGET_SHARD_IDS_CHECK` определен в [`kqp_write_table.h`](ydb/core/kqp/runtime/kqp_write_table.h) и включен для build `relwithdebinfo`.

### Почему TargetShardIds = std::nullopt?

Потому что пользователь откатил изменения в `kqp_write_actor.cpp`, которые ранее возвращали пустой set для OLAP таблиц:

**Текущий код** (`kqp_write_actor.cpp:2859`):
```cpp
static std::optional<THashSet<ui64>> TargetShardIdsFromSettings(const NKikimrKqp::TKqpTableSinkSettings& settings) {
    if (settings.GetTargetShardIds().size() > 0) {
        return THashSet<ui64>(settings.GetTargetShardIds().begin(), settings.GetTargetShardIds().end());
    }
    return std::nullopt;  // <-- Возвращает nullopt, если TargetShardIds пуст!
}
```

### Почему settings.GetTargetShardIds() пуст?

Потому что в `BuildInternalSinks` ([`kqp_tasks_graph.cpp:3877-3948`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:3877)) условие для заполнения `TargetShardIds`:

```cpp
if (settings.GetIsOlap()
        && stageInfo.Meta.Tx.Body->EnableCsWriteAffinity()) {
    // ... заполнение resolvedShardIds из ColumnTableInfoPtr или ShardKey
    if (!resolvedShardIds.empty()) {
        // ... заполнение settings.AddTargetShardIds(shardId)
    }
}
```

**Для CTAS Destination таблицы** `stageInfo.Meta.ColumnTableInfoPtr` может быть **null**, потому что:
1. CTAS создает новую таблицу
2. Табличный резолвер может не установить `ColumnTableInfoPtr` для destination таблицы до момента создания
3. Без `ColumnTableInfoPtr` и `ShardKey`, `resolvedShardIds` остается пустым
4. `settings.AddTargetShardIds()` не вызывается
5. `TargetShardIds` остается пустым в proto settings

### Почему CountComputeTasks не создал per-shard задачи?

В [`CountComputeTasks`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp:4875):
```cpp
if (isCsWriteAffinitySink && stageInfo.Meta.Tx.Body->EnableCsWriteAffinity()) {
    // Build a list of (shardId, nodeId) for shards.
    TVector<std::pair<ui64 /* shardId */, ui64 /* nodeId */>> shardNodes;

    // For OLAP, use ColumnTableInfo to get column shard IDs
    if (stageInfo.Meta.ColumnTableInfoPtr
            && stageInfo.Meta.ColumnTableInfoPtr->Description.HasSharding()) {
        // ... заполнение shardNodes
    } else if (stageInfo.Meta.ShardKey) {
        // ... заполнение shardNodes из ShardKey
    }

    if (!shardNodes.empty()) {
        // Создать per-shard задачи
        // ...
        return; // Early-return
    }
    // Fall through: стандартный путь с 1 задачей
}
```

Для CTAS `ColumnTableInfoPtr` может быть null, поэтому `shardNodes` пустой и происходит fall-through к стандартному пути с 1 задачей.

## Решение

Поскольку цель — **каждый WriteActor получает данные только одного CS**, нужно обеспечить:

1. **ColumnTableInfoPtr должен быть доступен** для CTAS destination таблицы ДО `CountComputeTasks`
2. **CountComputeTasks** должен создать per-shard задачи
3. **BuildInternalSinks** должен установить `TargetShardIds` с ровно 1 shard'ом на задачу
4. **TargetShardIdsFromSettings** должен вернуть этот set (не nullopt)

### Вариант 1: Установить ColumnTableInfoPtr в резолвере для CTAS

В [`kqp_table_resolver.cpp`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp) нужно убедиться, что для CTAS destination таблицы `ColumnTableInfoPtr` устанавливается после создания таблицы.

### Вариант 2: Использовать GetColumnShards из шардинга в runtime

В `OnPartitioningChanged` ([`kqp_write_table.cpp:1872-1884`](ydb/core/kqp/runtime/kqp_write_table.cpp:1872)) уже доступен `schemeEntry` с полной информацией о таблице. Можно использовать его для заполнения `TargetShardIds` когда он пуст:

```cpp
void OnPartitioningChanged(const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry) override {
    IsOlap = true;
    SchemeEntry = schemeEntry;
    BeforePartitioningChanged();

    // If TargetShardIds is empty but CS Write Affinity is enabled for OLAP,
    // populate it from scheme entry sharding info.
    if (Settings.TargetShardIds.has_value() && Settings.TargetShardIds->empty() && Settings.GetIsOlap()) {
        if (schemeEntry.ColumnTableInfo && schemeEntry.ColumnTableInfo->Description.HasSharding()) {
            const auto& sharding = schemeEntry.ColumnTableInfo->Description.GetSharding();
            *Settings.TargetShardIds = THashSet<ui64>(
                sharding.GetColumnShards().begin(),
                sharding.GetColumnShards().end());
        }
    }

    for (auto& [_, writeInfo] : WriteInfos) {
        writeInfo.Serializer = CreateColumnShardPayloadSerializer(
            *SchemeEntry,
            Settings.TargetShardIds,
            writeInfo.Metadata.InputColumnsMetadata,
            Alloc);
    }
    AfterPartitioningChanged();
}
```

**НО** это нарушает цель ветки — каждый WriteActor получает данные только одного CS. Этот вариант назначает все shard'ы одному актору.

### Вариант 3 (Правильный): Обеспечить ColumnTableInfoPtr в резолвере

Необходимо в [`kqp_table_resolver.cpp`](ydb/core/kqp/executer_actor/kqp_table_resolver.cpp) в функции обработки CTAS убедиться, что `ColumnTableInfoPtr` устанавливается для destination таблицы. Это позволит:

1. `CountComputeTasks` создать per-shard задачи
2. `BuildInternalSinks` установить `TargetShardIds` с 1 shard'ом на задачу
3. `TargetShardIdsFromSettings` вернуть корректный set

## Итог

| Компонент | Проблема | Решение |
|-----------|----------|---------|
| Table Resolver | ColumnTableInfoPtr не установлен для CTAS destination | Установить в HandleResolveNames |
| CountComputeTasks | Fall-through к 1 задаче из-за отсутствия ColumnTableInfoPtr | Будет работать после исправления резолвера |
| BuildInternalSinks | Не может заполнить TargetShardIds без ColumnTableInfoPtr | Будет работать после исправления резолвера |
| TargetShardIdsFromSettings | Возвращает nullopt когда TargetShardIds пуст | Будет работать после исправления резолвера |
