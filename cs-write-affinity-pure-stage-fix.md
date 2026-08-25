# Fix: Pure Stage OLAP Write Affinity

## Проблема

Тест `KqpQuery::CTAS_WriteAffinity_Twin+EnableCsWriteAffinity` падает при выполнении
`REPLACE INTO '/Root/Source' VALUES (...)` в OLAP-таблицу с `EnableCsWriteAffinity=true`.

### Root cause

`REPLACE INTO VALUES` → `BuildUpsertRowsEffect` с `IsDqPureExpr(node.Input()) == true`
→ `RebuildPureStageWithSink` → **один** `TDqStage` без входных каналов (pure stage).

`CountComputeTasks` видит OLAP sink, создаёт **N=8 задач** (по числу шардов), но у каждой задачи
`task.Inputs` пуст — нет upstream HashShuffle-каналов.

Каждая задача независимо вычисляет все VALUES и получает все строки. При этом
`BuildInternalSinks` назначает каждой задаче ровно 1 `TargetShardId`.
`TColumnShardPayloadSerializer` проверяет (KQP_WRITE_TABLE_TARGET_SHARD_IDS_CHECK):
`TargetShardIds.has_value()` → ок, но потом в деструкторе:
`*TargetShardIds == ActualShardIds` → FAIL, потому что реально пришли все 8 шардов,
а ожидался только 1.

### Инвариант, который нужно сохранить

Каждая Sink-задача получает строки **ровно одного шарда** и имеет
`TargetShardIds = { тот_один_шард }`. Обеспечивается через `ColumnShardHashV1` HashShuffle
на Transform→Sink соединении.

---

## Что уже сделано (текущие изменения в ветке)

> **ВАЖНО:** Шаги 3–5 — временные обходные решения, которые нужно откатить.

- `kqp_tasks_graph.cpp`: `taskIdx` выведен в наружный scope (исправление compile error)
- `kqp_opt_effects.cpp`: добавлен `Y_UNUSED(kqpCtx)` (исправление compile error)
- `kqp_write_table.cpp`: добавлено `#include <util/string/join.h>`; диагностика в деструкторе
- `kqp_tasks_graph.cpp`: множество `AFL_VERIFY` для диагностики
- `kqp_tasks_graph.cpp` (ОБХОДНОЕ РЕШЕНИЕ): `if (stage.InputsSize() == 0) { ... } else { ... }` — пропуск per-shard задач для pure stage
- `kqp_tasks_graph.cpp` (ОБХОДНОЕ РЕШЕНИЕ): `BuildInternalSinks` для OLAP single-task записывает все шарды в TargetShardIds
- `kqp_runtime/ya.make` (ОБХОДНОЕ РЕШЕНИЕ): убран `KQP_WRITE_TABLE_TARGET_SHARD_IDS_EXPECTED_COUNT=1`

---

## Что нужно сделать

### Шаг 1 (уже выполнен — анализ) ✅

Проблема в `BuildUpsertRowsEffect` при `IsDqPureExpr(node.Input()) == true` и OLAP-таблице:
вместо двух отдельных stage (Transform + Sink) создаётся один pure stage без входов.

### Шаг 2: Fix — `BuildUpsertRowsEffect` для pure OLAP + EnableCsWriteAffinity

**Файл:** [`ydb/core/kqp/opt/kqp_opt_effects.cpp`](ydb/core/kqp/opt/kqp_opt_effects.cpp)

**Место:** функция `BuildUpsertRowsEffect`, ветка `if (IsDqPureExpr(node.Input()))` (строки 388-411).

Текущий код (упрощённо):
```cpp
if (IsDqPureExpr(node.Input())) {
    // ... (обработка returning)
    auto stageInput = RebuildPureStageWithSink(
        node.Input(), node.Table(), ...);
    effect = Build<TKqpSinkEffect>(...).Stage(stageInput.Ptr())...;
    return true;
}
```

Нужно: для OLAP + EnableCsWriteAffinity создать **два** stage, как в `BuildFillTableEffect`:

```
Pure Expression (VALUES)
    ↓ (ToFlow)
Transform Stage (pure, 0 inputs, 1 task)
    ↓ TDqCnBroadcast
Sink Stage (N tasks, одна задача на шард, вход через Broadcast)
    ↓ TDqSink → TKqpDirectWriteActor
```

Конкретные шаги:
1. Вычислить `enableCsWriteAffinity` из `kqpCtx.Config->EnableCsWriteAffinity` (аналогично строке 239-244 в `BuildFillTableEffect`)
2. Если `isOlap && enableCsWriteAffinity`:
   - Создать **Transform Stage** (`TDqStage` без входов, программа `ToFlow(node.Input())`, без сinks/outputs)
   - Создать `TDqCnBroadcast` из output[0] Transform Stage
   - Создать **Sink Stage** (`TDqStage` с одним входом — `TDqCnBroadcast`, программа `ToFlow(sinkRowArg)`, output — `TDqSink` с полными sink settings)
   - `effect = TKqpSinkEffect(sinkStage, sinkIndex=0)`
3. Иначе (non-OLAP или affinity выкл): оставить текущую логику `RebuildPureStageWithSink`

Примечание по sink settings для INSERT/REPLACE (в отличие от CTAS):
- `allowInconsistentWrites = settings.AllowInconsistentWrites` (для VALUES это `true`)
- `useStreamWrite` — как вычисляется в `BuildUpsertRowsEffect`
- `mode = settings.Mode` (например `"upsert"`)
- `isIndexImplTable = table.Metadata->IsIndexImplTable`
- `priority` — как вычисляется в `BuildUpsertRowsEffect`
- `defaultColumns = node.DefaultColumns()`

Пример готового кода из `BuildFillTableEffect` (строки 284-331) — взять за шаблон.

### Шаг 3: Откат обходного решения в `CountComputeTasks`

**Файл:** [`ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp)

Удалить добавленное условие `if (stage.InputsSize() == 0) { ... } else { ... }` внутри
`if (isCsWriteAffinitySink)`, вернув исходную логику: per-shard задачи создаются всегда
при наличии OLAP sink + известных шардов.

### Шаг 4: Откат изменений в `BuildInternalSinks`

**Файл:** [`ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp`](ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp)

Вернуть условие:
```cpp
if (settings.GetIsOlap() && stageInfo.Tasks.size() > 1) { ... }
```

Убрать ветку single-task (`else { for all shards ... }`).

### Шаг 5: Откат `ya.make`

**Файл:** [`ydb/core/kqp/runtime/ya.make`](ydb/core/kqp/runtime/ya.make)

Вернуть `KQP_WRITE_TABLE_TARGET_SHARD_IDS_EXPECTED_COUNT=1`:
```
CFLAGS(
    -DKQP_WRITE_TABLE_TARGET_SHARD_IDS_CHECK
    -DKQP_WRITE_TABLE_TARGET_SHARD_IDS_EXPECTED_COUNT=1
    -DQP_FORCE_CS_WRITE_AFFINITY
)
```

### Шаг 6: Запуск теста

```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/query/ \
    -F 'KqpQuery::CTAS_WriteAffinity_Twin+EnableCsWriteAffinity' \
    2>&1 | tail -50
```

Ожидаемый результат: тест `CTAS_WriteAffinity_Twin+EnableCsWriteAffinity` **проходит**.

---

## Справочные материалы

### Как работает Transform+Sink split в BuildFillTableEffect

```
mapCn (TDqCnMap из upstream)
    ↓
Transform Stage:
    Inputs: [mapCn]
    Program: (rowArg) -> ToFlow(rowArg)
    Outputs: []            // нет outputs, только сток через Broadcast
                           // фактически: stage сам по себе, выход — Broadcast ниже

Broadcast (TDqCnBroadcast из Transform Stage output[0])

Sink Stage:
    Inputs: [Broadcast]
    Program: (sinkRow) -> ToFlow(sinkRow)
    Outputs: [TDqSink с TKqpTableSinkSettings]
```

Для **pure** варианта (VALUES без upstream) разница одна:
- Transform Stage имеет **пустые Inputs** (`{}`) и в программе нет аргументов: `() -> ToFlow(expr)`
  где `expr` — сам pure expression (список значений)

### Ключевые типы

- `TDqStage` — stage DQ-плана
- `TDqCnBroadcast` — broadcast-соединение между stages
- `TDqSink` — sink внутри stage
- `TKqpTableSinkSettings` — настройки OLAP sink
- `TKqpSinkEffect` — эффект записи, содержит stage + индекс sink

### Функции-образцы

- `BuildFillTableEffect` — строки 162-357 в `kqp_opt_effects.cpp`
- `RebuildPureStageWithSink` — строки 44-93, строит pure single-stage (НЕ то что нужно)
- `BuildUpsertRowsEffect` с ветвью `!(IsDqPureExpr)` — строки 418-560, строит stage с входом

---

## Диагностические AFL_VERIFY (можно оставить)

В `kqp_tasks_graph.cpp` под `#ifdef QP_FORCE_CS_WRITE_AFFINITY` добавлены проверки:
- В `CountComputeTasks`: `AFL_VERIFY(!stageInfo.Meta.CsShardingColumns.empty())`
- В `BuildKqpStageChannels` (kBroadcast): проверка что ColumnShardHashV1 строится
- В `BuildInternalSinks`: проверка что задачи с TargetShardIds имеют HashShuffle-входы
- В `kqp_write_table.cpp`: проверка `GetColumnShards()[i] == OrderedShardIds[i]`

Эти проверки полезны и их стоит оставить после фикса.
