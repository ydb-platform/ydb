# Code Review: CS Write Affinity Feature

## Задача

Реализация **ColumnShard Write Affinity** — оптимизации для OLAP-записей (INSERT, REPLACE, UPDATE, DELETE, CTAS), которая маршрутизирует строки напрямую к соответствующему shard-у целевой таблицы через `ColumnShardHashV1` HashShuffle вместо Broadcast. Это устраняет M× сетевой трафик (где M — число shard-ов): каждая строка отправляется только одному Sink-задаче, которая владеет целевым shard-ом для PK-хэша строки.

---

## Изменения по файлам (24 файла, +3213/-111 строк)

### 1. `ydb/core/kqp/opt/kqp_opt_effects.cpp` — Оптимизатор

**Задача:** На этапе оптимизации разбить единый stage (transform + sink) на два отдельных stage: Transform Stage (1 задача) и Sink Stage (N задач, по одной на shard), соединённых `TDqCnHashShuffle` с `ColumnShardHashV1`.

| Операция | Функция | Изменение |
|----------|---------|-----------|
| **CTAS** | `BuildFillTableEffect()` | Добавлен параметр `kqpCtx`. При `enableCsWriteAffinity=true` создаётся Transform Stage → HashShuffle(ColumnShardHashV1) → Sink Stage. KeyColumns берутся из metadata таблицы или fallback на первый столбец SELECT-вывода (для CTAS temp-таблицы, которой ещё нет в metadata). |
| **REPLACE/INSERT (pure expr)** | `BuildUpsertRowsEffect()` | Для OLAP + affinity: разделение на Transform + Sink stage с HashShuffle. KeyColumns из table metadata. |
| **REPLACE/INSERT (non-pure, со source)** | `BuildUpsertRowsEffect()` | Аналогично: Transform Stage (с Map от source) → HashShuffle → Sink Stage. |
| **UPDATE** | `BuildUpsertRowsEffect()` | Аналогично non-pure INSERT. |
| **DELETE** | `BuildDeleteRowsEffect()` | Аналогично: Transform Stage → HashShuffle → Sink Stage. |

**Альтернатива:** Вместо дублирования кода для каждого типа операции, можно было вынести создание Transform→HashShuffle→Sink паттерна в одну вспомогательную функцию. Сейчас код для каждого случая почти идентичен (особенно для pure и non-pure OLAP). Это увеличивает объём кода и сложность поддержки.

### 2. `ydb/core/kqp/executer_actor/kqp_tasks_graph.cpp` — Runtime (TasksGraph)

**Задача:** На этапе построения графа задач создать N задач для Sink Stage (по одной на shard), назначить `TargetShardIds` и построить каналы маршрутизации.

| Изменение | Операция | Описание |
|-----------|----------|----------|
| `BuildColumnShardHashV1ForWriteAffinity()` | Все | Общий helper, который строит параметры `ColumnShardHashV1` (SourceShardCount, TaskIndexByHash, SourceTableKeyColumnTypes) и возвращает key columns для `BuildHashShuffleChannels`. Поддерживает как CTAS (через raw sink settings), так и другие операции (через ResolvedSinkSettings). |
| `kColumnShardHashV1` в `BuildKqpStageChannels()` | CTAS | Распознаёт write affinity режим и вызывает helper вместо shuffle elimination логики. |
| `kBroadcast` в `BuildKqpStageChannels()` | INSERT/UPDATE/DELETE | Конвертирует Broadcast в HashShuffle(ColumnShardHashV1) когда sink stage имеет CsShardingColumns и shard info. |
| `kMap` в `BuildKqpStageChannels()` | Все OLAP | Для OLAP sink с >1 задачи заменяет Map на Broadcast/HashShuffle (Map требует одинаковое число задач у source и target). |
| `BuildInternalSinks()` | Все OLAP | Заполняет `TargetShardIds` для каждой задачи: multi-task path — 1 shard на задачу. |
| `CountComputeTasks()` | Все OLAP | Создаёт N задач для Sink Stage (по одной на shard), pinned к node shard-а. |
| `QP_FORCE_CS_WRITE_AFFINITY` asserts | Все | Debug-инварианты для проверки корректности маршрутизации. |

**Альтернатива:** Логика в `kMap` case дублирует `BuildColumnShardHashV1ForWriteAffinity()` инлайн (~130 строк). Лучше было бы вызывать тот же helper.

### 3. `ydb/core/kqp/ut/query/kqp_write_affinity_ut.cpp` — Тесты (новый файл)

| Тест | Операция | Проверяет |
|------|----------|-----------|
| `Replace` | REPLACE INTO (pure expr) | 3 stage с affinity (HashShuffle + ColumnShardHashV1 + Sink), 2 без |
| `Insert` | INSERT (pure expr) | Корректная запись данных |
| `Update` | UPDATE | Корректное обновление данных |
| `Delete` | DELETE | Корректное удаление данных |
| `Ctas` | CREATE TABLE AS | 4 stage с affinity (extra stage для table creation), 3 без |

**Альтернатива:** Тесты `Insert`, `Update`, `Delete` не проверяют план (только данные). Для полноты coverage стоило бы добавить план-верификацию через `VerifyPlanWithAffinity` и для этих тестов.

### 4. Остальные файлы

| Файл | Роль |
|------|------|
| `kqp_executer_impl.h` | Добавляет CTAS temp-таблицу в ShardIdToNodeId для affinity-маршрутизации |
| `kqp_table_resolver.cpp` | Заполняет CsShardingColumns и ResolvedSinkSettings для sink stage |
| `kqp_write_actor.cpp`, `kqp_write_table.cpp` | Runtime-поддержка TargetShardIds в WriteActor |
| `kqp.proto` | `EnableCsWriteAffinity` флаг в TKqpPhyTx |
| `yql_kikimr_settings.*` | PRAGMA `ydb.EnableCsWriteAffinity` |
| `kqp_opt_hash_func_propagate_transformer.cpp` | Поддержка propagation ColumnShardHashV1 через transform |
| `kqp_query_compiler.cpp` | Передача EnableCsWriteAffinity в транзакцию |
| `kqp_session_actor.cpp` | Session-level поддержка флага |
| `kqp_prepared_query.h` | Хранение флага в prepared query |

---

## Замечания

### 1. Дублирование кода в `kqp_opt_effects.cpp` ✅ Исправлено
Создание Transform→HashShuffle→Sink паттерна повторяется 5+ раз (~400+ строк дублирования). Вынос в общую функцию сократил бы код и уменьшил риск расхождения между операциями.

**Исправление:** Создан helper `BuildCsWriteAffinitySinkStage()` который инкапсулирует общий паттерн. Все 4 места дублирования (`BuildFillTableEffect`, `BuildUpsertRowsEffect` pure/non-pure OLAP, `BuildDeleteRowsEffect`) теперь вызывают этот helper.

### 2. Дублирование в `kMap` case `kqp_tasks_graph.cpp` ✅ Исправлено
~130 строк инлайн-кода в `kMap` case дублируют логику `BuildColumnShardHashV1ForWriteAffinity()`. Лучше было бы вызывать тот же helper.

**Исправление:** Заменён инлайн-блок (~130 строк) на вызов `BuildColumnShardHashV1ForWriteAffinity()`.

### 3. Неполная план-верификация в тестах ✅ Исправлено
Только `Replace` и `Ctas` проверяют план через `VerifyPlanWithAffinity`. Остальные тесты (`Insert`, `Update`, `Delete`) проверяют только данные, не план.

**Исправление:** Добавлена план-верификация через `VerifyPlanWithAffinity` в тесты `Insert`, `Update`, `Delete`. Для UPDATE и DELETE указаны корректные ожидаемые числа стадий (4 с affinity, 3 без), т.к. эти операции включают дополнительный Scan stage для чтения из таблицы.

### 4. `QP_FORCE_CS_WRITE_AFFINITY`
Хороший debug-механизм с AFL_VERIFY инвариантами, но стоит убедиться, что он не попадает в production-сборки (использует `#ifdef`).

### 5. Fallback на Broadcast ✅ Исправлено
Когда ColumnShardHashV1 не может быть построен, код падает back на Broadcast. Это корректно с точки зрения безопасности, но может привести к неожиданной деградации производительности без видимого предупреждения. Рекомендуется добавить логирование при fallback.

**Исправление:** Когда `EnableCsWriteAffinity=true` и ColumnShardHashV1 не может быть построен, теперь возвращается ошибка (`Y_ENSURE(false, ...)`) вместо тихого fallback на Broadcast. Когда affinity отключён — fallback на Broadcast сохраняется как безопасный дефолт.

### 6. Архитектурная целостность
Поддержка реализована последовательно через все слои:
- **Оптимизатор:** создаёт правильный план с HashShuffle
- **Table resolver:** заполняет CsShardingColumns
- **TasksGraph:** создаёт per-shard задачи и каналы
- **Runtime:** маршрутизирует строки через TargetShardIds

Это правильный подход — каждый слой делает свою часть работы.
