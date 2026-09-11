# Реализация TRUNCATE TABLE в ColumnShard

Документ описывает реализацию операции `TRUNCATE` колоночной таблицы. Основной
фокус — уровень ColumnShard (`ydb/core/tx/columnshard`), но отдельно разобран
протокол обмена сообщениями со SchemeShard (раздел 2). В конце — сравнение с
операциями `DropTable`, `MoveTable`, `CopyTable` (раздел 7).

## 1. Общая идея

`TRUNCATE` колоночной таблицы реализован как «подмена идентификатора»:

- старая таблица (со всеми порциями данных) **логически удаляется** (drop) и
  отправляется в фоновую очистку;
- для того же внешнего `SchemeShardLocalPathId` аллоцируется **новый**
  `InternalPathId`;
- под новым `InternalPathId` регистрируется **пустая** таблица.

Таким образом, после применения TRUNCATE чтения идут по новому (пустому)
`InternalPathId`, а физические данные старого `InternalPathId` удаляются в фоне
через механизм `PathsToDrop`/GC.

Ключевое понятие: в ColumnShard есть два пространства идентификаторов:
- `TSchemeShardLocalPathId` — внешний id пути, которым оперирует SchemeShard;
- `TInternalPathId` — внутренний id, под которым в движке хранятся данные/порции.

Связь хранится в `TTablesManager::SchemeShardLocalToInternal`
(`SchemeShardLocalPathId → InternalPathId`). После TRUNCATE маппинг **сразу**
указывает на новый пустой `InternalPathId`; отдельного барьера снапшота в
маппинге нет. Любой новый скан (независимо от снапшота) видит новый пустой id —
**silent empty**, как DropTable до завершения GC. Жёсткий барьер снапшота
(`IsPathMappingValidAt`) признан излишним и **не реализован**: сама подмена
`InternalPathId` обеспечивает нужную изоляцию (см. §5.4, §6.1).

Гонка «запись vs TRUNCATE» устранена: на propose TRUNCATE ожидает завершения
всех in-flight транзакций шарда (`TWaitTxs`), аналогично MoveTable. Тем самым
гарантируется, что ни одна запись, стартовавшая до propose, не завершится после
подмены `InternalPathId` — это исключает потерю данных и срабатывание
`AFL_VERIFY(IsReadyForFinishWrite)` (см. §4, §6.2, §8).

## 2. Обмен сообщениями со SchemeShard

ColumnShard участвует в TRUNCATE как обычный участник распределённой схемной
транзакции (`TxTruncateColumnTable = 127`). Со стороны ColumnShard всё
взаимодействие сводится к приёму нескольких сообщений и ответам на них.
Перед рассылкой в шарды SchemeShard сам отклоняет операцию
(`StatusPreconditionFailed`), если выключен feature flag
`EnableTruncateColumnTable` (proto field **303**, default `false`) или
`ColumnShardConfig.GenerateInternalPathId`, либо путь не проходит проверки
(`IsColumnTable`, `NotReadOnlyColumnTable`, locks и т.д.) —
см. `schemeshard/olap/operations/truncate_table.cpp`.

```mermaid
sequenceDiagram
    autonumber
    participant SS as SchemeShard
    participant CO as Coordinator
    participant CS as ColumnShard(ы)

    Note over SS: Локальный Propose: FF / GenerateInternalPathId / path checks
    Note over SS,CS: 1. ConfigureParts — рассылка propose в шарды
    SS->>CS: TEvProposeTransaction (TX_KIND_SCHEMA, TruncateTable{PathId}, SeqNo)
    CS->>CS: DoStartProposeOnExecute: SeqNo + reject read-only
    CS-->>SS: TEvProposeTransactionResult (PREPARED, Min/MaxStep)

    Note over CO,CS: 2. Применение на назначенном шаге плана
    CO->>CS: TEvPlanStep (planStep)
    CS->>CS: RunSchemaTx → RunTruncateTable @ planStep:<br/>DropTable(old) + новый InternalPathId + пустая TTableInfo

    Note over SS,CS: 3. Подтверждение завершения
    SS->>CS: TEvNotifyTxCompletion (txId)
    CS-->>SS: TEvNotifyTxCompletionResult (origin)
```

### Сообщения и реакция ColumnShard

1. **`TEvProposeTransaction` (`TX_KIND_SCHEMA`, тело `TruncateTable{PathId}`,
   `SeqNo`)** — propose схемной транзакции (приходит на фазе ConfigureParts,
   уже после локальных проверок SS). ColumnShard выполняет
   `DoStartProposeOnExecute` (раздел 4): проверка глобального SeqNo и read-only.
   Ответ — **`TEvProposeTransactionResult`** со статусом `PREPARED` и диапазоном
   `planStep` (`Min/MaxStep`); для устаревшего SeqNo — `SCHEMA_CHANGED`, для
   read-only — `SCHEMA_ERROR`. Если путь на шарде неизвестен, propose проходит,
   а применение на плане будет no-op.
2. **`TEvPlanStep` (`planStep`)** — назначенный координатором шаг плана. На этом
   шаге ColumnShard исполняет транзакцию `RunSchemaTx → RunTruncateTable`
   (раздел 5) — собственно усечение: старый `InternalPathId` помечается dropped и
   уходит в фоновую очистку, аллоцируется новый `InternalPathId`, регистрируется
   пустая таблица.
3. **`TEvNotifyTxCompletion` (`txId`)** — запрос статуса завершения. Когда план для
   `txId` исполнен, ColumnShard отвечает **`TEvNotifyTxCompletionResult(origin)`**.

Повторная доставка сообщений и повтор плана для ColumnShard идемпотентны:
`DoOnTabletInit` для `kTruncateTable` — пустой; `RunTruncateTable` для
неизвестного/незамапленного пути или отсутствующей записи таблицы — no-op
(раздел 5). «Уже усечённый» живой путь при этом **не** no-op: маппинг указывает
на новый `InternalPathId`, и повторный TRUNCATE снова дропнет его и аллоцирует
следующий.

## 3. Транспорт схемной транзакции

- Protobuf: добавлено сообщение `TTruncateTable { optional uint64 PathId = 1; }`
  и новый вариант `TTruncateTable TruncateTable = 9;` в `oneof` тела
  `TSchemaTxBody` (`ydb/core/protos/tx_columnshard.proto`).
- Сериализация id пути: специализации
  `TSchemeShardLocalPathId::FromProto/ToProto` для `TTruncateTable`
  (`ydb/core/tx/columnshard/common/path_id.cpp`).
- Идентификаторы после rebase на `main` (занятые ранее номера сдвинуты):
  - `TxTruncateColumnTable = 127` (`schemeshard_subop_types.h`);
  - `EnableTruncateColumnTable = 303` (`feature_flags.proto`);
  - счётчики `COUNTER_IN_FLIGHT_OPS_TxTruncateColumnTable = 239`,
    `COUNTER_FINISHED_OPS_TxTruncateColumnTable = 151`
    (`counters_schemeshard.proto`).

## 4. Обработка в операторе схемной транзакции

`TSchemaTransactionOperator` (`transactions/operators/schema.cpp`):

- **Propose (`DoStartProposeOnExecute`)**, ветка `kTruncateTable`:
  - Проверка SeqNo выполняется по **глобальному** счётчику шарда
    (`LastSchemaSeqNo`), а не по per-path счётчику — `kTruncateTable` не входит в
    `switch`, который задаёт `targetPathId` (там только `kDropTable` и
    `kCopyTable`). То есть TRUNCATE сериализуется относительно всех схемных
    операций шарда; устаревший SeqNo → `SCHEMA_CHANGED`.
  - Содержательная проверка: если путь резолвится и таблица **read-only**
    (например, backup-копия через `CopyTable`), возвращается `SCHEMA_ERROR`
    («Cannot truncate read-only table ...»). Read-only —
    `TablesManager.GetTable(internalPathId).IsReadOnly(schemeShardLocalPathId)`.
    Если путь на шарде неизвестен, проверка пропускается (propose успешен).
  - **Ожидание in-flight транзакций** — аналогично MoveTable:
    `GetProgressTxController().GetTxs()` собирает все текущие tx шарда;
    если список непуст — создаётся `TWaitTxs(txId, txIdsToWait)`, propose
    становится асинхронным, `TTxFinishAsyncTransaction` будет запущен, когда
    последний ожидаемый tx завершится. Маппинг при этом не меняется.
    (`//TODO` в коде: в будущем можно сузить до tx на конкретном pathId.)
- **`DoOnTabletInit`**, ветка `kTruncateTable`: при рестарте повторно собирает
  список in-flight tx шарда (за вычетом собственного txId) и повторно ставит
  ожидание `TWaitTxs` — так же как MoveTable. Это нужно, чтобы propose не
  завис навсегда в случае рестарта во время ожидания.
- **`DoGetOpType`**: тип операции «Scheme:TruncateTable».

## 5. Применение на плане

`TColumnShard::RunTruncateTable` (`columnshard_impl.cpp`), вызывается из
`RunSchemaTx` для `kTruncateTable`:

1. Резолвит `InternalPathId` по `SchemeShardLocalPathId`. Если путь неизвестен или
   уже удалён — **no-op** (только лог).
2. Если таблицы нет (`!HasTable`) — тоже no-op.
3. Вызывает `TablesManager.TruncateTable(...)`, получая новый `InternalPathId`.
4. Создаёт запись версии таблицы (`AddTableVersion`) для нового `InternalPathId`,
   ссылающуюся на **тот же** schema preset шарда
   (`GetSchemaPresets().begin()`), т.к. preset общий для всех таблиц шарда.
5. Обновляет счётчик `COUNTER_TABLES`.

`TTablesManager::TruncateTable` (`tables_manager.cpp`) — ядро операции:

1. `DropTable(schemeShardLocalPathId, oldPathId, version, db)` — помечает старый
   `InternalPathId` как dropped на версии `version` (и добавляет в `PathsToDrop`
   для фоновой очистки данных).
2. Удаляет из `SchemeShardLocalToInternal` маппинг для старого пути.
3. Аллоцирует новый `InternalPathId = MaxInternalPathId + 1`, обновляет
   `MaxInternalPathId`. Здесь стоит инвариант: операция имеет смысл только при
   включённом `GenerateInternalPathId` (иначе внутренний id обязан совпадать с
   внешним, а счётчик `MaxInternalPathId` не персистится между рестартами).
   На уровне ColumnShard это защищено `AFL_VERIFY(GenerateInternalPathId)`;
   отказ пользователю — на propose в SchemeShard
   (`StatusPreconditionFailed`, см. раздел 2).
4. Регистрирует свежую пустую `TTableInfo` под новым `InternalPathId`
   (`RegisterTable`): пишет `TableInfo`/`TableInfoV1`, персистит
   `MaxInternalPathId`, обновляет маппинг `ss → newInternal`, и при наличии
   индекса вызывает `PrimaryIndex->RegisterTable`. Версионный барьер появится
   следом через `AddTableVersion` в `RunTruncateTable` (та же plan-версия).

### 5.1. Восстановление маппинга после рестарта

После TRUNCATE в БД остаются **две** записи таблиц с одним и тем же
`SchemeShardLocalPathId`: старая (dropped) с `oldInternalPathId` и новая с
`newInternalPathId`. При загрузке `InitFromDB` порядок не гарантирован, поэтому
`AddTableInfo` детерминированно предпочитает «живую» (не-dropped) таблицу при
построении `SchemeShardLocalToInternal` — иначе маппинг мог бы указать на dropped
путь, и таблица стала бы нерезолвимой/незаписываемой после рестарта.

### 5.2. Фоновая очистка

`TryFinalizeDropPathOnComplete` при удалении данных старого `InternalPathId`
стирает маппинг `SchemeShardLocalToInternal` **только если он всё ещё указывает на
этот старый id** (после TRUNCATE маппинг уже на новый id, поэтому безусловное
стирание было бы ошибкой).

### 5.3. Очередь удаления при нескольких усечениях

`PathsToDrop` — упорядоченная map «версия дропа → набор `InternalPathId`». Каждый
`TRUNCATE` дропает текущий старый `InternalPathId` на **своём** plan-step
(уникальная версия) и аллоцирует новый. Поэтому при нескольких подряд усечениях
одной таблицы в очереди оказывается несколько записей — **разные `InternalPathId`
под разными версиями, но с одним `SchemeShardLocalPathId`**. Живой (последний)
`InternalPathId` в очередь не попадает. Несколько версий обрабатываются корректно и
независимо:

- **Очистка идёт по всей очереди.** `SetupCleanupPortions`/`SetupCleanupTables`
  берут весь `GetPathsToDrop()` и обрабатывают все накопленные пути, а не один.
- **Каждая версия чистится независимо и только когда это безопасно.**
  `StartCleanupPortions` сверяется с активными read-снапшотами
  (`GetSnapshotHolders()`) и `DataLocksManager`: пути, которые ещё кому-то нужны,
  ждут, остальные удаляются. То есть несколько версий — это несколько независимых
  «ворот».
- **Финализация одной версии не задевает остальные и живой маппинг.**
  `TryFinalizeDropPathOnComplete` удаляет ровно один `pathId` из его версии (и саму
  версию, если набор опустел) и стирает маппинг лишь при
  `it->second == pathId`; для старых id это всегда false.
- **Переживает рестарт.** `InitFromDB` заново отстраивает всю очередь из
  персистентных drop-версий, а `AddTableInfo` детерминированно выбирает живую
  таблицу для маппинга (см. 5.1).

Замечания:
- **Накопление, а не порча.** Корректность не страдает, но до завершения GC все
  старые версии занимают место (порции + записи `TableInfo`/версий схемы в БД).
  Частые усечения подряд → данные копятся, пока фон не догонит.
- **Долгий read-снапшот тормозит конкретную версию**, не блокируя очистку прочих.
- **Инвариант уникальности.** `AFL_VERIFY(PathsToDrop[version].emplace(pathId).second)`
  рассчитывает на уникальность пары (версия, `InternalPathId`); при штатном потоке
  версии разные, коллизий нет.

### 5.4. Поведение снапшотов после truncate

После TRUNCATE маппинг `SchemeShardLocalPathId → InternalPathId` **сразу**
указывает на новый пустой `InternalPathId`. Жёсткий барьер снапшота
(`IsPathMappingValidAt`) признан излишним и **не реализован**: сама подмена id
обеспечивает нужную изоляцию.

При старте любого нового скана (с любым снапшотом, в т.ч. до truncate):

1. `BuildTableMetadataAccessor` резолвирует `SchemeShardLocalPathId` → новый
   `InternalPathId` по текущему маппингу;
2. `HasTable(newId, false)` = `true` → возвращает `TUserTableAccessor` с пустым
   engine-гранулом;
3. скан читает пустой результат — **silent empty**, как DropTable до завершения GC.

| | `CopyVersion` (CopyTable) | Поведение Truncate |
|---|---|---|
| Источник | `TPathInfo.CopyVersion` | нет отдельного барьера |
| Снапшот до операции (новый скан) | `ResolveReadSnapshot` подменяет на CopyVersion | маппинг → новый пустой id → пустой результат |
| Явная ошибка клиенту | нет | нет |
| GC | пинит через `RegisterReadOnlyTableSnapshot` | не пинит для чтения старых данных |
| Персист | `TableVersionInfo` (CopyStep/TxId) | `TableVersionInfo` (первая версия нового id) |

## 6. Обработка записей и чтений, начатых до TRUNCATE

Важная особенность реализации: и для чтения, и для записи `InternalPathId`
**резолвится один раз** — в момент старта операции, по тогдашнему состоянию
маппинга `SchemeShardLocalToInternal`. TRUNCATE этот маппинг подменяет, но операции,
уже захватившие старый `InternalPathId`, продолжают работать с ним. При этом
TRUNCATE, в отличие от MoveTable, **не ждёт** завершения in-flight операций.

### 6.1. Чтения (scan)

`TTxScan` (`engines/reader/transaction/tx_scan.cpp`):

- запрошенный снапшот пропускается через
  `TablesManager.ResolveReadSnapshot(ssPathId, snapshot)` — для read-only/copy
  таблиц возвращается зафиксированная copy-version, для обычных — сам запрошенный
  снапшот;
- затем `ssPathId` → `InternalPathId` по **текущему** маппингу
  (`BuildTableMetadataAccessor`). Жёсткий барьер `IsPathMappingValidAt` **не
  применяется**.

Поведение:

| Сценарий | Поведение |
|---|---|
| Скан, уже стартовавший **до** TRUNCATE (успел зарезолвить старый `InternalPathId`) | Продолжает читать гранулу старого id до фоновой очистки. Консистентен в рамках своего снапшота. |
| Новый скан **после** TRUNCATE с любым снапшотом (в т.ч. старше truncate) | **Silent empty** — маппинг указывает на новый пустой id. Ошибки нет. |

TRUNCATE не даёт MVCC-дочитывания данных через старый снапшот для новых сканов —
это осознанный компромисс (нет snapshot-isolation, как у CopyTable). Вместо этого
поведение совпадает с `DropTable` до завершения GC.

### 6.2. Записи (insert/upsert/delete)

Путь записи (`columnshard__write.cpp`):

- на `EvWrite` `schemeShardLocalPathId` → `InternalPathId` резолвируется по текущему
  маппингу и **сохраняется** в метаданных операции (`writeMeta`/`Pack`), туда же
  попадает проверка `IsReadyForStartWrite` и проверка read-only;
- финализация записи (`TTxBlobsWritingFinished`,
  `blobs_action/transaction/tx_blobs_written.cpp`) работает уже с **зафиксированным**
  `InternalPathId`: пишет в гранулу `index.MutableGranuleVerified(Pack.GetPathId())`
  и проверяет `AFL_VERIFY(IsReadyForFinishWrite(writeMeta.GetPathId().InternalPathId,
  minReadSnapshot))`.

Поэтому запись, начатая до TRUNCATE, нацелена на **старый** `InternalPathId`:

- старый `InternalPathId` после TRUNCATE помечен dropped, но гранула ещё
  существует (до фоновой очистки), поэтому `MutableGranuleVerified(старый)` обычно
  проходит;
- поведение `IsReadyForFinishWrite(старый, minReadSnapshot)` зависит от соотношения
  `minReadSnapshot` (`GetMinSnapshotForNewReads()`) и версии усечения:
  - если `dropVersion < minReadSnapshot`, `HasTable(..., withDeleted=false, ...)`
    вернёт `false`, и сработает `AFL_VERIFY` — это потенциальный аварийный исход
    (abort таблетки);
  - иначе запись «успешно» ляжет в **старый, уже усечённый** `InternalPathId`, и
    эти данные будут затем вычищены фоном вместе со старой таблицей → фактическая
    потеря записи.

Ключевой момент: TRUNCATE **не сериализуется** с конкурентными записями того же
пути (нет ожидания in-flight транзакций и нет per-path-барьера на propose). Это
делает гонку «запись vs TRUNCATE» источником потенциальной потери данных или
срабатывания проверок-инвариантов. (Для сравнения, `MoveTable` явно дожидается всех
in-flight транзакций шарда перед завершением propose.)

## 7. Сравнение: DropTable, CopyTable, MoveTable, TRUNCATE (на уровне ColumnShard)

Все четыре — схемные операции, по-разному манипулирующие связкой
`SchemeShardLocalPathId` ↔ `InternalPathId` ↔ данные.

### Потоки по фазам

| Фаза | DropTable | CopyTable | MoveTable | TRUNCATE |
|---|---|---|---|---|
| **Propose** | SeqNo per-path; содержательной логики нет (`break`); **не ждёт** in-flight tx | Резолвит src, проверяет отсутствие dst; `CopyTablePropose` → dst в `CopyingLocalToInternal`; **не ждёт** in-flight tx (умышленно — иначе зависания при долгоживущих backup-tx) | Резолвит src, проверяет отсутствие dst; `MoveTablePropose` → src-маппинг в `RenamingLocalToInternal`; **ждёт все in-flight tx шарда** (`TWaitTxs`) | Проверка read-only; **ждёт все in-flight tx шарда** (`TWaitTxs`); маппинг не меняется |
| **OnTabletInit** | пустой (`break`) — нет промежуточного состояния | повтор `CopyTablePropose` | повтор `MoveTablePropose` + повторное ожидание tx | повторное ожидание in-flight tx (за вычетом собственного txId) |
| **План** | `SetDropVersion` → `PathsToDrop[ver]`; при partial drop сразу стирает маппинг; при full drop — маппинг до GC; новый `InternalPathId` **не** аллоцируется | dst-алиас на тот же `InternalPathId` (`SchemeShardLocalToInternal[dst]`); `SetCopyVersion`; dst → `ReadOnly`; `RegisterReadOnlyTableSnapshot`; данные не копируются | Переименование `SchemeShardLocalPathId` в `TTableInfo` и БД; убирает из `RenamingLocalToInternal`; добавляет новый маппинг; данные не трогаются | `DropTable(old)` → `PathsToDrop`; аллоцирует новый `InternalPathId`; регистрирует пустую `TTableInfo`; `AddTableVersion`; старые данные уходят в GC |

### Ключевые отличия

| Аспект | DropTable | CopyTable | MoveTable | TRUNCATE |
|---|---|---|---|---|
| InternalPathId | **тот же** (помечается dropped) | **тот же** (dst — алиас src) | **тот же** | аллоцируется **новый** (`MaxInternalPathId+1`) |
| Данные | все → drop + фоновая очистка GC | общие, не копируются; dst read-only | сохраняются полностью | старые → drop + фоновая очистка; новая таблица пустая |
| Маппинг на propose | не меняется | src сохраняется; dst → `CopyingLocalToInternal` | src снимается в `RenamingLocalToInternal` | не меняется |
| Маппинг после плана | остаётся до GC (full drop); сразу стирается (partial drop) | добавляется dst-маппинг | переименовывается | старый маппинг заменяется на новый |
| Ожидание in-flight tx | **нет** | **нет** (умышленно) | **есть** (`TWaitTxs` по всем tx шарда) | **есть** (`TWaitTxs` по всем tx шарда) |
| OnTabletInit | пустой | повтор `CopyTablePropose` | повтор propose + повторное ожидание | повторное ожидание in-flight tx |
| Маппинги SSLocalPathId → InternalPathId | один (до GC) | **два** (src и dst) на один internal | один (переименован на месте) | **две** записи таблиц на один SSLocalPathId → нужен детерминированный выбор живой |
| `MaxInternalPathId` | не трогает | не трогает | не трогает | потребляет (требует `GenerateInternalPathId`) |
| read-only / copy-version | n/a | dst read-only, copy-version зафиксирована | n/a | n/a |
| SeqNo | **per-path** | per-path | глобальный шардовый | глобальный шардовый |
| Снимок старых данных для новых сканов после операции | не сохраняется; до GC — mapped id dropped → **silent empty** для новых сканов | сохраняется (`RegisterReadOnlyTableSnapshot`) | n/a (данные не удаляются) | **не** сохраняется; новый скан видит новый пустой id → **silent empty** |
| Конкурентная запись на старый id | **не исключена** (нет ожидания tx) | n/a (данные не меняются) | исключена ожиданием tx | **исключена** ожиданием tx на propose |
| Барьер чтений | нет — dropped id | CopyVersion (пин) | n/a | нет — маппинг → новый пустой id |

## 8. Потенциальные проблемы

### TRUNCATE
- **Silent empty вместо MVCC для старых чтений.** После TRUNCATE любой новый скан
  (с любым снапшотом, в т.ч. старше truncate) видит новый пустой `InternalPathId` —
  **silent empty**. Жёсткий барьер (`IsPathMappingValidAt`) не реализован. Это
  осознанный компромисс: поведение совпадает с `DropTable` до завершения GC (тихий
  пустой результат вместо явной ошибки). Полноценного snapshot-isolation нет — в
  отличие от CopyTable. Скан, уже зарезолвивший старый id до truncate, может
  дочитать старые данные до GC.
- ~~**Отсутствие ожидания in-flight транзакций.**~~ **Устранено:** TRUNCATE теперь
  ожидает завершения всех in-flight tx шарда через `TWaitTxs` на propose (аналогично
  MoveTable). Запись, стартовавшая до propose, гарантированно завершится до
  применения TRUNCATE на плане, исключая потерю данных и
  `AFL_VERIFY(IsReadyForFinishWrite)`.
- **Ожидание всех tx шарда (не только данного пути).** Как и у MoveTable,
  `GetProgressTxController().GetTxs()` возвращает *все* tx шарда. При наличии
  долгоиграющих несвязанных транзакций propose TRUNCATE может затянуться.
  Уточнение до per-path — TODO в коде.
- **Зависимость от фоновой очистки.** Физическое удаление старых порций
  асинхронно (`PathsToDrop`/GC); до завершения данные занимают место. Для больших
  таблиц очистка может быть длительной.
- **Требование `GenerateInternalPathId` и feature flag.** На SchemeShard propose
  отклоняется с `StatusPreconditionFailed`, если выключен
  `EnableTruncateColumnTable` («TRUNCATE TABLE is not supported for column
  tables») или `GenerateInternalPathId` («... requires GenerateInternalPathId to
  be enabled»). В ColumnShard остаётся `AFL_VERIFY(GenerateInternalPathId)`.
  Без генерации внутренний id обязан совпадать с внешним, а `MaxInternalPathId`
  не персистится — повторные TRUNCATE после рестарта могли бы переиспользовать
  внутренние id.
- **Неоднозначность маппинга после рестарта** (исторически): из-за двух записей с
  одним `SchemeShardLocalPathId`. Закрыто детерминированным выбором живой таблицы в
  `AddTableInfo`. Регрессия в порядке загрузки/учёте dropped-флага способна снова
  сломать резолвинг.
- **Грубость SeqNo.** TRUNCATE использует глобальный SeqNo шарда (а не per-path,
  как Drop/Copy), поэтому он сериализуется относительно любых схемных операций
  шарда; TRUNCATE с меньшим раундом, чем любая прошедшая на шарде схемная
  операция, будет отвергнут.

### MoveTable
- **Ожидание всех транзакций шарда.** `GetProgressTxController().GetTxs()`
  возвращает *все* транзакции шарда, а не только относящиеся к перемещаемому пути.
  При наличии долгоиграющих неотносящихся транзакций propose может «зависать»
  (та же проблема ранее наблюдалась в CopyTable, где от ожидания всех tx
  отказались).
- **Сложность восстановления.** Промежуточное состояние `RenamingLocalToInternal`
  нужно корректно переигрывать в `OnTabletInit`; ошибка в этом приводит к
  рассинхронизации маппинга.
- **«Дыра» в резолвинге между propose и планом.** На propose src-маппинг снимается,
  поэтому до применения на плане путь не резолвится для новых операций.

### CopyTable
- **Накопление read-only-снапшотов.** Каждая копия фиксирует copy-version и
  регистрирует read-only-снапшот; пока копии живы, соответствующие снапшоты данных
  нельзя вычистить, что удерживает старые порции от GC.
- **Общий `InternalPathId`/данные.** src и dst делят один `InternalPathId`,
  поэтому запись/усечение по одному пути затрагивает данные, видимые другому;
  именно поэтому dst делается read-only, а TRUNCATE для read-only-копии запрещён
  (раздел 4).
