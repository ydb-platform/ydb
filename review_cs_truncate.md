# TRUNCATE TABLE в ColumnShard: архитектурный документ

**Дата:** 2026-08-15 (ревизия структуры: 2026-08-17; ревью: 2026-08-20)
**Область:** standalone columnshard-таблицы (не in-store)
**Статус:** базовый TRUNCATE реализован; TRUNCATE источника копий (retention, §9) реализован;
рефакторинг `tables_manager` (§11: `TGenerationIndex`, `TPendingOpFence`) выполнен;
осталось — наблюдаемость/раскатка (Ц5, M3). Целевая картина и разметка «сделано/осталось» — §3.

---

## Оглавление

1. [Задача](#1-задача)
2. [Ключевые структуры до изменений ветки (origin/main)](#2-ключевые-структуры-до-изменений-ветки-originmain)
3. [Целевая картина и статус реализации](#3-целевая-картина-и-статус-реализации)
4. [Архитектура реализации TRUNCATE](#4-архитектура-реализации-truncate)
5. [Ключевые структуры после доработок](#5-ключевые-структуры-после-доработок)
6. [Проблема: TRUNCATE источника, с которого сделаны копии](#6-проблема-truncate-источника-с-которого-сделаны-копии)
7. [Доработки на логическом уровне (для источника копий)](#7-доработки-на-логическом-уровне-для-источника-копий)
8. [Сравнение вариантов реализации](#8-сравнение-вариантов-реализации)
9. [Детальный дизайн выбранного варианта (retention)](#9-детальный-дизайн-выбранного-варианта-retention)
10. [План дальнейших шагов](#10-план-дальнейших-шагов)
11. [Рефакторинг tables_manager: группировка точечных изменений](#11-рефакторинг-tables_manager-группировка-точечных-изменений)
12. [Приложение A: обзор изменений по файлам](#приложение-a-обзор-изменений-по-файлам)
13. [Приложение B: TtlProtos](#приложение-b-ttlprotos)
14. [Приложение C: SchemeShardLocalToInternalAll](#приложение-c-schemeshardlocaltointernalall)
15. [Приложение D: пофайловое ревью изменений (зачем + альтернативы)](#приложение-d-пофайловое-ревью-изменений-зачем--альтернативы)

---

## 1. Задача

Реализовать `TRUNCATE TABLE` для колоночных (columnshard) таблиц YDB так, чтобы:

- операция очищала таблицу от данных, оставляя схему;
- **сохранялись MVCC time-travel гарантии**: чтение по snapshot'у, взятому до TRUNCATE, продолжало видеть старые данные, пока не закрыто read-window;
- операция была устойчива к рестартам таблетки (recovery после crash между фазами);
- поведение согласовывалось с уже существующими schema-операциями `MoveTable`/`CopyTable`.

**Вторая (текущая) задача этого документа** — спроектировать снятие ограничения: сейчас TRUNCATE
**источника**, с которого была сделана одна или несколько read-only копий (`CopyTable`),
отклоняется. Нужна проработка того, как поддержать этот сценарий на логическом уровне, со сравнением
вариантов и планом внедрения.

---

## 2. Ключевые структуры до изменений ветки (origin/main)

Этот раздел фиксирует состояние **до** данной ветки — то, на что опирается TRUNCATE. Все перечисленные
структуры и механизмы уже существовали в `origin/main`; ветка их **переиспользует** (а частично —
дорабатывает, см. §3).

### 2.1 TTableInfo: направление «несколько имён → одно поколение» (это НЕ основа TRUNCATE)

Здесь важно не перепутать два **ортогональных** отношения между `SchemeShardLocalPathId` (логическое имя
таблицы в SchemeShard) и `InternalPathId` (физическое поколение данных в columnshard):

| Направление | Что означает | Кто использует |
|-------------|--------------|----------------|
| **несколько имён → одно поколение** | несколько `SchemeShardLocalPathId` указывают на один `InternalPathId` (делят порции) | **`CopyTable`**: источник + копии на одном поколении |
| **одно имя → несколько поколений** | один `SchemeShardLocalPathId` за свою жизнь проходит через несколько `InternalPathId` | **`TRUNCATE`**: смена поколения во времени (time-travel) |

[`TTableInfo`](ydb/core/tx/columnshard/tables_manager.h:107) в origin/main реализует **только первое**
направление: один `InternalPathId` и **множество** `SchemeShardLocalPathId → TPathInfo`:

```cpp
struct TPathInfo {
    std::optional<NOlap::TSnapshot> DropVersion;   // когда путь дропнут
    std::optional<NOlap::TSnapshot> CopyVersion;   // на каком snapshot зафиксирована копия
    std::optional<TString> LastCompletedBackupTransaction;
    bool IsReadOnly = false;                        // копия — read-only
};
```

Уже существовали методы `CanBeUsedAt(snapshot)` (видимость пути с учётом `CopyVersion`/`DropVersion`),
`GetPathIds()`, `IsReadOnly()`, `GetCopyVersionOptional()`, `SetDropVersion()`, `IsDropped()`.

**Как это относится к TRUNCATE:**

- **Базовому TRUNCATE (без копий) эта модель НЕ помогает напрямую.** Ему нужно обратное отношение
  «одно имя → несколько поколений», которого в origin/main **не было**. Его добавляет ветка через
  `SchemeShardLocalToInternalAll` + `ResolveInternalPathIdForSnapshot` (§3.1, §5.3). Каждое поколение —
  это **отдельный** `TTableInfo` с собственным `InternalPathId`; связь между поколениями одного имени
  живёт не внутри `TTableInfo`, а во внешнем маппинге.
- **Для задачи TRUNCATE источника копий (§6–9) — наоборот, эта модель ключевая.** Именно потому, что
  источник и копии сидят в **одном** `TTableInfo` (одно поколение, общие порции) и копии `IsReadOnly`,
  приколотые к `CopyVersion`, решение §9 (retention) может «снять» только имя источника, оставив копии
  на том же поколении. Здесь работают оба направления сразу: несколько имён на `OLD` (копии) и новое
  поколение `NEW` под именем источника.

**Вывод:** «несколько имён → одно поколение» — это baseline-модель **копий**, а не базового TRUNCATE.
Базовый TRUNCATE стоит на добавленном веткой обратном отношении (§5.3); а copy-модель становится ключом
только в §6–9.

### 2.2 Маппинги путей (origin/main)

[`tables_manager.h` (origin/main)](ydb/core/tx/columnshard/tables_manager.h) содержал три маппинга:

| Маппинг | Назначение |
|---------|-----------|
| `SchemeShardLocalToInternal` | SS-путь → **актуальное** поколение (live reads/writes) |
| `RenamingLocalToInternal` | fence для in-flight `MoveTable` |
| `CopyingLocalToInternal` | fence для in-flight `CopyTable` |

Обратите внимание: **не было** ни `SchemeShardLocalToInternalAll`, ни `TruncatingLocalToInternal` —
их добавляет ветка (§3).

### 2.3 CopyTable и partial-drop (origin/main)

Уже в origin/main существовали:

- **`CopyTablePropose` / `CopyTablePlanStep` / `CopyTableProgress`** — создание read-only копии, которая
  указывает на тот же `InternalPathId`, что и источник (`CopySchemeShardLocalPathId`,
  `SetReadOnly(true)`, `SetCopyVersion`).
- **`MoveTableProgress`** — перенос SS path id при `MoveTable`.
- **`DropTable` с режимом partial-drop** (`isPartialDrop = GetPathIds().size() > 1`): при дропе одного из
  нескольких SS-путей поколение не помечается на полный cleanup, удаляется только этот путь. Это штатный
  путь `DROP` **копии**.
- **`TryFinalizeDropPathOnComplete`** — финализация полного cleanup dropped-поколения после закрытия
  read-window.
- **`Ttl`** — `THashMap<InternalPathId, map<Snapshot, optional<TTiering>>>` — десериализованные
  lifecycle-настройки.

### 2.4 Разрешение пути (origin/main)

Существовал только «плоский» резолвинг актуального поколения: `ResolveInternalPathIdOptional` /
`ResolveInternalPathId`. Разрешения **по snapshot'у** (time-travel по поколениям) не было — оно не
требовалось, пока не было операции, порождающей несколько поколений на один SS path id.

---

## 3. Целевая картина и статус реализации

Раздел описывает **желаемое конечное состояние** фичи `TRUNCATE TABLE` для columnshard (целевая
картина) и **прямо здесь размечает**, что из неё уже сделано в ветке, а что осталось. Статус выставлен
по повторному чтению фактических изменений (`git diff origin/main…HEAD`, исходники `tables_manager.*`,
`schema.cpp`). Детальный чеклист со статусами дублируется в живом разделе в конце документа.

> Легенда: ✅ сделано · 🟡 частично · 🔲 осталось.

### 3.1 Целевая картина по осям (Ц1–Ц5) со статусом

Каждая ось — желаемое конечное состояние; в скобках у пунктов — фактический статус.

**Ц1. Функциональность для пользователя** — ✅ сделано.

- ✅ `TRUNCATE TABLE` работает для **standalone** columnshard-таблиц: очищает данные, сохраняет схему.
- ✅ Операция **идемпотентно восстановима** после рестарта таблетки между propose и plan
  (fence + `DoOnTabletInit`).
- ✅ Поддержаны таблицы с **чистым TTL** (delete-action); таблицы с **tiering** — осознанный реджект
  с понятной ошибкой.
- ✅ `TRUNCATE` **источника read-only копий** (`CopyTable`) — retention-режим реализован (§9.2):
  копии продолжают видеть свои `CopyVersion` на `OLD`, источник получает пустое `NEW` поколение.

**Ц2. MVCC / time-travel гарантии** — ✅ сделано.

- ✅ Чтение на `R < T` видит старые данные до закрытия read-window; на `R >= T` — пустое новое поколение
  (`ResolveInternalPathIdForSnapshot`).
- ✅ Гарантии держатся через **цепочку поколений** (multiple truncate `truncate → insert → truncate`;
  покрыто тестом).
- ✅ `MoveTable`/`CopyTable` над таблицей с историей поколений **сохраняют** историю (`MoveTableProgress`
  переносит `SchemeShardLocalToInternalAll`).

**Ц3. Модель хранения** — ✅ сделано.

- ✅ TRUNCATE **не копирует данные физически**: новое поколение — новый `InternalPathId`; старые порции
  живут как dropped-поколение и чистятся штатным cleanup'ом после read-window.
- ✅ Retention `OLD` под копиями реализован: `OLD` удерживается, пока есть хоть одна копия;
  финализация после DROP последней копии через существующий partial-drop →
  `TryFinalizeDropPathOnComplete` путь.

**Ц4. Внутренняя структура кода (сопровождаемость)** — ✅ сделано.

- ✅ Инкапсуляция «live + история согласованы» в [`TGenerationIndex`](ydb/core/tx/columnshard/tables_manager.h:599) (§11):
  маппинги `Live`/`All` инкапсулированы, все `insert/erase` — через методы индекса.
- ✅ Сведение fence-карт `Renaming`/`Copying`/`Truncating` к одному [`TPendingOpFence`](ydb/core/tx/columnshard/tables_manager.h:556) (§11):
  три экземпляра одного типа с единым протоколом `Propose`/`Get`/`Complete`/`Abort`.
- ✅ Документ приведён в соответствие с фактической реализацией.

**Ц5. Наблюдаемость и раскатка** — 🟡 частично.

- 🔲 Метрика размера истории поколений (`SchemeShardLocalToInternalAll`) и числа поколений на путь;
  hard-limit/алерт на аномальный рост.
- ✅ Тестовое покрытие: функциональные + compatibility (restart/rolling upgrade) + стресс
  (`truncate_insert`/`truncate_concurrent`).
- 🔲 Feature-flag на стендах, поэтапный rollout, пользовательская документация.

### 3.2 Что уже сделано в ветке (факт)

| Область | Статус | Где |
|---------|:---:|-----|
| Proto `TTruncateTable` + поле в `TSchemaTxBody` | ✅ | [`tx_columnshard.proto`](ydb/core/protos/tx_columnshard.proto) |
| Plan-фаза `RunTruncateTable` | ✅ | [`columnshard_impl.cpp`](ydb/core/tx/columnshard/columnshard_impl.cpp) |
| Lifecycle `TruncateTable` / `TruncateTablePropose` | ✅ | [`tables_manager.cpp`](ydb/core/tx/columnshard/tables_manager.cpp:550) |
| `TGenerationIndex` (live + история поколений) | ✅ | [`tables_manager.h:599`](ydb/core/tx/columnshard/tables_manager.h:599) |
| `TPendingOpFence` (fence для Move/Copy/Truncate) | ✅ | [`tables_manager.h:556`](ydb/core/tx/columnshard/tables_manager.h:556) |
| Выбор поколения `ResolveInternalPathIdForSnapshot` | ✅ | [`tables_manager.cpp:79`](ydb/core/tx/columnshard/tables_manager.cpp:79) |
| Бит-в-бит перенос TTL `TtlProtos` | ✅ | [`tables_manager.h`](ydb/core/tx/columnshard/tables_manager.h) |
| Recovery-fence в `DoOnTabletInit` | ✅ | [`schema.cpp:408`](ydb/core/tx/columnshard/transactions/operators/schema.cpp:408) |
| Валидация propose (4 проверки: GenerateInternalPathId, IsStoreTablet, Check 1 read-only, Check 3 tiering) | ✅ | [`schema.cpp:232`](ydb/core/tx/columnshard/transactions/operators/schema.cpp:232) |
| Retention источника копий (`hasCopies` ветка) | ✅ | [`tables_manager.cpp:556`](ydb/core/tx/columnshard/tables_manager.cpp:556) |
| Функциональные тесты (17 сценариев, 888 строк) | ✅ | [`ut_columnshard_truncate_table.cpp`](ydb/core/tx/columnshard/ut_schema/ut_columnshard_truncate_table.cpp) |
| Compatibility-тест (restart/rolling upgrade) | ✅ | [`test_truncate_table.py`](ydb/tests/compatibility/olap/test_truncate_table.py) |
| Стресс-нагрузки `truncate_insert` / `truncate_concurrent` | ✅ | [`ydb/tests/stress/olap_truncate/`](ydb/tests/stress/olap_truncate/workload/type/truncate_insert.py) |

> **Про abort.** SchemeShard не отменяет schema tx после propose, поэтому явного abort-пути для TRUNCATE
> нет. Единственный «откат» — рестарт до plan, закрываемый фазой Recovery (`DoOnTabletInit`).


### 3.3 Что осталось до целевой картины (зазор)

Сгруппировано по осям целевой картины (§3.1). Порядок — рекомендуемый.

**До Ц1/Ц2 (базовый TRUNCATE «под ключ»):**
- ✅ Тесты: multiple-truncate (цепочка поколений), tiering edge case, eviction `LoadLastTableVersionInfo`
  (закрыты, см. живой раздел M1).
- ✅ Проверить всех callers [`BuildTableMetadataAccessor`](ydb/core/tx/columnshard/tables_manager.cpp)
   после смены сигнатуры `optional<TSnapshot>&` → `TSnapshot&`: 2 caller'а —
   [`tx_scan.cpp:113`](ydb/core/tx/columnshard/engines/reader/transaction/tx_scan.cpp:113) (изменённый overload, передаёт `TSnapshot`) и
   [`tx_internal_scan.cpp:61`](ydb/core/tx/columnshard/engines/reader/transaction/tx_internal_scan.cpp:61) (неизменённый overload с `internalPathId`, передаёт `TSnapshot` в `optional`).

**До Ц4 (сопровождаемость, рефакторинг §11) — ✅ сделано:**
- ✅ Ось A: [`TGenerationIndex`](ydb/core/tx/columnshard/tables_manager.h:599) введён, заменил сырые
  `SchemeShardLocalToInternal`/`SchemeShardLocalToInternalAll`; все `insert/erase` — через методы индекса;
  `ResolveInternalPathIdForSnapshot` делегирует в `GenerationIndex.ResolveForSnapshot()` (шаблон с callback'ами).
- ✅ Ось B: [`TPendingOpFence`](ydb/core/tx/columnshard/tables_manager.h:556) введён, заменил три сырые
  карты `RenamingLocalToInternal`/`CopyingLocalToInternal`/`TruncatingLocalToInternal` на экземпляры
  `Renaming`/`Copying`/`Truncating`.
- ✅ Все 105+ тестов schema-операций проходят после рефакторинга (поведение не изменилось).

**До Ц1/Ц3 (TRUNCATE источника копий, retention §9) — ✅ сделано:**
- ✅ Реджект Check 2 (`GetPathIds().size() > 1`) для источника убран из [`schema.cpp`](ydb/core/tx/columnshard/transactions/operators/schema.cpp:232)
  (реджект копии — Check 1 `IsReadOnly` — оставлен).
- ✅ Retention-ветка в [`TruncateTable`](ydb/core/tx/columnshard/tables_manager.cpp:550): при `hasCopies`
  помечает источник dropped на `T`, не удаляя из `OLD`; выделяет `NEW`; не помечает `OLD` на полный cleanup (§9.2).
- ✅ `DoOnTabletInit` recovery для TRUNCATE реализован ([`schema.cpp:408`](ydb/core/tx/columnshard/transactions/operators/schema.cpp:408)).
- ✅ Финализация `OLD` после DROP последней копии — существующий partial-drop →
  [`TryFinalizeDropPathOnComplete`](ydb/core/tx/columnshard/tables_manager.cpp) путь.
- ✅ Тесты: `TruncateCopySourceRetention`, `TruncateSourceAfterDropCopySucceeds`.

**До Ц5 (наблюдаемость и раскатка):**
- 🔲 Метрика размера `SchemeShardLocalToInternalAll` и числа поколений на путь; hard-limit/алерт.
- 🔲 Включить флаг на стендах; поэтапный rollout; обновить пользовательскую документацию.

### 3.4 Новые сущности ветки (не было в origin/main)

Ниже — сводка **добавленного/изменённого** относительно §2. Детали — в §4–5 и Приложении A.

> Обозначения: **origin/main** — состояние до данной ветки; **ветка** — изменения, вносимые здесь.

| Сущность | Файл | Роль |
|----------|------|------|
| `TTruncateTable` (proto) | [`tx_columnshard.proto`](ydb/core/protos/tx_columnshard.proto) | новый тип schema tx |
| `RunTruncateTable` | [`columnshard_impl.cpp`](ydb/core/tx/columnshard/columnshard_impl.cpp) | выполнение на plan-фазе |
| `TruncateTable` / `TruncateTablePropose` | [`tables_manager.cpp`](ydb/core/tx/columnshard/tables_manager.cpp:587) | lifecycle операции |
| `SchemeShardLocalToInternalAll` | [`tables_manager.h:429`](ydb/core/tx/columnshard/tables_manager.h:429) | история **всех** поколений пути (MVCC time-travel) |
| `TruncatingLocalToInternal` | [`tables_manager.h:434`](ydb/core/tx/columnshard/tables_manager.h:434) | fence для in-flight TRUNCATE |
| `ResolveInternalPathIdForSnapshot` | [`tables_manager.cpp:79`](ydb/core/tx/columnshard/tables_manager.cpp:79) | выбор поколения по snapshot чтения |
| `TtlProtos` | [`tables_manager.h:352`](ydb/core/tx/columnshard/tables_manager.h:352) | сырой lifecycle-proto для бит-в-бит переноса на новое поколение |

### 3.5 Изменённое поведение существующих механизмов

| Механизм | Было (origin/main) | Стало (ветка) |
|----------|--------------------|---------------|
| `AddTableInfo` | заполнял только `SchemeShardLocalToInternal` | плюс заполняет `SchemeShardLocalToInternalAll`; не перетирает live-маппинг dropped-поколением |
| `MoveTableProgress` | переносил один SS path id | плюс переносит **всю историю поколений** (`SchemeShardLocalToInternalAll`) и переименовывает SS path id в исторических `TTableInfo` |
| `TryFinalizeDropPathOnComplete` | безусловный `erase` из `SchemeShardLocalToInternal` | erase только если маппинг всё ещё указывает на дропаемый id; чистит `SchemeShardLocalToInternalAll` и `Ttl`/`TtlProtos` |
| `CopyTableProgress` | регистрировал копию | плюс добавляет копию в `SchemeShardLocalToInternalAll` |
| `BuildTableMetadataAccessor` | `const optional<TSnapshot>& = nullopt` | `const TSnapshot&`; внутри `ResolveInternalPathIdForSnapshot` (breaking, все callers обновлены и проверены) |

### 3.6 Что НЕ менялось (переиспользуется как есть)

- Модель `TTableInfo`/`TPathInfo` «несколько имён → одно поколение» и семантика `CanBeUsedAt` (§2.1) —
  это модель **копий**; базовый TRUNCATE стоит на добавленном отношении «одно имя → несколько поколений».
- `CopyTable*` — API создания копий (ветка лишь дописывает `SchemeShardLocalToInternalAll`).
- `DropTable` partial-drop как путь `DROP` копии (для источника копий §9 предлагает **новую** ветку).
- `TryFinalizeDropPathOnComplete` как механизм отложенного cleanup после read-window.

---

## 4. Архитектура реализации TRUNCATE

### 4.1 Базовая идея: TRUNCATE как смена поколения

TRUNCATE **не удаляет данные физически**. Вместо этого:

1. текущему пути выделяется **новый `InternalPathId`** — новое «поколение» (generation) таблицы;
2. старый `InternalPathId` помечается как dropped на версии `T`;
3. `SchemeShardLocalPathId` (логическое имя таблицы) перенаправляется на новое поколение;
4. старые порции не удаляются немедленно — они очищаются штатным cleanup'ом только после закрытия
   read-window.

Так MVCC time-travel получается «бесплатно»: старое поколение живёт как обычная dropped-таблица,
а `ResolveInternalPathIdForSnapshot` выбирает нужное поколение по snapshot'у чтения.

### 4.2 Жизненный цикл операции: propose → fence → plan

Паттерн заимствован из `MoveTable`/`CopyTable`:

| Фаза | Метод | Действие |
|------|-------|----------|
| **Propose** | [`TruncateTablePropose`](ydb/core/tx/columnshard/tables_manager.cpp:610) | Валидация; снятие пути из `SchemeShardLocalToInternal`; установка fence в `TruncatingLocalToInternal`; блокировка новых writes |
| **Plan** | [`TruncateTable`](ydb/core/tx/columnshard/tables_manager.cpp:587) | `DropTable(old)` + `GenerateNextInternalPathId()` + `RegisterTable(new)`; снятие fence |
| **Recovery** | [`DoOnTabletInit`](ydb/core/tx/columnshard/transactions/operators/schema.cpp:415) | Повторная установка fence после рестарта между propose и plan |

> **Нет фазы Abort.** SchemeShard не отменяет schema tx после propose, поэтому явного abort-пути для
> TRUNCATE нет. Единственный «откат» — рестарт до plan, закрываемый фазой Recovery через `DoOnTabletInit`.

Точка выполнения на plan-фазе — `RunTruncateTable` в
[`columnshard_impl.cpp`](ydb/core/tx/columnshard/columnshard_impl.cpp): резолвит старый
`InternalPathId`, захватывает schema/TTL старого поколения, вызывает `TablesManager.TruncateTable(...)`,
регистрирует версию и TTL на новом `InternalPathId`.

### 4.3 Валидация на propose

TRUNCATE отклоняется (`SCHEMA_ERROR`), если:

1. таблетка не генерирует internal path ids (`!IsGenerateInternalPathId()`);
2. это store-таблетка (`IsStoreTablet()`);
3. таблица read-only (это **копия**, созданная `CopyTable`) — Check 1;
4. у таблицы есть tiering (`!ttl->GetUsedTiers().empty()`) — Check 3.

> **Check 2 убран.** Ранее TRUNCATE источника с копиями отклонялся (`GetPathIds().size() > 1`).
> После реализации retention (§9) этот реджект удалён: TRUNCATE источника с копиями идёт в
> retention-режиме, где `OLD` сохраняется для копий, а источник получает `NEW`.

> **Про feature-flag.** В columnshard propose проверки feature-flag **нет** (grep по
> `EnableTruncate*` в `schema.cpp` пуст). Соответствующий флаг `EnableTruncateTable` в
> [`feature_flags.proto`](ydb/core/protos/feature_flags.proto:254) находится в состоянии
> `reserved` и веткой не трогается; гейтинг фичи, если нужен, живёт на уровне SchemeShard.

---

## 5. Ключевые структуры после доработок

Итоговое состояние структур (baseline §2 + доработки §3). **Жирным** помечено добавленное веткой.

### 5.1 TTableInfo: одно поколение с несколькими именами (без изменений)

[`TTableInfo`](ydb/core/tx/columnshard/tables_manager.h:107) хранит один `InternalPathId` и **множество**
`SchemeShardLocalPathId → TPathInfo` (см. §2.1) — это направление «несколько имён → одно поколение»
(модель копий). Веткой **не менялся**. Напомним разграничение из §2.1: связь «одно имя → несколько
поколений» (нужную базовому TRUNCATE) `TTableInfo` **не** выражает — она живёт во внешнем
`SchemeShardLocalToInternalAll` (§5.2–5.3). Эта же copy-модель — фундамент решения §9:

```cpp
struct TPathInfo {
    std::optional<NOlap::TSnapshot> DropVersion;   // когда путь дропнут
    std::optional<NOlap::TSnapshot> CopyVersion;   // на каком snapshot зафиксирована копия
    std::optional<TString> LastCompletedBackupTransaction;
    bool IsReadOnly = false;                        // копия — read-only
};
```

Важные методы (все из origin/main):
- [`CanBeUsedAt(snapshot)`](ydb/core/tx/columnshard/tables_manager.h:119) — видим ли путь на snapshot'е:
  учитывает `CopyVersion` (нижняя граница) и `DropVersion` (верхняя, эксклюзивная);
- [`GetPathIds()`](ydb/core/tx/columnshard/tables_manager.h:136) — все `(Internal, SS)` пары поколения;
- `IsDropped()` — все ли SS-пути поколения дропнуты (условие полного cleanup).

### 5.2 Маппинги путей в TTablesManager

[`tables_manager.h:427-434`](ydb/core/tx/columnshard/tables_manager.h:427):

| Маппинг | Origin/main? | Назначение |
|---------|:---:|-----------|
| `SchemeShardLocalToInternal` | да | SS-путь → **актуальное** поколение (live reads/writes) |
| **`SchemeShardLocalToInternalAll`** | **нет (ветка)** | SS-путь → **все** поколения (MVCC time-travel; Приложение C) |
| `RenamingLocalToInternal` | да | fence для in-flight `MoveTable` |
| `CopyingLocalToInternal` | да | fence для in-flight `CopyTable` |
| **`TruncatingLocalToInternal`** | **нет (ветка)** | fence для in-flight `TRUNCATE` |

### 5.3 Выбор поколения по snapshot'у (новое в ветке)

[`ResolveInternalPathIdForSnapshot`](ydb/core/tx/columnshard/tables_manager.cpp:79) — добавлен веткой,
сердце MVCC-маршрутизации по поколениям (в origin/main его не было — §2.4):

1. берёт все поколения из `SchemeShardLocalToInternalAll[ss]`;
2. пропускает поколение, если оно не содержит данный SS-путь (`!HasSchemeShardLocalPathId`) — важно после MOVE;
3. пропускает поколение, если `dropVersion <= readSnapshot` (эксклюзивная граница, согласована с `CanBeUsedAt`);
4. среди оставшихся выбирает с наименьшим `dropVersion > readSnapshot` (`best`);
5. если dropped-кандидатов нет — возвращает live-поколение;
6. финальный fallback — `ResolveInternalPathIdOptional` (поведение origin/main).

---

## 6. Проблема: TRUNCATE источника, с которого сделаны копии

### 6.1 Что такое копия в columnshard

`CopyTable` **не копирует данные**. Он добавляет новый `SchemeShardLocalPathId` (имя копии) в тот же
самый [`TTableInfo`](ydb/core/tx/columnshard/tables_manager.h:107), что и источник, помечая его
`IsReadOnly=true` и фиксируя `CopyVersion=S`
([`CopyTablePlanStep`](ydb/core/tx/columnshard/tables_manager.cpp:850),
[`CopySchemeShardLocalPathId`](ydb/core/tx/columnshard/tables_manager.h:261)):

```
TTableInfo (InternalPathId = OLD)
├── source = { DropVersion: -,  CopyVersion: -,  IsReadOnly: false }
├── copy1  = { DropVersion: -,  CopyVersion: S1, IsReadOnly: true  }
└── copy2  = { DropVersion: -,  CopyVersion: S2, IsReadOnly: true  }
```

Источник и все копии **делят одни и те же физические порции** (portions, chunks, blobs).

### 6.2 Текущее поведение — реджект

TRUNCATE источника отклоняется на propose-фазе:
`SCHEMA_ERROR: "Cannot truncate table that shares storage with a copy"` — проверка
`table.GetPathIds().size() > 1` в
[`schema.cpp:259`](ydb/core/tx/columnshard/transactions/operators/schema.cpp:259).

### 6.3 Почему наивный TRUNCATE опасен

Базовый TRUNCATE выделил бы источнику новое поколение `NEW` и пометил `OLD` полностью dropped. Если бы
это шло по обычному пути:

1. **Копия становится orphan** — `OLD` уходит на cleanup, хотя копии всё ещё на него ссылаются.
2. **Нарушается MVCC-гарантия копии** — копия обязана видеть данные на момент своей фиксации `Si`.
3. **Коррупция shared storage** — удаление порций `OLD` затрагивает и копии.

Поэтому запрет — это защита инварианта целостности, а не техническая лень. Задача §7–9 — снять запрет
корректно.

### 6.4 Ключевое наблюдение (делает задачу разрешимой)

Копия **изолирована во времени**: [`CanBeUsedAt`](ydb/core/tx/columnshard/tables_manager.h:119) для
read-only пути возвращает `true` только при `Si <= snapshot < DropVersion`. Значит:

- копия никогда не видит данные, записанные после `Si`;
- при TRUNCATE источника на `T > Si` диапазон видимости копии `[Si, T)` целиком лежит в живой истории
  `OLD`;
- копии **не нужны новые данные** — им нужно лишь, чтобы старые порции продолжали существовать.

**Вывод:** deep copy данных не требуется. Достаточно **не удалять `OLD`**, пока на него ссылается хотя бы
одна копия. Это ровно то свойство shared-порций, ради которого `CopyTable` не копирует данные.

---

## 7. Доработки на логическом уровне (для источника копий)

Ниже — что нужно изменить в модели, независимо от конкретной реализации. Формулировки в терминах
инвариантов.

### 7.1 Разделить две семантики «drop пути»

Сейчас [`DropTable`](ydb/core/tx/columnshard/tables_manager.cpp:553) в режиме partial-drop
(`GetPathIds().size() > 1`) **полностью удаляет** SS-путь из поколения (`table->Remove(path)` +
вычистка из `SchemeShardLocalToInternalAll`). Это верно для `DROP` копии, но ломает time-travel по
**источнику** после TRUNCATE, потому что `ResolveInternalPathIdForSnapshot`:

- отсеет `OLD` из-за `!HasSchemeShardLocalPathId(source)` (путь удалён);
- не найдёт `OLD` в `SchemeShardLocalToInternalAll[source]`.

Нужна **третья семантика — «drop пути с retention поколения»**:

> Путь источника помечается dropped на `T` (для time-travel), но **остаётся** в `TTableInfo(OLD)` и в
> `SchemeShardLocalToInternalAll[source]`; поколение `OLD` **не** уходит в `PathsToDrop`, пока его держат
> копии.

### 7.2 Инварианты, которые должны сохраняться

- **I1 (видимость источника до T):** read источника на `R ∈ [Smin, T)` → `OLD`.
- **I2 (видимость источника от T):** read источника на `R >= T` → `NEW` (пусто).
- **I3 (видимость копий):** read копии `copyN` на её `Sn` → `OLD`; TRUNCATE источника **не меняет**
  `TPathInfo` копий, поэтому их видимость не зависит от `T`. Это и обеспечивает корректность для
  **нескольких** копий одновременно.
- **I4 (cleanup):** `OLD` физически удаляется только когда дропнуты **все** его SS-пути (источник на `T`
  + все копии) и закрыто read-window.
- **I5 (fence):** пока TRUNCATE источника in-flight, новый `CopyTable` с источника не должен
  зафиксироваться на `OLD` (иначе появится копия, о которой TRUNCATE «не знал»).

### 7.3 Целевое состояние (2 копии)

```
TTableInfo(OLD)
├── source = { DropVersion: T,  CopyVersion: -,  IsReadOnly: false }   ← жив для time-travel [Smin, T)
├── copy1  = { DropVersion: -,  CopyVersion: S1, IsReadOnly: true  }
└── copy2  = { DropVersion: -,  CopyVersion: S2, IsReadOnly: true  }

TTableInfo(NEW)  (пусто)
└── source = { DropVersion: -,  CopyVersion: -,  IsReadOnly: false }   ← актуальный источник

SchemeShardLocalToInternal:    source→NEW, copy1→OLD, copy2→OLD
SchemeShardLocalToInternalAll: source→{OLD, NEW}, copy1→{OLD}, copy2→{OLD}
```

Проверка инвариантов по `ResolveInternalPathIdForSnapshot`:

| Путь | Read snapshot R | Поколение | Обоснование |
|------|-----------------|-----------|-------------|
| source | `R < T` | `OLD` | `dropVersion=T`, `T > R` → `best=OLD` (стр. 104–114) |
| source | `R >= T` | `NEW` | `OLD` отсеян (`T <= R`), остаётся `live=NEW` (стр. 119) |
| copy1 | `R = S1` | `OLD` | `SchemeShardLocalToInternal[copy1]=OLD`; `CanBeUsedAt(S1)` true |
| copy2 | `R = S2` | `OLD` | аналогично |

---

## 8. Сравнение вариантов реализации

| # | Вариант | Суть | Сложность | Риск | Данные дублируются | Вердикт |
|---|---------|------|-----------|------|--------------------|---------|
| 1 | **Deep copy** | Физически скопировать `OLD` в изолированное хранилище копий | Высокая | Высокий | Да (×2 диск) | ✗ |
| 2 | **Cascade drop** | При TRUNCATE удалить все копии | Средняя | Высокий (UX) | Нет | ✗ |
| 3 | **Deferred TRUNCATE** | Отложить TRUNCATE до удаления/истечения копий | Средняя | Средний | Нет | ✗ |
| 4 | **Документированный реджект** | Оставить как есть, требовать `DROP copy` вручную | Низкая | Низкий | Нет | ~ (текущее) |
| 5 | **Retention старого поколения** | Переселить только источник; держать `OLD` живым, пока есть копии | **Низкая–средняя** | **Низкий** | **Нет** | ✓ **выбран** |

### 8.1 Вариант 1 — deep copy

**Против:** таблицы бывают терабайтными; синхронное копирование в рамках schema tx блокирует таблетку;
нужна атомарность относительно активных сканов; удвоение диска; новый async-механизм копирования.

### 8.2 Вариант 2 — cascade drop

**Против:** SchemeShard не знает о копиях на уровне columnshard — нужна координация; silent-drop копий
опасен для пользователя; активные сканы копий надо дождать; распределённый сценарий по нескольким
шардам.

### 8.3 Вариант 3 — deferred TRUNCATE

**Против:** новый persistent state `PENDING_COPIES_EXPIRED`; coupling DROP-копии с pending TRUNCATE;
риск «вечного pending», если копии пересоздаются; нужна FIFO-очередь операций на путь.

### 8.4 Вариант 4 — документированный реджект (текущее)

**За:** нулевая сложность, безопасно. **Против:** UX (лишние шаги `DROP copy` → `TRUNCATE`). Гонку
«создание копии между проверкой и выполнением» закрывает fence. Подходит как краткосрочная мера.

### 8.5 Вариант 5 — retention (рекомендуемый)

**За:** не требует ни deep copy, ни cascade, ни новой state-машины; использует уже готовую модель
shared-порций и partial-drop; данные не дублируются. **Против:** требует аккуратного разведения двух
семантик drop (§7.1) и тестов на concurrency/cleanup. Детальный дизайн — §9.

---

## 9. Детальный дизайн выбранного варианта (retention)

### 9.1 Почему механика уже почти готова: partial-drop

[`DropTable`](ydb/core/tx/columnshard/tables_manager.cpp:553) уже умеет partial-drop при
`GetPathIds().size() > 1`: не пишет глобальный `SaveTableDropVersion` (поколение не идёт на полный
cleanup), удаляет только один SS-путь, вычищает его из маппингов. Это ровно поведение `DROP` копии.
TRUNCATE источника — зеркальный случай: «снять» источник с `OLD`, переселив его на `NEW`, но оставить
копии на `OLD`. Разница с partial-drop — источник нельзя удалять полностью (§7.1).

### 9.2 Поведение `TruncateTable` (retention-ветка)

Срабатывает, когда у источника есть копии (`table.GetPathIds().size() > 1`). Вместо реджекта:

1. **Пометить источник dropped на `T`, не удаляя из `OLD`:** `SetDropVersion(source, T)` +
   `SaveTableDropVersionV1(...)`; **не** делать `table->Remove(source)`; **не** удалять `OLD` из
   `SchemeShardLocalToInternalAll[source]`. Инвариант I1 сохраняется.
2. **Снять источник с live-маппинга:** `SchemeShardLocalToInternal.erase(source)`.
3. **Не помечать `OLD` на полный cleanup:** `IsDropped()` останется `false` (копии живы) → `PathsToDrop`
   не пополняется. Инвариант I4.
4. **Выделить `NEW` и зарегистрировать пустую таблицу под источником:** `GenerateNextInternalPathId()`
   → `RegisterTable(NEW под source)`; `SchemeShardLocalToInternal[source]=NEW`;
   `SchemeShardLocalToInternalAll[source].insert(NEW)`. Инвариант I2.
5. **Снять fence:** `TruncatingLocalToInternal.erase(source)`.

Реализовано без feature-флага: TRUNCATE источника с копиями разрешён всегда (retention-вариант).

### 9.3 Cleanup `OLD` после удаления копий

`OLD` удаляется только когда перестаёт быть нужен всем (I4):

- каждый `DROP copyN` идёт по существующему partial-drop и убирает свой путь из `OLD`;
- источник на `OLD` уже dropped на `T`;
- после удаления **последней** копии на `OLD` остаётся единственный (dropped) путь-источник →
  `IsDropped()` == `true` → `OLD` попадает в `PathsToDrop[T]` и очищается штатным
  [`TryFinalizeDropPathOnComplete`](ydb/core/tx/columnshard/tables_manager.cpp) — **но не раньше**
  закрытия read-window.

**Тонкость:** переход `OLD` в `PathsToDrop` наступает не в TRUNCATE, а в DROP последней копии. Нужно
убедиться (тестом), что `DropTable` в этот момент корректно финализирует полный drop поколения.

### 9.4 Concurrency и fence (I5)

`TruncateTablePropose` уже снимает `source` из `SchemeShardLocalToInternal` и ставит fence. Значит:

- **`CopyTable` во время in-flight TRUNCATE:** `CopyTablePropose`/`CopyTablePlanStep` резолвят источник
  через `ResolveInternalPathId(source)`, который после fence вернёт `nullopt` → copy отклоняется/
  сериализуется до завершения TRUNCATE. Гонка «копия зафиксировалась на `OLD` между propose и plan»
  исключена.
- **TRUNCATE копии** по-прежнему реджектится (Check 1, `IsReadOnly`) — не снимаем.

### 9.5 Персистентность и recovery

Всё состояние восстановимо из NiceDb существующими путями:

- источник на `OLD` с `DropVersion=T` — `SaveTableDropVersionV1`;
- `NEW` под источником — `RegisterTable` (`SaveTableInfo*`);
- копии на `OLD` — уже персистированы `CopyTablePlanStep` (`SaveTableCopyVersionV1`).

После рестарта `InitFromDB → AddTableInfo` пересобирает `SchemeShardLocalToInternalAll[source]={OLD,NEW}`
и `copyN→{OLD}`; `TruncatingLocalToInternal` пуст (fence снят на plan). Ветка
[`DoOnTabletInit`](ydb/core/tx/columnshard/transactions/operators/schema.cpp:415) должна перестать делать
`break` по `GetPathIds().size() > 1` для TRUNCATE, иначе recovery разойдётся с новым propose.

### 9.6 Точки изменений в коде

1. ✅ [`schema.cpp` propose](ydb/core/tx/columnshard/transactions/operators/schema.cpp:232):
   убран реджект `GetPathIds().size() > 1` для источника (Check 1 — реджект TRUNCATE копии `IsReadOnly` — оставлен).
2. ✅ [`TruncateTable`](ydb/core/tx/columnshard/tables_manager.cpp:550): retention-ветка реализована (§9.2) —
   при `hasCopies` помечает источник dropped на `T`, не удаляя из `OLD`; выделяет `NEW`; не помечает `OLD` на полный cleanup.
3. ✅ [`DropTable`](ydb/core/tx/columnshard/tables_manager.cpp:553): подтверждено, что DROP
   последней копии финализирует полный drop `OLD` (существующий partial-drop путь).
4. ✅ [`DoOnTabletInit`](ydb/core/tx/columnshard/transactions/operators/schema.cpp:408): recovery для TRUNCATE
   реализован — после рестарта повторно устанавливает fence через `TruncateTablePropose`.

### 9.7 Тесты для валидации

| Тест | Сценарий | Проверка |
|------|----------|----------|
| `TruncateSourceWithOneCopyKeepsCopyReadable` | copy(S) → truncate source(T>S) | copy@S видит старые данные; source@≥T пуст |
| `TruncateSourceWithMultipleCopies` | copy1(S1), copy2(S2) → truncate source(T) | обе копии видят `OLD` на своих `Si`; source→`NEW` |
| `TruncateSourceTimeTravelBeforeT` | truncate source(T) | source@`R∈[Smin,T)`→`OLD`, `R≥T`→`NEW` |
| `TruncateSourceThenDropAllCopiesCleansOldGen` | truncate → drop copy1 → drop copy2 | после последнего drop `OLD`→`PathsToDrop`, порции очищены после read-window |
| `TruncateSourceThenTruncateAgain` | truncate(T1) → truncate(T2) | цепочка поколений; time-travel по всем интервалам |
| `CopyDuringTruncateSourceRejectedOrSerialized` | copy source в момент in-flight TRUNCATE | copy отклонён/сериализован (fence) |
| `TruncateSourceRecoveryAfterRestart` | truncate source → restart | маппинги пересобраны; видимость сохранена |
| `TruncateReadOnlyCopyStillRejected` | truncate copy | реджект `SCHEMA_ERROR` (Check 1 `IsReadOnly` не снят) |

### 9.8 Итог по разрешимости

TRUNCATE источника с одной или несколькими копиями **разрешим без deep copy и без cascade-drop**:
модель `CopyTable` уже делит порции, копии изолированы во времени своими `CopyVersion`. Достаточно
удержать `OLD` живым, пока на него ссылаются копии, переселив только путь источника на `NEW` с
сохранением его drop-версии для time-travel. Основная работа — не в физике данных, а в аккуратном
разведении семантик drop (§7.1) и тестовом покрытии concurrency/cleanup.

---

## 10. План дальнейших шагов

### Этап 0 — краткосрочно (сделано ✅)
- ✅ Базовая реализация TRUNCATE (без копий) — 17 тестов, compatibility + стресс.
- ✅ Рефакторинг `tables_manager`: [`TGenerationIndex`](ydb/core/tx/columnshard/tables_manager.h:599) (§11), [`TPendingOpFence`](ydb/core/tx/columnshard/tables_manager.h:556) (ось B).
- ✅ `ResolveInternalPathIdForSnapshot` перенесён в `TGenerationIndex::ResolveForSnapshot` (шаблон с callback'ами).

### Этап 1 — реализация retention (Вариант 5) — сделано ✅
- ✅ `TruncateTable`: retention-ветка (§9.2) — partial drop источника, сохранение `OLD` для копий.
- ✅ `schema.cpp` propose: убран реджект `GetPathIds().size() > 1` для источника.
- ✅ `DropTable`: подтверждена финализация `OLD` после DROP последней копии.
- ✅ Тесты: `TruncateCopySourceRetention`, `TruncateSourceAfterDropCopySucceeds` — все 17 тестов проходят.

### Этап 2 — тестирование (сделано ✅)
- ✅ `TruncateCopySourceRetention`: copy(S) → truncate source(T>S) → copy@S видит старые данные; source@≥T пуст.
- ✅ `TruncateSourceAfterDropCopySucceeds`: copy → drop copy → truncate source — работает.

### Этап 3 — наблюдаемость и защита
- 🔲 Метрика размера `SchemeShardLocalToInternalAll` и числа поколений на путь.
- 🔲 Рассмотреть hard-limit / алерт на аномальный рост поколений при частых TRUNCATE.

### Этап 4 — раскатка
- 🔲 Включить флаг в тестовых стендах; прогнать нагрузочные сценарии copy+truncate.
- 🔲 Поэтапный rollout; обновить пользовательскую документацию.

**Риски и открытые вопросы:**
- Согласование поведения с SchemeShard-уровнем (видит ли пользователь копии как независимые таблицы).
- Стоимость `RenameTableSchemeShardLocalPathId` при `MoveTable` над таблицей с длинной историей поколений.
- Поведение при комбинации TRUNCATE-источника + последующий `MoveTable` (перенос истории — §Приложение C).

---

## 11. Рефакторинг tables_manager: группировка точечных изменений

Ветка вносит в [`tables_manager.cpp`/`.h`](ydb/core/tx/columnshard/tables_manager.cpp) много **точечных**
вставок, размазанных по существующим методам (`AddTableInfo`, `DropTable`, `MoveTableProgress`,
`CopyTableProgress`, `TryFinalizeDropPathOnComplete`) плюс новые методы TRUNCATE. Проблема не в объёме, а
в том, что **один инвариант размазан по многим местам** и его легко нарушить при будущих правках.

### 11.1 Диагноз: что именно размазано

Инвентаризация точечных вставок (относительно origin/main) по «осям ответственности»:

| Ось | Где сейчас разбросано | Суть дублирующегося кода |
|-----|----------------------|--------------------------|
| **A. Индекс поколений** (`SchemeShardLocalToInternal` + `SchemeShardLocalToInternalAll`) | `AddTableInfo`, `DropTable`, `MoveTableProgress`, `CopyTableProgress`, `TryFinalizeDropPathOnComplete`, `TruncateTablePropose`, `TruncateTable` | ручные `insert`/`erase` в **две** структуры + правило «не перетирать live dropped-поколением» + «erase только если указывает на этот id» + чистка пустого `THashSet` |
| **B. Fence-карты** (`Renaming`/`Copying`/`TruncatingLocalToInternal`) | `MoveTable*`, `CopyTable*`, `TruncateTablePropose`/`TruncateTable`, `*AbortPropose` | три структурно одинаковых `THashMap<SS, Internal>` с одинаковым протоколом propose/erase/abort |
| **C. Генерация id** | `GetOrCreateInternalPathId`, `TruncateTable` | инкремент `MaxInternalPathId` (уже вынесен в `GenerateNextInternalPathId` — хороший пример) |
| **D. TTL-история** (`Ttl` + `TtlProtos`) | `TTtlVersions::AddVersionFromProto`, `GetMemoryUsage`, `RemovePathId` | два параллельных map'а, которые нужно держать «в ногу» (уже инкапсулировано в `TTtlVersions` — тоже хороший пример) |
| **E. Выбор поколения по snapshot** | `ResolveInternalPathIdForSnapshot`, `BuildTableMetadataAccessor` | логика time-travel (уже отдельный метод — ок) |

Оси **C, D, E** уже сгруппированы правильно (отдельный метод / класс). Проблемные — **A и B**: именно там
точечные вставки в 5–7 методов.

### 11.2 Предлагаемая группировка

#### Ось A → выделить класс `TGenerationIndex` (владелец обоих маппингов)

Инкапсулировать `SchemeShardLocalToInternal` и `SchemeShardLocalToInternalAll` в один класс, который
**единственный** знает, как их держать согласованными. Внешний код вызывает намеренные операции, а не
правит два `THashMap` вручную:

```cpp
class TGenerationIndex {
    THashMap<TSchemeShardLocalPathId, TInternalPathId> Live;        // было SchemeShardLocalToInternal
    THashMap<TSchemeShardLocalPathId, THashSet<TInternalPathId>> All; // было SchemeShardLocalToInternalAll
public:
    // Регистрация нового/live поколения (AddTableInfo, RegisterTable, CopyTableProgress)
    void SetLive(TSchemeShardLocalPathId ss, TInternalPathId id, bool isDropped);
    // Отвязать имя от конкретного поколения, сохранив историю (partial-drop копии)
    void ForgetLive(TSchemeShardLocalPathId ss, TInternalPathId id);
    // Полностью убрать поколение из истории (финализация cleanup)
    void ForgetGeneration(TSchemeShardLocalPathId ss, TInternalPathId id);
    // Перенос всей истории на новое имя (MoveTableProgress)
    void Rename(TSchemeShardLocalPathId from, TSchemeShardLocalPathId to, ...);
    // Резолвинг
    std::optional<TInternalPathId> ResolveLive(TSchemeShardLocalPathId ss) const;
    const THashSet<TInternalPathId>* Generations(TSchemeShardLocalPathId ss) const;
};
```

Это убирает из `AddTableInfo`/`DropTable`/`TryFinalizeDropPathOnComplete` повторяющиеся блоки
«найти в `All`, стереть id, если пусто — стереть ключ» и правило «не перетирать live dropped-поколением»
(становится телом `SetLive`). `ResolveInternalPathIdForSnapshot` переезжает внутрь как метод, работающий
с `Generations()`.

#### Ось B → один шаблон/класс `TPendingOpFence` для трёх fence-карт

`Renaming`/`Copying`/`TruncatingLocalToInternal` — это один паттерн «имя → зафиксированное поколение на
время in-flight schema tx». Свести к одному типу с единым протоколом:

```cpp
class TPendingOpFence {                     // по экземпляру на Move/Copy/Truncate
    THashMap<TSchemeShardLocalPathId, TInternalPathId> Fenced;
public:
    void Propose(TSchemeShardLocalPathId ss, TInternalPathId id); // идемпотентно
    std::optional<TInternalPathId> Get(TSchemeShardLocalPathId ss) const;
    void Complete(TSchemeShardLocalPathId ss);   // на plan
    void Abort(TSchemeShardLocalPathId ss);      // на abort
};
```

Тогда `*AbortPropose`-методы и conditional-insert логика (сейчас скопированы между Move/Copy/Truncate)
становятся одной реализацией. Различается только момент вызова из `schema.cpp`.

### 11.3 Соответствие документу

- **Ось A** — это ровно материал §5.2–5.3 и Приложения C. Класс `TGenerationIndex` делает код
  соответствующим тексту: «индекс всех поколений» становится явной сущностью, а не парой полей.
- **Ось B** — материал §4.2 (lifecycle propose/fence/plan/abort). `TPendingOpFence` делает «три fence
  как один паттерн» из §5.2 явным в коде.
- **Ось D** (`TTtlVersions`) — уже сделано так, как описано в Приложении B; служит образцом для A и B.

### 11.4 Порядок рефакторинга (безопасными шагами)

1. **Ось A, чистый рефактор без смены поведения:** ввести `TGenerationIndex`, перенести оба маппинга и
   все ручные `insert/erase` в его методы; заменить обращения в 7 методах на вызовы. Покрыто существующими
   тестами TRUNCATE/Move/Copy — поведение не меняется.
2. **Ось B:** ввести `TPendingOpFence`, свести три карты и `*AbortPropose` к одному типу.
3. Только после 1–2 — реализовывать retention (§9): новая ветка `TruncateTable` станет вызовом
   `GenerationIndex.ForgetLive(source, OLD)` + `SetLive(source, NEW)` без ручного трогания `All`, что
   резко снижает риск нарушить MVCC-инвариант (§7.2).

**Выгода:** инвариант «Live и All всегда согласованы» и протокол fence проверяются в одном месте, а не
в 7; будущие операции (retention, будущие schema tx) переиспользуют готовые примитивы вместо копирования
точечных вставок.

---

## Приложение A: обзор изменений по файлам

> Детальный per-file разбор изменений ветки относительно origin/main (базовая реализация TRUNCATE).
> Сохранён как справочник к ревью; архитектурные выводы вынесены в §3–5.

### A.1 `ydb/core/protos/tx_columnshard.proto`
Добавлен `message TTruncateTable { optional uint64 PathId = 1; }` и поле `TruncateTable = 9` в
`TSchemaTxBody`. Необходимо, чтобы отличать truncate от drop. Поле `9` не конфликтует с 1–8.
Ограничение: только `PathId`, без `if exists`/`cascade` — расширение потребует новых полей.

### A.2 `columnshard_impl.h` / `columnshard_impl.cpp`
Добавлен `RunTruncateTable(...)` и `case kTruncateTable` в `RunSchemaTx`. Метод резолвит старый
`InternalPathId` (fence → fallback `ResolveInternalPathId`), проверяет существование и read-only,
захватывает `TTableVersionInfo`+TTL, вызывает `TruncateTable`, регистрирует версию/TTL на новом id.
Риск: при `nullopt` из `LoadLastTableVersionInfo` новая таблица создаётся без preset — нужен тест.

### A.3 `common/path_id.cpp`
Специализации `FromProto`/`ToProto` для `TTruncateTable` — стандартный паттерн, без рисков.

### A.4 `hooks/testing/controller.h`
`AddPathId` очищает старый reverse-маппинг перед вставкой (truncate переиспользует SS path id с новым
internal id). Только тестовый контроллер; желателен комментарий, когда допустимо переиспользование
SS path id.

### A.5 `tables_manager.cpp` / `tables_manager.h`
- **`SchemeShardLocalToInternalAll`** — история поколений для MVCC (Приложение C). Не персистится,
  пересобирается из `InitFromDB`; риск роста памяти при частых TRUNCATE.
- **`ResolveInternalPathIdForSnapshot`** — выбор поколения по snapshot (§5.3).
- **`AddTableInfo`** — при загрузке из БД не перетирает live-маппинг dropped-поколением (порядок загрузки).
- **`TruncateTable`/`TruncateTablePropose`** — lifecycle (§4.2). Race-window между `DropTable` и
  `RegisterTable` (маппинг пуст) — короткий, в рамках одной транзакции.
- **`LoadLastTableVersionInfo`** — graceful `nullopt` вместо `AFL_VERIFY(IsReady())` на cold-страницах.
- **`TryFinalizeDropPathOnComplete`** — erase только если маппинг всё ещё указывает на дропаемый id;
  добавлен `Ttl.RemovePathId`.
- **`MoveTableProgress`** — перенос истории поколений на новый SS path id (Приложение C).
- **`BuildTableMetadataAccessor`** — сигнатура `optional<TSnapshot>&` → `TSnapshot&` (breaking; проверить
  всех callers).

### A.6 `transactions/operators/schema.cpp` / `schema.h`
`case kTruncateTable` в propose/`DoOnTabletInit`/`targetPathId`. Валидация propose — §4.3. Tiering-check
реджектит truncate таблиц с тирами.

### A.7 `test_helper/columnshard_ut_common.h` / `.cpp`
Добавлен `TruncateTableTxBody(pathId, version)` (сборка `TSchemaTxBody` с `TTruncateTable`), новый
overload `PrepareTablet(runtime, schemaTxBody)` и параметр `inStore` в `PrepareTablet(...)`
(`tableDescription.InStore = inStore`). Feature-flag в test helper **не** добавлялся (см. §4.3).

### A.8 `ut_schema/ut_columnshard_schema.cpp`
**Файл не изменялся веткой** (`git diff origin/main…HEAD` пуст). Тесты
`TestDropMvccAndCleanupWithActiveScan` и `TestEmptyDroppedTableCleanupWaitsForReadWindow` присутствуют
без изменений.

### A.9 `ut_schema/ut_columnshard_truncate_table.cpp` (новый)
888 строк, 17 тестов (`Y_UNIT_TEST_SUITE(TruncateTable)`): `EmptyTable`, `WithData`, `TruncateAndInsert`,
`TruncateAbsentTable`, `MultipleTruncates`, `TruncatePreservesTtl`, `TruncateSnapshotBoundary`,
`TruncateAndDrop`, `TruncateReadOnlyTableFails`, `TruncateCopySourceRetention`,
`TruncateSourceAfterDropCopySucceeds`, `TruncateSeqNoCheck`, `TruncateWithCommitInProgress` (DUO: Normal/Reboot),
`TruncateFencesWritesOnPropose`, `TruncateInStoreTableFails`, `TruncateSurvivesRestart`.
Покрытие включает: tiering (`TruncatePreservesTtl`), multiple-truncate (`MultipleTruncates`),
retention источника копий (`TruncateCopySourceRetention`), drop-копии → truncate
(`TruncateSourceAfterDropCopySucceeds`), concurrent fence (`TruncateFencesWritesOnPropose`),
seqno-контроль (`TruncateSeqNoCheck`), in-flight commit wait + restart
(`TruncateWithCommitInProgress`), restart между propose/plan (`TruncateSurvivesRestart`).

> **Примечание:** тест `TruncateCopySourceFails` переименован в `TruncateCopySourceRetention` —
> после снятия Check 2 TRUNCATE источника с копиями больше не отклоняется, а идёт в retention-режиме.

### Итоговая оценка базовой реализации
**Сильные стороны:** дизайн через новое поколение элегантен (MVCC «бесплатно»); паттерн
fence/propose/plan согласован с Move/Copy; хорошее покрытие основных сценариев.
**Желательные улучшения:** нет (проверка callers `BuildTableMetadataAccessor` проведена).

---

## Приложение B: TtlProtos

### Зачем нужен
[`tables_manager.h:352`](ydb/core/tx/columnshard/tables_manager.h:352):
```cpp
THashMap<TInternalPathId, std::map<NOlap::TSnapshot, NKikimrSchemeOp::TColumnDataLifeCycle>> TtlProtos;
```
Параллелен основному `Ttl` (десериализованный `NOlap::TTiering`). Причина — [`TTiering`](ydb/core/tx/columnshard/engines/scheme/tiering/tier_info.h:128)
**не имеет `SerializeToProto`**: round-trip `proto → TTiering → proto` lossy. При TRUNCATE новое
поколение должно получить lifecycle-настройки старого **бит-в-бит**.

### Как используется в TRUNCATE
Перед `TruncateTable()` (дропает старый id) захватывается сырой proto через `GetTableTtlProto()`; новое
поколение регистрируется с теми же настройками через `tableVerProto.MutableTtlSettings()`; при
инициализации proto десериализуется в `TTiering`.

### Альтернативы
- **`SerializeToProto` в `TTiering`** — lossy по дизайну, хрупко при эволюции proto. ✗
- **Читать из `TTableVersionInfo`** — не хранит MVCC-историю по snapshot'ам; cold-pages. ✗
- **Колонка в NiceDb** — миграция схемы; in-memory кэш всё равно нужен. ~
- **Текущий параллельный маппинг** — просто, точно, быстро, MVCC-aware. ✓

Память учитывается в `GetMemoryUsage()` (`proto.ByteSizeLong()` на запись), очистка — `RemovePathId`
из `TryFinalizeDropPathOnComplete`.

---

## Приложение C: SchemeShardLocalToInternalAll

### Зачем нужен
[`tables_manager.h:429`](ydb/core/tx/columnshard/tables_manager.h:429):
```cpp
THashMap<TSchemeShardLocalPathId, THashSet<TInternalPathId>> SchemeShardLocalToInternalAll;
```
Хранит **все** поколения пути (live + dropped). Без него после TRUNCATE старое поколение недоступно по
имени, и pre-truncate read видит пустоту. Используется в
[`ResolveInternalPathIdForSnapshot`](ydb/core/tx/columnshard/tables_manager.cpp:79) (§5.3).

### Жизненный цикл записей
- **Создание:** `AddTableInfo` (из БД), `TruncateTablePropose` (lazy-populate при rolling deploy),
  `TruncateTable` (новое поколение), `MoveTableProgress` (перенос истории), `CopyTableProgress` (копия).
- **Удаление:** `TryFinalizeDropPathOnComplete` (когда dropped-поколение полностью очищено).

### Альтернативы
- **История в `TTableInfo`** — циклическая зависимость (индекс по `InternalPathId`, а искать надо по SS). ✗
- **Персист в NiceDb** — disk I/O на каждый time-travel read; кэш всё равно нужен. ~
- **Вычислять из schema tx истории** — слишком медленно. ✗
- **Текущий in-memory маппинг** — O(1) по SS, O(N) по поколениям (N мало), lazy-populate. ✓

### Почему `THashSet`, а не упорядоченный set
Ключ сортировки (drop-версия) хранится не в наборе, а в `TTableInfo`
(`GetPathDropVersionOptional`), поэтому бинарный поиск бесполезен — всё равно нужен доступ к
`TTableInfo` для каждого поколения (`HasSchemeShardLocalPathId` + drop-версия). `THashSet` даёт O(1)
insert/erase, N обычно 2–3. Линейный скан оптимален.

### Связь с MoveTable
[`MoveTableProgress`](ydb/core/tx/columnshard/tables_manager.cpp:873) переносит **всю** историю
поколений со старого SS path id на новый (с переименованием SS path id в исторических `TTableInfo`),
иначе time-travel после `MOVE` теряет предыдущие TRUNCATE-поколения.

### Контроль памяти
Очистка — в `TryFinalizeDropPathOnComplete`. При активных long reads cleanup откладывается → возможен
рост. Рекомендуется мониторинг размера, периодический cleanup и hard-limit на число поколений на путь.

---

## План доработок и следующих шагов (живой раздел)

> **Этот раздел обновляется по ходу выполнения работ.** Он — единая точка отслеживания статуса.
> Разделы §1–§11 описывают дизайн и остаются относительно стабильными; здесь фиксируется прогресс.
>
> Легенда статусов: 🔲 не начато · 🔄 в работе · ✅ сделано · ⏸️ отложено · ❌ отменено
>
> Последнее обновление: 2026-08-18

### Дорожная карта (крупные вехи)

| Веха | Цель | Статус |
|------|------|--------|
| M0 | Базовый TRUNCATE (без копий) — реализация, тесты, сборка | ✅ |
| M1 | Закрытие остаточных пунктов ревью базовой реализации | ✅ |
| M2 | Рефакторинг `tables_manager` (§11: `TGenerationIndex`, `TPendingOpFence`) | ✅ |
| M3 | Наблюдаемость, защита от роста поколений, раскатка | 🔲 |

> **Соответствие целевой картине.** Целевая картина имеет пять осей Ц1–Ц5; вехи M0–M3 покрывают их так:
> Ц1/Ц2 (базовый TRUNCATE) — M0+M1; Ц4 (сопровождаемость/рефакторинг) — M2; Ц5 (наблюдаемость/раскатка)
> — M3. Ось Ц1/Ц3 в части TRUNCATE источника копий (retention, §9) реализована одновременно с базовым
> TRUNCATE (без отдельной вехи).

### Детальный чеклист

#### M1 — остаточные пункты базовой реализации
- ✅ Тест: multiple truncate (`truncate → insert → truncate`), проверка цепочки поколений.
- ✅ Тест: tiering edge case (TTL с тирами — reject, TTL без тиров — ok).
- ✅ Тест: `LoadLastTableVersionInfo` eviction scenario (cold NiceDb page → `nullopt`).
- ✅ Compatibility-тест restart / rolling upgrade-downgrade ([`test_truncate_table.py`](ydb/tests/compatibility/olap/test_truncate_table.py)).
- ✅ Стресс-нагрузки `truncate_insert` / `truncate_concurrent` ([`ydb/tests/stress/olap_truncate/`](ydb/tests/stress/olap_truncate/workload/type/truncate_insert.py)).
- ✅ Проверить всех callers [`BuildTableMetadataAccessor`](ydb/core/tx/columnshard/tables_manager.cpp) на новую сигнатуру:
  2 caller'а — [`tx_scan.cpp:113`](ydb/core/tx/columnshard/engines/reader/transaction/tx_scan.cpp:113) (изменённый overload) и
  [`tx_internal_scan.cpp:61`](ydb/core/tx/columnshard/engines/reader/transaction/tx_internal_scan.cpp:61) (неизменённый overload).

#### M2 — рефакторинг tables_manager (см. §11) — **Обе оси завершены**
- ✅ Ось A: ввести [`TGenerationIndex`](ydb/core/tx/columnshard/tables_manager.h:599), перенести `SchemeShardLocalToInternal` + `SchemeShardLocalToInternalAll`.
- ✅ Ось A: заменить ручные `insert/erase` в 7 методах на вызовы `TGenerationIndex` (чистый рефактор).
- ✅ Ось A: перенести `ResolveInternalPathIdForSnapshot` внутрь `TGenerationIndex` (как шаблонный метод [`ResolveForSnapshot`](ydb/core/tx/columnshard/tables_manager.h:648) с callback'ами).
- ✅ Ось B: ввести [`TPendingOpFence`](ydb/core/tx/columnshard/tables_manager.h:556), свести `Renaming`/`Copying`/`Truncating` карты к одному типу.
- ✅ Прогнать существующие тесты TRUNCATE/Move/Copy — поведение не должно измениться (105+ GOOD).

#### M3 — наблюдаемость и раскатка
- 🔲 Метрика размера `SchemeShardLocalToInternalAll` и числа поколений на путь.
- 🔲 Hard-limit / алерт на аномальный рост поколений.
- 🔲 Включить флаг на тестовых стендах; нагрузочные сценарии.
- 🔲 Поэтапный rollout; обновить пользовательскую документацию.

### Открытые вопросы
- 🔲 Согласование с SchemeShard-уровнем: видит ли пользователь копии как независимые таблицы.
- 🔲 Стоимость `RenameTableSchemeShardLocalPathId` при `MoveTable` над таблицей с длинной историей поколений.

---

## Приложение D: пофайловое ревью изменений (зачем + альтернативы)

> Ревью 2026-08-20 относительно `origin/main`. Для каждого изменения: **зачем** оно нужно, **можно ли
> было иначе**, и **консистентно** ли оно с остальной веткой. Легенда: ✅ консистентно · ⚠️ замечание.

### D.1 `ydb/core/protos/tx_columnshard.proto` — ✅
- **Зачем:** отдельный `message TTruncateTable { optional uint64 PathId = 1; }` и поле `TruncateTable = 9`
  в `TSchemaTxBody`, чтобы отличать TRUNCATE от DROP на уровне schema tx.
- **Альтернатива:** переиспользовать `TDropTable` с флагом `is_truncate` — дешевле, но смешивает две
  семантики и усложняет валидацию. Отдельный message согласован с паттерном `MoveTable`/`CopyTable`.
- **Консистентность:** ✅. Поле `9` не конфликтует с 1–8; `FromProto`/`ToProto` добавлены в `path_id.cpp`.

### D.2 `columnshard_impl.cpp` / `.h` — `RunTruncateTable` — ✅
- **Зачем:** plan-фаза: резолвит старый `InternalPathId` (fence → fallback `ResolveInternalPathId`),
  захватывает schema/TTL **до** дропа, вызывает `TruncateTable`, регистрирует версию/TTL на новом id.
- **Альтернатива:** перенести захват schema/TTL внутрь `TruncateTable` — но тогда `TruncateTable` тянет
  proto-логику `TTableVersionInfo`, смешивая слои. Текущее разделение (менеджер — маппинги, caller —
  версии) чище и повторяет `RunDropTable`/`RunMoveTable`.
- **Консистентность:** ✅. `case kTruncateTable` в `RunSchemaTx` симметричен соседним; `RunTruncateTable`
  объявлен в `.h` рядом с `RunDropTable`.

### D.3 `common/path_id.cpp` — `FromProto`/`ToProto` для `TTruncateTable` — ✅
- **Зачем:** стандартные конвертеры `TSchemeShardLocalPathId ↔ TTruncateTable`.
- **Альтернатива:** без них пришлось бы вручную читать `GetPathId()` в каждом месте.
- **Консистентность:** ✅. Хелпер `FromProto(TTruncateTable)` ([`path_id.cpp:188`](ydb/core/tx/columnshard/common/path_id.cpp:188))
  используется в propose ([`schema.cpp:245`](ydb/core/tx/columnshard/transactions/operators/schema.cpp:245)),
  в `RunTruncateTable` и в seqno-switch ([`schema.cpp:123`](ydb/core/tx/columnshard/transactions/operators/schema.cpp:123))
  — унифицировано через `FromProto(...).GetRawValue()` (как `kDropTable`). Замечание о непоследовательности
  снято.

### D.4 `hooks/testing/controller.h` — `AddPathId` — ✅
- **Зачем:** тестовый контроллер ведёт reverse-маппинг `Internal → {SS}`. TRUNCATE переиспользует тот же
  `SchemeShardLocalPathId` с новым `InternalPathId`, поэтому перед вставкой нужно снять старую пару.
- **Альтернатива:** хранить историю поколений в контроллере — избыточно для тестов. Текущее «снять старое,
  вставить новое» достаточно.
- **Консистентность:** ✅. Только тестовый код; не влияет на прод-логику.

### D.5 `tables_manager.cpp` — ядро — ✅ (с оговорками)
- **`ResolveInternalPathIdOptional` → `GenerationIndex.ResolveLive`** — чистый рефактор (ось A).
- **`ResolveInternalPathIdForSnapshot`** — новое, делегирует в `GenerationIndex.ResolveForSnapshot`.
  **Зачем:** MVCC time-travel по поколениям. **Альтернатива:** держать историю в `TTableInfo` — циклическая
  зависимость (индекс по `InternalPathId`, искать надо по SS). Текущий внешний индекс — правильный выбор.
- **`AddTableInfo` → `SetLive` с dropped-guard** — **Зачем:** при recovery порядок загрузки поколений не
  гарантирован; dropped-поколение не должно перетирать live-маппинг. **Альтернатива:** сортировать загрузку —
  хрупко. Guard в `SetLive` надёжнее.
- **`GenerateNextInternalPathId`** — вынесен инкремент `MaxInternalPathId`. **Зачем:** единая точка для
  создания и TRUNCATE. ✅ хороший рефактор.
- **`LoadLastTableVersionInfo`** — **Зачем:** перенос `SchemaPresetId`/`VersionAdj` на новое поколение.
  **Альтернатива:** `AFL_VERIFY(IsReady())` — краш на cold-страницах; graceful `nullopt` безопаснее
  (деградация до default preset). ✅.
- **`DropTable` → `ForgetLive`/`ForgetGeneration`** — рефактор (ось A).
- **`TruncateTable`** — retention-ветка (§9.2). **Зачем:** TRUNCATE источника с копиями. **Альтернатива:**
  deep copy / cascade drop / deferred — отвергнуты (§8). ✅.
- **`TruncateTablePropose`** — fence + lazy-populate `All`. **Зачем:** блокировка writes + корректный
  time-travel при rolling deploy. ✅.
- **`TryFinalizeDropPathOnComplete` → `ForgetLiveIfMatches` + `Ttl.RemovePathId`** — **Зачем:** erase только
  если маппинг всё ещё указывает на дропаемый id; чистка TTL-истории. ✅.
- **`MoveTableProgress`** — перенос всей истории поколений + переименование SS в исторических `TTableInfo`.
  **Зачем:** time-travel после MOVE. ✅.
- **`BuildTableMetadataAccessor`** — сигнатура `optional<TSnapshot>&` → `TSnapshot&`. **Зачем:** теперь
  резолвинг по snapshot обязателен. **Альтернатива:** оставить `optional` и внутри выбирать live при
  `nullopt` — но тогда time-travel для sys-view чтений теряется. Breaking-изменение, все 2 caller'а
  обновлены (проверено). ✅.

### D.6 `tables_manager.h` — `TGenerationIndex`, `TPendingOpFence`, `TtlProtos` — ✅
- **`TGenerationIndex`** — инкапсулирует `Live`/`All`. **Зачем:** инвариант «Live и All согласованы» в одном
  месте (ось A). ✅.
- **`TPendingOpFence`** — единый тип для `Renaming`/`Copying`/`Truncating`. **Зачем:** три одинаковых
  `THashMap` с одинаковым протоколом (ось B). ✅.
- **`TtlProtos`** — параллельный map сырых proto. **Зачем:** `TTiering` не имеет `SerializeToProto`,
  round-trip lossy; нужен бит-в-бит перенос на новое поколение. **Альтернатива:** `SerializeToProto` в
  `TTiering` (lossy), чтение из `TTableVersionInfo` (нет MVCC-истории), колонка в NiceDb (миграция).
  Текущий параллельный map — простейший MVCC-aware вариант. ✅.
- **`GetPathDropVersionOptional`/`HasSchemeShardLocalPathId`** — доступ к path-local drop-версии для
  `ResolveForSnapshot`. ✅.

### D.7 `transactions/operators/schema.cpp` / `.h` — ✅
- **Seqno-switch `targetPathId`:** тип `optional<TSchemeShardLocalPathId>` → `optional<ui64>`.
  **Зачем:** добавить `kTruncateTable`. `kTruncateTable` использует `FromProto(TTruncateTable).GetRawValue()`
  (унифицировано с `kDropTable`, см. D.3). **Альтернатива:** оставить `TSchemeShardLocalPathId` и не
  переводить в `ui64` — но тогда `kCopyTable`/`kMoveTable` (читают `GetDstPathId()` как `ui64`) всё равно
  требовали бы конверсии. Текущий вариант единообразен.
- **Propose-валидация** (`kTruncateTable`): 4 проверки (GenerateInternalPathId, IsStoreTablet, Check 1
  read-only, Check 3 tiering) + fence + wait txs. **Зачем:** безопасность операции. ✅.
- **`DoOnTabletInit` recovery** (`kTruncateTable`): повторный fence после рестарта. **Зачем:** writes
  остаются заблокированными, пока TRUNCATE pending. ✅.
- **`schema.h` `GetType`** — `case kTruncateTable` → `"Scheme:TruncateTable"`. ✅.

### D.8 `test_helper/columnshard_ut_common.h` / `.cpp` — ✅
- **`TruncateTableTxBody`** — сборка `TSchemaTxBody` с `TTruncateTable`. **Зачем:** тесты.
- **`PrepareTablet` overload + `inStore`** — **Зачем:** тест `TruncateInStoreTableFails` требует standalone
  таблицу. **Альтернатива:** отдельный хелпер — но параметр с default `true` сохраняет обратную
  совместимость. ✅.

### D.9 `ut_schema/ut_columnshard_truncate_table.cpp` — ✅
- 17 тестов, 888 строк. Покрывают: базовый TRUNCATE, time-travel, multiple-truncate, TTL-перенос,
  snapshot-boundary, drop-после-truncate, read-only reject, retention источника копий, drop-копии →
  truncate, seqno, in-flight commit wait (DUO), fence writes, in-store reject, restart. ✅.

### D.10 `ydb/services/persqueue_v1/actors/schema_actors.cpp` — ✅ (изменение откачено)
- **Зачем:** ветка добавляла `#include <ydb/core/ydb_convert/topic_description.h>` в начало файла.
- **Консистентность:** ⚠️ **избыточный дублирующий include.** Проверка показала, что этот же заголовок
  **уже включён** в файле ниже ([`schema_actors.cpp:10`](ydb/services/persqueue_v1/actors/schema_actors.cpp:10))
  — и в `origin/main`, и в ветке. Ветка добавляла **второй, дублирующий** `#include` того же заголовка.
  Изменение не относилось к TRUNCATE и функционально ничего не давало.
- **Итог:** ✅ **изменение откачено** — дублирующий include убран, файл приведён к `origin/main`
  (`git diff origin/main` для этого файла пуст). Замечание снято.

### D.11 Итоговая консистентность
- **В целом ветка консистентна:** retention, рефакторинг (`TGenerationIndex`/`TPendingOpFence`), recovery
  и тесты согласованы между собой и с дизайном §4–9.
- **Замечание (1) снято:** `schema.cpp` seqno-switch теперь использует `FromProto(TTruncateTable)` (D.3, D.7).
- **Замечание (2) снято:** избыточный дублирующий `#include` в `persqueue_v1/schema_actors.cpp` убран,
  файл приведён к `origin/main` (D.10).
- **Все замечания ревью закрыты.**
- **Альтернативы рассмотрены** для каждого нетривиального решения (TTL-перенос, история поколений,
  retention, graceful degradation) — выбранные варианты обоснованы.

### Журнал изменений плана
| Дата | Что изменилось |
|------|----------------|
| 2026-08-17 | Инициализация живого раздела: вехи M0–M4, детальные чеклисты, открытые вопросы. M0 отмечена как выполненная. |
| 2026-08-17 | Целевая картина (Ц1–Ц5) и разметка «сделано/осталось» сведены в §3 «Целевая картина и статус реализации» (§3.1 оси со статусом, §3.2 факт, §3.3 зазор, §3.4–3.6 сводка изменений ветки). В M1 добавлены выполненные compatibility- и стресс-тесты; убрана веха retention (реализуется вместе с TRUNCATE прочих типов таблиц), M4→M3. |
| 2026-08-17 | Убраны исторические/несвязанные с задачей отсылки: механика `ExecuteOnAbort`/`*AbortPropose` (SchemeShard не отменяет tx после propose — оставлено лишь короткое «нет фазы Abort»), а также review-fix'ы `ApplyColumnShardConfig`, `PublishMinSnapshotForNewScans`, `GetNodePortionsCountLimitVerified`. |
| 2026-08-17 | Исправлена ошибочная атрибуция: `snapshot_holders.h`/`TxInFlight` не менялись веткой (`git diff origin/main…HEAD` пуст; полный скан `TxInFlight` уже в origin/main). Убраны строка «баг-фикс long-read guard», раздел §A.4 и связанные пункты плана; приложение A перенумеровано (A.4–A.9). |
| 2026-08-18 | Сверка всех оставшихся утверждений с `git diff origin/main…HEAD`. Исправлено: (1) §4.3 — убран несуществующий feature-flag `EnableTruncateColumnTable` из списка проверок; фактически 5 проверок (GenerateInternalPathId, IsStoreTablet, Check 1 read-only, Check 2 копии, Check 3 tiering), гейтинга флага в columnshard нет (`EnableTruncateTable` в `feature_flags.proto` — `reserved`). (2) §A.8 — файл `ut_columnshard_schema.cpp` веткой **не менялся**; ложное утверждение о переименовании/удалении тестов убрано. (3) §A.9/§3.2 — тестов **15** (не 9), **890** строк (не 803); список тестов приведён к фактическому; убраны устаревшие «пробелы» (tiering/multiple-truncate/copy-source/concurrent покрыты). (4) §A.7 — `EnableTruncateColumnTable` в test helper нет; описан фактический хелпер (`TruncateTableTxBody`, overload `PrepareTablet`, параметр `inStore`). (5) M2-чеклист — `TGenerationIndex`/`TPendingOpFence` в коде **не существуют** (grep пуст); статус возвращён на 🔲. (6) Синхронизированы номера Check (1/2/3) в §9.4/§9.6/§9.7 и номера строк (`MoveTableProgress` 873, propose `schema.cpp:228/246`). |
| 2026-08-18 | Проверены все callers [`BuildTableMetadataAccessor`](ydb/core/tx/columnshard/tables_manager.cpp) после смены сигнатуры `optional<TSnapshot>&` → `TSnapshot&`: 2 caller'а — [`tx_scan.cpp:113`](ydb/core/tx/columnshard/engines/reader/transaction/tx_scan.cpp:113) (изменённый overload, передаёт `TSnapshot`) и [`tx_internal_scan.cpp:61`](ydb/core/tx/columnshard/engines/reader/transaction/tx_internal_scan.cpp:61) (неизменённый overload с `internalPathId`). Статус 🔲 → ✅ в §3.3, §3.5, §10, §A.Итог, M1-чеклист. |
| 2026-08-18 | **Ось A рефакторинга `TGenerationIndex` выполнена.** Класс [`TGenerationIndex`](ydb/core/tx/columnshard/tables_manager.h:428) введён в [`tables_manager.h`](ydb/core/tx/columnshard/tables_manager.h), заменил сырые `SchemeShardLocalToInternal`/`SchemeShardLocalToInternalAll`. Все методы используют методы индекса. Комментарии обновлены. Исправлены баги: лишний `}` закрывал namespace, `const auto*` → `const auto&` для `std::optional`, `mapIt` → `*currentLive`, moved-from `tableInfo.IsDropped()` → `it->second.IsDropped()`. Сборка OK, 17 truncate-тестов GOOD. |
| 2026-08-18 | **`ResolveInternalPathIdForSnapshot` перенесён внутрь `TGenerationIndex`.** Добавлен шаблонный метод [`ResolveForSnapshot<TMemberCheck, TDropVersionGet>()`](ydb/core/tx/columnshard/tables_manager.h:511) с callback'ами для table-dependent проверок (membership + drop version). [`TTablesManager::ResolveInternalPathIdForSnapshot`](ydb/core/tx/columnshard/tables_manager.cpp:79) теперь делегирует в `GenerationIndex.ResolveForSnapshot()`, передавая lambdas для `Tables.FindPtr()` и `TTableInfo::GetPathDropVersionOptional()`. 17 truncate-тестов GOOD. |
| 2026-08-19 | **Ось B рефакторинга `TPendingOpFence` выполнена.** Класс [`TPendingOpFence`](ydb/core/tx/columnshard/tables_manager.h:556) введён в [`tables_manager.h`](ydb/core/tx/columnshard/tables_manager.h), заменил сырые карты `RenamingLocalToInternal`/`CopyingLocalToInternal`/`TruncatingLocalToInternal` на экземпляры `Renaming`/`Copying`/`Truncating`. Методы: `Propose()` (идемпотентный insert), `Get()`, `Complete()`, `Abort()`, `FindPtr()`, `Erase()` (с AFL_VERIFY). Все 11 точек использования в [`tables_manager.cpp`](ydb/core/tx/columnshard/tables_manager.cpp) обновлены. Комментарии в [`columnshard_impl.cpp`](ydb/core/tx/columnshard/columnshard_impl.cpp) и [`schema.cpp`](ydb/core/tx/columnshard/transactions/operators/schema.cpp) синхронизированы. Сборка OK, 105 тестов GOOD. |
| 2026-08-20 | **Исправлена ошибка сборки в `schema.cpp`:** `targetPathId` имеет тип `optional<ui64>` (сырое значение), но `LastSchemaSeqNoByPath` использует `TSchemeShardLocalPathId` как ключ. Деструкция `*targetPathId` давала `ui64`, и `THashMap::operator[]` пытался сконструировать `TSchemeShardLocalPathId(ui64)`, но конструктор приватный. Исправлено: явное `TSchemeShardLocalPathId::FromRawValue(*targetPathId)` в двух местах (строки 132, 290). Сборка OK, 106 тестов GOOD. |
| 2026-08-20 | **Ревью относительно origin/main завершено.** Подтверждено: (1) retention-ветка в [`TruncateTable`](ydb/core/tx/columnshard/tables_manager.cpp:550) реализована — при `hasCopies` помечает источник dropped, сохраняет `OLD` для копий, выделяет `NEW`. (2) Check 2 убран из [`schema.cpp:232`](ydb/core/tx/columnshard/transactions/operators/schema.cpp:232). (3) [`TGenerationIndex`](ydb/core/tx/columnshard/tables_manager.h:599) и [`TPendingOpFence`](ydb/core/tx/columnshard/tables_manager.h:556) существуют в коде — M2 завершена. (4) Recovery в `DoOnTabletInit` ([`schema.cpp:408`](ydb/core/tx/columnshard/transactions/operators/schema.cpp:408)) реализован. (5) Тесты `TruncateCopySourceRetention`, `TruncateSourceAfterDropCopySucceeds` покрывают retention-сценарии. (6) Изменение в `persqueue_v1/schema_actors.cpp` — несвязанное добавление `#include`. (7) Ц1–Ц4 достигнуты; осталось Ц5 (наблюдаемость/раскатка). |
| 2026-08-20 | **Добавлено Приложение D — пофайловое ревью изменений (зачем + альтернативы + консистентность).** Для каждого из 10 файлов/групп изменений описано назначение, рассмотрены альтернативы и оценена консистентность. Выявлены 2 замечания: (1) `schema.cpp` seqno-switch ([`schema.cpp:123`](ydb/core/tx/columnshard/transactions/operators/schema.cpp:123)) читает `GetPathId()` напрямую, не используя добавленный `FromProto(TTruncateTable)` ([`path_id.cpp:188`](ydb/core/tx/columnshard/common/path_id.cpp:188)) — непоследовательно с `kDropTable`; (2) `persqueue_v1/schema_actors.cpp` — несвязанный `#include`, рекомендуется вынести в отдельный коммит. Остальные изменения консистентны; альтернативы для нетривиальных решений (TTL-перенос, история поколений, retention, graceful degradation) обоснованы. |
| 2026-08-20 | **Исправлено замечание (1) из Приложения D:** `schema.cpp` seqno-switch для `kTruncateTable` теперь использует `TSchemeShardLocalPathId::FromProto(SchemaTxBody.GetTruncateTable()).GetRawValue()` вместо прямого `GetPathId()` — унифицировано с `kDropTable`. Обновлены D.3, D.7, D.11. Осталось одно замечание — несвязанный `#include` в `persqueue_v1/schema_actors.cpp` (D.10). |
| 2026-08-20 | **Уточнено замечание (2) из Приложения D (D.10):** проверка показала, что `#include <ydb/core/ydb_convert/topic_description.h>`, добавленный веткой в `persqueue_v1/schema_actors.cpp`, — **избыточный дубликат**: этот же заголовок уже включён ниже в файле ([`schema_actors.cpp:10`](ydb/services/persqueue_v1/actors/schema_actors.cpp:10)) и в `origin/main`, и в ветке. Изменение не относится к TRUNCATE и функционально ничего не даёт. Рекомендация уточнена: **откатить** изменение целиком (убрать дублирующий include), а не выносить в отдельный коммит. Обновлены D.10, D.11. |
| 2026-08-20 | **Замечание (2) из Приложения D закрыто:** дублирующий `#include <ydb/core/ydb_convert/topic_description.h>` убран из `persqueue_v1/schema_actors.cpp`; файл приведён к `origin/main` (`git diff origin/main` для него пуст). Обновлены D.10, D.11. **Все замечания ревью закрыты.** |
