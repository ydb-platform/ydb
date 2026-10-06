# Аудит поддержки типов в строковых и колоночных таблицах YDB

Дата проверки: 2026-09-11  
Коммит: `35fe6b99b6b321ebc724e0c644432b2301f49da6`

## Область проверки

Проверены:

- создание строковых и колоночных таблиц со всеми скалярными YQL-типами;
- допустимые типы первичного ключа;
- запись и чтение ненулевых значений;
- `ORDER BY ASC/DESC`, включая `NULL`;
- точечные и диапазонные условия по первичному ключу;
- PostgreSQL-типы;
- типы TTL-колонок;
- соответствие реализации документации;
- существующее тестовое покрытие найденных проблем.

Проверка выполнялась на локальном 9-нодовом кластере, запущенном через
`ydb/tests/tools/local_cluster/local_cluster`. Для запросов использовался
`ydb/apps/ydb/ydb`.

## Краткий итог

| Область | Строковая таблица | Колоночная таблица | Результат |
|---|---:|---:|---|
| Скалярные YQL-типы в обычных колонках | 27 | 27 | Наборы совпадают |
| Скалярные YQL-типы в первичном ключе | 22 | 22 | Наборы совпадают |
| `ORDER BY` для сравнимых типов | 24 | 24 | Результаты совпадают |
| Несравнимые типы | `Json`, `JsonDocument`, `Yson` | те же | Одинаково отклоняются |
| Диапазоны по первичному ключу | корректны для 22 типов | некорректны для `Decimal` | Найден дефект |
| Типы TTL-колонки при стандартных флагах | 9 | 8 | ColumnShard запрещает `DyNumber` |
| PostgreSQL-типы при стандартных флагах | 0 | 5 | Найдено расхождение и повреждение данных |

## Найденные проблемы

### P1. Некорректные диапазоны по `Decimal` в первичном ключе ColumnShard

**Критичность:** высокая, ошибка корректности результатов запроса.

Column table разрешает использовать `Decimal` в первичном ключе, но
storage-level сравнение ключей не соответствует числовому порядку `Decimal`.
Из-за этого запросы с `<`, `<=`, `>`, `>=` и составными диапазонами могут
пропускать существующие строки. Особенно явно проблема проявляется на
отрицательных значениях и при переходе через ноль.

Точечное условие `=` и обычный SQL `ORDER BY` работают корректно, поэтому
дефект легко не заметить. Если выражением отключить PK-range pushdown, запрос
возвращает правильный результат.

Предполагаемая причина: row table сравнивает 128-битный `Decimal` численно, а
ColumnShard хранит его как `fixed_size_binary(16)` и использует лексикографический
`memcmp` над little-endian представлением.

Требуется:

- использовать числовой компаратор `Decimal` при сравнении ключей ColumnShard;
- добавить проверки всех операторов сравнения;
- покрыть отрицательные значения, ноль, положительные значения и различные
  precision/scale;
- отдельно проверить составные первичные ключи с `Decimal`.

Подробности и запрос для воспроизведения приведены в разделе
«Дефект диапазонов по `Decimal` в первичном ключе».

### P2. Повреждение PostgreSQL-значений при записи в column table через QueryService

**Критичность:** высокая, повреждение сохраняемых данных.

Column table принимает `pgint2`, `pgint4`, `pgint8`, `pgfloat4` и `pgfloat8`.
При записи SQL-литералов через QueryService байты PostgreSQL binary format
интерпретируются как native Arrow numeric values без преобразования порядка
байтов. В результате, например, `pgint4('42')` сохраняется и читается как
`704643072`.

Повреждённое значение участвует в дальнейших вычислениях: сравнение с исходным
`pgint4('42')` возвращает `false`. Следовательно, проблема не ограничивается
форматированием результата.

Требуется:

- до исправления рассмотреть запрет PG-типов для column table;
- либо выполнять явное преобразование PostgreSQL binary/network order в
  соответствующий native Arrow type;
- добавить интеграционные тесты записи через QueryService для всех пяти
  разрешённых PG-типов;
- проверить `INSERT`, `UPSERT`, параметры запросов и другие пути записи.

### P3. ColumnShard игнорирует `EnableTablePgTypes`

**Критичность:** высокая из-за связи с P2; отдельно — нарушение feature gate.

При стандартном `EnableTablePgTypes=false` row table отклоняет PG-колонки, а
column table продолжает принимать пять PG-типов. Проверка ColumnShard использует
собственный allowlist, но не проверяет глобальный feature flag.

При включённом флаге остаётся обратная асимметрия: row table поддерживает более
широкий набор PG-типов и сравнимые PG-типы в первичном ключе, а column table —
только пять неключевых числовых типов.

Требуется:

- определить целевой контракт поддержки PG-типов в ColumnShard;
- применять `EnableTablePgTypes` одинаково для row и column tables;
- синхронизировать allowlist, документацию и тесты с выбранным контрактом.

### P4. `DyNumber` разрешён для TTL row table, но запрещён для column table

**Критичность:** средняя, функциональное расхождение с документацией.

Общий TTL-валидатор разрешает `DyNumber`, и row table с такой TTL-колонкой
успешно создаётся. ColumnShard содержит дополнительный явный запрет и возвращает
`Unsupported column type for TTL in column tables`.

Документация описывает `DyNumber` как допустимый тип TTL-колонки без оговорки о
типе таблицы. Более того, русская версия прямо говорит, что раздел относится к
строковым и колоночным таблицам.

Требуется либо реализовать TTL по `DyNumber` в ColumnShard, либо явно описать
ограничение и отклонять неподдержанный сценарий согласованным способом.

### P5. Документация содержит устаревшие списки типов column table

**Критичность:** средняя, неверный публичный контракт.

Документация сообщает о 19 поддержанных типах и 11 типах первичного ключа.
Фактически column table принимает 27 скалярных YQL-типов и 22 типа PK.
Документация также ошибочно относит `Decimal` только к неключевым колонкам.

Не перечислены реально поддержанные типы:

```text
Bool Date32 Datetime64 Timestamp64 Interval Interval64 DyNumber Uuid
```

В списке PK дополнительно отсутствуют `Int8`, `Int16` и `Decimal`.

Обновление документации по `Decimal` следует согласовать с исправлением P1:
нельзя просто рекомендовать `Decimal` в PK, пока диапазонные запросы могут
возвращать неполные результаты.

### P6. Документация TTL одновременно переоценивает и недооценивает поддержку

**Критичность:** низкая/средняя, неверный публичный контракт.

Документация:

- обещает `DyNumber` для TTL без ограничения ColumnShard;
- не перечисляет поддержанные обеими таблицами `Date32`, `Datetime64` и
  `Timestamp64`.

Список TTL-типов следует разделить по типам таблиц либо устранить реализационное
расхождение и опубликовать единый актуальный список.

### P7. Существующие тесты не покрывают проблемные пути

**Критичность:** средняя, риск повторного появления и сохранения дефектов.

`TestDecimalAsPrimaryKey` покрывает точечные запросы, но не диапазоны по PK.
`PgInt4Column` использует native Arrow `BulkUpsert`, который не воспроизводит
повреждение PG-значений в QueryService.

Оба штатных теста проходят, несмотря на воспроизводимые ошибки корректности.

Добавлен отдельный параметризованный функциональный набор:

```text
ydb/tests/functional/type_support_consistency/test_type_support_consistency.py
```

Он проверяет P1–P4 для обоих значений `table_kind`: `row` и `column`.
Документационные расхождения P5/P6 намеренно не включены в исполняемые тесты.

Проверки покрывают:

- диапазоны `Decimal` PK с отрицательными значениями и переходом через ноль;
- QueryService round-trip для `pgint2`, `pgint4`, `pgint8`, `pgfloat4` и
  `pgfloat8`;
- соблюдение выключенного `EnableTablePgTypes`;
- создание таблицы с TTL по `DyNumber`.

На текущей реализации все восемь row-параметров проходят, а все восемь
column-параметров падают по причинам, описанным в P1–P4. После исправления
реализации ожидается прохождение всех параметров.

## 1. Базовый набор YQL-типов

Канонический список содержит 27 скалярных типов:

```text
Int8 Uint8 Int16 Uint16 Int32 Uint32 Int64 Uint64
Bool Double Float String Utf8 Yson Json Decimal
Date Datetime Timestamp Interval JsonDocument DyNumber Uuid
Date32 Datetime64 Timestamp64 Interval64
```

Источник: `ydb/public/lib/scheme_types/scheme_type_id.h`, массив `YqlIds`.

Для неключевых колонок строковые и колоночные таблицы принимают все 27 типов.
Таблицы, содержащие одновременно все эти типы, были успешно созданы, заполнены
ненулевыми значениями и прочитаны обратно.

Псевдонимы `Bytes` (`String`) и `Text` (`Utf8`) также принимаются обеими
таблицами. Они не считаются отдельными физическими типами.

## 2. Типы первичного ключа

Обе реализации разрешают один и тот же набор из 22 YQL-типов:

```text
Bool Int8 Uint8 Int16 Uint16 Int32 Uint32 Int64 Uint64
String Utf8 Date Datetime Timestamp Date32 Datetime64 Timestamp64
Interval Interval64 Decimal DyNumber Uuid
```

Одинаково запрещены:

```text
Float Double Json JsonDocument Yson
```

Row allowlist находится в:

```text
ydb/core/scheme/scheme_tabledefs.h:27
```

Column allowlist находится в:

```text
ydb/core/tx/schemeshard/olap/columns/update.cpp:330
```

Для каждого из 22 допустимых типов были созданы отдельные row/column таблицы.
Для пяти запрещённых типов обе реализации вернули ошибку создания таблицы.

## 3. Сортировка

Для всех 24 сравнимых YQL-типов выполнено сравнение:

```sql
SELECT Key FROM row_table ORDER BY Value ASC, Key;
SELECT Key FROM column_table ORDER BY Value ASC, Key;

SELECT Key FROM row_table ORDER BY Value DESC, Key;
SELECT Key FROM column_table ORDER BY Value DESC, Key;
```

В данных присутствовали отрицательные и положительные значения, граничные
значения целых типов и `NULL`.

Результаты row и column совпали для всех 24 типов, включая `Decimal`,
`DyNumber`, `Uuid` и расширенные date/time-типы. Порядок `NULL` также совпал.

Для `Json`, `JsonDocument` и `Yson` обе таблицы вернули одинаковую ошибку
`Expected comparable type`.

Важно: корректный SQL `ORDER BY Decimal` не исключает найденную ниже проблему.
Обычная сортировка выполняется вычислительным движком, а диапазон первичного
ключа обрабатывается storage-level компаратором ColumnShard.

## 4. Дефект диапазонов по `Decimal` в первичном ключе

### Воспроизведение

```sql
CREATE TABLE row_dec_bounds (
    k Decimal(22, 9) NOT NULL,
    PRIMARY KEY (k)
);

CREATE TABLE col_dec_bounds (
    k Decimal(22, 9) NOT NULL,
    PRIMARY KEY (k)
)
PARTITION BY HASH(k)
WITH (STORE = COLUMN);
```

В обе таблицы записываются ключи:

```text
-100, -1, 0, 2, 10, 100
```

Запрос:

```sql
SELECT k
FROM row_dec_bounds
WHERE k >= Decimal('-1', 22, 9)
ORDER BY k;
```

возвращает:

```text
-1, 0, 2, 10, 100
```

Тот же запрос к `col_dec_bounds` возвращает:

```text
-1, 2, 10, 100
```

Значение `0` потеряно.

Для другого условия:

```sql
SELECT k
FROM col_dec_bounds
WHERE k < Decimal('0', 22, 9)
ORDER BY k;
```

колоночная таблица возвращает пустой результат, тогда как строковая возвращает
`-100, -1`.

Ранее дефект также воспроизведён составным диапазоном:

```sql
WHERE k >= Decimal('-1', 22, 9)
  AND k < Decimal('12.46', 22, 9)
```

Row возвращает четыре значения (`-1`, `0`, `3.14`, `8.16`), column — только
`-1`.

Точечный поиск по `Decimal` работает. Если избежать PK pushdown, например:

```sql
WHERE k + Decimal('0', 22, 9) >= Decimal('-1', 22, 9)
  AND k + Decimal('0', 22, 9) < Decimal('12.46', 22, 9)
```

обе таблицы возвращают правильные четыре строки. Следовательно, проблема
локализована в обработке диапазона первичного ключа, а не в вычислении
Decimal-выражений.

Проблема воспроизводится как минимум для `Decimal(22,9)` и `Decimal(35,10)`.

### Причина в коде

Строковая таблица выполняет числовое сравнение 128-битного `Decimal`:

```text
ydb/core/scheme/scheme_tablecell.h:345
```

В Arrow-представлении ColumnShard `Decimal` хранится как
`fixed_size_binary(16)`:

```text
ydb/core/formats/arrow/arrow_helpers.cpp:43
```

Компаратор фиксированных бинарных значений использует побайтовый `memcmp`:

```text
ydb/library/formats/arrow/switch/compare.h:42
ydb/library/formats/arrow/switch/compare.h:113
```

Побайтовый порядок little-endian 128-битного представления не совпадает с
числовым порядком `Decimal`.

### Пробел в тестах

Существующий `TestDecimalAsPrimaryKey` проверяет точечные чтения, но не
диапазоны, отрицательные значения и переход через ноль.

Запущенный штатный тест:

```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/olap/types \
  -F '*TestDecimalAsPrimaryKey*'
```

Результат: `6 OK`.

## 5. PostgreSQL-типы в ColumnShard

### Расхождение feature flag и DDL

Глобальный флаг `EnableTablePgTypes` по умолчанию выключен:

```text
ydb/core/protos/feature_flags.proto:134
```

Строковые таблицы учитывают этот флаг и отклоняют PG-типы. Проверка типов
колоночной таблицы его не учитывает:

```text
ydb/core/tx/schemeshard/olap/operations/checks.h:28
```

При стандартной конфигурации column table принимает пять неключевых PG-типов:

```text
pgint2 pgint4 pgint8 pgfloat4 pgfloat8
```

Allowlist:

```text
ydb/core/tx/schemeshard/olap/columns/update.cpp:316
```

PG-типы в первичном ключе column table запрещены. При включённом
`EnableTablePgTypes` row table, напротив, допускает сравнимые PG-типы в PK и
имеет более широкий набор PG-типов для обычных колонок.

### Повреждение значений при SQL-записи

Запись через QueryService:

```sql
UPSERT INTO column_pgint4 (id, value)
VALUES (1u, pgint4('42'));
```

приводит к следующему результату:

| Тип | Записано | Прочитано |
|---|---:|---:|
| `pgint2` | `42` | `10752` |
| `pgint4` | `42` | `704643072` |
| `pgint8` | `42` | `3026418949592973312` |
| `pgfloat4` | `42` | `1.4442e-41` |
| `pgfloat8` | `42` | `8.759e-320` |

Для `pgint4` дополнительно проверено:

```sql
value = pgint4('42')
```

возвращает `false`. Следовательно, это повреждение сохранённого значения, а не
ошибка отображения CLI.

Результаты соответствуют интерпретации PostgreSQL binary/network-order bytes
как native little-endian Arrow numeric. Например, байты `00 00 00 2a`
интерпретируются как `0x2a000000`, то есть `704643072`.

Существующий тест `PgInt4Column` записывает native Arrow-значения через
`BulkUpsert`, поэтому этот путь работает и не обнаруживает проблему
QueryService.

Запущенный штатный тест:

```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/scheme \
  -F '*PgInt4Column*'
```

Результат: `1 OK`.

## 6. Расхождение поддержки типов TTL

Общий TTL-валидатор разрешает:

```text
Date Datetime Timestamp Date32 Datetime64 Timestamp64
Uint32 Uint64 DyNumber
```

Источник:

```text
ydb/core/tx/schemeshard/common/validation.cpp:13
```

ColumnShard дополнительно и явно запрещает `DyNumber`:

```text
ydb/core/tx/schemeshard/olap/ttl/validator.cpp:53
```

Это подтверждено на локальном кластере:

```sql
CREATE TABLE row_ttl_dynumber (
    ts DyNumber NOT NULL,
    PRIMARY KEY (ts)
)
WITH (TTL = Interval('P1D') ON ts AS SECONDS);
```

создаётся успешно. Аналогичная column table завершается ошибкой:

```text
Unsupported column type for TTL in column tables
```

`Date32`, `Datetime64` и `Timestamp64` успешно принимаются как TTL-колонки в
обоих видах таблиц.

Для колоночной таблицы действует дополнительное ограничение: TTL-колонка должна
быть первой колонкой PK либо иметь подходящий `MIN_MAX`-индекс.

## 7. Расхождения с документацией

Описание колоночных таблиц находится в:

```text
ydb/docs/ru/core/concepts/datamodel/_includes/table.md:248
ydb/docs/en/core/concepts/datamodel/_includes/table.md:251
```

Документация утверждает:

- всего поддерживается 19 типов;
- в первичном ключе поддерживается 11 типов;
- `Decimal` доступен только вне первичного ключа.

Фактически:

- в обычных колонках принимаются все 27 скалярных YQL-типов;
- в первичном ключе принимаются 22 типа;
- `Decimal` принимается в PK, но его диапазонное чтение работает некорректно.

Документация не перечисляет восемь поддержанных типов:

```text
Bool Date32 Datetime64 Timestamp64 Interval Interval64 DyNumber Uuid
```

Для PK дополнительно не перечислены `Int8`, `Int16` и `Decimal`.

TTL-документация находится в:

```text
ydb/docs/ru/core/concepts/_includes/ttl.md:40
ydb/docs/en/core/concepts/_includes/ttl.md:40
```

Она:

- обещает `DyNumber` без оговорки, что ColumnShard его запрещает;
- не перечисляет реально поддержанные `Date32`, `Datetime64` и `Timestamp64`.

## 8. Feature flags

При стандартных значениях флагов базовые наборы YQL-типов совпадают. Однако
настройки позволяют получить конфигурационные расхождения:

- `EnableColumnshardBool` управляет `Bool` только в ColumnShard;
- `EnableColumnshardInterval` управляет `Interval` только в ColumnShard;
- `EnableColumnshardUuid` управляет `Uuid` только в ColumnShard;
- `EnableColumnshardDyNumber` управляет `DyNumber` только в ColumnShard;
- row PK для `Uuid` использует отдельный `EnableUuidAsPrimaryKey`;
- `EnableTablePgTypes` учитывается row table, но игнорируется при создании
  PG-колонок ColumnShard.

Определения флагов:

```text
ydb/core/protos/feature_flags.proto:133
ydb/core/protos/feature_flags.proto:240
ydb/core/protos/feature_flags.proto:343
```

## Выводы и приоритеты

1. **Высокий приоритет:** исправить storage-level сравнение `Decimal` в PK
   ColumnShard и добавить диапазонные regression-тесты с отрицательными,
   нулевыми и положительными значениями.
2. **Высокий приоритет:** либо запретить PG-типы в column table до исправления,
   либо корректно преобразовывать PostgreSQL binary representation в native
   Arrow values. Обязательно учитывать `EnableTablePgTypes`.
3. **Средний приоритет:** определить продуктовый контракт для TTL на
   `DyNumber` — реализовать поддержку в ColumnShard либо явно задокументировать
   различие.
4. **Средний приоритет:** обновить русскую и английскую документацию по типам
   column table и TTL.
5. **Низкий приоритет:** устранить независимые дублирующиеся allowlist’ы PK в
   SchemeShard и ColumnShard, чтобы они не разошлись в будущем.

## Состояние репозитория после проверки

Исходники не изменялись. Перед созданием этого отчёта рабочее дерево было
чистым. Все тестовые кластеры остановлены.
