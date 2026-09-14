# Функциональный аудит автоматического партиционирования

Дата проверки: 2026-09-12.

Проверяемая версия: `main`, commit `35fe6b99b6b321ebc724e0c644432b2301f49da6`.

Область проверки: автоматическое партиционирование только строковых таблиц. Баги в трекере не создавались. ASAN по последнему указанию не использовался.

## Итог

Найдены две проблемы реализации и два расхождения документации с фактическими defaults:

| ID | Тип | Краткое описание | Regression test |
|---|---|---|---|
| F1 | реализация | `PARTITION_AT_KEYS = (NULL)` возвращает `INTERNAL_ERROR` с assertion-подобной диагностикой | `test_null_partition_boundary_is_rejected_without_internal_error` |
| F2 | реализация/валидация | CREATE и ALTER принимают `MIN_PARTITIONS_COUNT > MAX_PARTITIONS_COUNT` | `test_min_partitions_count_cannot_exceed_max_partitions_count` (`create`, `alter`) |
| D1 | документация | Default preferred partition size указан как 2000 MB, фактически это 2048 MB | не нужен |
| D2 | документация | Default max partitions указан как 50, фактически unset соответствует 32768 | не нужен |

Regression tests намеренно падают на текущей реализации; итог запуска: `3 tests: 3 - FAIL`. Один тест параметризован по операциям CREATE/ALTER.

## Методика

Сначала матрица выполнялась вручную через `ydb sql`, `ydb yql`, `SHOW CREATE`, `scheme describe --partition-boundaries --stats` и `.sys/partition_stats`. Затем две implementation-проблемы перенесены в отдельный functional test.

Проверялись:

- defaults новой строковой таблицы;
- CREATE и ALTER всех `AUTO_PARTITIONING_*` параметров;
- `UNIFORM_PARTITIONS` для `Uint32`, `Uint64`, составного ключа, неподдерживаемых `Int64` и `String`;
- `PARTITION_AT_KEYS` для простого и составного ключа, включая частичную составную границу;
- неверный тип, лишние компоненты, `NULL`, дубликаты и нарушение сортировки границ;
- нулевые значения size/min/max/uniform;
- сочетания initial/min/max partition count;
- `SHOW CREATE` и восстановление его DDL;
- маршрутизация и чтение строк по обе стороны составных границ;
- реальный size-based split при пороге 1 MB и ограничении max=4;
- поведение после массового удаления и compaction.

Load-based split не использовался как regression-сигнал: согласно документации, минимальное окно наблюдения нагрузки составляет две минуты, результат чувствителен к профилю локального стенда. При этом включение/выключение настройки и её отражение в схеме проверены.

## Проблемы реализации

### F1. NULL в PARTITION_AT_KEYS приводит к INTERNAL_ERROR

Запрос:

```yql
CREATE TABLE null_partition_boundary (
    id Uint64 NOT NULL,
    PRIMARY KEY (id)
) WITH (
    PARTITION_AT_KEYS = (NULL)
);
```

И Query API (`ydb sql`), и Scripting API (`ydb yql`) возвращают:

```text
Status: INTERNAL_ERROR
Fatal: yql/essentials/ast/yql_expr.h:2074: index out of range
```

`NULL` не является корректной границей для `Uint64 NOT NULL`, но ошибка пользовательского ввода не должна выходить как internal failure с путём и строкой внутреннего assertion. Соседние неверные случаи корректно диагностируются как type/scheme errors: например, String вместо Uint64 и лишняя компонента составного ключа.

Воспроизведение: `test_null_partition_boundary_is_rejected_without_internal_error` в `ydb/tests/functional/automatic_partitioning_doc_audit/test_automatic_partitioning_doc_audit.py`.

### F2. Принимается противоречивая политика min > max

Обе операции успешно выполняются:

```yql
CREATE TABLE invalid_min_max (...)
WITH (
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4,
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 3
);

ALTER TABLE existing SET (
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4,
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 3
);
```

`scheme describe` затем показывает одновременно:

```text
Min partitions count: 4
Max partitions count: 3
```

Это создаёт пустой диапазон между нижней границей merge и верхней границей обычного split. UI уже проверяет `minimum <= maximum`, а аналогичные настройки topic отклоняются сервером. Table DDL должен валидировать пару и возвращать понятную пользовательскую ошибку.

Воспроизведение: параметризованный `test_min_partitions_count_cannot_exceed_max_partitions_count` в `ydb/tests/functional/automatic_partitioning_doc_audit/test_automatic_partitioning_doc_audit.py`.

Явное начальное число партиций выше max отдельно дефектом не считается: `UNIFORM_PARTITIONS`/`PARTITION_AT_KEYS` задают initial layout, тогда как max ограничивает автоматические split.

## Проблемы документации

### D1. Default размера: 2000 MB вместо 2048 MB

В `ydb/docs/ru/core/concepts/datamodel/_includes/table.md` указано:

```text
Значение по умолчанию: 2000 MB (2 ГБ)
```

и отдельно описан внутренний ориентир 2000 MB. У новой таблицы без `WITH` команды `scheme describe` и `SHOW CREATE` стабильно показывают:

```text
Partitioning by size: true
Preferred partition size (Mb): 2048
```

Нужно выбрать единицы и одно значение: 2048 MiB/2 GiB либо 2000 MB. Сейчас численное значение, подпись и фактическая схема расходятся.

### D2. Default max partitions: 50 вместо 32768

Та же страница указывает default `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 50`. Фактически у новой таблицы поле max не установлено: CLI не показывает его, а effective default в `TTableInfo::GetMaxPartitionsCount()` равен `32 * 1024`, то есть 32768.

Это существенная разница для capacity planning и поведения auto-split. Документацию следует синхронизировать с effective server default либо сервер должен материализовать заявленные 50.

## Что прошло

### Создание и изменение настроек

- Default: size enabled, load disabled, min=1.
- CREATE и ALTER сохраняют флаги, preferred size и min/max; `scheme describe` отражает изменения.
- Установка preferred size через ALTER для таблицы с size disabled включает size partitioning; результат согласован с CLI/UI-поведением.
- Значения size/min/max/uniform, равные нулю, корректно отклоняются.

### Начальные границы

- `UNIFORM_PARTITIONS=3` работает для `Uint32`, `Uint64` и составного PK с допустимой первой компонентой.
- Для `Int64` и `String` возвращается точная ошибка: поддерживаются только `Uint32` и `Uint64`.
- Составные границы `((10, "b"), (20), (30, "z"))` создают четыре ожидаемых диапазона; частичная `(20)` отображается как `[20, null]` и корректно маршрутизирует строки.
- Дубликаты и несортированные границы отклоняются; неверный тип и лишние компоненты получают пользовательскую диагностику.

### SHOW CREATE

`SHOW CREATE` сохраняет текущие границы и настройки. Когда min совпадает с фактическим числом партиций, он может быть опущен, но восстановленный DDL с `PARTITION_AT_KEYS` неявно устанавливает тот же min. Round-trip для такой таблицы дал те же две партиции и min=2.

### Динамический split

В таблицу с порогом 1 MB и max=4 записано 1200 строк общим payload 4 915 200 байт. После обновления shard stats число партиций прошло 1 → 2 → 3 → 4 и остановилось на max. Все 1200 строк сохранились.

После `DELETE` запрос сразу возвращал 0 строк, но физические shard stats продолжили учитывать старые версии/tombstones. Отсутствие немедленного merge не считается дефектом: документированное условие зависит от физического размера, а не от логического `COUNT(*)`.

## Открытые вопросы

1. Должна ли table DDL применять ту же обязательную проверку `min <= max`, которая уже есть для topics и в UI?
2. Какой публичный default размера фиксируем: 2048 MiB (2 GiB) или 2000 MB?
3. Является ли effective max=32768 намеренным публичным default, или сервер должен устанавливать документированные 50?
4. Следует ли явно документировать, что ALTER preferred size одновременно включает size-based partitioning?
5. Стоит ли описать задержку merge после массового DELETE и зависимость от освобождения физического размера/старых версий?
