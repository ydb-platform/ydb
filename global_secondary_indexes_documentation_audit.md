# Функциональный аудит глобальных secondary indexes

Дата проверки: 2026-09-12.

Проверяемая версия: `main`, commit `35fe6b99b6b321ebc724e0c644432b2301f49da6`.

Область проверки: глобальные индексы только строковых таблиц. Проверка выполнена
black-box через YDB CLI и Query API локального кластера. Баги в трекере не
создавались. ASAN не использовался.

## Итог

Найдена одна проблема реализации и одно неуточнённое место документации.

| ID | Тип | Краткое описание | Regression test |
|---|---|---|---|
| P1 | реализация | Успешный index build публикует error-level issues для пустых партиций | `test_successful_build_on_empty_partitions_has_no_error_issues` — падает |
| D1 | документация | Не описана семантика `NULL` в global unique index | не нужен |

## P1. SUCCESS index build содержит ошибки для пустых партиций

### Воспроизведение

Создана пустая строковая таблица с четырьмя партициями:

```yql
CREATE TABLE `empty_partitioned` (
    id Uint64 NOT NULL,
    value Uint64,
    PRIMARY KEY (id)
) WITH (UNIFORM_PARTITIONS = 4);
```

После запуска `table index add global-sync` операция заканчивается состоянием
`ready=true`, `status=SUCCESS`, `state=Done`, `progress=100.00%`. При этом
`operation get` возвращает секцию `Issues`:

```text
<main>: Error: Shard or requested range is empty
TShardStatus { ... Status: DONE UploadStatus: STATUS_CODE_UNSPECIFIED
DebugMessage: <main>: Error: Shard or requested range is empty ... }
```

Воспроизводится по одному shard issue на каждую пустую партицию. На заполненной
таблице с четырьмя партициями та же проблема проявилась для трёх партиций, чьи
key ranges не содержали строк.

Пустой диапазон является нормальным результатом сканирования, а индекс доступен
и корректен. Поэтому error-level issue противоречит итоговому `SUCCESS`, создаёт
ложное срабатывание мониторинга операций и не совпадает с примером успешного
`operation get` в документации.

Вероятная точка формирования сообщения:

- `ydb/core/tx/datashard/build_index/secondary_index.cpp`;
- `ydb/core/tx/datashard/build_index/common_helper.h`.

Оба пути добавляют обычный `NYql::TIssue("Shard or requested range is empty")`
для успешного пустого scan; severity по умолчанию отображается как `Error`.

### Regression test

Добавлен тест
`ydb/tests/functional/global_index_doc_audit/test_global_index_doc_audit.py::TestGlobalIndexDocumentationAudit::test_successful_build_on_empty_partitions_has_no_error_issues`.

Он строит индекс через настоящий CLI, дожидается `SUCCESS` через `operation get`
и требует отсутствия сообщения об ошибке. На проверяемой версии тест ожидаемо
падает:

```text
AssertionError: ... status SUCCESS ...
Issues:
  - <main>: Error: Shard or requested range is empty
```

Команда проверки:

```bash
./ya make --build relwithdebinfo -tA \
  ydb/tests/functional/global_index_doc_audit \
  -F '*test_successful_build_on_empty_partitions_has_no_error_issues*' \
  2>&1 | tail
```

Результат: `1 test: 1 - FAIL`.

## D1. Не описано поведение unique index для NULL

Документация говорит, что каждое уникальное значение индексируемых колонок
может присутствовать не более одного раза, но не уточняет, считается ли `NULL`
таким значением.

Фактическое поведение: global unique index допускает несколько строк с `NULL` в
индексируемой колонке. Все такие строки доступны через index view. Ненулевой
дубликат отклоняется с `PRECONDITION_FAILED`.

Это соответствует распространённой SQL-семантике, но важно явно записать её для
одноколоночного и составного unique index. Из текущей формулировки пользователь
может сделать вывод, что разрешена только одна строка с `NULL`.

## Что прошло

### Sync, covering и составные ключи

- Составной `GLOBAL SYNC ON (tenant, score) COVER (payload)` возвращает тот же
  набор строк, что full scan, включая повторяющиеся и `NULL` keys.
- Покрытая колонка обновляется после `UPDATE`, `UPSERT` и `REPLACE`.
- `INSERT`, `UPDATE`, `UPSERT`, `REPLACE` и `DELETE` корректно добавляют,
  перемещают и удаляют index entries.
- Освобождённое unique-значение можно присвоить другой строке в той же
  multistatement-транзакции.

### Unique index

- Дубликат ненулевого ключа отклоняется с `PRECONDITION_FAILED`.
- Multirow INSERT с одним конфликтующим значением откатывается целиком.
- UPDATE одновременно с изменением другой колонки при unique-конфликте также
  откатывается целиком; исходные index и base row сохранены.
- Online build unique index на уже конфликтующих данных завершается ошибкой
  `Duplicate key found`, не публикуя неполный индекс.
- Несколько `NULL` keys разрешены.

### Async index

- Обычная Serializable-транзакция чтения async index отклонена с объяснением,
  что требуется `StaleRO`.
- Через `table query execute --tx-mode stale-ro` индекс становится доступен.
- После ожидания eventual convergence результат совпал с основной таблицей для
  всех проверенных DML, включая изменение indexed и covered columns.

### Online build и lifecycle

- Global sync covering index успешно построен на заполненной таблице из 50 000
  строк и четырёх партиций.
- INSERT, UPDATE и DELETE, выполненные одновременно с build, отражены в готовом
  индексе; снимочные данные не затёрли более новые изменения.
- `operation list`, `operation get` и `operation forget` отработали ожидаемо;
  после forget получение операции вернуло `PRECONDITION_FAILED`.
- Новый индекс атомарно заменил старый через `table index rename --replace`, и
  запросы по прежнему имени продолжили возвращать корректные данные.

### Optimizer, VIEW и schema evolution

- Для предиката по `(tenant, score)` и выборки только covered column план
  автоматически выбрал `indexImplTable` без lookup основной таблицы.
- Point lookup полного primary key выбрал основную таблицу.
- Явный `VIEW` имеет приоритет; для неподходящего предиката результат остаётся
  корректным и выдаётся warning.
- Добавление независимой колонки при существующих индексах разрешено.
- Удаление index key или cover column отклонено с указанием конкретного индекса.
- `GLOBAL UNIQUE ASYNC`, повтор index key в `COVER` и primary-key column в
  `COVER` отклоняются.

## Ограничения аудита

- Cancel активного build не был надёжно проверен: локальные builds завершаются
  быстрее отдельного CLI round trip.
- Не выполнялись рестарты SchemeShard/DataShard во время build.
- Не измерялись performance и пределы числа индексов.
- Специализированные vector/fulltext/JSON indexes покрыты отдельными аудитами и
  здесь не повторялись.

## Открытые вопросы

1. Нужно ли полностью убрать empty-range issue из успешной операции или оставить
   его только как debug/trace без публикации пользователю?
2. Должен ли shard `UploadStatus` в успешном пустом scan быть явно `SUCCESS`, а
   не `STATUS_CODE_UNSPECIFIED`?
3. Какова точная unique-семантика для составного ключа, если `NULL` присутствует
   только в части его колонок?
4. Следует ли CLI поддержать создание `GLOBAL UNIQUE SYNC` так же явно, как YQL?
