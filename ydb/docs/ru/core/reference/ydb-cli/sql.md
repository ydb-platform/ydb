# Выполнение запросов

С помощью подкоманды `{{ ydb-cli }} sql` вы можете выполнить SQL-запрос. Запрос может быть любого типа (DDL, DML и т.д.), а так же состоять из нескольких подзапросов. Подкоманда `{{ ydb-cli }} sql` устанавливает стрим и получает данные через него. Выполнение запроса в стриме позволяет снять ограничение на размер читаемых данных. Эта команда также позволяет записывать данные в YDB, что более эффективно при выполнении повторяющихся запросов с передачей данных через параметры.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] sql [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).

Посмотрите описание команды выполнения запроса:

```bash
{{ ydb-cli }} sql --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `-h`, `--help` | Выводит общую справку по использованию команды. ||
|| `-hh` | Выводит полную справку по использованию команды. Вывод содержит некоторые специфичный команды, которых нет в выводе `--help`. ||
|| `-s`, `--script` | Текст скрипта(запроса) для выполнения. ||
|| `-f`, `--file` | Путь к файлу, содержащему текст запроса для выполнения. Путь `-` означает, что текст запроса будет прочитан из `stdin`, при этом передача параметров через `stdin` будет невозможна. ||
|| `--stats` | Режим сбора статистики.<br/>Возможные значения:<br/><ul><li>`none` (по умолчанию) — не собирать;</li><li>`basic`: Собирать обобщенную статистику по обновлениям(update) и удалениям(delete) в таблицах.</li><li>`full`: К тому, что содержит режим `basic` добавляется статистика и план выполнения запроса.</li><li>`profile`: Собирать детальную статистику выполнения, содержащую статистику по каждому отдельному таску и каналу выолнения запроса.</li></ul> ||
|| `--explain` | Выполнить explain-запрос, будет выведен логический план запроса. Сам запрос не будет выполнен, поэтому не затронет данные в базе. ||
|| `--explain-ast` | То же, что и `--explain`, но вдобавок к логическому плану выводит [AST (abstract syntax tree)](https://ru.wikipedia.org/wiki/Абстрактное_синтаксическое_дерево). Раздел с AST  содержит представление на внутреннем языке [miniKQL](../../concepts/glossary.md#minikql). ||
|| `--explain-analyze` | Выполнить запрос в режиме `EXPLAIN ANALYZE`. Показывает план выполнения запроса. Возвращаемые в рамках запроса данные игнорируются.<br/>**Важное замечание: Запрос фактически выполняется, поэтому может внести изменения в базу**. ||
|| `--diagnostics-file` | Путь для сохранения файла с диагностикой. ||
|| `--format` | Формат вывода.<br/>Возможные значения:

{% include notitle [format](./_includes/result_format_common.md) %}

{% include notitle [format](./_includes/result_format_csv_tsv.md) %}

||
|#

### Сбор диагностики {#diagnostics-collection}

Опция `--diagnostics-file <path_to_diagnostics>` позволяет сохранять расширенную информацию о выполнении SQL-запросов в отдельный JSON-файл.

Диагностика формируется при сборе статистики `--stats full`, а также при выполнении `EXPLAIN`-запросов. Для каждого запроса будет создан файл `<path_to_diagnostics>` со следующими полями:

- `plan` — [план](../../yql/query_plans.md) выполнения запроса.
- `stats` — статистика выполнения запроса.
- `meta` — дополнительная информация о запросе в JSON-формате, формируется при сборе статистики `--stats full`, а также при выполнении `EXPLAIN`-запросов (`--explain`, `--explain-ast` или `--explain-analyze`), включает следующие поля:
    - `created_at` — время начала запроса (timestamp в секундах).
    - `query_cluster` — название кластера или провайдера (всегда константа).
    - `query_database` — путь к базе данных.
    - `query_id` — уникальный идентификатор запроса.
    - `query_syntax` — используемый синтаксис запроса; по умолчанию используется синтаксис V1. Также доступен экспериментальный синтаксис PostgreSQL.
    - `query_text` — текст SQL-запроса.
    - `query_type` — тип запроса, возможные значения: `QUERY_TYPE_SQL_GENERIC_QUERY`, `QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY`.
    - `table_metadata` — список Protobuf-описаний (сериализованных в JSON) для всех таблиц, участвующих в запросе. Каждый элемент списка содержит описание схемы таблицы, индексов и статистики, все эти поля изначально представлены в формате Protobuf.

      {% cut "Пример описания таблицы в table_metadata" %}

      ```json
      {
          "DoesExist": true,
          "Cluster": "db",
          "Name": "/local/users",
          "SysView": "",
          "PathId": {
              "OwnerId": 72075186232723360,
              "TableId": 33
          },
          "SchemaVersion": 1,
          "Kind": 1,
          "Columns": [
              {
                  "Name": "emails",
                  "Id": 3,
                  "Type": "Utf8",
                  "TypeId": 4608,
                  "NotNull": false,
                  "DefaultFromSequence": "",
                  "DefaultKind": 0,
                  "DefaultFromLiteral": {},
                  "IsBuildInProgress": false,
                  "DefaultFromSequencePathId": {
                      "OwnerId": 18446744073709551615,
                      "TableId": 18446744073709551615
                  }
              },
              {
                  "Name": "id",
                  "Id": 1,
                  "Type": "Int32",
                  "TypeId": 1,
                  "NotNull": false,
                  "DefaultFromSequence": "",
                  "DefaultKind": 0,
                  "DefaultFromLiteral": {},
                  "IsBuildInProgress": false,
                  "DefaultFromSequencePathId": {
                      "OwnerId": 18446744073709551615,
                      "TableId": 18446744073709551615
                  }
              },
              {
                  "Name": "name",
                  "Id": 2,
                  "Type": "Utf8",
                  "TypeId": 4608,
                  "NotNull": false,
                  "DefaultFromSequence": "",
                  "DefaultKind": 0,
                  "DefaultFromLiteral": {},
                  "IsBuildInProgress": false,
                  "DefaultFromSequencePathId": {
                      "OwnerId": 18446744073709551615,
                      "TableId": 18446744073709551615
                  }
              }
          ],
          "KeyColunmNames": [
              "id"
          ],
          "RecordsCount": 0,
          "DataSize": 0,
          "StatsLoaded": false
      }
      ```

      {% endcut %}

- `ast` — абстрактное синтаксическое дерево [AST](https://ru.wikipedia.org/wiki/Абстрактное_синтаксическое_дерево) запроса.

  {% cut "Пример ast" %}

  ```text
  (
  (let  (KqpTable '"/local/users" '"72075186232723360:33" '"" '1))
  (let  '('"emails" '"id" '"name"))
  (let  (KqpRowsSourceSettings   '() (Void) '()))
  (let  (DqPhyStage '((DqSource (DataSource '"KqpReadRangesSource") )) (lambda '() (FromFlow (Filter (ToFlow ) (lambda '(0) (Coalesce (== (Member 0 '"emails") (String '"john@example.com")) (Bool 'false)))))) '('('"_logical_id" '401) '('"_id" '"45d03f9b-f40c98ba-b6705ab-90ee6ea"))))
  (let  (DqCnUnionAll (TDqOutput  '"0")))
  (let  (DqPhyStage '() (lambda '(1) 1) '('('"_logical_id" '477) '('"_id" '"28936ac-4af296f3-7afa38af-7dc0798"))))
  (let  (DqCnResult (TDqOutput  '"0") '()))
  (let  (OptionalType (DataType 'Utf8)))
  (return (KqpPhysicalQuery '((KqpPhysicalTx '( ) '() '() '('('"type" '"generic")))) '((KqpTxResultBinding (ListType (StructType '('"emails" ) '('"id" (OptionalType (DataType 'Int32))) '('"name" ))) '"0" '"0")) '('('"type" '"query"))))
  )
  ```

  {% endcut %}

{% note warning %}

Файл диагностики может содержать конфиденциальные данные и чувствительную информацию, особенно в полях `meta.query_text`, `plan` и `ast`. Перед передачей такого файла сторонним лицам (например, в техническую поддержку) рекомендуется вручную просмотреть и отредактировать содержимое файла, чтобы удалить или заменить чувствительную информацию.

{% endnote %}

#### Пример

Команда, чтобы собрать диагностику в файл `diagnostics.json` и проверить его содержимое:

```bash
ydb -e <endpoint> -d <database> sql -s "SELECT * FROM users WHERE email = 'alice@example.com';" \
--stats full --diagnostics-file diagnostics.json
cat diagnostics.json
```

Если вы хотите получить диагностические данные, относящиеся к плану запроса, без фактического выполнения запроса, вы можете вместо этого выполнить `EXPLAIN`-запрос, добавив опцию `--explain`:

```bash
ydb -e <endpoint> -d <database> sql -s "SELECT * FROM users WHERE email = 'alice@example.com';" --explain --diagnostics-file diagnostics.json
```

В диагностическом файле `diagnostics.json` в поле `meta.query_text` будет содержаться такая строка:

```json
"query_text": "SELECT * FROM users WHERE email = 'alice@example.com';"
```

Здесь присутствует чувствительная информация — адрес электронной почты пользователя. Перед передачей диагностического файла рекомендуется заменить реальные значения на шаблонные:

```json
"query_text": "SELECT * FROM users WHERE email = '<EMAIL>';"
```

В данном примере адрес электронной почты можно обнаружить в полях `plan` и `ast`, такие вхождения тоже нужно заменить.

### Работа с параметризованными запросами {#parameterized-query}

Подробное описание работы с параметрами с примерами смотрите в статье [{#T}](parameterized-query-execution.md).

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Запрос создания строковой таблицы, заполнения её данными, и получения выборки из этой таблицы:

```bash
{{ ydb-cli }} -p quickstart sql -s '
    CREATE TABLE series (series_id Uint64, title Utf8, series_info Utf8, release_date Date, PRIMARY KEY (series_id));
    COMMIT;
    UPSERT INTO series (series_id, title, series_info, release_date) values (1, "Title1", "Info1", Cast("2023-04-20" as Date));
    COMMIT;
    SELECT * from series;
  '
```

Вывод команды:

```text
┌──────────────┬───────────┬─────────────┬──────────┐
| release_date | series_id | series_info | title    |
├──────────────┼───────────┼─────────────┼──────────┤
| "2023-04-20" | 1         | "Info1"     | "Title1" |
└──────────────┴───────────┴─────────────┴──────────┘
```

Выполнение запроса из примера выше, записанного в файле `script1.yql`, с выводом результатов в формате `JSON`:

```bash
{{ ydb-cli }} -p quickstart sql -f script1.yql --format json-unicode
```

Вывод команды:

```text
{"release_date":"2023-04-20","series_id":1,"series_info":"Info1","title":"Title1"}
```

Примеры передачи параметров в скрипты приведены в [статье о передаче параметров в команды исполнения запросов](parameterized-query-execution.md).
