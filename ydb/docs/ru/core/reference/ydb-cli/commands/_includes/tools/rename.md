# Переименование таблицы

С помощью подкоманды `tools rename` вы можете [переименовать](../../../../../concepts/datamodel/table.md#rename) одну или несколько таблиц одновременно, перенести таблицу в другую директорию в пределах той же БД, заменить одну таблицу другой в рамках одной транзакции.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] tools rename [options...]
```

* `global options` — [глобальные параметры](../../../commands/global-options.md).
* `options` — [параметры подкоманды](#options).

Посмотрите описание команды для переименования таблицы:

```bash
{{ ydb-cli }} tools rename --help
```

## Параметры подкоманды {#options}

Один запуск команды `tools rename` выполняет одну транзакцию переименования, которая может включать одну или несколько операций переименования разных таблиц.

Имя параметра | Описание параметра
---|---
`--item <свойство>=<значение>,...` | Описание операции переименования. Может быть указан несколько раз, если необходимо выполнить несколько операций переименования в одной транзакции.<br/><br/>Обязательные свойства:<br/><ul><li>`source`, `src`, `s` — путь до таблицы источника.</li><li>`destination`, `dst`, `d` —  путь до таблицы назначения. Если путь назначения содержит директории, они должны быть [созданы заранее](../../dir.md#mkdir).</li></ul>Дополнительные свойства:<br/><ul> <li>`replace`, `force` — перезаписывать таблицу назначения. Если значение `True`, то таблица назначения будет перезаписана, а существующие в ней данные удалены. `False` — если таблица назначения существует, то будет выдана ошибка, и транзакция переименования будет целиком откачена. Значение по умолчанию: `False`.</li></ul>
`--timeout <значение>` | Таймаут операции, мс.

При включении нескольких операций переименования в один вызов `tools rename` они исполняются в заданном порядке, но в рамках одной транзакции, что позволяет ротировать таблицу под нагрузкой без потери данных: первой операцией идет переименование рабочей таблицы в резервную, второй -- переименование новой таблицы в рабочую.

## Примеры {#examples}

- Переименование одной таблицы:

  ```bash
  {{ ydb-cli }} tools rename --item src=old_name,dst=new_name
  ```

- Переименование нескольких таблиц в одной транзакции:
  ```bash
  {{ ydb-cli }} tools rename \
    --item source=new-project/main_table,destination=new-project/episodes \
    --item source=new-project/second_table,destination=new-project/seasons \
    --item source=new-project/third_table,destination=new-project/series
  ```

- Перемещение таблиц в другую директорию:

  ```bash
  {{ ydb-cli }} tools rename \
    --item source=new-project/main_table,destination=cinema/main_table \
    --item source=new-project/second_table,destination=cinema/second_table \
    --item source=new-project/third_table,destination=cinema/third_table
  ```

- Замена таблицы

  ```bash
  {{ ydb-cli }} tools rename \
    --item replace=True,source=pre-prod-project/main_table,destination=prod-project/main_table
  ```

- Ротация таблицы

  ```bash
  {{ ydb-cli }} tools rename \
    --item source=prod-project/main_table,destination=prod-project/main_table.backup \
    --item source=pre-prod-project/main_table,destination=prod-project/main_table
  ```
