# Создание дампа данных

С помощью подкоманды `tools dump` вы можете сохранить снапшот директории БД со всеми вложенными объектами, конкретной таблицы или схемы БД локально.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] tools dump [options...]
```

* `global options` — [глобальные параметры](../../../commands/global-options.md).
* `options` — [параметры подкоманды](#options).

Посмотрите описание команды для дампа данных:

```bash
{{ ydb-cli }} tools dump --help
```

## Параметры подкоманды {#options}

Имя параметра | Описание параметра
---|---
`-p <значение>`<br/>`--path <значение>` | Путь к директории или таблице, дамп которых нужно создать.<br/>По умолчанию будет сделан полный дамп БД.
`-o <значение>`<br/>`--output <значение>` | Обязательный параметр.<br/>Путь на локальной файловой системе, по которому будут размещены объекты дампа.<br/>Директория для дампа не должна существовать, либо быть пустой.
`--scheme-only` | Сделать дамп только схемы БД.
`--avoid-copy` | Не делать копию.<br/>По умолчанию, для уменьшения влияния на пользовательскую нагрузку и гарантии консистентности данных, {{ ydb-short-name }} сначала делает копию таблицы, а затем — дамп копии. В некоторых случаях, например при дампе таблиц с внешними блобами, такой способ не подходит.
`--save-partial-result` | Сохранять результаты неполного дампа.<br/>При указании этой опции, все данные, которые успели записаться до прерывания операции, сохраняются.
`--consistency-level <значение>` | Уровень консистентности.<br/>Дамп данных с уровнем консистентности на уровне базы данных выполняется дольше, и его влияние на пользовательскую нагрузку более вероятно. Возможные значения:<br/><ul><li>`database` — консистентность на уровне базы данных.</li><li>`table` — консистентность на уровне таблицы.</li></ul>Значение по умолчанию — `database`.

## Примеры {#examples}

### Полный дамп БД

Создайте дамп таблицы `seasons` в локальной директории `~/dump` с консистентностью на уровне таблицы:

```bash
{{ ydb-cli }} tools dump \
  --path seasons \
  --output ~/dump \
  --consistency-level table
```

Посмотрите список объектов директории `dump`:

```bash
ls -R ~/dump
```

Результат:

```text
/home/user/dump:
episodes  my-directory  seasons  series

/home/user/dump/episodes:
data_00.csv  scheme.pb

/home/user/dump/my-directory:
sub-directory1  sub-directory2

/home/user/dump/my-directory/sub-directory1:
sub-directory1-1

/home/user/dump/my-directory/sub-directory1/sub-directory1-1:

/home/user/dump/my-directory/sub-directory2:

/home/user/dump/seasons:
data_00.csv  scheme.pb

/home/user/dump/series:
data_00.csv  scheme.pb  update_feed

/home/user/dump/series/update_feed:
changefeed_description.pb  topic_description.pb
```
