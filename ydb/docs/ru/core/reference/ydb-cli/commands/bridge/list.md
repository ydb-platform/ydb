# admin cluster bridge list

С помощью команды `admin cluster bridge list` вы можете вывести состояние каждого пайла в [режиме bridge](../../../../concepts/bridge.md).

Общий вид команды:

```bash
ydb [global options...] admin cluster bridge list [options...]
```

* `global options` — глобальные параметры.
* `options` — [параметры подкоманды](#options).

Посмотрите справку по команде:

```bash
ydb admin cluster bridge list --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `--format <pretty, json, csv>` | Формат вывода. Значение по умолчанию: `pretty`. ||
|#

## Примеры {#examples}

Вывести список пайлов в человекочитаемом виде:

```bash
ydb admin cluster bridge list

pile-a: PRIMARY
pile-b: SYNCHRONIZED
```


Вывести состояние в формате JSON:

```bash
ydb admin cluster bridge list --format json

{
  "pile-a": "PRIMARY",
  "pile-b": "SYNCHRONIZED"
}
```

Вывести состояние в формате CSV:

```bash
ydb admin cluster bridge list --format csv

pile,state
pile-a,PRIMARY
pile-b,SYNCHRONIZED
```
