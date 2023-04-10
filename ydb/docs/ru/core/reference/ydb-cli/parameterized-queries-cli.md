# Передача параметров в команды выполнения YQL-запросов

{{ ydb-short-name }} поддерживает и рекомендует к использованию [параметризованные запросы](https://en.wikipedia.org/wiki/Prepared_statement). Для выполнения параметризованных YQL-запросов вы можете использовать команды {{ ydb-short-name }} CLI:

* [ydb yql](yql.md);
* [ydb scripting yql](scripting-yql.md);
* [ydb table query execute](table-query-execute.md).

Команды поддерживают одинаковый синтаксис и возможности передачи параметров запросов. Значения параметров могут быть заданы в командной строке, загружены из файлов в формате [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}, а также считаны с `stdin` в бинарном формате или в формате [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}. При передаче параметров через `stdin` поддерживается многократное поточное исполнение YQL-запроса с разными значениями параметров и возможностью пакетирования.

Для работы с параметрами в YQL-запросе должны присутствовать их определения [командой YQL `DECLARE`](../../yql/reference/syntax/declare.md).

В примерах используется команда `table query execute`, они также выполняются для любой команды исполнения YQL-скрипта.

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

## Простой параметризованный запрос {#example-simple}

Передача значения параметра `a` через `--param`:

```bash
{{ ydb-cli }} -p db1 table query execute -q 'declare $a as Int64;select $a' --param '$a=10'
```

Передача значения параметра `a` в JSON-формате через `stdin`:

```bash
echo '{"a":10}' | {{ ydb-cli }} -p db1 table query execute -q 'declare $a as Int64;select $a'
```

Передача значения параметра `a` в JSON-формате через файл `p1.json`:

```bash
echo '{"a":10}' > p1.json
{{ ydb-cli }} -p db1 table query execute -q 'declare $a as Int64;select $a' --param-file p1.json
```

## Передача значений параметров разных типов из нескольких источников {#example-multisource}

```bash
echo '{ "a":10, "b":"Some text", "x":"Ignore me" }' > p1.json
echo '{ "c":"2012-04-23T18:25:43.511Z" }' | {{ ydb-cli }} -p db1 table query execute \
  -q 'declare $a as Int64;declare $b as Utf8;declare $c as DateTime;declare $d as Int64;
      select $a, $b, $c, $d' \
  --param-file p1.json \
  --param '$d=30'
```

## Передача бинарных строк в кодировке Base64 {#example-base64}

```bash
{{ ydb-cli }} -p db1 table query execute \
  -q 'declare $a as String;select $a' \
  --input-format json-base64 \
  --param '$a="SGVsbG8sIHdvcmxkCg=="' 
```

## Передача бинарного контента в поле типа «бинарная строка» {#example-raw}

```bash
curl -L http://ydb.tech/docs | {{ ydb-cli }} -p db1 table query execute \
  -q 'declare $a as String;select LEN($a)' \
  --stdin-format raw \
  --stdin-par a
```

## Итеративная обработка нескольких наборов параметров {#example-iterate}

```bash
echo -e '{"a":10,"b":20}\n{"a":15,"b":25}\n{"a":35,"b":48}' > p1.json
cat p1.json | {{ ydb-cli }} -p db1 table query execute \
  -q 'declare $a as Int64;declare $b as Int64;select $a+$b' \
  --stdin-format newline-delimited
```

## Полная пакетная обработка {#example-full}

В данном примере использован вариант передачи именованных параметров в список структур, применимый для полного и адаптивного режимов пакетирования.

```bash
echo -e '{"a":10,"b":20}\n{"a":15,"b":25}\n{"a":35,"b":48}' | \
{{ ydb-cli }} -p db1 table query execute \
  -q 'declare $x as List<Struct<a:Int64,b:Int64>>;select ListLength($x), $x' \
  --stdin-format newline-delimited \
  --stdin-par x \
  --batch full
```

## Адаптивная пакетная обработка {#example-adaptive}

### Триггер по задержке обработки {#example-adaptive-delay}

Для демонстрации работы адаптивного пакетирования со срабатыванием триггера по задержке обработки в первой строке команды ниже производится генерация 1000 строк с задержкой в 0.2 секунды. Команда будет отображать пакеты параметров в каждом следующем вызове YQL-запроса.

В данном примере использован вариант передачи параметров без имени в типизированный список, применимый для полного и адаптивного режимов пакетирования.

Значение `--batch-max-delay` по умолчанию установлено в 1 секунду, поэтому в каждый запрос будет попадать около 5 строк. При этом в первый пакет пойдут все строки, накопившиеся за время открытия соединения с БД, и он будет больше чем последующие.

```bash
for i in $(seq 1 1000);do echo "Line$i";sleep 0.2;done | \
{{ ydb-cli }} -p db1 table query execute \
  -q 'declare $x as List<Utf8>;select ListLength($x), $x' \
  --stdin-format newline-delimited \
  --stdin-format raw \
  --stdin-par x \
  --batch adaptive
```

### Триггер по количеству записей {#example-adaptive-limit}

Для демонстрации работы адаптивного пакетирования со срабатыванием триггера по количеству наборов параметров в первой строке команды ниже производится генерация 200 строк. Команда будет отображать пакеты параметров в каждом следующем вызове YQL-запроса, учитывая заданное ограничение `--batch-limit` равное 20 (по умолчанию 1000).

В данном примере также показана возможность объединения параметров из разных источников, и формирование JSON на выходе.

```bash
for i in $(seq 1 200);do echo "Line$i";done | \
{{ ydb-cli }} -p db1 table query execute \
  -q 'declare $x as List<Utf8>;declare $p2 as Int64;
      select ListLength($x) as count, $p2 as p2, $x as items' \
  --stdin-format newline-delimited \
  --stdin-format raw \
  --stdin-par x \
  --batch adaptive \
  --batch-limit 20 \
  --param '$p2=10' \
  --format json-unicode
```

## Конвейерная обработка сообщений, считываемых из топика {#example-adaptive-pipeline-from-topic}

О конвейерной обработке сообщений, считываемых из топика, читайте в статье [{#T}](topic-pipeline.md#example-read-to-yql-param).

## См. также {#see-also}

* [Параметризованные YQL-запросы в {{ ydb-short-name }} SDK](../ydb-sdk/parameterized_queries.md)
