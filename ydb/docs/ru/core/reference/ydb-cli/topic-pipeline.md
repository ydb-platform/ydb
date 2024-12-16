# Конвейерная обработка сообщений

Возможности работы команд `topic read` и `topic write` со стандартными устройствами ввода/вывода, а также поддержка потокового режима на чтении, позволяет выстраивать полноценные сценарии интеграции с передачей сообщений между топиками и их преобразованиями. В данном разделе собраны несколько примеров подобных сценариев.

* Перекладывание одного сообщения из `topic1` в базе данных `quickstart` в `topic2` в базе данных `db2`, с ожиданием его появления в топике-источнике

  ```bash
  {{ ydb-cli }} -p quickstart topic read topic1 -c c1 -w | {{ ydb-cli }} -p db2 topic write topic2
  ```

* Фоновая передача всех появляющихся однострочных сообщений в топике `topic1` в базе данных `quickstart`, в топик `topic2` в базе данных `db2`. Данный сценарий можно использовать в случае, если гарантируется отсутствие байтов `0x0A` (перевод строки) в исходных сообщениях.

  ```bash
  {{ ydb-cli }} -p quickstart topic read topic1 -c c1 --format newline-delimited -w | \
  {{ ydb-cli }} -p db2 topic write topic2 --format newline-delimited
  ```

* Фоновая передача точной бинарной копии всех появляющихся сообщений в топике `topic1` в базе данных `quickstart`, в топик `topic2` в базе данных `db2`, с использованием кодировки сообщений base64 на потоке передачи.

  ```bash
  {{ ydb-cli }} -p quickstart topic read topic1 -c c1 --format newline-delimited -w --transform base64 | \
  {{ ydb-cli }} -p quickstart topic write topic2 --format newline-delimited --transform base64
  ```

* Передача ограниченного пакета однострочных сообщений с фильтрацией по подстроке `ERROR`

  ```bash
  {{ ydb-cli }} -p quickstart topic read topic1 -c c1 --format newline-delimited | \
  grep ERROR | \
  {{ ydb-cli }} -p db2 topic write topic2 --format newline-delimited
  ```

* Запись результата исполнения YQL-запроса в виде сообщений в топик `topic1`

  ```bash
  {{ ydb-cli }} -p quickstart yql -s "select * from series" --format json-unicode | \
  {{ ydb-cli }} -p quickstart topic write topic1 --format newline-delimited
  ```

## Исполнение YQL-запроса с передачей сообщений из топика в качестве параметров {#example-read-to-yql-param}

* Исполнение YQL-запроса с передачей параметром каждого сообщения, считанного из топика `topic1`

  ```bash
  {{ ydb-cli }} -p quickstart topic read topic1 -c c1 --format newline-delimited -w | \
  {{ ydb-cli }} -p quickstart table query execute -q 'declare $s as String;select Len($s) as Bytes' \
  --stdin-format newline-delimited --stdin-par s --stdin-format raw
  ```

* Исполнение YQL-запроса с адаптивным пакетированием параметров из сообщений, считанных из топика `topic1`

  ```bash
  {{ ydb-cli }} -p quickstart topic read topic1 -c c1 --format newline-delimited -w | \
  {{ ydb-cli }} -p quickstart table query execute \
  -q 'declare $s as List<String>;select ListLength($s) as Count, $s as Items' \
  --stdin-format newline-delimited --stdin-par s --stdin-format raw \
  --batch adaptive
  ```
