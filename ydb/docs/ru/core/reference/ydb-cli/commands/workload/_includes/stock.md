# Stock нагрузка

Симулирует работу склада интернет-магазина: создание заказов из нескольких товаров, получение списка заказов по клиенту.

## Виды нагрузки {#workload_types}

Данный нагрузочный тест содержит 5 видов нагрузки:
* [user-hist](#getCustomerHistory) - читает заданное количество заказов покупателя с id = 10 000. Создается нагрузка на чтение одних и тех же строк из разных потоков.
* [rand-user-hist](#getRandomCustomerHistory) - читает заданное количество заказов у случайно выбранного покупателя. Создается нагрузка на чтение из разных потоков.
* [add-rand-order](#insertRandomOrder) - создает случайно сгенерированный заказ. Например, клиент создал заказ из 2 товаров, но еще не оплатил его, поэтому остатки товаров не снижаются. В БД записывается информация о заказе и товарах. Создается нагрузка на запись и чтение (insert перед вставкой проверяет есть ли уже запись).
* [put-rand-order](#submitRandomOrder) - создает и обрабатывает случайно сгенерированный заказ. Например, покупатель создал и оплатил заказ из 2 товаров. В БД записывается информация о заказе, товарах, проверяется их наличие и уменьшаются остатки. Создается смешанная нагрузка.
* [put-same-order](#submitSameOrder) - создает заказы с одним и тем же набором товаров. Например, все покупатели покупают один и тот же набор товаров (только что вышедший телефон и заряжающее устройство). Создается нагрузка в виде конкурентного обновления одних и тех же строк в таблице.

## Инициализация нагрузочного теста {#init}

Для начала работы необходимо создать таблицы и заполнить их данными:
```bash
{{ ydb-cli }} workload stock init [init options...]
```

* `init options` — [параметры инициализации](#init_options).

Посмотрите описание команды для запуска нагрузки:

```bash
{{ ydb-cli }} workload init --help
```

### Доступные параметры {#init_options}

Имя параметра | Короткое имя | Описание параметра
---|---|---
`--products <значение>` | `-p <значение>` | Количество видов товаров. Возможные значения от 1 до 500000. Значение по умолчанию: 100.
`--quantity <значение>` | `-q <значение>` | Количество каждого вида товара на складе. Значение по умолчанию: 1000.
`--orders <значение>` | `-o <значение>` | Первоначальное количество заказов в БД. Значение по умолчанию: 100.
`--min-partitions <значение>` | - | Минимальное количество шардов для таблиц. Значение по умолчанию: 40.
`--auto-partition <значение>` | - | Включение/выключение автошардирования. Возможные значения: 0 или 1. Значение по умолчанию: 1.

Создаются 3 таблицы со следующими DDL:
```sql
CREATE TABLE `stock`(product Utf8, quantity Int64, PRIMARY KEY(product)) WITH (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = <min-partitions>);
CREATE TABLE `orders`(id Uint64, customer Utf8, created Datetime, processed Datetime, PRIMARY KEY(id), INDEX ix_cust GLOBAL ON (customer, created)) WITH (READ_REPLICAS_SETTINGS = "per_az:1", AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = <min-partitions>, UNIFORM_PARTITIONS = <min-partitions>, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000);
CREATE TABLE `orderLines`(id_order Uint64, product Utf8, quantity Int64, PRIMARY KEY(id_order, product)) WITH (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = <min-partitions>, UNIFORM_PARTITIONS = <min-partitions>, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000);
```

### Примеры инициализации нагрузки {#init-stock-examples}

Создание БД с 1000 видов товаров, где каждого товара 10 000 штук, нет заказов:

```bash
{{ ydb-cli }} workload stock init -p 1000 -q 10000 -o 0
```

Создание БД с 10 видами товаров, где каждого товара 100 штук, есть 10 заказов и минимальное количество шардов 100:
```bash
{{ ydb-cli }} workload stock init -p 10 -q 100 -o 10 ----min-partitions 100
```

## Запуск нагрузочного теста {#run}

Для запуска нагрузки необходимо выполнить команду:
```bash
{{ ydb-cli }} workload stock run [workload type...] [global workload options...] [specific workload options...]
```
В течение теста на экран выводится статистика по нагрузке для каждого временного окна.

* `workload type` — [виды нагрузки](#workload_types).
* `global workload options` - [общие параметры для всех видов нагрузки](#global_workload_options).
* `specific workload options` - параметры конкретного вида нагрузки.

Посмотрите описание команды для запуска нагрузки:

```bash
{{ ydb-cli }} workload run --help
```

### Общие параметры для всех видов нагрузки {#global_workload_options}

Имя параметра | Короткое имя | Описание параметра
---|---|---
`--seconds <значение>` | `-s <значение>` | Продолжительность теста, сек. Значение по умолчанию: 10.
`--threads <значение>` | `-t <значение>` | Количество параллельных потоков, создающих нагрузку. Значение по умолчанию: 10.
`--rate <значение>` | - | Суммарная частота запросов всех потоков, в транзакциях в секунду. Значение по умолчанию: 0 (не ограничена).
`--quiet` | - | Выводит только итоговый результат теста.
`--print-timestamp` | - | Печатать время вместе со статистикой каждого временного окна.
`--client-timeout` | - | [Транспортный таймаут в миллисекундах](../../../../../dev/timeouts.md).
`--operation-timeout` | - | [Таймаут на операцию в миллисекундах](../../../../../dev/timeouts.md).
`--cancel-after` | - | [Таймаут отмены операции в миллисекундах](../../../../../dev/timeouts.md).
`--window` | - | Длительность окна сбора статистики в секундах. Значение по умолчанию: 1.


## Нагрузка user-hist {#getCustomerHistory}

Данный вид нагрузки читает заданное количество заказов покупателя с id = 10 000.

YQL Запрос:
```sql
DECLARE $cust AS Utf8;
DECLARE $limit AS UInt32;

SELECT id, customer, created FROM orders view ix_cust
    WHERE customer = 'Name10000'
    ORDER BY customer DESC, created DESC
    LIMIT $limit;
```

Для запуска данного вида нагрузки необходимо выполнить команду:
```bash
{{ ydb-cli }} workload stock run user-hist [global workload options...] [specific workload options...]
```

* `global workload options` - [общие параметры для всех видов нагрузки](#global_workload_options).
* `specific workload options` - [параметры конкретного вида нагрузки](#customer_history_options)

### Параметры для user-hist {#customer_history_options}
Имя параметра | Короткое имя | Описание параметра
---|---|---
`--limit <значение>` | `-l <значение>` | Необходимое количество заказов. Значение по умолчанию: 10.

## Нагрузка rand-user-hist {#getRandomCustomerHistory}

Данный вид нагрузки читает заданное количество заказов случайно выбранных покупателей.

YQL Запрос:
```sql
DECLARE $cust AS Utf8;
DECLARE $limit AS UInt32;

SELECT id, customer, created FROM orders view ix_cust
    WHERE customer = $cust
    ORDER BY customer DESC, created DESC
    LIMIT $limit;
```

Для запуска данного вида нагрузки необходимо выполнить команду:
```bash
{{ ydb-cli }} workload stock run rand-user-hist [global workload options...] [specific workload options...]
```

* `global workload options` - [общие параметры для всех видов нагрузки](#global_workload_options).
* `specific workload options` - [параметры конкретного вида нагрузки](#random_customer_history_options)

### Параметры для rand-user-hist {#random_customer_history_options}
Имя параметра | Короткое имя | Описание параметра
---|---|---
`--limit <значение>` | `-l <значение>` | Необходимое количество заказов. Значение по умолчанию: 10.

## Нагрузка add-rand-order {#insertRandomOrder}

Данный вид нагрузки создает случайно сгенерированный заказ. В заказ помещаются несколько различных товаров по 1 штуке. Количество видов товара в заказе генерируется случайно по экспоненциальному распределению.

YQL Запрос:
```sql
DECLARE $ido AS UInt64;
DECLARE $cust AS Utf8;
DECLARE $lines AS List<Struct<product:Utf8,quantity:Int64>>;
DECLARE $time AS DateTime;

INSERT INTO `orders`(id, customer, created) VALUES
    ($ido, $cust, $time);
UPSERT INTO `orderLines`(id_order, product, quantity)
    SELECT $ido, product, quantity FROM AS_TABLE( $lines );
```

Для запуска данного вида нагрузки необходимо выполнить команду:
```bash
{{ ydb-cli }} workload stock run add-rand-order [global workload options...] [specific workload options...]
```

* `global workload options` - [общие параметры для всех видов нагрузки](#global_workload_options).
* `specific workload options` - [параметры конкретного вида нагрузки](#insert_random_order_options)

### Параметры для add-rand-order {#insert_random_order_options}
Имя параметра | Короткое имя | Описание параметра
---|---|---
`--products <значение>` | `-p <значение>` | Количество видов товара в тесте. Значение по умолчанию: 100.

## Нагрузка put-rand-order {#submitRandomOrder}

Данный вид нагрузки создает случайно сгенерированный заказ и обрабатывает его. В заказ помещаются несколько различных товаров по 1 штуке. Количество видов товара в заказе генерируется случайно по экспоненциальному распределению. Обработка заказа заключается в уменьшении количества заказанных товаров на складе.

YQL Запрос:
```sql
DECLARE $ido AS UInt64;
DECLARE $cust AS Utf8;
DECLARE $lines AS List<Struct<product:Utf8,quantity:Int64>>;
DECLARE $time AS DateTime;

INSERT INTO `orders`(id, customer, created) VALUES
    ($ido, $cust, $time);

UPSERT INTO `orderLines`(id_order, product, quantity)
    SELECT $ido, product, quantity FROM AS_TABLE( $lines );

$prods = SELECT * FROM orderLines AS p WHERE p.id_order = $ido;

$cnt = SELECT COUNT(*) FROM $prods;

$newq =
    SELECT
        p.product AS product,
        COALESCE(s.quantity, 0) - p.quantity AS quantity
    FROM $prods AS p
    LEFT JOIN stock AS s
    ON s.product = p.product;

$check = SELECT COUNT(*) AS cntd FROM $newq as q WHERE q.quantity >= 0;

UPSERT INTO stock
    SELECT product, quantity FROM $newq WHERE $check=$cnt;

$upo = SELECT id, $time AS tm FROM orders WHERE id = $ido AND $check = $cnt;

UPSERT INTO orders SELECT id, tm AS processed FROM $upo;

SELECT * FROM $newq AS q WHERE q.quantity < 0
```

Для запуска данного вида нагрузки необходимо выполнить команду:
```bash
{{ ydb-cli }} workload stock run put-rand-order [global workload options...] [specific workload options...]
```

* `global workload options` - [общие параметры для всех видов нагрузки](#global_workload_options).
* `specific workload options` - [параметры конкретного вида нагрузки](#submit_random_order_options)

### Параметры для put-rand-order {#submit_random_order_options}
Имя параметра | Короткое имя | Описание параметра
---|---|---
`--products <значение>` | `-p <значение>` | Количество видов товара в тесте. Значение по умолчанию: 100.

## Нагрузка put-same-order {#submitSameOrder}

Данный вид нагрузки создает заказ с одним и тем же набором товаров и обрабатывает его. Обработка заказа заключается в уменьшении количества заказанных товаров на складе.

YQL Запрос:
```sql
DECLARE $ido AS UInt64;
DECLARE $cust AS Utf8;
DECLARE $lines AS List<Struct<product:Utf8,quantity:Int64>>;
DECLARE $time AS DateTime;

INSERT INTO `orders`(id, customer, created) VALUES
    ($ido, $cust, $time);

UPSERT INTO `orderLines`(id_order, product, quantity)
    SELECT $ido, product, quantity FROM AS_TABLE( $lines );

$prods = SELECT * FROM orderLines AS p WHERE p.id_order = $ido;

$cnt = SELECT COUNT(*) FROM $prods;

$newq =
    SELECT
        p.product AS product,
        COALESCE(s.quantity, 0) - p.quantity AS quantity
    FROM $prods AS p
    LEFT JOIN stock AS s
    ON s.product = p.product;

$check = SELECT COUNT(*) as cntd FROM $newq AS q WHERE q.quantity >= 0;

UPSERT INTO stock
    SELECT product, quantity FROM $newq WHERE $check=$cnt;

$upo = SELECT id, $time AS tm FROM orders WHERE id = $ido AND $check = $cnt;

UPSERT INTO orders SELECT id, tm AS processed FROM $upo;

SELECT * FROM $newq AS q WHERE q.quantity < 0
```

Для запуска данного вида нагрузки необходимо выполнить команду:
```bash
{{ ydb-cli }} workload stock run put-same-order [global workload options...] [specific workload options...]
```

* `global workload options` - [общие параметры для всех видов нагрузки](#global_workload_options).
* `specific workload options` - [параметры конкретного вида нагрузки](#submit_same_order_options)

### Параметры для put-same-order {#submit_same_order_options}
Имя параметра | Короткое имя | Описание параметра
---|---|---
`--products <значение>` | `-p <значение>` | Количество видов товара в каждом заказе. Значение по умолчанию: 100.

## Примеры запуска нагрузок

* Запуск `add-rand-order` нагрузки на 5 секунд в 10 потоков с 1000 видами товаров.
```bash
{{ ydb-cli }} workload stock run add-rand-order -s 5 -t 10 -p 1000
```
Возможный результат:
```text
Elapsed Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
1           132 0       0       69      108     132     157
2           157 0       0       63      88      97      104
3           156 0       0       62      84      104     120
4           160 0       0       62      77      90      94
5           174 0       0       61      77      97      100

Txs     Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
779       155.8 0       0       62      89      108     157
```

* Запуск `put-same-order` нагрузки на 5 секунд в 5 потоков с 2 видами товаров в заказе с распечаткой только итоговых результатов.
```bash
{{ ydb-cli }} workload stock run put-same-order -s 5 -t 5 -p 1000 --quiet
```
Возможный результат:
```text
Txs     Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
16          3.2 67      3       855     1407    1799    1799
```

* Запуск `rand-user-hist` нагрузки на 5 секунд в 100 потоков с распечаткой времени каждого временного окна.
```bash
{{ ydb-cli }} workload stock run rand-user-hist -s 5 -t 10 --print-timestamp
```
Возможный результат:
```text
Elapsed Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)        Timestamp
1          1046 0       0       7       16      25      50      2022-02-08T17:47:26Z
2          1070 0       0       7       17      22      28      2022-02-08T17:47:27Z
3          1041 0       0       7       17      22      28      2022-02-08T17:47:28Z
4          1045 0       0       7       17      23      31      2022-02-08T17:47:29Z
5           998 0       0       8       18      23      42      2022-02-08T17:47:30Z

Txs     Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
5200       1040 0       0       8       17      23      50
```

## Интерпретация результатов
* `Elapsed` - номер временного окна. По умолчанию временное окно равно 1 секунде.
* `Txs/sec` - количество успешных транзакций нагрузки во временном окне.
* `Retries` - количество повторных попыток исполнения транзакции клиентом во временном окне.
* `Errors` - количество ошибок, случившихся во временном окне.
* `p50(ms)` - 50-й перцентиль latency запросов в мс.
* `p95(ms)` - 95-й перцентиль latency запросов в мс.
* `p99(ms)` - 99-й перцентиль latency запросов в мс.
* `pMax(ms)` - 100-й перцентиль latency запросов в мс.
* `Timestamp` - временная метка конца временного окна.
