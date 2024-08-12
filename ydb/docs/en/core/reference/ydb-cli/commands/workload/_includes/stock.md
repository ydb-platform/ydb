# Stock load

Simulates a warehouse of an online store: creates multi-product orders, gets a list of orders per customer.

## Types of load {#workload_types}

This load test runs 5 types of load:
* [user-hist](#getCustomerHistory): Reads the specified number of orders made by the customer with id = 10000. This creates a workload to read the same rows from different threads.
* [rand-user-hist](#getRandomCustomerHistory): Reads the specified number of orders made by a randomly selected customer. A load that reads data from different threads is created.
* [add-rand-order](#insertRandomOrder): Generates an order at random. For example, a customer has created an order of 2 products, but hasn't yet paid for it, hence the quantities in stock aren't decreased for the products. The database writes the data about the order and products. The read/write load is created (the INSERT checks for an existing entry before inserting the data).
* [put-rand-order](#submitRandomOrder): Generates an order at random and processes it. For example, a customer has created and paid an order of 2 products. The data about the order and products is written to the database, product availability is checked and quantities in stock are decreased. A mixed data load is created.
* [put-same-order](#submitSameOrder): Creates orders with the same set of products. For example, all customers buy the same set of products (a newly released phone and a charger). This creates a workload of competing updates of the same rows in the table.

## Load test initialization {#init}

To get started, create tables and populate them with data:
```bash
{{ ydb-cli }} workload stock init [init options...]
```

* `init options`: [Initialization options](#init_options).

See the description of the command to init the data load:

```bash
{{ ydb-cli }} workload init --help
```

### Available parameters {#init_options}

| Parameter name | Short name | Parameter description |
---|---|---
| `--products <value>` | `-p <value>` | Number of products. Valid values: between 1 and 500000. Default: 100. |
| `--quantity <value>` | `-q <value>` | Quantity of each product in stock. Default: 1000. |
| `--orders <value>` | `-o <value>` | Initial number of orders in the database. Default: 100. |
| `--min-partitions <value>` | - | Minimum number of shards for tables. Default: 40. |
| `--auto-partition <value>` | - | Enabling/disabling auto-sharding. Possible values: 0 or 1. Default: 1. |

3 tables are created using the following DDL statements:
```sql
CREATE TABLE `stock`(product Utf8, quantity Int64, PRIMARY KEY(product)) WITH (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = <min-partitions>);
CREATE TABLE `orders`(id Uint64, customer Utf8, created Datetime, processed Datetime, PRIMARY KEY(id), INDEX ix_cust GLOBAL ON (customer, created)) WITH (READ_REPLICAS_SETTINGS = "per_az:1", AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = <min-partitions>, UNIFORM_PARTITIONS = <min-partitions>, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000);
CREATE TABLE `orderLines`(id_order Uint64, product Utf8, quantity Int64, PRIMARY KEY(id_order, product)) WITH (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = <min-partitions>, UNIFORM_PARTITIONS = <min-partitions>, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000);
```

### Examples of load initialization {#init-stock-examples}

Creating a database with 1000 products, 10000 items of each product, and no orders:

```bash
{{ ydb-cli }} workload stock init -p 1000 -q 10000 -o 0
```

Creating a database with 10 products, 100 items of each product, 10 orders, and a minimum number of shards equal 100:
```bash
{{ ydb-cli }} workload stock init -p 10 -q 100 -o 10 --min-partitions 100
```

## Running a load test {#run}

To run the load, execute the command:
```bash
{{ ydb-cli }} workload stock run [workload type...] [global workload options...] [specific workload options...]
```
During this test, workload statistics for each time window are displayed on the screen.

* `workload type`: The [types of workload](#workload_types).
* `global workload options`: The [global options for all types of load](#global_workload_options).
* `specific workload options`: Options of a specific load type.

See the description of the command to run the data load:

```bash
{{ ydb-cli }} workload run --help
```

### Global parameters for all types of load {#global_workload_options}

| Parameter name | Short name | Parameter description |
---|---|---
| `--seconds <value>` | `-s <value>` | Duration of the test, in seconds. Default: 10. |
| `--threads <value>` | `-t <value>` | The number of parallel threads creating the load. Default: 10. |
| `--rate <value>` | - | Total rate for all threads, in transactions per second. Default: 0 (no rate limit). |
| `--quiet` | - | Outputs only the total result. |
| `--print-timestamp` | - | Print the time together with the statistics of each time window. |
| `--client-timeout` | - | [Transport timeout in milliseconds](../../../../../dev/timeouts.md). |
| `--operation-timeout` | - | [Operation timeout in milliseconds](../../../../../dev/timeouts.md). |
| `--cancel-after` | - | [Timeout for canceling an operation in milliseconds](../../../../../dev/timeouts.md). |
| `--window` | - | Statistics collection window in seconds. Default: 1. |


## The user-hist workload {#getCustomerHistory}

This type of load reads the specified number of orders for the customer with id = 10000.

YQL query:
```sql
DECLARE $cust AS Utf8;
DECLARE $limit AS UInt32;

SELECT id, customer, created FROM orders view ix_cust
    WHERE customer = 'Name10000'
    ORDER BY customer DESC, created DESC
    LIMIT $limit;
```

To run this type of load, execute the command:
```bash
{{ ydb-cli }} workload stock run user-hist [global workload options...] [specific workload options...]
```

* `global workload options`: The [global options for all types of load](#global_workload_options).
* `specific workload options`: [Options of a specific load type](#customer_history_options).

### Parameters for user-hist {#customer_history_options}
| Parameter name | Short name | Parameter description |
---|---|---
| `--limit <value>` | `-l <value>` | The required number of orders. Default: 10. |

## The rand-user-hist workload {#getRandomCustomerHistory}

This type of load reads the specified number of orders from randomly selected customers.

YQL query:
```sql
DECLARE $cust AS Utf8;
DECLARE $limit AS UInt32;

SELECT id, customer, created FROM orders view ix_cust
    WHERE customer = $cust
    ORDER BY customer DESC, created DESC
    LIMIT $limit;
```

To run this type of load, execute the command:
```bash
{{ ydb-cli }} workload stock run rand-user-hist [global workload options...] [specific workload options...]
```

* `global workload options`: The [global options for all types of load](#global_workload_options).
* `specific workload options`: [Options of a specific load type](#random_customer_history_options).

### Parameters for rand-user-hist {#random_customer_history_options}
| Parameter name | Short name | Parameter description |
---|---|---
| `--limit <value>` | `-l <value>` | The required number of orders. Default: 10. |

## The add-rand-order workload {#insertRandomOrder}

This type of load creates a randomly generated order. The order includes several different products, 1 item per product. The number of products in the order is generated randomly based on an exponential distribution.

YQL query:
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

To run this type of load, execute the command:
```bash
{{ ydb-cli }} workload stock run add-rand-order [global workload options...] [specific workload options...]
```

* `global workload options`: The [global options for all types of load](#global_workload_options).
* `specific workload options`: [Options of a specific load type](#insert_random_order_options).

### Parameters for add-rand-order {#insert_random_order_options}
| Parameter name | Short name | Parameter description |
---|---|---
| `--products <value>` | `-p <value>` | Number of products in the test. Default: 100. |

## The put-rand-order workload {#submitRandomOrder}

This type of load creates a randomly generated order and processes it. The order includes several different products, 1 item per product. The number of products in the order is generated randomly based on an exponential distribution. Order processing consists in decreasing the number of ordered products in stock.

YQL query:
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

To run this type of load, execute the command:
```bash
{{ ydb-cli }} workload stock run put-rand-order [global workload options...] [specific workload options...]
```

* `global workload options`: The [global options for all types of load](#global_workload_options).
* `specific workload options`: [Options of a specific load type](#submit_random_order_options).

### Parameters for put-rand-order {#submit_random_order_options}
| Parameter name | Short name | Parameter description |
---|---|---
| `--products <value>` | `-p <value>` | Number of products in the test. Default: 100. |

## The put-same-order workload {#submitSameOrder}

This type of load creates an order with the same set of products and processes it. Order processing consists in decreasing the number of ordered products in stock.

YQL query:
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

To run this type of load, execute the command:
```bash
{{ ydb-cli }} workload stock run put-same-order [global workload options...] [specific workload options...]
```

* `global workload options`: The [global options for all types of load](#global_workload_options).
* `specific workload options`: [Options of a specific load type](#submit_same_order_options).

### Parameters for put-same-order {#submit_same_order_options}
| Parameter name | Short name | Parameter description |
---|---|---
| `--products <value>` | `-p <value>` | Number of products per order. Default: 100. |

## Examples of running the loads

* Run the `add-rand-order` workload for 5 seconds across 10 threads with 1000 products.
```bash
{{ ydb-cli }} workload stock run add-rand-order -s 5 -t 10 -p 1000
```
Possible result:
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

* Run the `put-same-order` workload for 5 seconds across 5 threads with 2 products per order, printing out only final results.
```bash
{{ ydb-cli }} workload stock run put-same-order -s 5 -t 5 -p 1000 --quiet
```
Possible result:
```text
Txs     Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
16          3.2 67      3       855     1407    1799    1799
```

* Run the `rand-user-hist` workload for 5 seconds across 100 threads, printing out time for each time window.
```bash
{{ ydb-cli }} workload stock run rand-user-hist -s 5 -t 10 --print-timestamp
```
Possible result:
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

## Interpretation of results
* `Elapsed`: Time window ID. By default, a time window is 1 second.
* `Txs/sec`: Number of successful load transactions in the time window.
* `Retries`: The number of repeat attempts to execute the transaction by the client in the time window.
* `Errors`: The number of errors that occurred in the time window.
* `p50(ms)`: 50th percentile of request latency, in ms.
* `p95(ms)`: 95th percentile of request latency, in ms.
* `p99(ms)`: 99th percentile of request latency, in ms.
* `pMax(ms)`: 100th percentile of request latency, in ms.
* `Timestamp`: Timestamp of the end of the time window.
