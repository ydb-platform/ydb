# {{ ydb-short-name }} Quick Start

## 1. Download the executable

{% note info %}

Prerequisites: you'll need a Linux machine with x86\_64 CPU to follow this guide. If you don't have access to one, [run {{ ydb-short-name }} with Docker](self_hosted/ydb_docker.md) and then return to step 3 below.

{% endnote %}

```
curl https://binaries.ydb.tech/local_scripts/install.sh | bash
```

# 2. Start the {{ ydb-short-name }} server

After the previous step, everything will be prepared in the current working directory to launch `ydbd`, the {{ ydb-short-name }} server-side daemon. The simplest way to do so is with this command:

```
./start.sh ram
```

With this command, {{ ydb-short-name }} will store data only transiently in RAM which is perfect for playing around. In production environments, {{ ydb-short-name }} normally works directly with multiple SSD/NVMe or HDD drives via block devices (avoiding any filesystem), setting this up is out of the scope of this guide - see [production storage configuration](../administration/production-storage-config.md) to learn more. There's also an intermediate option, launched with `./start.sh disk`, which emulates a block device with a file in a filesystem (80Gb by default), but this is also only for experimentation purposes and shouldn't be used for production workloads or even benchmarks.

# 3. Run your first "Hello, world!" query

The simplest way to launch your first {{ ydb-short-name }} query is via the built-in web interface. It is launched by default on port 8765 of the {{ ydb-short-name }} server, so assuming you have launched it locally, you need to open [localhost:8765](http://localhost:8765) in your web browser. If not, replace `localhost` with your server's hostname in this URL, or use `ssh -L 8765:localhost:8765 <my-hostname>` to setup port forwarding and still open [localhost:8765](http://localhost:8765). You'll see something like this:

![Web UI home page](_assets/web-ui-home.png)

{{ ydb-short-name }} is designed to be a multi-tenant system, with potentially thousands of users working with the same cluster simultaneously. Hence, most logical entities inside a {{ ydb-short-name }} cluster resides in a flexible hierarchical structure more akin to Unix's virtual filesystem rather than a fixed-depth schema you might be familiar with from other database management systems. As you can see, the first level of hierarchy consists of databases that are running inside a single {{ ydb-short-name }} process, that might belong to different tenants. `/Root` is for system purposes, while `/Root/test` is a playground that `start.sh` created for us in the previous step. Let's click on the latter, `/Root/test`, then enter our first query and hit the run button:

```sql
SELECT "Hello, world!"u;
```

The query returns the salute back, as it is supposed to:

![SELECT "Hello, world!"u;](_assets/select-hello-world.png)

{% note info %}

Have you noticed an odd `u` suffix? {{ ydb-short-name }} and its query language YQL are strongly typed. Regular strings in {{ ydb-short-name }} can contain any binary data, while this suffix indicates that this string literal is of `Utf8` data type, which can contain only valid [UTF-8](https://en.wikipedia.org/wiki/UTF-8) sequences. [Learn more](../yql/reference/types/index.md) about {{ ydb-short-name }}'s type system.

{% endnote %}

The second simplest way to run a SQL query with {{ ydb-short-name }} is the [command line interface (CLI)](../reference/ydb-cli/index.md), while most real-world applications will likely talk with {{ ydb-short-name }} via one of the available [software development kits (SDK)](../reference/ydb-sdk/index.md). Feel free to follow the rest of the guide with one of those methods instead of the web UI if you feel comfortable doing so.

# 4. Create your first table

The main purpose why database management systems exist is to store some data for later retrieval. As an SQL-based system, {{ ydb-short-name }}'s main abstraction for data storage is a table. To create our first one, run the following query:

```sql
CREATE TABLE example
(
    key UInt64,
    value String,
    PRIMARY KEY (key)
);
```

As you can see, it is a simple key-value table. Let's walk through it step-by-step:

* `CREATE TABLE` indicates the kind of SQL statement we are running. In the previous query `SELECT` had the same purpose. The syntax for each kind of statement is different, see [YQL reference](../yql/reference/index.md) for more detailed guidance.
* `example` is the table name identifier, while `key` and `value` are column name identifiers. It is recommended to use simple names for identifiers like this, but if you need one that contains non-trivial symbols - wrap the name in backticks.
* `UInt64` and `String` are type names. `String` represents a binary string, and `UInt64` is a 64-bit unsigned integer. Thus our example table stores string values identified by unsigned integer keys.
* `PRIMARY KEY` is one of the fundamental concepts of SQL that has a huge impact on both application logic and performance. Following the SQL standard, the primary key also implies a unique constraint, thus the table can't have multiple rows with equal primary keys. In this example table, it's quite straightforward what column should the user choose as the primary key, which we specify as `(key)` in round brackets after the respective keyword. In real-world scenarios, tables often have dozens of columns, and primary keys can be compound (consisting of multiple columns in specified order), thus choosing the right primary key becomes more like an art. If you are interested in this topic, there's a [guide on choosing the primary key for maximizing performance](../best_practices/pk_scalability.md).


# 5. Add sample data

Now let's fill our table with some data. The simplest way is to just use literals:

```sql
INSERT INTO example (key, value)
VALUES (123, "hello"),
       (321, "world");
```

Step-by-step walkthrough:

* `INSERT INTO` is the classic SQL statement kind for adding new rows to a table. It is not the most performant though, as according to the SQL standard it has to check whether the table already had rows with given primary key values and raise an error if they were. Thus, if you run this query multiple times, all attempts except the first will return an error. If your application logic doesn't require this behavior, it is better to use `UPSERT INTO` instead of `INSERT INTO`. Upsert (which stands for "update or insert") will blindly write the provided values, overwriting existing rows if there were any. The rest of the syntax will be the same.
* `(key, value)` specifies the names of the columns we're inserting to and their order. The values provided next need to match this specification, both in the number of columns and their data types.
* After `VALUES` keyword there's a list of tuples, each representing a table row. In this example, we got two rows identified by 123 and 321 in the `key` column, and "hello" and "world" values in the `value` column, respectively.

To double-check that the rows indeed were added to the table, there's a common query that should return `2`:

```sql
SELECT COUNT(*) FROM example;
```

A few notable details in this one:

* `FROM` clause specifies a table to retrieve data from.
* `COUNT` is an aggregate function, counting the number of values. By default, when there are no other special clauses around, the presense of any aggregate function collapses the result to one row containing aggregates over the whole input data (the `example` table in this case).
* Asterisk `*` is a placeholder that normally means "all columns", thus `COUNT` will return the overall row count.

Another important way to fill a table with data is by combining `INSERT INTO` (or `UPSERT INTO`) and `SELECT`. In this case, values to be stored are calculated inside the database instead of being provided by the client as literals. We'll use a bit more realistic query to do this:

```sql
$subquery = SELECT ListFromRange(1000, 10000) AS keys;

UPSERT INTO example
SELECT
    key,
    CAST(RandomUuid(key) AS String) AS value
FROM $subquery FLATTEN LIST BY keys AS key
```

There's quite a lot going on in this query, let's dig into it:

* `$subquery` is a named expression. This syntax is YQL's extension to the SQL standard that allows to make complex queries more readable. It behaves the same as if you wrote that first `SELECT` inline where `$subquery` is later used on the last row, but allows to comprehend what's going on piece by piece like the variables do in regular programming languages.
* `ListFromRange` is a function that produces a list of consequent integers, starting from the value provided in the first argument and ending with the value provided in the second argument. There's also a third optional argument that can allow skipping integers with specified step, but we omit it in our example which defaults to returning all integers in the given range. `List` is one of the most common [container data types](../yql/reference/types/containers.md).
* `AS` is a keyword that is used to give a name to the value we're returning from `SELECT`, in this example `keys`.
* `FROM ... FLATTEN LIST BY ... AS ...` has a few notable things happening:
  * Another `SELECT` used in the `FROM` clause is called a subquery. That's why we chose this name for our `$subquery` named expression, but we could've chosen something more meaningful to explain what it is. Subqueries normally aren't materialized, they are just passing the output of one `SELECT` to the input of another. It can be used as means to produce arbitrarily complex execution graphs, especially if used in conjunction with other YQL features.
  * `FLATTEN LIST BY` clause modifies input passed via `FROM` the following way: for each row in the input data, it takes a column of list data type and produces multiple rows by the number of elements in that list. Normally, that list column is replaced by the column with the current single element, but `AS` keyword in this context allows access to both the whole list (under the original name) and the current element (under the name specified after `AS`), or just to make it more clear what is what like in this example.
* `RandomUuid` is a function that returns a pseudorandom [UUID version 4](https://datatracker.ietf.org/doc/html/rfc4122#section-4.4). Unlike most other functions, it doesn't actually use what is passed as an argument (the `key` column), instead, it indicates that we need to call the function on each row. See [reference](../yql/reference/builtins/basic.md#random) for more examples of how this works.
* `CAST(... AS ...)` is a common function for converting values to a specified data type. In this context the type specification is expected after `AS` (in this case `String`), not an arbitrary name.
* `UPSERT INTO` will blindly write the values to the specified tables, as we discussed previously. Note, that it didn't require `(key, value)` column names specification when used in conjunction with `SELECT`, as now columns can just be matched by names returned from `SELECT`. 

{% note info "Quick question!" %}

What the `SELECT COUNT(*) FROM example;` query will return now?

{% endnote %}

# 6. Done! What's next?

After getting hold of some basics demonstrated in this guide, you should be ready to jump into more advanced topics. Choose what looks the most relevant depending on your use case and role:

* Walk through a more detailed [YQL tutorial](../yql/reference/) that focuses on writing queries.
* Try to build your first app storing data in {{ ydb-short-name }} using [one of SDKs](../reference/ydb-sdk/index.md).
* Learn how to set up a [production-ready deployment of {{ ydb-short-name }}](../cluster/index.md).