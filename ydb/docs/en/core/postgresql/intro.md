# {{ ydb-short-name }} compatibility with PostgreSQL

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

PostgreSQL compatibility is a mechanism for executing SQL queries in the PostgreSQL dialect on {{ ydb-short-name }} infrastructure using standard {{ ydb-short-name }} tools and APIs. This feature allows developers to write queries using the PostgreSQL syntax while benefiting from YDB's advantages such as horizontal scalability and fault tolerance.

{{ ydb-short-name }}'s compatibility with PostgreSQL simplifies the migration of applications that were previously operating within the PostgreSQL ecosystem. This feature allows for a smoother transition of database-driven applications to {{ ydb-short-name }}. At present, a limited set of PostgreSQL 14 instructions and functions are supported. PostgreSQL compatibility enables switching from PostgreSQL to {{ ydb-short-name }} without modifying the project code (provided that the SQL constructs used in the project are supported by {{ ydb-short-name }}), by changing how queries are submitted to {{ ydb-short-name }}.

The operation of PostgreSQL compatibility can be described as follows:

1. The application sends PostgreSQL-dialect queries to {{ ydb-short-name }} through standard {{ ydb-short-name }} tools.
2. The query processor translates the PostgreSQL queries into YQL AST.
3. After the queries are processed, the results are compiled and sent back to the application. During query processing, it can be parallelized and executed on any number of {{ ydb-short-name }} nodes.

The functionality of PostgreSQL compatibility can be graphically represented as follows:
![Diagram of the PostgreSQL compatibility functionality](./_includes/ydb_pg_scheme_eng.png)
