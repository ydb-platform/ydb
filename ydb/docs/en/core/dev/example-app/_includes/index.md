# Test application

This section reviews the code of a sample test application implemented using the {{ ydb-short-name }} SDK in different programming languages:

{% if oss %}

- [C++](../example-cpp.md)

{% endif %}

- [C#](../example-dotnet.md)
- [Go](../go/index.md)
- [Java](../java/index.md)
- [JavaScript](../example-js.md)
- [Python](../python/index.md)
- [Rust](../rust/index.md)

{% note info %}

For more information on how these sample applications work, see the [{{ ydb-short-name }} SDK reference documentation](../../../reference/ydb-sdk/index.md).

{% endnote %}

The test application performs the following steps:

{% include [init.md](steps/01_init.md) %}

{% if oss %} [C++](../example-cpp.md#init) | {% endif %} [C#](../example-dotnet.md#init) | [Go](../go/index.md#init) | [Java](../java/index.md#init) | JavaScript | [PHP](../example-php.md#init) | [Python](../python/index.md#init) | [Rust](../rust/index.md#download)

{% include [create_table.md](steps/02_create_table.md) %}

{% if oss %} [C++](../example-cpp.md#create-table) | {% endif %} [C#](../example-dotnet.md#create-table) | [Go](../go/index.md#create-table) | [Java](../java/index.md#create-table) | JavaScript | [PHP](../example-php.md#create-table) | [Python](../python/index.md#create-table) | [Rust](../rust/index.md#query-client)

{% include [write_queries.md](steps/03_write_queries.md) %}

{% if oss %} [C++](../example-cpp.md#write-queries) | {% endif %} [C#](../example-dotnet.md#write-queries) | Go | [Java](../java/index.md#write-queries) | JavaScript | [PHP](../example-php.md#write-queries) | [Python](../python/index.md#write-queries) | [Rust](../rust/index.md#query-client)

{% include [query_processing.md](steps/04_query_processing.md) %}

{% if oss %} [C++](../example-cpp.md#query-processing) | {% endif %} [C#](../example-dotnet.md#query-processing) | [Go](../go/index.md#query-processing) | [Java](../java/index.md#query-processing) | JavaScript | [PHP](../example-php.md#query-processing) | [Python](../python/index.md#query-processing) | [Rust](../rust/index.md#query-client)

{% include [param_queries.md](steps/06_param_queries.md) %}

{% if oss %} [C++](../example-cpp.md#param-queries) | {% endif %} [C#](../example-dotnet.md#param-queries) | [Go](../go/index.md#param-queries) | [Java](../java/index.md#param-queries) | JavaScript | [PHP](../example-php.md#param-queries) | [Python](../python/index.md#param-queries) | [Rust](../rust/index.md#query-client)

{% include [multistep_transactions.md](steps/09_multistep_transactions.md) %}

{% if oss %} [C++](../example-cpp.md#multistep-transactions) |  {% endif %} C# | Go | [Java](../java/index.md#multistep-transactions) | JavaScript | PHP | Python

{% include [transaction_control.md](steps/10_transaction_control.md) %}

{% if oss %} [C++](../example-cpp.md#tcl) |  {% endif %} C# | Go | [Java](../java/index.md#tcl) | JavaScript | PHP | [Python](../python/index.md#tcl) | [Rust](../rust/index.md#query-client)

{% include [error_handling.md](steps/50_error_handling.md) %}
