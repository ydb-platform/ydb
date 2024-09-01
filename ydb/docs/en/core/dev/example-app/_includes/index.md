# Example applications working with {{ ydb-short-name }}

This section describes the code of same-type test apps implemented using {{ ydb-short-name }} SDKs in different programming languages:

{% if oss %}

- [C++](../example-cpp.md)
{% endif %}
- [C# (.NET)](../example-dotnet.md)
- [Go](../go/index.md)
- [Java](../java/index.md)
- [Node.js](../example-nodejs.md)
- [Python](../python/index.md)

{% node info %}

Additional information on how these example applications work is available in [{{ ydb-short-name }} SDK reference documentation](../../../reference/ydb-sdk/index.md).

{% endnote %}

A test app performs the following steps:

{% include [init.md](steps/01_init.md) %}

{% if oss %}[C++](../example-cpp.md#init) | {% endif %} [C# (.NET)](../example-dotnet.md#init) | [Go](../go/index.md#init) | [Java](../java/index.md#init) | Node.js | [PHP](../example-php.md#init) | [Python](../python/index.md#init)

{% include [create_table.md](steps/02_create_table.md) %}

{% if oss %}[C++](../example-cpp.md#create-table) | {% endif %} [C# (.NET)](../example-dotnet.md#create-table) | [Go](../go/index.md#create-table) | [Java](../java/index.md#create-table) | Node.js | [PHP](../example-php.md#create-table) | [Python](../python/index.md#create-table)

{% include [write_queries.md](steps/03_write_queries.md) %}

{% if oss %}[C++](../example-cpp.md#write-queries) | {% endif %} [C# (.NET)](../example-dotnet.md#write-queries) | Go | [Java](../java/index.md#write-queries) | Node.js | [PHP](../example-php.md#write-queries) | [Python](../python/index.md#write-queries)

{% include [query_processing.md](steps/04_query_processing.md) %}

{% if oss %}[C++](../example-cpp.md#query-processing) | {% endif %} [C# (.NET)](../example-dotnet.md#query-processing) | [Go](../go/index.md#query-processing) | [Java](../java/index.md#query-processing) | Node.js | P[PHP](../example-php.md#query-processing) | [Python](../python/index.md#query-processing)

{% include [param_queries.md](steps/06_param_queries.md) %}

{% if oss %}[C++](../example-cpp.md#param-queries) | {% endif %} [C# (.NET)](../example-dotnet.md#param-queries) | [Go](../go/index.md#param-queries) | [Java](../java/index.md#param-queries) | Node.js | [PHP](../example-php.md#param-queries) | [Python](../python/index.md#param-queries)

{% include [multistep_transactions.md](steps/09_multistep_transactions.md) %}

{% if oss %}[C++](../example-cpp.md#multistep-transactions) | {% endif %} C# (.NET) | Go | [Java](../java/index.md#multistep-transactions) | Node.js | PHP | Python

{% include [transaction_control.md](steps/10_transaction_control.md) %}

{% if oss %}[C++](../example-cpp.md#tcl) | {% endif %} C# (.NET) | Go | [Java](../java/index.md#tcl) | Node.js | PHP | [Python](../python/index.md#tcl)

{% include [error_handling.md](steps/50_error_handling.md) %}

