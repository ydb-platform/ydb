# Тестовое приложение

В данном разделе разбирается код однотипного тестового приложения, реализованного с использованием YDB SDK на разных языках программирования:

{% if oss %}
- C++ [https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/basic_example](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/basic_example)
{% endif %}
- С# (.NET) [https://github.com/ydb-platform/ydb-dotnet-examples](https://github.com/ydb-platform/ydb-dotnet-examples)
- Go [https://github.com/ydb-platform/ydb-go-examples/tree/master/basic](https://github.com/ydb-platform/ydb-go-examples/tree/master/basic)
- Java [https://github.com/yandex-cloud/ydb-java-sdk/tree/master/examples/maven_project](https://github.com/yandex-cloud/ydb-java-sdk/tree/master/examples/maven_project)
- Node.js [https://github.com/ydb-platform/ydb-nodejs-sdk/tree/master/examples/basic-example-v1](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/master/examples/basic-example-v1)
- Python [https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/basic_example_v1](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/basic_example_v1)

Инструкции по скачиванию и запуску тестового приложения находятся в файле `readme.md` по приведенным выше ссылкам.

Тестовое приложение выполняет следующие шаги:

{% include [init.md](steps/01_init.md) %}

{% if oss %}[C++](../example-cpp.md#init) | {% endif %} [C# (.NET)](../example-dotnet.md#init) | [Go](../go/index.md#init) | [Java](../example-java.md#init) | Node.js | [PHP](../example-php.md#init) | [Python](../python/index.md#init)

{% include [create_table.md](steps/02_create_table.md) %}

{% if oss %}[C++](../example-cpp.md#create-table) | {% endif %} [C# (.NET)](../example-dotnet.md#create-table) | [Go](../go/index.md#create-table) | [Java](../example-java.md#create-table) | Node.js | PHP | [Python](../python/index.md#create-table)

{% include [write_queries.md](steps/03_write_queries.md) %}

{% if oss %}[C++](../example-cpp.md#write-queries) | {% endif %} [C# (.NET)](../example-dotnet.md#write-queries) | Go | [Java](../example-java.md#write-queries) | Node.js | PHP | [Python](../python/index.md#write-queries)

{% include [query_processing.md](steps/04_query_processing.md) %}

{% if oss %}[C++](../example-cpp.md#query-processing) |  {% endif %} [C# (.NET)](../example-dotnet.md#query-processing) | [Go](../go/index.md#query-processing) | [Java](../example-java.md#query-processing) | Node.js | PHP | [Python](../python/index.md#query-processing)

{% include [param_queries.md](steps/06_param_queries.md) %}

{% if oss %}[C++](../example-cpp.md#param-queries) |  {% endif %} [C# (.NET)](../example-dotnet.md#param-queries) | [Go](../go/index.md#param-queries) | [Java](../example-java.md#param-queries) | Node.js | PHP | [Python](../python/index.md#param-queries)

{% include [scan_query.md](steps/08_scan_query.md) %}

{% if oss %}C++ |  {% endif %} [C# (.NET)](../example-dotnet.md#scan-query) | [Go](../go/index.md#scan-query) | [Java](../example-java.md#scan-query) | [Node.js](../example-nodejs.md#scan-query) | PHP | [Python](../python/index.md#scan-query)

{% include [multistep_transactions.md](steps/09_multistep_transactions.md) %}

{% if oss %}[C++](../example-cpp.md#multistep-transactions) |  {% endif %} C# (.NET) | Go | [Java](../example-java.md#multistep-transactions) | Node.js | PHP | Python

{% include [transaction_control.md](steps/10_transaction_control.md) %}

{% if oss %}[C++](../example-cpp.md#tcl) |  {% endif %} C# (.NET) | Go | [Java](../example-java.md#tcl) | Node.js | PHP | [Python](../python/index.md#tcl)

{% include [error_handling.md](steps/50_error_handling.md) %}

