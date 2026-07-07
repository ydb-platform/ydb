# Reference for {{ ydb-short-name }} SDK

{% include [index_intro_overlay.md](_includes/index_intro_overlay.md) %}

To work with {{ ydb-short-name }}, OpenSource SDKs are available for the following programming languages:

| Language | GitHub Repository | API Reference |
| --- | --- | --- |

| Go | [ydb-platform/ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk) | [https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3) |
| Java | [ydb-platform/ydb-java-sdk](https://github.com/ydb-platform/ydb-java-sdk) | N/A |
| Python | [ydb-platform/ydb-python-sdk](https://github.com/ydb-platform/ydb-python-sdk) | [https://ydb-platform.github.io/ydb-python-sdk](https://ydb-platform.github.io/ydb-python-sdk/) |
| C# | [ydb-platform/ydb-dotnet-sdk](https://github.com/ydb-platform/ydb-dotnet-sdk) | N/A |
| JavaScript | [ydb-platform/ydb-js-sdk](https://github.com/ydb-platform/ydb-js-sdk) | [https://ydb.js.org](https://ydb.js.org) |
| Rust | [ydb-platform/ydb-rs-sdk](https://github.com/ydb-platform/ydb-rs-sdk) | N/A |

{% if oss %}

| C++ | [ydb-platform/ydb-cpp-sdk](https://github.com/ydb-platform/ydb-cpp-sdk) | N/A |

{% endif %}

| PHP | [ydb-platform/ydb-php-sdk](https://github.com/ydb-platform/ydb-php-sdk) | N/A |

The SDK documentation contains the following sections:

- [Installation](install.md)
- [Authentication](auth.md)
- [{#T}](error_handling.md)
- [SDK feature comparison](feature-parity.md)
- [Parameterized queries](parameterized_queries.md)
- [Data formats](data-formats/index.md)
- [Working with topics](topic.md)
- [Working with coordination nodes](coordination.md)
- [{#T}](observability/index.md)

See also:

- [Documentation for application developers](../../dev/index.md)
- [Test applications](../../dev/example-app/index.md)
- [Code recipes](../../recipes/ydb-sdk/index.md)
