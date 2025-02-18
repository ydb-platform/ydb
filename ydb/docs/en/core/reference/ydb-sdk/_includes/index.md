# {{ ydb-short-name }} SDK reference

{% include [index_intro_overlay.md](index_intro_overlay.md) %}

OpenSource SDKs in the following programming languages are available to work with {{ ydb-short-name }}:

| Language | GitHub repository | API reference |
|----------|------------|----------------|
{% if oss %}
| C++ | [ydb-platform/ydb/tree/main/ydb/public/sdk/cpp](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp) | N/A |
{% endif %}
| С# (.NET) | [ydb-platform/ydb-dotnet-sdk](https://github.com/ydb-platform/ydb-dotnet-sdk) | N/A |
| Go | [ydb-platform/ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk) | [https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3) |
| Java | [ydb-platform/ydb-java-sdk](https://github.com/ydb-platform/ydb-java-sdk) | N/A |
| Node.js | [ydb-platform/ydb-nodejs-sdk](https://github.com/ydb-platform/ydb-nodejs-sdk) | N/A |
| PHP | [ydb-platform/ydb-php-sdk](https://github.com/ydb-platform/ydb-php-sdk) | N/A |
| Python | [ydb-platform/ydb-python-sdk](https://github.com/ydb-platform/ydb-python-sdk) | [https://ydb-platform.github.io/ydb-python-sdk](https://ydb-platform.github.io/ydb-python-sdk/) |
| Rust | [ydb-platform/ydb-rs-sdk](https://github.com/ydb-platform/ydb-rs-sdk) | N/A |

The SDK documentation contains the following sections:

- [Installation](../install.md)
- [Authentication](../auth.md)
- [{#T}](../error_handling.md)
- [Code recipes](../../../recipes/ydb-sdk/index.md)
- [Comparison of SDK features](../feature-parity.md)

See also:

- [Documentation for Application Developers](../../../dev/index.md)
- [Example applications](../../../dev/example-app/index.md)
