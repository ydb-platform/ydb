# Справка по {{ ydb-short-name }} SDK

{% include [index_intro_overlay.md](_includes/index_intro_overlay.md) %}

Для работы с {{ ydb-short-name }} доступны OpenSource SDK для следующих языков программирования:

| Язык | Репозиторий на GitHub | Справка по API |
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

Документация по SDK содержит следующие разделы:

- [Установка](install.md)
- [Аутентификация](auth.md)
- [{#T}](error_handling.md)
- [Сравнение возможностей SDK](feature-parity.md)
- [Параметризованные запросы](parameterized_queries.md)
- [Работа с топиками](topic.md)
- [Работа с узлами координации](coordination.md)

Смотрите также:

- [Документация для разработчиков приложений](../../dev/index.md)
- [Тестовые приложения](../../dev/example-app/index.md)
- [Рецепты кода](../../recipes/ydb-sdk/index.md)
