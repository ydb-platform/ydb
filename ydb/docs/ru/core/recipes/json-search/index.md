# Рецепты поиска по JSON-документам

Этот раздел содержит готовые рецепты по использованию [JSON-индексов](../../dev/json-indexes.md) для эффективного отбора данных по содержимому колонок типа `Json` и `JsonDocument`.

Рецепты построены вокруг функций [JSON_EXISTS](../../yql/reference/builtins/json.md) и [JSON_VALUE](../../yql/reference/builtins/json.md) с выражениями [JsonPath](../../yql/reference/builtins/json.md#jsonpath). Для гарантированного использования индекса все примеры используют явное обращение через `VIEW IndexName`.

* [{#T}](json-index-quickstart.md) — минимальный сценарий: создание таблицы, индекса, вставка данных и базовые запросы.
* [{#T}](json-index-catalog.md) — пример каталога товаров с вложенными атрибутами, диапазонными условиями и поиском по вложенным массивам.
* [{#T}](json-index-parameters.md) — параметризованные запросы и передача переменных в JsonPath через секцию `PASSING`.
* [{#T}](json-index-typecheck.md) — проверка типа поля и наличия пути с помощью методов JsonPath.

Перед запуском примеров убедитесь, что на кластере [включена поддержка JSON-индексов](../../reference/configuration/feature_flags.md).
