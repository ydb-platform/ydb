PRAGMA AnsiLike;
PRAGMA warning("disable", "4510");
USE plato;

SELECT
    key,
    YQL::StringContains(value, value_utf),
    YQL::StringContains(value, 'o'),
    value LIKE 'o',
    YQL::StringContains('foobar'u, value),
    StartsWith(value, value_utf),
    EndsWith(value, 'ar'u),
    StartsWith('тестпроверка'u, value_utf),
    EndsWith('тестпроверка'u, value_utf),
    value LIKE 'ba%ar',
    value_utf LIKE 'про%ерка',
FROM Input
ORDER BY
    key;
