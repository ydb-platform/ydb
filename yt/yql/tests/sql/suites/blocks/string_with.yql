pragma AnsiLike;
pragma warning("disable", "4510");

USE plato;

SELECT
    key,

    YQL::StringContains(value, value_utf),
    YQL::StringContains(value, 'o'),
    value like 'o',
    YQL::StringContains('foobar'u, value),

    StartsWith(value, value_utf),
    EndsWith(value, 'ar'u),
    StartsWith('тестпроверка'u, value_utf),
    EndsWith('тестпроверка'u, value_utf),

    value like 'ba%ar',
    value_utf like 'про%ерка',
FROM Input
ORDER BY key
