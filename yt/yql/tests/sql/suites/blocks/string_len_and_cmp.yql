USE plato;

SELECT
    key,

    value       == value_utf,
    value_utf   != value,
    value        < value_utf,
    value_utf   <= value_utf,
    value        > value_utf,
    value_utf   >= value_utf,

    value       == 'foo',
    'foo'       != value,
    value        < 'foo',
    'тест'      <= value_utf,
    value        > 'foo',
    'проверка'u >= value_utf,

    ''          == value_utf,
    ''          != value,
    ''           < value_utf,
    ''          <= value_utf,
    ''           > value_utf,
    ''          >= value_utf,

    value       == ''u,
    value_utf   != ''u,
    value        < ''u,
    value_utf   <= ''u,
    value        > ''u,
    value_utf   >= ''u,

    len(value),
    len(value_utf),
    len(''),
    len(''u),
    len('проверка'),
    len('тест'u),

FROM Input
ORDER BY key
