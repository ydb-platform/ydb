/* syntax version 1 */
/* postgres can not */
SELECT
    <||>,
    <|,|>,
    <|
        CAST(Unicode::ToUpper("a"u) AS String): Unicode::ToUpper("b"u),
        c: FALSE,
        'e': 7,
        `g`: 2.0,
    |>
;
