/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA FlexibleTypes;

SELECT
    <|'type_' || FormatType(TypeOf(text)): text|> AS result
FROM (
    SELECT
        1 AS text
);

SELECT
    <|text: ''|>
;
