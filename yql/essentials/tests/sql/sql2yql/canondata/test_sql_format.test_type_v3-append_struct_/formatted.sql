/* syntax version 1 */
/* multirun can not */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) == 10 */
USE plato;

PRAGMA yt.UseNativeYtTypes = '1';

INSERT INTO Input
SELECT
    '10' AS key,
    <|a: '10', b: Just(10), c: 'e'|> AS subkey
;

COMMIT;

INSERT INTO Input
SELECT
    *
FROM
    Input
WHERE
    key > '100'
;

INSERT INTO Input
SELECT
    *
FROM
    Input
WHERE
    key <= '100'
;

COMMIT;

SELECT
    *
FROM
    Input
;
