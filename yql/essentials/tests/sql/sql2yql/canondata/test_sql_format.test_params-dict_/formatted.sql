/* syntax version 1 */
DECLARE $x AS Dict<String, Int64?>;
DECLARE $x2 AS Dict<Utf8, Int64?>;

SELECT
    $x['a1'],
    $x['a2'],
    $x['a3'],
    $x2['a1'],
    $x2['a2'],
    $x2['a3']
;
