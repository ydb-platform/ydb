/* syntax version 1 */
SELECT
    random_value(x)
FROM
    AS_TABLE(ListMap(
        ListFromRange(1, 50), ($x) -> {
            RETURN AsStruct($x AS x);
        }
    ))
;
