/* syntax version 1 */
SELECT
    random_sample(x, 3l)
FROM
    AS_TABLE(ListMap(
        ListFromRange(1, 10), ($x) -> {
            RETURN AsStruct(
                if($x > 1, NULL, Just($x)) AS x
            );
        }
    ))
;
