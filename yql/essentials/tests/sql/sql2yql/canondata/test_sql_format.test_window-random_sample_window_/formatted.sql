SELECT
    random_sample(x, 10) OVER w
FROM
    AS_TABLE(ListMap(
        ListFromRange(1, 10), ($x) -> {
            RETURN AsStruct(
                $x AS x
            );
        }
    ))
WINDOW
    w AS (
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
;
