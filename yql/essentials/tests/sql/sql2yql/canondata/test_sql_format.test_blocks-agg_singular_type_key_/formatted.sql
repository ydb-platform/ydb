PRAGMA config.flags('PeepholeFlags', 'UseAggPhases');

$n = 3;

$data = ListMap(
    ListFromRange(1, $n), ($x) -> (
        <|
            idx: $x,
            empty_list: [],
            empty_dict: {},
            nil: NULL,
            val: $x + 5,
            vid: Void(),
            emtpy_tuple: AsTuple(),
            empty_struct: AsStruct()
        |>
    )
);

SELECT
    empty_list,
    SOME(idx)
FROM
    as_table($data)
GROUP BY
    empty_list
;

SELECT
    empty_dict,
    SOME(idx)
FROM
    as_table($data)
GROUP BY
    empty_dict
;

SELECT
    nil,
    SOME(idx)
FROM
    as_table($data)
GROUP BY
    nil
;

SELECT
    vid,
    SOME(idx)
FROM
    as_table($data)
GROUP BY
    vid
;

SELECT
    emtpy_tuple,
    SOME(idx)
FROM
    as_table($data)
GROUP BY
    emtpy_tuple
;

SELECT
    empty_struct,
    SOME(idx)
FROM
    as_table($data)
GROUP BY
    empty_struct
;
