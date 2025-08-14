PRAGMA config.flags('PeepholeFlags', 'UseAggPhases');

$n = 3;
$data = ListMap(ListFromRange(1, $n), ($x) -> (<|idx: $x, 
                                                 empty_list: [], 
                                                 empty_dict: {}, 
                                                 nil: NULL, 
                                                 val: $x + 5,
                                                 vid: Void(),
                                                 emtpy_tuple: AsTuple(), 
                                                 empty_struct: AsStruct()|>));

SELECT
    idx,
    SOME(empty_dict),
    SOME(empty_list),
    SOME(nil),
    SOME(empty_dict),
    SOME(vid),
    SOME(emtpy_tuple),
    SOME(empty_struct),
FROM
    as_table($data)
GROUP BY
    idx
;
