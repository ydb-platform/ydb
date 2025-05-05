PRAGMA config.flags('PeepholeFlags', 'UseAggPhases');

$n = 3;

$data = ListMap(ListFromRange(1, $n), ($x) -> (<|idx: $x, 
                                                 empty_list: Just([]), 
                                                 empty_dict: Just({}), 
                                                 nil: Just(NULL), 
                                                 val: $x + 5,
                                                 vid: Just(Void()),
                                                 emtpy_tuple: Just(AsTuple()), 
                                                 empty_struct: Just(AsStruct())|>));

SELECT empty_list, SOME(idx) FROM as_table($data) GROUP BY empty_list;

SELECT empty_dict, SOME(idx) FROM as_table($data) GROUP BY empty_dict;

SELECT nil, SOME(idx) FROM as_table($data) GROUP BY nil;

SELECT vid, SOME(idx) FROM as_table($data) GROUP BY vid;

SELECT emtpy_tuple, SOME(idx) FROM as_table($data) GROUP BY emtpy_tuple;

SELECT empty_struct, SOME(idx) FROM as_table($data) GROUP BY empty_struct;
