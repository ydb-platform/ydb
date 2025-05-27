/* syntax version 1 */
/* postgres can not */
USE plato;

$user_process = ($key, $t1, $t2, $t3) -> {
    return AsStruct(
        $key AS key,
        COALESCE(cast($t1.subkey as Int32), 0) + COALESCE(cast($t2.subkey as Int32), 0) + COALESCE(cast($t3.subkey as Int32), 0) AS subkey
    );
};

$reducer = ($key, $stream) -> {
    $stream = YQL::OrderedMap($stream, ($item) -> {
        return AsStruct(
            YQL::Guess($item, AsAtom("0")).t1 AS t1,
            YQL::Guess($item, AsAtom("1")).t2 AS t2,
            YQL::Guess($item, AsAtom("2")).t3 AS t3,
        );
    });
    $recs = YQL::Collect(YQL::Condense1(
        $stream,
        ($item) -> {return AsStruct(
            $item.t1 AS t1,
            $item.t2 AS t2,
            $item.t3 AS t3,
        );},
        ($_item, $_state) -> {return false;},
        ($item, $state) -> {return AsStruct(
            COALESCE($state.t1, $item.t1) AS t1,
            COALESCE($state.t2, $item.t2) AS t2,
            COALESCE($state.t3, $item.t3) AS t3,
        );},
    ));
    $rec = Ensure($recs, ListLength($recs) == 1ul)[0];
    return $user_process($key, $rec.t1, $rec.t2, $rec.t3);
};

INSERT INTO Output WITH TRUNCATE
REDUCE
    (SELECT key, TableRow() AS t1 FROM Input),
    (SELECT key, TableRow() AS t2 FROM Input),
    (SELECT key, TableRow() AS t3 FROM Input)
ON key
USING $reducer(TableRow())
ASSUME ORDER BY key;

