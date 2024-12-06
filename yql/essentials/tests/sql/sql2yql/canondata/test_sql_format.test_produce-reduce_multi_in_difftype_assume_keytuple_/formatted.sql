/* syntax version 1 */
/* postgres can not */
USE plato;
$user_process = ($key, $t1, $t2, $t3) -> {
    RETURN AsStruct(
        $key.0 AS key,
        COALESCE(CAST($t1.subkey AS Int32), 0) + COALESCE(CAST($t2.subkey AS Int32), 0) + COALESCE(CAST($t3.subkey AS Int32), 0) AS subkey
    );
};
$reducer = ($key, $stream) -> {
    $stream = YQL::OrderedMap(
        $stream, ($item) -> {
            RETURN AsStruct(
                YQL::Guess($item, AsAtom("0")).t1 AS t1,
                YQL::Guess($item, AsAtom("1")).t2 AS t2,
                YQL::Guess($item, AsAtom("2")).t3 AS t3,
            );
        }
    );
    $recs = YQL::Collect(
        YQL::Condense1(
            $stream,
            ($item) -> {
                RETURN AsStruct(
                    $item.t1 AS t1,
                    $item.t2 AS t2,
                    $item.t3 AS t3,
                );
            },
            ($_item, $_state) -> {
                RETURN FALSE;
            },
            ($item, $state) -> {
                RETURN AsStruct(
                    COALESCE($state.t1, $item.t1) AS t1,
                    COALESCE($state.t2, $item.t2) AS t2,
                    COALESCE($state.t3, $item.t3) AS t3,
                );
            },
        )
    );
    $rec = Ensure($recs, ListLength($recs) == 1ul)[0];
    RETURN $user_process($key, $rec.t1, $rec.t2, $rec.t3);
};

INSERT INTO Output WITH TRUNCATE
REDUCE (
    SELECT
        key,
        subkey,
        TableRow() AS t1
    FROM
        Input
), (
    SELECT
        key,
        subkey,
        TableRow() AS t2
    FROM
        Input
), (
    SELECT
        key,
        subkey,
        TableRow() AS t3
    FROM
        Input
)
ON
    key,
    subkey
USING $reducer(TableRow())
ASSUME ORDER BY
    key,
    subkey
;
