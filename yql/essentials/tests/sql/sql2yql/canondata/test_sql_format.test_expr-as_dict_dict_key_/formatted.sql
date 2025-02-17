/* postgres can not */
$d1 = AsDict(
    AsTuple(AsList(1, 2, 3), 'foo'),
    AsTuple(AsList(1, 2), 'bar')
);

$d2 = AsDict(
    AsTuple(AsList(1, 3), 'baz'),
    AsTuple(AsList(1, 2), 'qwe')
);

$d3 = DictCreate(DictKeyType(TypeOf($d2)), DictPayloadType(TypeOf($d2)));

$d = AsDict(
    AsTuple($d1, 17),
    AsTuple($d2, 32)
);

SELECT
    ListSort(
        ListFlatten(
            ListMap(
                DictItems($d), ($x) -> {
                    RETURN ListMap(
                        DictItems($x.0), ($y) -> {
                            RETURN ($y, $x.1);
                        }
                    );
                }
            )
        )
    ),
    ListSort(
        ListFlatten(
            ListMap(
                DictKeys($d), ($x) -> {
                    RETURN DictItems($x);
                }
            )
        )
    ),
    ListSort(DictPayloads($d))
;

SELECT
    DictLookup($d, $d1),
    DictLookup($d, $d3)
;

SELECT
    DictContains($d, $d1),
    DictContains($d, $d3)
;
