/* postgres can not */
$first = ($x) -> {
    RETURN $x.0;
};

$second = ($x) -> {
    RETURN $x.1;
};

$i = AsDict(AsTuple(1, 'A'), AsTuple(2, 'B'));
$j = AsDict(AsTuple(1, 'A'), AsTuple(2, 'C'));
$k = AsDict(AsTuple(1, 'A'), AsTuple(2, 'D'));

$l = AsList(
    AsTuple($i, 'foo'),
    AsTuple($i, 'bar'),
    AsTuple($j, 'baz')
);

$d = ToDict($l);

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
    DictLookup($d, $i),
    DictLookup($d, $k)
;

SELECT
    DictContains($d, $i),
    DictContains($d, $k)
;

$d = ToMultiDict($l);

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
    DictLookup($d, $i),
    DictLookup($d, $k)
;

SELECT
    DictContains($d, $i),
    DictContains($d, $k)
;

$d = Yql::ToDict($l, $first, $second, AsTuple(AsAtom('Compact'), AsAtom('Hashed'), AsAtom('One')));

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
    DictLookup($d, $i),
    DictLookup($d, $k)
;

SELECT
    DictContains($d, $i),
    DictContains($d, $k)
;

$d = Yql::ToDict($l, $first, $second, AsTuple(AsAtom('Compact'), AsAtom('Hashed'), AsAtom('Many')));

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
    DictLookup($d, $i),
    DictLookup($d, $k)
;
