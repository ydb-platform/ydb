/* yt can not */
$list = ListFromRange(1, 10);

SELECT
    ListSampleN(NULL, NULL) IS NULL AS mustBeTrue1,
    ListSampleN(NULL, 5l) IS NULL AS mustBeTrue2,
    ListSampleN(NULL, Nothing(OptionalType(DataType('Uint64')))) IS NULL AS mustBeTrue3,
    ListSampleN(NULL, Just(5ul)) IS NULL AS mustBeTrue4,
    ListSampleN($list, NULL) == $list AS mustBeTrue5,
    ListSampleN($list, 5ul) AS test1,
    ListSampleN($list, Nothing(OptionalType(DataType('Uint64')))) == $list AS mustBeTrue7,
    ListSampleN($list, Just(5ul)) AS test2,
    ListSampleN(Just($list), NULL) == $list AS mustBeTrue9,
    ListSampleN(Just($list), 5ul) AS test3,
    ListSampleN(Just($list), Nothing(OptionalType(DataType('Uint64')))) == $list AS mustBeTrue11,
    ListSampleN(Just($list), Just(5ul)) AS test4,
    ListSampleN(Nothing(OptionalType(ListType(DataType('Uint64')))), NULL) IS NULL AS mustBeTrue13,
    ListSampleN(Nothing(OptionalType(ListType(DataType('Uint64')))), 1u) IS NULL AS mustBeTrue14,
    ListSampleN(Nothing(OptionalType(ListType(DataType('Uint64')))), Nothing(OptionalType(DataType('Uint64')))) IS NULL AS mustBeTrue15,
    ListSampleN(Nothing(OptionalType(ListType(DataType('Uint64')))), Just(1ul)) IS NULL AS mustBeTrue16,
    ListSampleN($list, 0u) == [] AS mustBeTrue19,
    ListSampleN($list, Just(0u)) == [] AS mustBeTrue20
;
