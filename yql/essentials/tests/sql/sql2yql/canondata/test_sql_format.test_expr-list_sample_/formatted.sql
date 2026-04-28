/* yt can not */
$list = ListFromRange(1, 10);

SELECT
    ListSample(NULL, NULL) IS NULL AS mustBeTrue1,
    ListSample(NULL, 1.0) IS NULL AS mustBeTrue2,
    ListSample(NULL, Nothing(OptionalType(DataType('Double')))) IS NULL AS mustBeTrue3,
    ListSample(NULL, Just(1.0f)) IS NULL AS mustBeTrue4,
    ListSample($list, NULL) == $list AS mustBeTrue5,
    ListSample($list, 1.0) == $list AS mustBeTrue6,
    ListSample($list, Nothing(OptionalType(DataType('Double')))) == $list AS mustBeTrue7,
    ListSample($list, Just(1.0f)) == $list AS mustBeTrue8,
    ListSample(Just($list), NULL) == $list AS mustBeTrue9,
    ListSample(Just($list), 1.0) == $list AS mustBeTrue10,
    ListSample(Just($list), Nothing(OptionalType(DataType('Double')))) == $list AS mustBeTrue11,
    ListSample(Just($list), Just(1.0f)) == $list AS mustBeTrue12,
    ListSample(Nothing(OptionalType(ListType(DataType('Int32')))), NULL) IS NULL AS mustBeTrue13,
    ListSample(Nothing(OptionalType(ListType(DataType('Int32')))), 1.0) IS NULL AS mustBeTrue14,
    ListSample(Nothing(OptionalType(ListType(DataType('Int32')))), Nothing(OptionalType(DataType('Double')))) IS NULL AS mustBeTrue15,
    ListSample(Nothing(OptionalType(ListType(DataType('Int32')))), Just(0.1f)) IS NULL AS mustBeTrue16
;
