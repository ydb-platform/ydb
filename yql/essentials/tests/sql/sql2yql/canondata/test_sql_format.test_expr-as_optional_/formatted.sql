SELECT
    AsOptional(NULL) IS NULL AS mustBeTrue1,
    AsOptional(Nothing(OptionalType(DataType('Int32')))) IS NULL AS mustBeTrue2,
    AsOptional(42) AS test1,
    AsOptional(1p) AS test2,
    AsOptional(Just(42)) AS test3,
    AsOptional(AsOptional(42)) AS test4,
    AsOptional(Just(Just(42))) AS test5
;
