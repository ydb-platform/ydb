/* postgres can not */
/* multirun can not */
USE plato;

INSERT INTO @a
SELECT
    Yql::Nothing(OptionalType(DataType("String"))) AS level1_null,
    Yql::Optional(OptionalType(DataType("String")), "val") AS level1_just_val,
    Yql::Nothing(OptionalType(OptionalType(DataType("String")))) AS level2_null,
    Yql::Optional(OptionalType(OptionalType(DataType("String"))), Yql::Nothing(OptionalType(DataType("String")))) AS level2_just_null,
    Yql::Optional(OptionalType(OptionalType(DataType("String"))), Yql::Just("val")) AS level2_just_just_val,
    Yql::Nothing(OptionalType(OptionalType(OptionalType(DataType("String"))))) AS level3_null,
    Yql::Optional(OptionalType(OptionalType(OptionalType(DataType("String")))), Yql::Nothing(OptionalType(OptionalType(DataType("String"))))) AS level3_just_null,
    Yql::Optional(OptionalType(OptionalType(OptionalType(DataType("String")))), Yql::Just(Yql::Nothing(OptionalType(DataType("String"))))) AS level3_just_just_null,
    Yql::Optional(OptionalType(OptionalType(OptionalType(DataType("String")))), Yql::Just(Yql::Just("val"))) AS level3_just_just_just_val,
    "const" AS const
;
COMMIT;

-- Everything should be True
SELECT
    level1_null IS NULL,
    Yql::Unwrap(level1_just_val) == "val",
    level2_null IS NULL,
    Yql::Unwrap(level2_just_null) IS NULL,
    Yql::Unwrap(Yql::Unwrap(level2_just_just_val)) == "val",
    level3_null IS NULL,
    Yql::Unwrap(level3_just_null) IS NULL,
    Yql::Unwrap(Yql::Unwrap(level3_just_just_null)) IS NULL,
    Yql::Unwrap(Yql::Unwrap(Yql::Unwrap(level3_just_just_just_val))) == "val",
    TRUE
FROM
    @a
WHERE
    const == "const"
;
