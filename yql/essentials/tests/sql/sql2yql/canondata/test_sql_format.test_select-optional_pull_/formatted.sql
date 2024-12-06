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

-- Check result representation
SELECT
    level1_null,
    level1_just_val,
    level2_null,
    level2_just_null,
    level2_just_just_val,
    level3_null,
    level3_just_null,
    level3_just_just_null,
    level3_just_just_just_val
FROM
    @a
;
