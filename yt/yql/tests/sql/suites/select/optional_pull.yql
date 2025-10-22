/* postgres can not */
/* multirun can not */
USE plato;

insert into @a
SELECT
    Yql::Nothing(OptionalType(DataType("String"))) as level1_null,
    Yql::Optional(OptionalType(DataType("String")), "val") as level1_just_val,
    Yql::Nothing(OptionalType(OptionalType(DataType("String")))) as level2_null,
    Yql::Optional(OptionalType(OptionalType(DataType("String"))), Yql::Nothing(OptionalType(DataType("String")))) as level2_just_null,
    Yql::Optional(OptionalType(OptionalType(DataType("String"))), Yql::Just("val")) as level2_just_just_val,
    Yql::Nothing(OptionalType(OptionalType(OptionalType(DataType("String"))))) as level3_null,
    Yql::Optional(OptionalType(OptionalType(OptionalType(DataType("String")))), Yql::Nothing(OptionalType(OptionalType(DataType("String"))))) as level3_just_null,
    Yql::Optional(OptionalType(OptionalType(OptionalType(DataType("String")))), Yql::Just(Yql::Nothing(OptionalType(DataType("String"))))) as level3_just_just_null,
    Yql::Optional(OptionalType(OptionalType(OptionalType(DataType("String")))), Yql::Just(Yql::Just("val"))) as level3_just_just_just_val,
    "const" as const
;

commit;

-- Check result representation
select
    level1_null,
    level1_just_val,
    level2_null,
    level2_just_null,
    level2_just_just_val,
    level3_null,
    level3_just_null,
    level3_just_just_null,
    level3_just_just_just_val
from @a;
