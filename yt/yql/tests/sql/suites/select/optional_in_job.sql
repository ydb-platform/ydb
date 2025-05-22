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

-- Everything should be True
select
    level1_null is null,
    Yql::Unwrap(level1_just_val) = "val",
    level2_null is null,
    Yql::Unwrap(level2_just_null) is null,
    Yql::Unwrap(Yql::Unwrap(level2_just_just_val)) = "val",
    level3_null is null,
    Yql::Unwrap(level3_just_null) is null,
    Yql::Unwrap(Yql::Unwrap(level3_just_just_null)) is null,
    Yql::Unwrap(Yql::Unwrap(Yql::Unwrap(level3_just_just_just_val))) = "val",
    True
from @a
where const = "const";
