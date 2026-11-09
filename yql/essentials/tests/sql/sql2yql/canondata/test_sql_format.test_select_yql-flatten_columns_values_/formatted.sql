PRAGMA YqlSelect = 'force';

PRAGMA OrderedColumns;

SELECT
    *
FROM (
    SELECT
        1 AS scalar,
        Just(2) AS optional_scalar,
        NULL AS null_scalar,
        AsStruct() AS empty_struct,
        AsStruct(
            3 AS required,
            Just(4) AS optional,
            NULL AS null_member,
            AsStruct(5 AS nested_member) AS nested_struct
        ) AS plain_struct,
        Just(
            AsStruct(
                6 AS optionalized,
                Just(7) AS already_optional,
                NULL AS still_null,
                AsStruct(8 AS nested_member) AS optional_nested_struct
            )
        ) AS optional_struct,
        Nothing(Struct<missing: Int32>?) AS empty_optional_struct
)
    FLATTEN COLUMNS
;
