RECURSE(
    client
    kicli
)

IF (NOT OPENSOURCE OR OPENSOURCE_PROJECT != "ydb")
    RECURSE(
        json_value
        yson_value
    )
ENDIF()
