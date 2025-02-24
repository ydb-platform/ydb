RECURSE(
    client
    kicli
)

IF (NOT OPENSOURCE)
    RECURSE(
        json_value
        yson_value
    )
ENDIF()
