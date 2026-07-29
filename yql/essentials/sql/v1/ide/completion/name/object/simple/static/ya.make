LIBRARY()

SRCS(
    schema_json.cpp
    schema.cpp
)

PEERDIR(
    yql/essentials/sql/v1/ide/completion/name/object/simple
    library/cpp/json
)

END()
