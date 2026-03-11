LIBRARY()

SRCS(
    case_helper.cpp
    index_defaults.cpp
    index_json_keys.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow
)

END()
