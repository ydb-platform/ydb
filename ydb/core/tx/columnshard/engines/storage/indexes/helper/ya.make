LIBRARY()

SRCS(
    case_helper.cpp
    index_defaults.cpp
    index_parameters.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow
)

END()
