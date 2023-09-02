LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    schema_validation.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/library/query/engine_api
)

END()
