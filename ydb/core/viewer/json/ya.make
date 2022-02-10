RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

OWNER(
    xenoxeno
    g:kikimr
)

SRCS(
    json.cpp
    json.h
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/string_utils/base64 
    ydb/core/viewer/protos
)

END()
