LIBRARY()

SRCS(
    operation_id.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/cgiparam
    library/cpp/uri
    ydb/public/lib/operation_id/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
