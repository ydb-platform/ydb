LIBRARY()

SRCS(
    objects.h
    objects.cpp
    converter.h
    converter.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/core/protos
    ydb/core/scheme
    ydb/library/mkql_proto/protos
    ydb/public/lib/deprecated/kicli
)

END()

RECURSE_FOR_TESTS(
    ut
)
