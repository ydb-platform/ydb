LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    library/cpp/protobuf/util
    ydb/library/aclib/protos/identity
    ydb/library/aclib/protos/acl
)

SRCS(
    aclib.cpp
    aclib.h
)

END()

RECURSE_FOR_TESTS(
    ut
    benchmark
)
