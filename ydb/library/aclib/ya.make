LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    library/cpp/protobuf/util 
    ydb/library/aclib/protos
)

OWNER(
    xenoxeno
    g:kikimr
)

SRCS(
    aclib.cpp
    aclib.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
