UNITTEST()

SIZE(SMALL)

SRCS(
    restore_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/protos/schemeshard
    ydb/core/tx/schemeshard/eds_import
)

END()
