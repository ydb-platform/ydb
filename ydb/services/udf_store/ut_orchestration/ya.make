UNITTEST_FOR(ydb/services/udf_store/compile_controller)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
    ydb/services/udf_store
    ydb/services/udf_store/compile_controller/protos
)

YQL_LAST_ABI_VERSION()

SRCS(
    compile_controller_ut.cpp
    dinode_client_ut.cpp
)

END()
