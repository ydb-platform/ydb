UNITTEST_FOR(ydb/services/metadata/initializer)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/protobuf/json
    ydb/core/cms/console
    ydb/core/testlib/default
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy
    ydb/core/wrappers
    ydb/library/accessor
    ydb/library/actors/core
    ydb/public/sdk/cpp/src/client/table
    ydb/services/metadata
    ydb/services/metadata/abstract
    ydb/services/metadata/manager
    ydb/services/metadata/secret
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_init.cpp
)

END()
