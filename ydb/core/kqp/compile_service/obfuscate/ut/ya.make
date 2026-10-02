UNITTEST_FOR(ydb/core/kqp/compile_service/obfuscate)

FORK_SUBTESTS()

SRCS(
    obfuscate_ut.cpp
)

PEERDIR(
    ydb/core/protos
    library/cpp/json
    library/cpp/string_utils/base64
    library/cpp/testing/common
    library/cpp/testing/unittest
)

DATA (
    arcadia/ydb/core/kqp/compile_service/obfuscate/ut/data
)

YQL_LAST_ABI_VERSION()

SIZE(SMALL)

END()
