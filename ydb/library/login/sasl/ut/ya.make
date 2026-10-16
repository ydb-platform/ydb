UNITTEST_FOR(ydb/library/login/sasl)

REQUIREMENTS(cpu:1)
PEERDIR(
    library/cpp/string_utils/base64
)

SRCS(
    scram_ut.cpp
)

END()
