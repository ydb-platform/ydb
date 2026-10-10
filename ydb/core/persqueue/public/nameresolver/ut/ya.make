UNITTEST()

SIZE(SMALL)
REQUIREMENTS(cpu:1)

SRCS(
    nameresolver_ut.cpp
)

PEERDIR(
    ydb/core/persqueue/public/nameresolver
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

END()
