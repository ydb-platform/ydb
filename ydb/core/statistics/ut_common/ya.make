LIBRARY()

SRCS(
    ut_common.cpp
    ut_common.h
)

PEERDIR(
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()
