UNITTEST_FOR(ydb/core/change_mirroring)

SIZE(SMALL)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib
    # temporary hack to fix linkage
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/sql/pg
    ydb/library/yql/sql
)

SRCS(
    reader_ut.cpp
)

END()
