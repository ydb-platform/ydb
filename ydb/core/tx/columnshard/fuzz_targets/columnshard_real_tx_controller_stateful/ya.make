FUZZ()

SIZE(MEDIUM)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/columnshard
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/columnshard/test_helper
    ydb/public/lib/yson_value
    ydb/services/metadata
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
