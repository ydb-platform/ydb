FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

SRCS(
    main.cpp
    ../../conflicts_cache.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/tablet_flat/test/libs/table
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
