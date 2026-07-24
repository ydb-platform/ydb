FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/tablet_flat
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()

END()
