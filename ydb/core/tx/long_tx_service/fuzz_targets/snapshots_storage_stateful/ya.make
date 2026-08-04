FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/tx/long_tx_service
    ydb/library/actors/testlib
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
