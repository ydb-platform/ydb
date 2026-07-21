FUZZ()

SIZE(MEDIUM)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/tx/columnshard/blobs_action
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/data_sharing/manager
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
