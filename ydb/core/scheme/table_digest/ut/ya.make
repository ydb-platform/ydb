UNITTEST()

SRCS(
    table_digest_ut.cpp
)

PEERDIR(
    ydb/core/scheme/table_digest
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
    yql/essentials/utils
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
