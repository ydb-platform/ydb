LIBRARY()

SRCS(
    table_digest.cpp
)

PEERDIR(
    contrib/libs/openssl
    ydb/core/scheme
    ydb/library/table_digest
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/public/decimal
    yql/essentials/types/binary_json
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
