UNITTEST_FOR(ydb/core/kqp/opt/rbo)

# Link the production helpers referenced by the provider directly. The enclosing
# host and gateway utility libraries also pull in the server/console actors,
# which are unnecessary for native operator fixtures.
SRCS(
    native_snapshot_ut.cpp
    ../../../../host/kqp_translate.cpp
    ../../../../host/kqp_transform.cpp
    ../../../../gateway/utils/scheme_helpers.cpp
)

PEERDIR(
    library/cpp/testing/common
    ydb/core/fq/libs/result_formatter
    ydb/core/kqp/common
    ydb/core/kqp/opt
    ydb/core/kqp/provider
    ydb/core/protos/schemeshard
    ydb/library/yql/providers/pq/common
    yql/essentials/parser/pg_wrapper
    yql/essentials/providers/common/dq
    yql/essentials/providers/common/provider
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
)

DEPENDS(
    contrib/tools/z3
    ydb/core/kqp/opt/rbo/verification/bin
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

YQL_LAST_ABI_VERSION()

END()
