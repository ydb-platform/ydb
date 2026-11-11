UNITTEST_FOR(ydb/core/kqp/host/lazy_provider)

SRCS(kqp_lazy_provider_ut.cpp)

PEERDIR(
    yql/essentials/core
    yql/essentials/providers/common/provider
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
