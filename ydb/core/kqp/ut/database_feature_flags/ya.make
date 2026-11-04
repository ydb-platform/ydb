UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(8)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    kqp_database_feature_flags_ut.cpp
)

PEERDIR(
    ydb/core/audit
    ydb/core/cms/console
    ydb/core/kqp/ut/common
    ydb/library/yaml_config
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
