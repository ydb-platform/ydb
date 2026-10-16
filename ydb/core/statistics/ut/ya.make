UNITTEST()

FORK_SUBTESTS()

SIZE(SMALL)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/scheme
    ydb/core/scheme_types
    yql/essentials/minikql/computation
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

SRCS(
    presort_key_agreement_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
