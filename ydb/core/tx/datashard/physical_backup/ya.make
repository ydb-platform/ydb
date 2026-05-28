UNITTEST()

FORK_SUBTESTS()

SIZE(SMALL)

SRCS(
    physical_backup_ut.cpp
)

ADDINCL(
    ydb/core/tablet_flat
)

PEERDIR(
    library/cpp/resource
    ydb/core/scheme
    ydb/core/tablet_flat
    ydb/core/tablet_flat/test/libs/exec
    ydb/core/tablet_flat/test/libs/table
    ydb/core/testlib/default
    yql/essentials/public/udf/service/exception_policy
)

END()
