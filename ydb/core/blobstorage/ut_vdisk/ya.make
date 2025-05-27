UNITTEST_FOR(ydb/core/blobstorage)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

SIZE(MEDIUM)

SRCS(
    defaults.h
    gen_restarts.cpp
    gen_restarts.h
    huge_migration_ut.cpp
    mon_reregister_ut.cpp
    vdisk_test.cpp
)

PEERDIR(
    ydb/apps/version
    ydb/library/actors/protos
    library/cpp/codecs
    ydb/core/base
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/ut_vdisk/lib
    ydb/core/erasure
    ydb/core/scheme
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
