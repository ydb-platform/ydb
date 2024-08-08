UNITTEST_FOR(ydb/core/blobstorage/vdisk/skeleton)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/blobstorage
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/skeleton
    ydb/core/testlib/default
    ydb/core/testlib/actors
)

SRCS(
    skeleton_oos_logic_ut.cpp
    skeleton_vpatch_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
