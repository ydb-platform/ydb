UNITTEST_FOR(ydb/core/mind)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TIMEOUT(600)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/nodewarden
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/vdisk/common
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    blobstorage_node_warden_ut_fat.cpp
)

END()
