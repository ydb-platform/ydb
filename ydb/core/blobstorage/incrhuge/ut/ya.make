UNITTEST()

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    ydb/apps/version
    ydb/library/actors/protos
    ydb/core/blobstorage
    ydb/core/blobstorage/incrhuge
    ydb/core/blobstorage/pdisk
)

SRCS(
    incrhuge_basic_ut.cpp
    incrhuge_id_dict_ut.cpp
    incrhuge_log_merger_ut.cpp
)

REQUIREMENTS(ram:9)

END()
