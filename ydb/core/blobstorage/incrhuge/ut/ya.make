UNITTEST()

FORK_SUBTESTS()

REQUIREMENTS(cpu:1)
IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ELSE()
    SIZE(MEDIUM)
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

END()
