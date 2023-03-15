LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/protos
)

SRCS(
    defs.h
    incrhuge.h
    incrhuge_data.h
    incrhuge_id_dict.h
    incrhuge_keeper.cpp
    incrhuge_keeper.h
    incrhuge_keeper_alloc.cpp
    incrhuge_keeper_alloc.h
    incrhuge_keeper_common.cpp
    incrhuge_keeper_common.h
    incrhuge_keeper_defrag.cpp
    incrhuge_keeper_defrag.h
    incrhuge_keeper_delete.cpp
    incrhuge_keeper_delete.h
    incrhuge_keeper_log.cpp
    incrhuge_keeper_log.h
    incrhuge_keeper_read.cpp
    incrhuge_keeper_read.h
    incrhuge_keeper_recovery.cpp
    incrhuge_keeper_recovery.h
    incrhuge_keeper_recovery_read_log.cpp
    incrhuge_keeper_recovery_read_log.h
    incrhuge_keeper_recovery_scan.cpp
    incrhuge_keeper_recovery_scan.h
    incrhuge_keeper_write.cpp
    incrhuge_keeper_write.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
