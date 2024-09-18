LIBRARY()

SRCS(
    timeout.cpp
    ss_dialog.cpp
)

PEERDIR(
    ydb/services/bg_tasks/abstract
    ydb/core/tx/scheme_cache
)

YQL_LAST_ABI_VERSION()

END()
