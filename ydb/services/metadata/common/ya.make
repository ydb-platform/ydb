LIBRARY()

SRCS(
    timeout.cpp
    ss_dialog.cpp
)

PEERDIR(
    ydb/services/metadata/initializer
    ydb/services/metadata/abstract
    ydb/services/bg_tasks/abstract
    ydb/core/tx/schemeshard
)

YQL_LAST_ABI_VERSION()

END()
