LIBRARY()

SRCS(
    manager.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    ydb/services/metadata/abstract
    ydb/services/metadata/manager
)

YQL_LAST_ABI_VERSION()

END()
