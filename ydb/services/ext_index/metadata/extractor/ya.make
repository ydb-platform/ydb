LIBRARY()

SRCS(
    abstract.cpp
    GLOBAL city.cpp
    container.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/tx/sharding
)

YQL_LAST_ABI_VERSION()

END()
