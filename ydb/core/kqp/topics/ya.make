LIBRARY()

SRCS(
    kqp_topics.cpp
    kqp_topics.h
)

PEERDIR(
    ydb/core/base
    ydb/core/tx/scheme_cache
)

YQL_LAST_ABI_VERSION()


END()
