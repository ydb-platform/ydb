LIBRARY()

SRCS(
    kqp_topics.cpp
    kqp_topics.h
)

PEERDIR(
    ydb/core/base
    ydb/core/persqueue/public
    ydb/core/tx/scheme_cache
)
# gn: peerdir ydb/core/persqueue/public:utils

YQL_LAST_ABI_VERSION()


END()
