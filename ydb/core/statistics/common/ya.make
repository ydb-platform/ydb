LIBRARY()

SRCS(
    stable_pickle.cpp
    stable_pickle.h
)

PEERDIR(
    util
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/public/decimal
    ydb/public/lib/scheme_types
)

YQL_LAST_ABI_VERSION()

END()
