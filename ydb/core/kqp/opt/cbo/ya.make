LIBRARY()

SRCS(
    cbo_hints.cpp
    cbo_interesting_orderings.cpp
    cbo_optimizer_new.cpp
    kqp_statistics.cpp
)

GENERATE_ENUM_SERIALIZATION(cbo_optimizer_new.h)

PEERDIR(
    library/cpp/disjoint_sets
    library/cpp/iterator
    library/cpp/json
    library/cpp/string_utils/base64
    yql/essentials/core
    yql/essentials/core/cbo
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
