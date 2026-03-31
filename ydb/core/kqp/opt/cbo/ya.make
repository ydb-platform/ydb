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
    yql/essentials/ast
    yql/essentials/core/minsketch
    yql/essentials/core/histogram
    yql/essentials/utils/log
)

CHECK_DEPENDENT_DIRS(DENY PEERDIRS
    yql/essentials/core/cbo
)

YQL_LAST_ABI_VERSION()

END()
