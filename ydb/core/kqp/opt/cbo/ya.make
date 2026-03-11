LIBRARY()

SRCS(
    cbo_interesting_orderings.cpp
    cbo_optimizer_new.cpp
    cbo_hints.cpp
)

GENERATE_ENUM_SERIALIZATION(cbo_optimizer_new.h)

PEERDIR(
    yql/essentials/core
    library/cpp/disjoint_sets
    library/cpp/iterator
    yql/essentials/utils/log
)

END()
