LIBRARY()

SRCS(
    cbo_interesting_orderings.cpp
)

GENERATE_ENUM_SERIALIZATION(cbo_interesting_orderings.h)

PEERDIR(
    library/cpp/disjoint_sets
    library/cpp/iterator
    yql/essentials/utils/log
)

END()

RECURSE(
    optimizer
)
