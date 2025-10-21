LIBRARY()

SRCS(
    all_agg_funcs.h
    all_agg_funcs.cpp
)

PEERDIR(
    yql/essentials/core/minsketch
    yql/essentials/public/types
)

END()

RECURSE(
    udfs
)
