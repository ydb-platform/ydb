YQL_LIBRARY()

SRCS(
    extract_predicate_dbg.cpp
    extract_predicate_dbg.h
    extract_predicate_impl.cpp
    extract_predicate_impl.h
    extract_predicate.h
)

PEERDIR(
    yql/essentials/core/services
    yql/essentials/core/type_ann
)

END()

RECURSE_FOR_TESTS(ut)
