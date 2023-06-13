LIBRARY()

SRCS(
    extract_predicate_dbg.cpp
    extract_predicate_dbg.h
    extract_predicate_impl.cpp
    extract_predicate_impl.h
    extract_predicate.h
)

PEERDIR(
    ydb/library/yql/core/services
)

YQL_LAST_ABI_VERSION()

END()
