YQL_LIBRARY()

SRCS(
    scheme.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/library/formats/arrow/csv/converter
    ydb/core/scheme_types
)

END()

RECURSE_FOR_TESTS(ut)
