LIBRARY()

SRCS(
    scheme.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/library/formats/arrow/csv/converter
    ydb/core/scheme_types
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
