RECURSE_FOR_TESTS(ut)

LIBRARY()

SRCS(
    csv_arrow.cpp
)

CFLAGS(
    -Wno-unused-parameter
)

PEERDIR(
    ydb/core/scheme_types
    ydb/core/formats/arrow
)

YQL_LAST_ABI_VERSION()

END()
