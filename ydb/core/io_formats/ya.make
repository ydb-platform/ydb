RECURSE_FOR_TESTS(ut)

LIBRARY()

SRCS(
    csv_ydb_dump.cpp
    csv_arrow.cpp
)

CFLAGS(
    -Wno-unused-parameter
)

PEERDIR(
    contrib/libs/double-conversion
    library/cpp/string_utils/quote
    ydb/core/formats/arrow
    ydb/core/scheme
    ydb/library/binary_json
    ydb/library/dynumber
    ydb/library/yql/minikql/dom
    ydb/library/yql/public/decimal
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    ydb/public/lib/scheme_types
)

YQL_LAST_ABI_VERSION()

END()
