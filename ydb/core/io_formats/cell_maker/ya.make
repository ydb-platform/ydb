LIBRARY()

SRCS(
    cell_maker.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/core/scheme_types
    ydb/library/binary_json
    ydb/library/dynumber
    ydb/library/yql/minikql/dom
    ydb/library/yql/public/decimal
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    contrib/libs/double-conversion
    library/cpp/json
    library/cpp/json/yson
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
)

YQL_LAST_ABI_VERSION()

END()
