LIBRARY()

SRCS(
    cell_maker.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/core/scheme_types
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
    yql/essentials/minikql/dom
    yql/essentials/public/decimal
    yql/essentials/public/udf
    yql/essentials/utils
    contrib/libs/double-conversion
    library/cpp/json
    library/cpp/json/yson
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
)

YQL_LAST_ABI_VERSION()

END()
