YQL_UDF_YDB(string_fast_udf)

YQL_ABI_VERSION(
    2
    43
    0
)

SRCS(
    string_fast_udf.cpp
)

PEERDIR(
    ydb/core/formats/arrow/program/ascii_contains
    yql/essentials/public/udf/arrow
)

END()
