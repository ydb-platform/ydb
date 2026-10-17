YQL_UDF_YDB(olap_kernels_udf)

YQL_ABI_VERSION(
    2
    43
    0
)

SRCS(
    olap_kernels_udf.cpp
)

PEERDIR(
    ydb/core/formats/arrow/program/ascii_contains
    yql/essentials/public/udf/arrow
)

END()
