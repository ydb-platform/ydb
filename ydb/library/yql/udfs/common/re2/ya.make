YQL_UDF(re2_udf)

YQL_ABI_VERSION(
    2
    28
    0
)

SRCS(
    re2_udf.cpp
)

PEERDIR(
    contrib/libs/re2
    library/cpp/deprecated/enum_codegen
)

END()
