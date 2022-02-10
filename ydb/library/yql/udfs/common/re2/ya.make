YQL_UDF(re2_udf) 

YQL_ABI_VERSION(
    2
    9 
    0
)

OWNER(g:yql g:yql_ydb_core) 

SRCS(
    re2_udf.cpp
)

PEERDIR(
    contrib/libs/re2
    library/cpp/deprecated/enum_codegen
)

END()
