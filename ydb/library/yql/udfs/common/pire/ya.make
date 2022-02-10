YQL_UDF(pire_udf) 

YQL_ABI_VERSION(
    2
    23
    0
)

OWNER(g:yql g:yql_ydb_core) 

SRCS(
    pire_udf.cpp
)

PEERDIR(
    library/cpp/regex/pire
)

END()
