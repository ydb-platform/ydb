LIBRARY()

YQL_ABI_VERSION(
    2
    9
    0
)

OWNER( 
    g:yql 
    g:yql_ydb_core 
) 

SRCS(
    unicode_base_udf.cpp
)

PEERDIR(
    library/cpp/deprecated/split
    library/cpp/string_utils/levenshtein_diff
    library/cpp/unicode/normalization
    ydb/library/yql/public/udf 
    ydb/library/yql/utils
)

END()
