YQL_UDF_CONTRIB(json_udf)

    YQL_ABI_VERSION(
        2
        28
        0
    )

    ENABLE(YQL_STYLE_CPP)

    SRCS(
        json_udf.cpp
    )

    PEERDIR(
        library/cpp/json/easy_parse
    )

    END()
