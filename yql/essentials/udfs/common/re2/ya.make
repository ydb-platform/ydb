YQL_UDF_CONTRIB(re2_udf)

    YQL_ABI_VERSION(
        2
        46
        0
    )

    SRCS(
        re2_udf.cpp
    )

    PEERDIR(
        yql/essentials/core/langver
        contrib/libs/re2
        library/cpp/deprecated/enum_codegen
    )

    END()

RECURSE_FOR_TESTS(
    test
)
