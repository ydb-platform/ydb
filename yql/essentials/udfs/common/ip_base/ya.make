YQL_UDF_CONTRIB(ip_udf)

    YQL_ABI_VERSION(
        2
        43
        0
    )

    ENABLE(YQL_STYLE_CPP)

    SRCS(
        ip_base.cpp
    )

    PEERDIR(
        yql/essentials/udfs/common/ip_base/lib
    )

    END()

RECURSE_FOR_TESTS(
    test
)
