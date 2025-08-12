YQL_UDF_CONTRIB(ip_udf)

    YQL_ABI_VERSION(
        2
        43
        0
    )

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
