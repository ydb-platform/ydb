# A udf that peers a service policy, which only makes sense as a test: the host supplies the
# policy, and the build has to keep its GLOBAL objects out of this .so.
YQL_UDF_CONTRIB(policy_probe_udf)

    YQL_ABI_VERSION(
        2
        28
        0
    )

    SRCS(
        policy_probe_udf.cpp
    )

    PEERDIR(
        yql/essentials/public/udf/service/stub
    )

    END()

RECURSE_FOR_TESTS(
    test
    udf_test
)
