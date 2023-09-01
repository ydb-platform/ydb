IF (OS_LINUX AND CLANG)

    YQL_UDF(hyperscan_udf)

    YQL_ABI_VERSION(
        2
        27
        0
    )

    SRCS(
        hyperscan_udf.cpp
    )

    PEERDIR(
        library/cpp/regex/hyperscan
        library/cpp/regex/pcre
    )

    END()

ELSE()
    LIBRARY()
    END()
ENDIF()

RECURSE_FOR_TESTS(
    test
)