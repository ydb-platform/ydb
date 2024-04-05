IF (YQL_PACKAGED)
    PACKAGE()

    FROM_SANDBOX(
        FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libhyperscan_udf.so
    )

    END()
ELSE()

    YQL_UDF_YDB(hyperscan_udf)
        NO_BUILD_IF(NOT OS_LINUX OR NOT CLANG)

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
ENDIF()

RECURSE_FOR_TESTS(
    test
)