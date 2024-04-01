IF (YQL_PACKAGED)
    PACKAGE()

    FROM_SANDBOX(
        FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libhyperscan_udf.so
    )

    END()
ELSE()
    IF (OS_LINUX AND CLANG)

        YQL_UDF_YDB(hyperscan_udf)

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

ENDIF()

RECURSE_FOR_TESTS(
    test
)