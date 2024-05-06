IF (YQL_PACKAGED)
    PACKAGE()

    FROM_SANDBOX(
        FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libhyperscan_udf.so
    )

    END()
ELSE()

    # NO_BUILD_IF does not like logical expressions by now
    # see DEVTOOLSSUPPORT-44378
    IF (NOT OS_LINUX OR NOT CLANG)
        SET(DISABLE_HYPERSCAN_BUILD)
    ENDIF()

    NO_BUILD_IF(DISABLE_HYPERSCAN_BUILD)

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

ENDIF()

RECURSE_FOR_TESTS(
    test
)