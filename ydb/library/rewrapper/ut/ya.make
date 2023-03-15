UNITTEST_FOR(ydb/library/rewrapper)

IF(ARCH_X86_64)
    SRCS(
        hyperscan_ut.cpp
        re2_ut.cpp
    )

    PEERDIR(
        ydb/library/rewrapper
        ydb/library/rewrapper/hyperscan
        ydb/library/rewrapper/re2
    )
ELSE()
    SRCS(
        re2_ut.cpp
    )

    PEERDIR(
        ydb/library/rewrapper
        ydb/library/rewrapper/re2
    )
ENDIF()

END()
