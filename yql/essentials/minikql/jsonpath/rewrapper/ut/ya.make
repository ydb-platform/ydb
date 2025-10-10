UNITTEST_FOR(yql/essentials/minikql/jsonpath/rewrapper)

IF(ARCH_X86_64)
    SRCS(
        hyperscan_ut.cpp
        re2_ut.cpp
    )

    PEERDIR(
        yql/essentials/minikql/jsonpath/rewrapper
        yql/essentials/minikql/jsonpath/rewrapper/hyperscan
        yql/essentials/minikql/jsonpath/rewrapper/re2
    )
ELSE()
    SRCS(
        re2_ut.cpp
    )

    PEERDIR(
        yql/essentials/minikql/jsonpath/rewrapper
        yql/essentials/minikql/jsonpath/rewrapper/re2
    )
ENDIF()

END()
