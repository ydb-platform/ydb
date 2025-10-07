IF (OS_LINUX)
IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(yql/essentials/udfs/common/math/lib)

    ENABLE(YQL_STYLE_CPP)

    SRCS(
        round_ut.cpp
    )

    END()
ENDIF()
ENDIF()
