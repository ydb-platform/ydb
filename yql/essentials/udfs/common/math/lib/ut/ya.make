IF (OS_LINUX)
IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(yql/essentials/udfs/common/math/lib)

    SRCS(
        round_ut.cpp
    )

    END()
ENDIF()
ENDIF()
