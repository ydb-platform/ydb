IF (OS_LINUX)
IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(ydb/library/yql/udfs/common/math/lib)

    TAG(ya:manual)

    SRCS(
        round_ut.cpp
    )

    END()
ENDIF()
ENDIF()
