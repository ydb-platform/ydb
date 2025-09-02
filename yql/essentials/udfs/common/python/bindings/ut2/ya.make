IF (OS_LINUX)
    IF (NOT WITH_VALGRIND)
        UNITTEST_FOR(yql/essentials/udfs/common/python/bindings)

        INCLUDE(../ya.make.test.inc)
        USE_PYTHON2()
        END()
    ENDIF()
ENDIF()
