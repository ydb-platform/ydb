LIBRARY()

NO_CLANG_COVERAGE()

NO_RUNTIME()

IF (OS_LINUX)
    SRCS(
        GLOBAL profile_name_handler.cpp
    )
ENDIF()

END()
