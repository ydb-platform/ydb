LIBRARY()

NO_CLANG_COVERAGE()

NO_RUNTIME()

IF (OS_LINUX)
    SRCS(
        GLOBAL write_profile_data.cpp
    )
ENDIF()

END()
