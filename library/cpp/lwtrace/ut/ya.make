UNITTEST_FOR(library/cpp/lwtrace)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread")
    SRCS(
        shuttle_race_ut.cpp
    )
ENDIF()

SRCS(
    log_ut.cpp
    trace_ut.cpp
)

END()
