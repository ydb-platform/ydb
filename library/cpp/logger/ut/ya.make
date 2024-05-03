UNITTEST()

PEERDIR(
    ADDINCL library/cpp/logger
    library/cpp/yconf/patcher
)

SRCDIR(library/cpp/logger)

SRCS(
    log_ut.cpp
    element_ut.cpp
    rotating_file_ut.cpp
    reopen_ut.cpp
)

IF (NOT OPENSOURCE OR OPENSOURCE_PROJECT != "catboost")
    PEERDIR(
        library/cpp/logger/init_context
    )

    SRCS(
        composite_ut.cpp
    )
ENDIF()

END()
