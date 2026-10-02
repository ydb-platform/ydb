LIBRARY()

IF(NOT OS_WINDOWS)

    PEERDIR(
        library/cpp/dwarf_backtrace
    )

    SRCS(
        GLOBAL set_format_backtrace.cpp
    )

ENDIF()

END()
