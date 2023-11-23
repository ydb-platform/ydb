LIBRARY()

IF(OS_LINUX)

    PEERDIR(
        library/cpp/dwarf_backtrace
    )

    SRCS(
        GLOBAL set_format_backtrace.cpp
    )

ENDIF()

END()
