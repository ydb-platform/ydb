# This file should be included in all YT projects (including YT ORM installations).

IF (CLANG)
    CXXFLAGS(
        -Wdeprecated-this-capture
        -Wimplicit-fallthrough
        -Wparentheses
        -Wno-logical-op-parentheses
        -Wno-bitwise-op-parentheses
    )

    IF (MUSL)
        CXXFLAGS(
            --system-header-prefix=endian.h
            --system-header-prefix=byteswap.h
        )
    ENDIF()
ENDIF()
