IF (CLANG_COVERAGE AND OS_LINUX)
    # Large coverage binaries can exceed the DWARF32 section-size limit.
    LDFLAGS(-Wl,--strip-debug)
ENDIF()
