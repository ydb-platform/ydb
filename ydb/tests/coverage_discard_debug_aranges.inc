IF (CLANG_COVERAGE AND OS_LINUX AND _LINKER_ID == "lld")
    # Large coverage binaries can overflow DWARF32 offsets from .debug_aranges into .debug_info.
    LINKER_SCRIPT(${ARCADIA_ROOT}/ydb/tests/coverage_discard_debug_aranges.ld.in)
ENDIF()
