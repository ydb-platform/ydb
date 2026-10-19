LIBRARY()

NO_UTIL()

IF (TSTRING_IS_STD_STRING)
    CFLAGS(GLOBAL -DTSTRING_IS_STD_STRING)
ENDIF()

JOIN_SRCS(
    all_charset.cpp
    generated/unidata.cpp  # gn: into util
    recode_result.cpp  # gn: into util
    unicode_table.cpp  # gn: into util
    unidata.cpp  # gn: into util
    utf8.cpp  # gn: into util
    wide.cpp  # gn: into util
)

IF (ARCH_X86_64 AND NOT DISABLE_INSTRUCTION_SETS)
    SRC_C_SSE41(wide_sse41.cpp  # gn: into util
    )
ELSE()
    SRC(
        wide_sse41.cpp  # gn: into util
        -DSSE41_STUB
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
