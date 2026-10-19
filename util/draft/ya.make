LIBRARY()

NO_UTIL()

IF (TSTRING_IS_STD_STRING)
    CFLAGS(GLOBAL -DTSTRING_IS_STD_STRING)
ENDIF()

SRCS(
    date.cpp  # gn: into util
    datetime.cpp  # gn: into util
    enum.cpp  # gn: into util
    holder_vector.cpp  # gn: into util
    ip.cpp  # gn: into util
    matrix.cpp  # gn: into util
    memory.cpp  # gn: into util
)

END()

RECURSE_FOR_TESTS(
    ut
)
