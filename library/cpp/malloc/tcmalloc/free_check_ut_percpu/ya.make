UNITTEST()

ALLOCATOR(TCMALLOC_256K)

SRCDIR(library/cpp/malloc/tcmalloc/free_check_ut)
SRCS(
    free_check_ut.cpp
    error_cases_ut.cpp
)

END()
