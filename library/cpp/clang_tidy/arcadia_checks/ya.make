LIBRARY()

PEERDIR(
    contrib/libs/clang16/lib/AST
    contrib/libs/clang16/lib/ASTMatchers
)

NO_COMPILER_WARNINGS()

NO_UTIL()

SRCS(
    ascii_compare_ignore_case_check.cpp
    taxi_coroutine_unsafe_check.cpp
    taxi_dangling_config_ref_check.cpp
    GLOBAL tidy_module.cpp
    usage_restriction_checks.cpp
)

END()
