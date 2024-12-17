UNITTEST_FOR(util)

SRCS(
    datetime/base_ut.cpp
    datetime/cputimer_ut.cpp
    datetime/parser_deprecated_ut.cpp
    datetime/parser_ut.cpp
    datetime/process_uptime_ut.cpp
    datetime/uptime_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/util/tests/ya_util_tests.inc)

END()
