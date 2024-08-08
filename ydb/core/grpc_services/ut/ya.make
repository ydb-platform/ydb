UNITTEST_FOR(ydb/core/grpc_services)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    rpc_calls_ut.cpp
    operation_helpers_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/client/scheme_cache_lib
    ydb/core/testlib/default
)

END()
