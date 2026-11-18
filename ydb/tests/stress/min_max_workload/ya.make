PY3_PROGRAM(min_max_workload)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/min_max_workload/workload
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        ${ARCADIA_ROOT}/ydb/tests/tsan.supp
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    tests
)
