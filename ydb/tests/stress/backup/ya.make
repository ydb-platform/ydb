PY3_PROGRAM(backup_stress)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/backup/workload
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