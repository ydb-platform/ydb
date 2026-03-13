PY3TEST()
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

    FORK_SUBTESTS()

    TEST_SRCS(
        base.py
        test_delete_by_explicit_row_id.py
        test_delete_all_after_inserts.py
    )

    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
    IF (SANITIZER_TYPE)
        REQUIREMENTS(cpu:4)
    ENDIF()

    PEERDIR(
        ydb/tests/library
        ydb/tests/library/test_meta
        ydb/tests/olap/common
    )

    DEPENDS(
        )

END()
