PY3TEST()
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

    FORK_SUBTESTS()

    TEST_SRCS(
        base.py
        test_delete_by_explicit_row_id.py
        test_delete_all_after_inserts.py
    )

    IF (SANITIZER_TYPE OR WITH_VALGRIND)
        SIZE(LARGE)
        REQUIREMENTS(cpu:2)
        TAG(ya:fat)
    ELSE()
        SIZE(MEDIUM)
        REQUIREMENTS(cpu:2)
    ENDIF()

    PEERDIR(
        ydb/tests/library
        ydb/tests/library/test_meta
        ydb/tests/olap/common
    )

    DEPENDS(
        )

END()
