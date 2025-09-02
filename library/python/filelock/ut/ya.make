PY23_TEST()

TEST_SRCS(
    test_filelock.py
)

PEERDIR(
    library/python/filelock
)

IF (OS_DARWIN)
    SIZE(LARGE)
    TAG(
        ya:fat
        ya:exotic_platform
        ya:large_tests_on_single_slots
    )
ELSEIF (OS_WINDOWS)
    SIZE(LARGE)
    TAG(
        ya:fat
        sb:ssd&WINDOWS
        ya:large_tests_on_single_slots
    )
ENDIF()

END()
