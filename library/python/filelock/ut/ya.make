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
    )
ELSEIF (OS_WINDOWS)
    SIZE(LARGE)
    TAG(
        ya:fat
        sb:ssd&~MULTISLOT&WINDOWS
    )
ENDIF()

END()
