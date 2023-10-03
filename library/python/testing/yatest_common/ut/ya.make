PY2TEST()

TEST_SRCS(test.py)

PEERDIR(
    devtools/ya/yalibrary/tools
)

DEPENDS(
    build/platform/java/jdk/testing
)

REQUIREMENTS(
    network:full
)

END()
