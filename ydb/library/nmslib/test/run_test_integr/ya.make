EXECTEST()

# Runs the test_integr integration driver (built in
# ydb/library/nmslib/test/test_integr) as a test. The driver returns a non-zero
# exit code if any testcase fails.
#
# This is a heavy suite (~65 index build/query scenarios over the sample data),
# so it gets its own EXECTEST chunk with a raised timeout.
#
# Sample datasets are resolved via the Arcadia source root (see
# test/testdataset.h) and made available to the test through DATA().

IF (SANITIZER_TYPE == "memory")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    TIMEOUT(2400)
ELSE()
    SIZE(MEDIUM)
    TIMEOUT(600)
ENDIF()

RUN(test_integr)

DEPENDS(
    ydb/library/nmslib/test/test_integr
)

DATA(
    arcadia/ydb/library/nmslib/sample_data
)

END()
