EXECTEST()

# Runs the bunit unit-test driver (built in ydb/library/nmslib/test/bunit) as a
# test. The driver returns a non-zero exit code if any testcase fails.
#
# Sample datasets are resolved via the Arcadia source root (see
# test/testdataset.h) and made available to the test through DATA().

IF (SANITIZER_TYPE == "memory")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    TIMEOUT(2400)
ELSE()
    SIZE(MEDIUM)
ENDIF()

RUN(bunit)

DEPENDS(
    ydb/library/nmslib/test/bunit
)

DATA(
    arcadia/ydb/library/nmslib/sample_data
)

END()
