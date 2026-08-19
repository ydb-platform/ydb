PROGRAM(test_integr)

NO_COMPILER_WARNINGS()

PEERDIR(
    ydb/library/nmslib
    library/cpp/testing/common
)

# Shared test headers (test_integr_util.h, testdataset.h) live in the parent
# test/ directory.
ADDINCL(
    ydb/library/nmslib/include
    ydb/library/nmslib/test
)

# Resolve sample data via the Arcadia source root (see test/testdataset.h).
CFLAGS(
    -DNMSLIB_ARCADIA_TEST
)

SRCS(
    test_integr.cc
)

END()
