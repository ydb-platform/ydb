PROGRAM(bunit)

NO_COMPILER_WARNINGS()

PEERDIR(
    ydb/library/nmslib
    library/cpp/testing/common
)

# Shared test headers (bunit.h, genrand_vect.h, testdataset.h) live in the
# parent test/ directory.
ADDINCL(
    ydb/library/nmslib/include
    ydb/library/nmslib/test
)

# Resolve sample data via the Arcadia source root (see test/testdataset.h).
CFLAGS(
    -DNMSLIB_ARCADIA_TEST
)

SRCS(
    bunit.cc
    test_distfunc.cc
    test_editdist.cc
    test_eval.cc
    test_falconn_heap.cc
    test_fp.cc
    test_lpnorm.cc
    test_object.cc
    test_overlap.cc
    test_pow.cc
    test_some_stat.cc
    test_space_scalar.cc
    test_space_serial.cc
    test_sqfd.cc
    test_thread_pool.cc
    test_timer.cc
)

END()
