#include <library/cpp/testing/unittest/registar.h>

#include <cstdlib>

// DEMO ONLY, must not be merged (see ydb-platform/ydb#52773).
//
// The test has to crash, not fail an assertion: a plain failure is recorded per test in the
// ya last-failed cache and -X reruns it with a per-test filter, which works fine. Only a
// crashed chunk marks the whole suite, and then -X restarts the suite without per-test
// filters. This suite lives on a path that also has a muted test, so the path lands in the
// try_2 blacklist; before the fix ya dropped the whole suite from the rerun at that point
// and the crash silently disappeared from try_2.
Y_UNIT_TEST_SUITE(RerunBlacklistDemo) {
    Y_UNIT_TEST(Crashes) {
        abort();
    }
}
