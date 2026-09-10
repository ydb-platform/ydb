#include <library/cpp/testing/unittest/registar.h>

// DEMO ONLY, must not be merged (see ydb-platform/ydb#52773).
//
// This suite lives on a path that already has a muted test in muted_ya.txt, so the path
// lands in the try_2 rerun blacklist. Before the fix that made ya drop the whole suite
// from the rerun, and the failure below silently disappeared from try_2.
Y_UNIT_TEST_SUITE(RerunBlacklistDemo) {
    Y_UNIT_TEST(AlwaysFails) {
        UNIT_ASSERT_C(false, "intentional failure: must be rerun in try_2, not skipped");
    }
}
