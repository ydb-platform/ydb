#include "yql_complete.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(YqlCompleteTests) {
    Y_UNIT_TEST(CaseInsensitivity) {
        TVector<TCandidate> expected = {
            {ECandidateKind::Keyword, "SELECT"},
        };

        TYqlCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete({"se"}), expected);
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete({"sE"}), expected);
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete({"Se"}), expected);
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete({"SE"}), expected);
    }
} // Y_UNIT_TEST_SUITE(YqlCompleteTests)
