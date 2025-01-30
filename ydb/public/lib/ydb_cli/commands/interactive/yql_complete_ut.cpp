#include "yql_complete.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(YqlCompleteTests) {
    Y_UNIT_TEST(CaseInsensitivity) {
        TVector<TCandidate> expected = {
            {ECandidateKind::Keyword, "SELECT"},
        };

        TYqlCompletionEngine engine;
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete({"se"}).Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete({"sE"}).Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete({"Se"}).Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(engine.Complete({"SE"}).Candidates, expected);
    }
} // Y_UNIT_TEST_SUITE(YqlCompleteTests)
