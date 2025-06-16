#include "ranking.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(RankingTests) {

    Y_UNIT_TEST(RankingIsBuilt) {
        auto ranking = MakeDefaultRanking(LoadFrequencyData());
        Y_DO_NOT_OPTIMIZE_AWAY(ranking);
    }

} // Y_UNIT_TEST_SUITE(RankingTests)
