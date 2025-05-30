#include "ranking.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(FrequencyTests) {

    Y_UNIT_TEST(FrequencyDataIsParsable) {
        TFrequencyData data = LoadFrequencyData();
        Y_UNUSED(data);
    }

} // Y_UNIT_TEST_SUITE(FrequencyTests)
