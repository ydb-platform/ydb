#include "hullbase_barrier.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TBlobStorageKeyBarrierTest) {

        Y_UNIT_TEST(ParseTest) {
            TKeyBarrier id;
            TKeyBarrier expected;
            TString explanation;
            bool res = false;

            res = TKeyBarrier::Parse(id, "[ 0:0:34:15]", explanation);
            expected = TKeyBarrier(0, 0, 34, 15, false);
            UNIT_ASSERT(res && id == expected);

            res = TKeyBarrier::Parse(id, "[1000:0:34:15  ]", explanation);
            expected = TKeyBarrier(1000, 0, 34, 15, false);
            UNIT_ASSERT(res && id == expected);

            res = TKeyBarrier::Parse(id, "[1000:0:34:15 ", explanation);
            UNIT_ASSERT(!res && explanation == "Can't find trailing ']' after generation counter");


            res = TKeyBarrier::Parse(id, "[1000:0: x34:15 ", explanation);
            UNIT_ASSERT(!res && explanation == "Can't find trailing ':' after generation");
        }
    }

} // NKikimr
