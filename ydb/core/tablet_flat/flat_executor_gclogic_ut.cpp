#include "flat_executor_gclogic.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

Y_UNIT_TEST_SUITE(TFlatTableExecutorGC) {
    bool TestDeduplication(TVector<TLogoBlobID> keep, TVector<TLogoBlobID> dontkeep, ui32 gen, ui32 step, TVector<TLogoBlobID> expectKeep, TVector<TLogoBlobID> expectnot) {
        DeduplicateGCKeepVectors(&keep, &dontkeep, gen, step);
        return (keep == expectKeep) && (dontkeep == expectnot);
    }

    Y_UNIT_TEST(TestGCVectorDeduplicaton) {
        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            0, 0,
            {
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            }
        ));


        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            1, 0,
            {
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 2, 1, 0, 1),
            }
        ));

        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
                TLogoBlobID(1, 1, 6, 1, 0, 0),
            },
            1, 3,
            {
                TLogoBlobID(1, 1, 3, 1, 0, 0),
                TLogoBlobID(1, 1, 4, 1, 0, 0),
                TLogoBlobID(1, 1, 5, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 0),
                TLogoBlobID(1, 1, 2, 1, 0, 1),
            }
        ));

        UNIT_ASSERT(TestDeduplication(
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 2, 1, 0, 0),
            },
            0, 0,
            {
                TLogoBlobID(1, 1, 1, 1, 0, 0),
            },
            {
                TLogoBlobID(1, 1, 2, 1, 0, 0),
            }
        ));
    }
}

}
}
