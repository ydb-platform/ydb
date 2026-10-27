#include "../dsproxy_fault_tolerance_ut_run.h"
#include "../dsproxy_fault_tolerance_ut_get.h"
#include "../dsproxy_fault_tolerance_ut_get_hardened.h"

Y_UNIT_TEST_SUITE(TBsProxyFaultToleranceTest) {

#define FAULT_TOLERANCE_TEST(ERASURE, T) \
    Y_UNIT_TEST(Check##T##ERASURE) { \
        RunTest<T>(TBlobStorageGroupType::ERASURE); \
    }

#define ERASURE_TEST(ERASURE) \
    FAULT_TOLERANCE_TEST(ERASURE, TGetWithRecoverFaultToleranceTest)

    ERASURE_TEST(Erasure4Plus2Block)
    ERASURE_TEST(ErasureMirror3dc)
    ERASURE_TEST(ErasureMirror3of4)

    Y_UNIT_TEST(CheckGetHardenedErasureMirror3dcCount6Idx0) { RunTest<TGetHardenedFaultToleranceTest>(TBlobStorageGroupType::ErasureMirror3dc, true, 6, 0); }
    Y_UNIT_TEST(CheckGetHardenedErasureMirror3dcCount6Idx1) { RunTest<TGetHardenedFaultToleranceTest>(TBlobStorageGroupType::ErasureMirror3dc, true, 6, 1); }
    Y_UNIT_TEST(CheckGetHardenedErasureMirror3dcCount6Idx2) { RunTest<TGetHardenedFaultToleranceTest>(TBlobStorageGroupType::ErasureMirror3dc, true, 6, 2); }
    Y_UNIT_TEST(CheckGetHardenedErasureMirror3dcCount6Idx3) { RunTest<TGetHardenedFaultToleranceTest>(TBlobStorageGroupType::ErasureMirror3dc, true, 6, 3); }
    Y_UNIT_TEST(CheckGetHardenedErasureMirror3dcCount6Idx4) { RunTest<TGetHardenedFaultToleranceTest>(TBlobStorageGroupType::ErasureMirror3dc, true, 6, 4); }
    Y_UNIT_TEST(CheckGetHardenedErasureMirror3dcCount6Idx5) { RunTest<TGetHardenedFaultToleranceTest>(TBlobStorageGroupType::ErasureMirror3dc, true, 6, 5); }

    Y_UNIT_TEST(CheckGetHardenedErasureBlock42Count6Idx0) { RunTest<TGetHardenedFaultToleranceTest>(TBlobStorageGroupType::Erasure4Plus2Block, true, 6, 0); }
    Y_UNIT_TEST(CheckGetHardenedErasureBlock42Count6Idx1) { RunTest<TGetHardenedFaultToleranceTest>(TBlobStorageGroupType::Erasure4Plus2Block, true, 6, 1); }
    Y_UNIT_TEST(CheckGetHardenedErasureBlock42Count6Idx2) { RunTest<TGetHardenedFaultToleranceTest>(TBlobStorageGroupType::Erasure4Plus2Block, true, 6, 2); }
    Y_UNIT_TEST(CheckGetHardenedErasureBlock42Count6Idx3) { RunTest<TGetHardenedFaultToleranceTest>(TBlobStorageGroupType::Erasure4Plus2Block, true, 6, 3); }
    Y_UNIT_TEST(CheckGetHardenedErasureBlock42Count6Idx4) { RunTest<TGetHardenedFaultToleranceTest>(TBlobStorageGroupType::Erasure4Plus2Block, true, 6, 4); }
    Y_UNIT_TEST(CheckGetHardenedErasureBlock42Count6Idx5) { RunTest<TGetHardenedFaultToleranceTest>(TBlobStorageGroupType::Erasure4Plus2Block, true, 6, 5); }

}
