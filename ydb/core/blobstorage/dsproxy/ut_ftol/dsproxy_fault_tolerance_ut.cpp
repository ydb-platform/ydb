#include "dsproxy_fault_tolerance_ut_runtime.h"
#include "dsproxy_fault_tolerance_ut_get.h"
#include "dsproxy_fault_tolerance_ut_get_hardened.h"
#include "dsproxy_fault_tolerance_ut_discover.h"
#include "dsproxy_fault_tolerance_ut_range.h"
#include "dsproxy_fault_tolerance_ut_put.h"

using namespace NActors;
using namespace NKikimr;

template<typename T>
void RunTest(TBlobStorageGroupType groupType, bool minimum = false, ui32 testPartCount = 1, ui32 testPartIdx = 0) {
    ui32 numVDisks;
    ui32 numFailDomains;
    ui32 numFailRealms;

    switch (groupType.GetErasure()) {
        case TBlobStorageGroupType::ErasureMirror3dc:
            numVDisks = 1;
            numFailDomains = minimum ? 3 : 4;
            numFailRealms = 3;
            break;

        default:
            numVDisks = 1;
            numFailDomains = groupType.BlobSubgroupSize() + (minimum ? 0 : 1);
            numFailRealms = 1;
            break;
    }

    TFaultToleranceTestRuntime runtime;
    runtime.Setup(groupType, numFailDomains, numVDisks, numFailRealms);

    TAutoEvent finishEvent;
    std::exception_ptr eptr;
    auto test = MakeHolder<T>(&finishEvent, &eptr, NUnitTest::NPrivate::GetCurrentTest(), runtime,
            testPartCount, testPartIdx);
    runtime.ActorSystem->Register(new TActorCoro(std::move(test)), TMailboxType::Simple, 0, {});
    finishEvent.WaitI();
    runtime.Finish();
    if (eptr) {
        std::rethrow_exception(eptr);
    }
}

Y_UNIT_TEST_SUITE(TBsProxyFaultToleranceTest) {

#define FAULT_TOLERANCE_TEST(ERASURE, T) \
    Y_UNIT_TEST(Check##T##ERASURE) { \
        RunTest<T>(TBlobStorageGroupType::ERASURE); \
    }

#define ERASURE_TEST(ERASURE) \
    FAULT_TOLERANCE_TEST(ERASURE, TGetWithRecoverFaultToleranceTest) \
    FAULT_TOLERANCE_TEST(ERASURE, TRangeFaultToleranceTest) \
    FAULT_TOLERANCE_TEST(ERASURE, TDiscoverFaultToleranceTest) \
    FAULT_TOLERANCE_TEST(ERASURE, TPutFaultToleranceTest)

    //ERASURE_TEST(ErasureMirror3)
    //ERASURE_TEST(Erasure3Plus1Block)
    //ERASURE_TEST(Erasure3Plus1Stripe)
    ERASURE_TEST(Erasure4Plus2Block)
    //ERASURE_TEST(Erasure3Plus2Block)
    //ERASURE_TEST(Erasure4Plus2Stripe)
    //ERASURE_TEST(Erasure3Plus2Stripe)
    //ERASURE_TEST(ErasureMirror3Plus2)
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
