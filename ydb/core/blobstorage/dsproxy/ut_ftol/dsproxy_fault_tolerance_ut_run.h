#pragma once

#include "dsproxy_fault_tolerance_ut_runtime.h"

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
