#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>
#include <ydb/core/mind/bscontroller/blob_checker_events.h>

#include <utility>

namespace NKikimr::NBsController {

Y_UNIT_TEST_SUITE(BlobCheckerE2E) {

struct TTestCtx : TTestCtxBase {
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

    TTestCtx()
        : TTestCtxBase(TEnvironmentSetup::TSettings{
            .NodeCount = 3,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .ControllerNodeId = 3,
        })
    {}

    void Setup() {
        Initialize();
        WriteCompressedData({
            .GroupId = GroupId,
            .TotalBlobs = 1,
            .BlobSize = 1_KB,
        });
    }

    TCounterPtr FindBlobCheckerCounter(TStringBuf name) const {
        const auto& root = Env->Runtime->GetNode(Env->Settings.ControllerNodeId)->AppData->Counters;
        const auto service = root->FindSubgroup("counters", "storage_pool_stat");
        if (!service) {
            return {};
        }
        const auto blobChecker = service->FindSubgroup("subsystem", "blob_checker");
        return blobChecker ? blobChecker->FindCounter(TString(name)) : TCounterPtr{};
    }

    ui64 GetBlobCheckerCounter(TStringBuf name) const {
        const auto counter = FindBlobCheckerCounter(name);
        return counter ? counter->Val() : 0;
    }

    template<typename TPredicate>
    void WaitUntil(TPredicate&& predicate, TStringBuf description,
            TDuration timeout = TDuration::Minutes(3)) {
        const TInstant deadline = Env->Now() + timeout;
        bool done = predicate();
        while (!done && Env->Now() < deadline) {
            Env->Sim(TDuration::Seconds(1));
            done = predicate();
        }
        UNIT_ASSERT_C(done, TStringBuilder() << "Timed out waiting for " << description);
    }

    void SetPeriodicity(TDuration periodicity) {
        Env->UpdateSettings({
            .BlobCheckerPeriodicity = periodicity,
        });
    }
};

Y_UNIT_TEST(EnableDisableReenable) {
    ui32 planRequests = 0;
    TTestCtx ctx;
    ctx.Setup();

    ctx.Env->Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobCheckerPlanCheck::EventType) {
            ++planRequests;
        }
        return true;
    };

    ctx.SetPeriodicity(TDuration::Seconds(1));
    ctx.WaitUntil([&] { return ctx.GetBlobCheckerCounter("ChecksCompleted") != 0; },
        "the first BlobChecker pass");

    ctx.SetPeriodicity(TDuration::Zero());
    // Let the settings notification and any already queued cancellation
    // acknowledgement settle before measuring the disabled interval.
    ctx.Env->Sim(TDuration::Zero());
    const ui32 requestsAtDisable = planRequests;
    const ui64 completedAtDisable = ctx.GetBlobCheckerCounter("ChecksCompleted");
    ctx.Env->Sim(TDuration::Minutes(2));
    UNIT_ASSERT_VALUES_EQUAL(planRequests, requestsAtDisable);
    UNIT_ASSERT_VALUES_EQUAL(ctx.GetBlobCheckerCounter("ChecksCompleted"), completedAtDisable);

    ctx.SetPeriodicity(TDuration::Seconds(1));
    ctx.WaitUntil([&] {
        return planRequests > requestsAtDisable &&
            ctx.GetBlobCheckerCounter("ChecksCompleted") > completedAtDisable;
    }, "a BlobChecker pass after re-enabling");
}

Y_UNIT_TEST(SettingsAndGroupStateSurviveControllerRestart) {
    ui32 planRequests = 0;
    bool firstRequestDropped = false;
    TTestCtx ctx;
    ctx.Setup();

    ctx.Env->Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobCheckerPlanCheck::EventType) {
            ++planRequests;
            if (!std::exchange(firstRequestDropped, true)) {
                return false;
            }
        }
        return true;
    };

    ctx.SetPeriodicity(TDuration::Seconds(10));
    ctx.WaitUntil([&] { return firstRequestDropped; }, "the initial planning request");
    ctx.Env->Sim(TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(ctx.GetBlobCheckerCounter("ChecksCompleted"), 0);

    ctx.Env->RestartNode(ctx.Env->Settings.ControllerNodeId);
    ctx.WaitUntil([&] {
        return planRequests >= 2 && ctx.GetBlobCheckerCounter("ChecksCompleted") != 0;
    }, "BlobChecker to resume after the controller restart");
}

} // Y_UNIT_TEST_SUITE(BlobCheckerE2E)

} // namespace NKikimr::NBsController
