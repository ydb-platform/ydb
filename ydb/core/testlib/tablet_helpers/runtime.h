#pragma once

#include <ydb/core/testlib/defs.h>

#include <ydb/core/testlib/basics/core/helpers.h>
#include <ydb/core/base/hive.h>

#include <functional>

namespace NKikimr {
    TActorId ResolveTablet(TTestActorRuntime& runtime, ui64 tabletId, ui32 nodeIndex = 0, bool sysTablet = false);
    void ForwardToTablet(TTestActorRuntime& runtime, ui64 tabletId, const TActorId& sender, IEventBase *ev, ui32 nodeIndex = 0, bool sysTablet = false);
    void InvalidateTabletResolverCache(TTestActorRuntime& runtime, ui64 tabletId, ui32 nodeIndex = 0);
    void RebootTablet(TTestActorRuntime& runtime, ui64 tabletId, const TActorId& sender, ui32 nodeIndex = 0, bool sysTablet = false);
    void GracefulRestartTablet(TTestActorRuntime& runtime, ui64 tabletId, const TActorId& sender, ui32 nodeIndex = 0);
    const TString INITIAL_TEST_DISPATCH_NAME = "Trace";

    void RunTestWithReboots(const TVector<ui64>& tabletIds, std::function<TTestActorRuntime::TEventFilter()> filterFactory,
        std::function<void(const TString& dispatchPass, std::function<void(TTestActorRuntime&)> setup, bool& activeZone)> testFunc,
        ui32 selectedReboot = Max<ui32>(), ui64 selectedTablet = Max<ui64>(), ui32 bucket = 0, ui32 totalBuckets = 0, bool killOnCommit = false);

    // Resets pipe when receiving client events
    void RunTestWithPipeResets(const TVector<ui64>& tabletIds, std::function<TTestActorRuntime::TEventFilter()> filterFactory,
        std::function<void(const TString& dispatchPass, std::function<void(TTestActorRuntime&)> setup, bool& activeZone)> testFunc,
        ui32 selectedReboot = Max<ui32>(), ui32 bucket = 0, ui32 totalBuckets = 0);

    struct TRunWithDelaysConfig {
        double DelayInjectionProbability;
        TDuration ReschedulingDelay;
        ui32 VariantsLimit;

        TRunWithDelaysConfig()
            : DelayInjectionProbability(0.2)
            , ReschedulingDelay(TDuration::MilliSeconds(300))
            , VariantsLimit(50)
        {}
    };

    void RunTestWithDelays(const TRunWithDelaysConfig& config, const TVector<ui64>& tabletIds,
        std::function<void(const TString& dispatchPass, std::function<void(TTestActorRuntime&)> setup, bool& activeZone)> testFunc);

    class ITabletScheduledEventsGuard {
    public:
        virtual ~ITabletScheduledEventsGuard() {}
    };

    TAutoPtr<ITabletScheduledEventsGuard> CreateTabletScheduledEventsGuard(const TVector<ui64>& tabletIds, TTestActorRuntime& runtime, const TActorId& sender);
    void WaitScheduledEvents(TTestActorRuntime &runtime, TDuration delay, const TActorId &sender, ui32 nodeIndex = 0);


}
