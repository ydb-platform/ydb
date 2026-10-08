#include "runtime.h"
#include "fake_hive_events.h"

#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_resolver.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/algorithm.h>
#include <util/random/mersenne.h>
#include <util/string/printf.h>
#include <util/system/env.h>

const bool SUPPRESS_REBOOTS = false;
const bool ENABLE_REBOOT_DISPATCH_LOG = true;
const bool TRACE_DELAY_TIMING = true;
const bool SUPPRESS_DELAYS = false;
const bool VARIATE_RANDOM_SEED = false;
static NActors::TTestActorRuntime& AsKikimrRuntime(NActors::TTestActorRuntimeBase& r) {
    try {
        return dynamic_cast<NActors::TTestActorRuntime&>(r);
    } catch (const std::bad_cast& e) {
        Cerr << e.what() << Endl;
        Y_ABORT("Failed to cast to TTestActorRuntime: %s", e.what());
    }
}

namespace NKikimr {
    class TTabletTracer : TNonCopyable {
    public:
        TTabletTracer(bool& tracingActive, const TVector<ui64>& tabletIds)
            : TracingActive(tracingActive)
            , TabletIds(tabletIds)
        {}

        void OnEvent(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() == TEvStateStorage::EvInfo) {
                auto info = event->CastAsLocal<TEvStateStorage::TEvInfo>();
                if (info->Status == NKikimrProto::OK && (Find(TabletIds.begin(), TabletIds.end(), info->TabletID) != TabletIds.end())) {
                    if (ENABLE_REBOOT_DISPATCH_LOG) {
                        Cerr << "Leader for TabletID " << info->TabletID << " is " << info->CurrentLeaderTablet << " sender: " << event->Sender << " recipient: " << event->Recipient << Endl;
                    }
                    if (info->CurrentLeader) {
                        TabletSys[info->TabletID] = info->CurrentLeader;
                    }
                    if (info->CurrentLeaderTablet) {
                        TabletLeaders[info->TabletID] = info->CurrentLeaderTablet;
                    } else {
                        if (ENABLE_REBOOT_DISPATCH_LOG) {
                            Cerr << "IGNORE Leader for TabletID " << info->TabletID << " is " << info->CurrentLeaderTablet << " sender: " << event->Sender << " recipient: " << event->Recipient << Endl;
                        }
                    }
                    TabletRelatedActors[info->CurrentLeaderTablet] = info->TabletID;

                }
            } else if (event->GetTypeRewrite() == TEvFakeHiveRuntime::EvNotifyTabletDeleted) {
                auto notifyEv = event->CastAsLocal<TEvFakeHiveRuntime::TEvNotifyTabletDeleted>();
                ui64 tabletId = notifyEv->TabletId;
                DeletedTablets.insert(tabletId);
                if (ENABLE_REBOOT_DISPATCH_LOG)
                    Cerr << "Forgetting tablet " << tabletId << Endl;
            }
        }

        void OnRegistration(TTestActorRuntime& runtime, const TActorId& parentId, const TActorId& actorId) {
            Y_UNUSED(runtime);
            auto it = TabletRelatedActors.find(parentId);
            if (it != TabletRelatedActors.end()) {
                TabletRelatedActors.insert(std::make_pair(actorId, it->second));
            }
        }

        const TMap<ui64, TActorId>& GetTabletSys() const {
            return TabletSys;
        }

        const TMap<ui64, TActorId>& GetTabletLeaders() const {
            return TabletLeaders;
        }

        bool IsTabletEvent(const TAutoPtr<IEventHandle>& event) const {
            for (const auto& kv : TabletLeaders) {
                if (event->GetRecipientRewrite() == kv.second) {
                    return true;
                }
            }

            return false;
        }

        bool IsTabletEvent(const TAutoPtr<IEventHandle>& event, ui64 tabletId) const {
            if (DeletedTablets.contains(tabletId))
                return false;

            auto it = TabletLeaders.find(tabletId);
            if (it != TabletLeaders.end() && event->GetRecipientRewrite() == it->second) {
                return true;
            }

            return false;
        }

        bool IsCommitResult(const TAutoPtr<IEventHandle>& event) const {
            // TEvCommitResult is sent to Executor actor not the Tablet actor
            if (event->GetTypeRewrite() == TEvTablet::TEvCommitResult::EventType) {
                return true;
            }

            return false;
        }

        bool IsCommitResult(const TAutoPtr<IEventHandle>& event, ui64 tabletId) const {
            // TEvCommitResult is sent to Executor actor not the Tablet actor
            if (event->GetTypeRewrite() == TEvTablet::TEvCommitResult::EventType &&
                event->Get<TEvTablet::TEvCommitResult>()->TabletID == tabletId)
            {
                return true;
            }

            return false;
        }

        bool IsTabletRelatedEvent(const TAutoPtr<IEventHandle>& event) {
            auto it = TabletRelatedActors.find(event->GetRecipientRewrite());
            if (it != TabletRelatedActors.end()) {
                return true;
            }

            return false;
        }

    protected:
        TMap<ui64, TActorId> TabletSys;
        TMap<ui64, TActorId> TabletLeaders;
        TMap<TActorId, ui64> TabletRelatedActors;
        TSet<ui64> DeletedTablets;
        bool& TracingActive;
        const TVector<ui64> TabletIds;
    };

    class TRebootTabletObserver : public TTabletTracer {
    public:
        TRebootTabletObserver(ui32 tabletEventCountBeforeReboot, ui64 tabletId, bool& tracingActive, const TVector<ui64>& tabletIds,
            TTestActorRuntime::TEventFilter filter, bool killOnCommit)
            : TTabletTracer(tracingActive, tabletIds)
            , TabletEventCountBeforeReboot(tabletEventCountBeforeReboot)
            , TabletId(tabletId)
            , Filter(filter)
            , KillOnCommit(killOnCommit)
            , CurrentEventCount(0)
            , HasReboot0(false)
        {
        }

        TTestActorRuntime::EEventAction OnEvent(TTestActorRuntime& runtime, TAutoPtr<IEventHandle>& event) {
            TTabletTracer::OnEvent(runtime, event);

            TActorId actor = event->Recipient;
            if (KillOnCommit && IsCommitResult(event) && HideCommitsFrom.contains(actor)) {
                // We dropped one of the previous TEvCommitResult coming to this Executore actor
                // after that we must drop all TEvCommitResult until this Executor dies
                if (ENABLE_REBOOT_DISPATCH_LOG)
                    Cerr << "!Hidden TEvCommitResult" << Endl;
                return TTestActorRuntime::EEventAction::DROP;
            }

            if (!TracingActive)
                return TTestActorRuntime::EEventAction::PROCESS;

            if (Filter(runtime, event))
                return TTestActorRuntime::EEventAction::PROCESS;

            if (!IsTabletEvent(event, TabletId) && !(KillOnCommit && IsCommitResult(event, TabletId)))
                return TTestActorRuntime::EEventAction::PROCESS;

            if (CurrentEventCount++ != TabletEventCountBeforeReboot)
                return TTestActorRuntime::EEventAction::PROCESS;

            HasReboot0 = true;
            TString eventType = event->GetTypeName();

            if (KillOnCommit && IsCommitResult(event)) {
                if (ENABLE_REBOOT_DISPATCH_LOG)
                    Cerr << "!Drop TEvCommitResult and kill " << TabletId << Endl;
                // We are going to drop current TEvCommitResult event so we must drop all
                // the following TEvCommitResult events in order not to break Tx order
                HideCommitsFrom.insert(actor);
            } else {
                runtime.PushFront(event);
            }

            TActorId targetActorId = TabletLeaders[TabletId];

            if (targetActorId == TActorId()) {
                if (ENABLE_REBOOT_DISPATCH_LOG)
                    Cerr << "!IGNORE " << TabletId << " event " << eventType << " becouse actor is null!\n";

                return TTestActorRuntime::EEventAction::DROP;
            }

            if (ENABLE_REBOOT_DISPATCH_LOG)
                Cerr << "!Reboot " << TabletId << " (actor " << targetActorId << ") on event " << eventType << " !\n";

            // We synchronously kill user part of the tablet to stop user-level logic at current event
            // However we don't kill the system part because tests historically expect pending commits to finish
            runtime.Send(new IEventHandle(targetActorId, TActorId(), new TEvents::TEvPoisonPill()));
            // Wait for the tablet to boot or to become deleted
            TDispatchOptions rebootOptions;
            rebootOptions.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvRestored, 2));
            rebootOptions.CustomFinalCondition = [this]() -> bool {
                return DeletedTablets.contains(TabletId);
            };
            runtime.DispatchEvents(rebootOptions);

            if (ENABLE_REBOOT_DISPATCH_LOG)
                Cerr << "!Reboot " << TabletId << " (actor " << targetActorId << ") rebooted!\n";

            InvalidateTabletResolverCache(runtime, TabletId);
            TDispatchOptions invalidateOptions;
            invalidateOptions.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvStateStorage::EvInfo));
            runtime.DispatchEvents(invalidateOptions);

            if (ENABLE_REBOOT_DISPATCH_LOG)
                Cerr << "!Reboot " << TabletId << " (actor " << targetActorId << ") tablet resolver refreshed! new actor is" << TabletLeaders[TabletId] << " \n";

            return TTestActorRuntime::EEventAction::DROP;
        }

        bool HasReboot() const {
            return HasReboot0;
        }

    private:
        const ui32 TabletEventCountBeforeReboot;
        const ui64 TabletId;
        const TTestActorRuntime::TEventFilter Filter;
        const bool KillOnCommit;    // Kill tablet after log is committed but before Complete() is called for Tx's
        ui32 CurrentEventCount;
        bool HasReboot0;
        TSet<TActorId> HideCommitsFrom;
    };

    // Breaks pipe after the specified number of events
    class TPipeResetObserver : public TTabletTracer {
    public:
        TPipeResetObserver(ui32 eventCountBeforeReboot, bool& tracingActive, TTestActorRuntime::TEventFilter filter, const TVector<ui64>& tabletIds)
            : TTabletTracer(tracingActive, tabletIds)
            , EventCountBeforeReboot(eventCountBeforeReboot)
            , TracingActive(tracingActive)
            , Filter(filter)
            , CurrentEventCount(0)
            , HasReset0(false)
        {}

        TTestActorRuntime::EEventAction OnEvent(TTestActorRuntime& runtime, TAutoPtr<IEventHandle>& event) {
            TTabletTracer::OnEvent(runtime, event);

            if (!TracingActive)
                return TTestActorRuntime::EEventAction::PROCESS;

            if (Filter(runtime, event))
                return TTestActorRuntime::EEventAction::PROCESS;

            // Intercept only EvSend and EvPush
            if (event->GetTypeRewrite() != TEvTabletPipe::EvSend && event->GetTypeRewrite() != TEvTabletPipe::EvPush)
                return TTestActorRuntime::EEventAction::PROCESS;

            if (CurrentEventCount++ != EventCountBeforeReboot)
                return TTestActorRuntime::EEventAction::PROCESS;

            HasReset0 = true;

            TActorId targetActorId = event->GetRecipientRewrite();

            if (ENABLE_REBOOT_DISPATCH_LOG) {
                Cerr << "!Reset pipe (actor " << targetActorId << ") on event " << event->GetTypeName() << Endl;
            }

            // Replace the event with PoisonPill in order to kill PipeClient or PipeServer
            runtime.Send(new IEventHandle(targetActorId, TActorId(), new TEvents::TEvPoisonPill()));

            return TTestActorRuntime::EEventAction::DROP;
        }

        bool HasReset() const {
            return HasReset0;
        }

    private:
        const ui32 EventCountBeforeReboot;
        bool& TracingActive;
        const TTestActorRuntime::TEventFilter Filter;
        ui32 CurrentEventCount;
        bool HasReset0;
    };


    class TDelayingObserver : public TTabletTracer {
    public:
        TDelayingObserver(bool& tracingActive, double delayInjectionProbability, const TVector<ui64>& tabletIds)
            : TTabletTracer(tracingActive, tabletIds)
            , DelayInjectionProbability(delayInjectionProbability)
            , ExecutionCount(0)
            , NormalStepsCount(0)
            , Random(VARIATE_RANDOM_SEED ? TInstant::Now().GetValue() : DefaultRandomSeed)
        {
            Decisions.Reset(new TDecisionTreeItem());
        }

        double GetDelayInjectionProbability() const {
            return DelayInjectionProbability;
        }

        TTestActorRuntime::EEventAction OnEvent(TTestActorRuntime& runtime, TAutoPtr<IEventHandle>& event) {
            TTabletTracer::OnEvent(runtime, event);
            if (!TracingActive)
                return TTestActorRuntime::EEventAction::PROCESS;

            if (!IsTabletEvent(event))
                return TTestActorRuntime::EEventAction::PROCESS;

            if (TRACE_DELAY_TIMING)
                Cout << CurrentItems.size();
            TDecisionTreeItem* currentItem = CurrentItems.back();
            if (!currentItem->NormalExecution || !currentItem->NormalExecution->Complete) {
                if (!currentItem->NormalExecution) {
                    currentItem->NormalExecution.Reset(new TDecisionTreeItem());
                    bool allowDelayedExecution = true;
                    if ((ExecutionCount > 1) && (CurrentItems.size() > NormalStepsCount)) {
                        allowDelayedExecution = false;
                    } else {
                        if (Random.GenRandReal1() >= DelayInjectionProbability) {
                            allowDelayedExecution = false;
                        }
                    }

                    if (!allowDelayedExecution) {
                        if (TRACE_DELAY_TIMING)
                            Cout << "= ";
                        currentItem->DelayedExecution.Reset(new TDecisionTreeItem());
                        currentItem->DelayedExecution->Complete = true;
                    } else {
                        if (TRACE_DELAY_TIMING)
                            Cout << "+ ";
                    }
                } else {
                    if (currentItem->DelayedExecution) {
                        if (TRACE_DELAY_TIMING)
                            Cout << "= ";
                        Y_ABORT_UNLESS(currentItem->DelayedExecution->Complete);
                    } else {
                        if (TRACE_DELAY_TIMING)
                            Cout << "+ ";
                    }
                }

                CurrentItems.push_back(currentItem->NormalExecution.Get());
                return TTestActorRuntime::EEventAction::PROCESS;
            } else if (!currentItem->DelayedExecution || !currentItem->DelayedExecution->Complete) {
                if (TRACE_DELAY_TIMING)
                    Cout << "- ";
                if (!currentItem->DelayedExecution) {
                    currentItem->DelayedExecution.Reset(new TDecisionTreeItem());
                }

                CurrentItems.push_back(currentItem->DelayedExecution.Get());
                return TTestActorRuntime::EEventAction::RESCHEDULE;
            } else {
                Y_ABORT();
            }
        }

        void PrepareExecution() {
            CurrentItems.clear();
            CurrentItems.push_back(Decisions.Get());
            ++ExecutionCount;
        }

        void FinishExecution() {
            if (TRACE_DELAY_TIMING)
                Cout << "\n";
            if (ExecutionCount == 1) {
                NormalStepsCount = CurrentItems.size();
                Cout << "Recorded execution before applying delays has " << NormalStepsCount << " steps\n";
            }

            for (auto it = CurrentItems.rbegin(); it != CurrentItems.rend(); ++it) {
                (*it)->Complete = true;
                if ((it + 1) != CurrentItems.rend()) {
                    if (!(*(it + 1))->DelayedExecution)
                        break;
                }
            }
        }

        ui64 GetExecutionCount() const {
            return ExecutionCount;
        }

        bool IsDone() const {
            return Decisions->Complete;
        }

    private:
        struct TDecisionTreeItem {
            bool Complete;

            TDecisionTreeItem()
                : Complete(false)
            {}

            TAutoPtr<TDecisionTreeItem> NormalExecution;
            TAutoPtr<TDecisionTreeItem> DelayedExecution;
        };

    private:
        const double DelayInjectionProbability;
        TAutoPtr<TDecisionTreeItem> Decisions;
        TVector<TDecisionTreeItem*> CurrentItems;
        ui64 ExecutionCount;
        ui32 NormalStepsCount;
        TMersenne<ui64> Random;
    };

    class TTabletScheduledFilter : TNonCopyable {
    public:
        TTabletScheduledFilter(TTabletTracer& tracer)
            : Tracer(tracer)
        {}

        bool operator()(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline) {
            if (runtime.IsScheduleForActorEnabled(event->GetRecipientRewrite()) || Tracer.IsTabletEvent(event)
                || Tracer.IsTabletRelatedEvent(event)) {
                deadline = runtime.GetTimeProvider()->Now() + delay;
                return false;
            }

            ui32 nodeIndex = event->GetRecipientRewrite().NodeId() - runtime.GetNodeId(0);
            if (event->GetRecipientRewrite() == runtime.GetLocalServiceId(MakeTabletResolverID(), nodeIndex)) {
                deadline = runtime.GetTimeProvider()->Now() + delay;
                return false;
            }

            return true;
        }

    private:
        TTabletTracer& Tracer;
    };

    TActorId ResolveTablet(TTestActorRuntime &runtime, ui64 tabletId, ui32 nodeIndex, bool sysTablet) {
        auto sender = runtime.AllocateEdgeActor(nodeIndex);
        runtime.Send(new IEventHandle(MakeTabletResolverID(), sender,
            new TEvTabletResolver::TEvForward(tabletId, nullptr)),
            nodeIndex, true);
        auto ev = runtime.GrabEdgeEventRethrow<TEvTabletResolver::TEvForwardResult>(sender);
        Y_ABORT_UNLESS(ev->Get()->Status == NKikimrProto::OK, "Failed to resolve tablet %" PRIu64, tabletId);
        if (sysTablet) {
            return ev->Get()->Tablet;
        } else {
            return ev->Get()->TabletActor;
        }
    }

    void ForwardToTablet(TTestActorRuntime &runtime, ui64 tabletId, const TActorId& sender, IEventBase *ev, ui32 nodeIndex, bool sysTablet) {
        runtime.Send(new IEventHandle(MakeTabletResolverID(), sender,
            new TEvTabletResolver::TEvForward(tabletId, new IEventHandle(TActorId(), sender, ev), { },
                sysTablet ? TEvTabletResolver::TEvForward::EActor::SysTablet : TEvTabletResolver::TEvForward::EActor::Tablet)), nodeIndex);
    }

    void InvalidateTabletResolverCache(TTestActorRuntime &runtime, ui64 tabletId, ui32 nodeIndex) {
        runtime.Send(new IEventHandle(MakeTabletResolverID(), TActorId(),
            new TEvTabletResolver::TEvTabletProblem(tabletId, TActorId())), nodeIndex);
    }

    void RebootTablet(TTestActorRuntime &runtime, ui64 tabletId, const TActorId& sender, ui32 nodeIndex, bool sysTablet) {
        ForwardToTablet(runtime, tabletId, sender, new TEvents::TEvPoisonPill(), nodeIndex, sysTablet);
        TDispatchOptions rebootOptions;
        rebootOptions.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
        runtime.DispatchEvents(rebootOptions);

        InvalidateTabletResolverCache(runtime, tabletId, nodeIndex);
        // FIXME: there's at least one nbs test that weirdly depends on this sleeping for at least ~50ms, unclear why
        WaitScheduledEvents(runtime, TDuration::MilliSeconds(50), sender, nodeIndex);
    }

    void GracefulRestartTablet(TTestActorRuntime &runtime, ui64 tabletId, const TActorId &sender, ui32 nodeIndex) {
        ForwardToTablet(runtime, tabletId, sender, new TEvTablet::TEvTabletStop(tabletId, TEvTablet::TEvTabletStop::ReasonStop), nodeIndex, /* sysTablet = */ true);
        TDispatchOptions rebootOptions;
        rebootOptions.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
        runtime.DispatchEvents(rebootOptions);

        InvalidateTabletResolverCache(runtime, tabletId, nodeIndex);
        WaitScheduledEvents(runtime, TDuration::MilliSeconds(50), sender, nodeIndex);
    }

    void RunTestWithReboots(const TVector<ui64>& tabletIds, std::function<TTestActorRuntime::TEventFilter()> filterFactory,
        std::function<void(const TString& dispatchPass, std::function<void(TTestActorRuntime&)> setup, bool& activeZone)> testFunc,
        ui32 selectedReboot, ui64 selectedTablet, ui32 bucket, ui32 totalBuckets, bool killOnCommit) {
        bool activeZone = false;

        if (selectedReboot == Max<ui32>())
        {
            TTabletTracer tabletTracer(activeZone, tabletIds);
            TTabletScheduledFilter scheduledFilter(tabletTracer);
            try {
                testFunc(INITIAL_TEST_DISPATCH_NAME, [&](TTestActorRuntimeBase& runtime) {
                    runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                        tabletTracer.OnEvent(AsKikimrRuntime(runtime), event);
                        return TTestActorRuntime::EEventAction::PROCESS;
                    });

                    runtime.SetRegistrationObserverFunc([&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                        tabletTracer.OnRegistration(AsKikimrRuntime(runtime), parentId, actorId);
                    });

                    runtime.SetScheduledEventFilter([&](TTestActorRuntimeBase& r, TAutoPtr<IEventHandle>& event,
                        TDuration delay, TInstant& deadline) {
                        auto& runtime = AsKikimrRuntime(r);
                        return !(!scheduledFilter(runtime, event, delay, deadline) || !TTestActorRuntime::DefaultScheduledFilterFunc(runtime, event, delay, deadline));
                    });

                    runtime.SetScheduledEventsSelectorFunc(&TTestActorRuntime::CollapsedTimeScheduledEventsSelector);
                }, activeZone);
            }
            catch (yexception& e) {
                UNIT_FAIL("Failed"
                          << " at dispatch " << INITIAL_TEST_DISPATCH_NAME
                          << " with exception " << e.what() << "\n");
            }
        }

        if (SUPPRESS_REBOOTS || GetEnv("FAST_UT")=="1")
            return;

        ui32 runCount = 0;
        for (ui64 tabletId : tabletIds) {
            if (selectedTablet != Max<ui64>() && tabletId != selectedTablet)
                continue;

            ui32 tabletEventCountBeforeReboot = 0;
            if (selectedReboot != Max<ui32>()) {
                tabletEventCountBeforeReboot = selectedReboot;
            }

            bool hasReboot = true;
            while (hasReboot) {
                if (totalBuckets && ((tabletEventCountBeforeReboot % totalBuckets) != bucket)) {
                    ++tabletEventCountBeforeReboot;
                    continue;
                }

                TString dispatchName = Sprintf("Reboot tablet %" PRIu64 " (#%" PRIu32 ") run %" PRIu32 "" , tabletId, tabletEventCountBeforeReboot, runCount);
                if (ENABLE_REBOOT_DISPATCH_LOG)
                    Cout << "===> BEGIN dispatch: " << dispatchName << "\n";

                try {
                    ++runCount;
                    activeZone = false;
                    TTestActorRuntime::TEventFilter filter = filterFactory();
                    TRebootTabletObserver rebootingObserver(tabletEventCountBeforeReboot, tabletId, activeZone, tabletIds, filter, killOnCommit);
                    TTabletScheduledFilter scheduledFilter(rebootingObserver);
                    testFunc(dispatchName,
                        [&](TTestActorRuntime& runtime) {
                            runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                                return rebootingObserver.OnEvent(AsKikimrRuntime(runtime), event);
                            });

                            runtime.SetRegistrationObserverFunc([&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                                rebootingObserver.OnRegistration(AsKikimrRuntime(runtime), parentId, actorId);
                            });

                            runtime.SetScheduledEventFilter([&](TTestActorRuntimeBase& r, TAutoPtr<IEventHandle>& event,
                                TDuration delay, TInstant& deadline) {
                                auto& runtime = AsKikimrRuntime(r);
                                return scheduledFilter(runtime, event, delay, deadline) && TTestActorRuntime::DefaultScheduledFilterFunc(runtime, event, delay, deadline);
                            });

                            runtime.SetScheduledEventsSelectorFunc(&TTestActorRuntime::CollapsedTimeScheduledEventsSelector);
                        }, activeZone);
                    hasReboot = rebootingObserver.HasReboot();
                } catch (yexception& e) {
                    UNIT_FAIL("Failed"
                              << " at dispatch " << dispatchName
                              << " with exception " << e.what() << "\n");
                }

                if (ENABLE_REBOOT_DISPATCH_LOG)
                    Cout << "===> END dispatch: " << dispatchName << "\n";

                ++tabletEventCountBeforeReboot;
                if (selectedReboot != Max<ui32>())
                    break;
            }
        }
    }

    void RunTestWithPipeResets(const TVector<ui64>& tabletIds, std::function<TTestActorRuntime::TEventFilter()> filterFactory,
        std::function<void(const TString& dispatchPass, std::function<void(TTestActorRuntime&)> setup, bool& activeZone)> testFunc,
        ui32 selectedReboot, ui32 bucket, ui32 totalBuckets) {
        bool activeZone = false;

        if (selectedReboot == Max<ui32>()) {
            TTabletTracer tabletTracer(activeZone, tabletIds);
            TTabletScheduledFilter scheduledFilter(tabletTracer);

            testFunc(INITIAL_TEST_DISPATCH_NAME, [&](TTestActorRuntime& runtime) {
                runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                    tabletTracer.OnEvent(AsKikimrRuntime(runtime), event);
                    return TTestActorRuntime::EEventAction::PROCESS;
                });

                runtime.SetRegistrationObserverFunc([&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                    tabletTracer.OnRegistration(AsKikimrRuntime(runtime), parentId, actorId);
                });

                runtime.SetScheduledEventFilter([&](TTestActorRuntimeBase& r, TAutoPtr<IEventHandle>& event,
                    TDuration delay, TInstant& deadline) {
                    auto& runtime = AsKikimrRuntime(r);
                    return scheduledFilter(runtime, event, delay, deadline) && TTestActorRuntime::DefaultScheduledFilterFunc(runtime, event, delay, deadline);
                });

                runtime.SetScheduledEventsSelectorFunc(&TTestActorRuntime::CollapsedTimeScheduledEventsSelector);
            }, activeZone);
        }

        if (SUPPRESS_REBOOTS || GetEnv("FAST_UT")=="1")
            return;

        ui32 eventCountBeforeReboot = 0;
        if (selectedReboot != Max<ui32>()) {
            eventCountBeforeReboot = selectedReboot;
        }

        bool hasReboot = true;
        while (hasReboot) {
            if (totalBuckets && ((eventCountBeforeReboot % totalBuckets) != bucket)) {
                ++eventCountBeforeReboot;
                continue;
            }

            TString dispatchName = Sprintf("Pipe reset at event #%" PRIu32, eventCountBeforeReboot);
            if (ENABLE_REBOOT_DISPATCH_LOG)
                Cout << "===> BEGIN dispatch: " << dispatchName << "\n";

            try {
                activeZone = false;
                TTestActorRuntime::TEventFilter filter = filterFactory();
                TPipeResetObserver pipeResetingObserver(eventCountBeforeReboot, activeZone, filter, tabletIds);
                TTabletScheduledFilter scheduledFilter(pipeResetingObserver);

                testFunc(dispatchName,
                    [&](TTestActorRuntime& runtime) {
                    runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                        return pipeResetingObserver.OnEvent(AsKikimrRuntime(runtime), event);
                    });

                    runtime.SetRegistrationObserverFunc([&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                        pipeResetingObserver.OnRegistration(AsKikimrRuntime(runtime), parentId, actorId);
                    });

                    runtime.SetScheduledEventFilter([&](TTestActorRuntimeBase& r, TAutoPtr<IEventHandle>& event,
                        TDuration delay, TInstant& deadline) {
                        auto& runtime = AsKikimrRuntime(r);
                        return scheduledFilter(runtime, event, delay, deadline) && TTestActorRuntime::DefaultScheduledFilterFunc(runtime, event, delay, deadline);
                    });

                    runtime.SetScheduledEventsSelectorFunc(&TTestActorRuntime::CollapsedTimeScheduledEventsSelector);
                }, activeZone);

                hasReboot = pipeResetingObserver.HasReset();
            }
            catch (yexception& e) {
                UNIT_FAIL("Failed at dispatch " << dispatchName << " with exception " << e.what() << "\n");
            }

            if (ENABLE_REBOOT_DISPATCH_LOG)
                Cout << "===> END dispatch: " << dispatchName << "\n";

            ++eventCountBeforeReboot;
            if (selectedReboot != Max<ui32>())
                break;
        }
    }

    void RunTestWithDelays(const TRunWithDelaysConfig& config, const TVector<ui64>& tabletIds,
        std::function<void(const TString& dispatchPass, std::function<void(TTestActorRuntime&)> setup, bool& activeZone)> testFunc) {
        if (SUPPRESS_DELAYS || GetEnv("FAST_UT")=="1")
            return;

        bool activeZone = false;
        TDelayingObserver delayingObserver(activeZone, config.DelayInjectionProbability, tabletIds);
        TTabletScheduledFilter scheduledFilter(delayingObserver);
        TString dispatchName;
        try {
            while (!delayingObserver.IsDone() && (delayingObserver.GetExecutionCount() < config.VariantsLimit)) {
                delayingObserver.PrepareExecution();
                dispatchName = Sprintf("Delayed execution branch #%" PRIu64, delayingObserver.GetExecutionCount());
                if (TRACE_DELAY_TIMING)
                    Cout << dispatchName << "\n";
                testFunc(dispatchName,
                    [&](TTestActorRuntime& runtime) {
                    runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                        return delayingObserver.OnEvent(AsKikimrRuntime(runtime), event);
                    });

                    runtime.SetRegistrationObserverFunc([&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                        delayingObserver.OnRegistration(AsKikimrRuntime(runtime), parentId, actorId);
                    });

                    runtime.SetScheduledEventFilter([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event,
                        TDuration delay, TInstant& deadline) {
                        return scheduledFilter(AsKikimrRuntime(runtime), event, delay, deadline);
                    });

                    runtime.SetScheduledEventsSelectorFunc(&TTestActorRuntime::CollapsedTimeScheduledEventsSelector);
                    runtime.SetReschedulingDelay(config.ReschedulingDelay);
                }, activeZone);

                delayingObserver.FinishExecution();
            }
        } catch (yexception& e) {
            Cout << "Fail at dispatch " << dispatchName << "\n";
            Cout << e.what() << "\n";
            throw;
        }

        Cout << "Processed " << delayingObserver.GetExecutionCount() << " variants using probability "
            << delayingObserver.GetDelayInjectionProbability() << "\n";
    }

    class TTabletScheduledEventsGuard : public ITabletScheduledEventsGuard {
    public:
        TTabletScheduledEventsGuard(const TVector<ui64>& tabletIds, TTestActorRuntime& runtime, const TActorId& sender)
            : Runtime(runtime)
            , Sender(sender)
            , TracingActive(true)
            , TabletTracer(TracingActive, tabletIds)
            , ScheduledFilter(TabletTracer)
        {
            PrevObserverFunc = Runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
                TabletTracer.OnEvent(AsKikimrRuntime(runtime), event);
                return TTestActorRuntime::EEventAction::PROCESS;
            });

            PrevRegistrationObserverFunc = Runtime.SetRegistrationObserverFunc(
                [&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
                TabletTracer.OnRegistration(AsKikimrRuntime(runtime), parentId, actorId);
                // Chain to the previous observer (normally
                // TTestActorRuntimeBase::DefaultRegistrationObserver) so that
                // the EnableScheduleForActor whitelist keeps being propagated
                // from parent actors to their children while the guard is
                // active.
                //
                // A tablet rebooted under the guard is respawned by
                // its bootstrapper and without this propagation the new tablet
                // instance is never whitelisted, and once the guard is destroyed
                // all the events the tablet schedules for itself are silently
                // dropped by TTestActorRuntime::DefaultScheduledFilterFunc.
                //
                // The problem was originally reproduced in a test that reboots
                // Hive. The test was hanging because some events Hive scheduled
                // to itself were never answered
                if (PrevRegistrationObserverFunc) {
                    PrevRegistrationObserverFunc(runtime, parentId, actorId);
                }
            });

            PrevScheduledFilterFunc = Runtime.SetScheduledEventFilter([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event,
                TDuration delay, TInstant& deadline) {
                if (event->GetRecipientRewrite() == Sender) {
                    deadline = runtime.GetTimeProvider()->Now() + delay;
                    return false;
                }

                return ScheduledFilter(AsKikimrRuntime(runtime), event, delay, deadline);
            });

            PrevScheduledEventsSelector = Runtime.SetScheduledEventsSelectorFunc(&TTestActorRuntime::CollapsedTimeScheduledEventsSelector);
        }

        virtual ~TTabletScheduledEventsGuard() {
            Runtime.SetObserverFunc(PrevObserverFunc);
            Runtime.SetScheduledEventFilter(PrevScheduledFilterFunc);
            Runtime.SetScheduledEventsSelectorFunc(PrevScheduledEventsSelector);
            Runtime.SetRegistrationObserverFunc(PrevRegistrationObserverFunc);
        }

    private:
        TTestActorRuntime& Runtime;
        const TActorId Sender;
        bool TracingActive;
        TTabletTracer TabletTracer;
        TTabletScheduledFilter ScheduledFilter;

        TTestActorRuntime::TEventObserver PrevObserverFunc;
        TTestActorRuntime::TScheduledEventFilter PrevScheduledFilterFunc;
        TTestActorRuntime::TScheduledEventsSelector PrevScheduledEventsSelector;
        TTestActorRuntime::TRegistrationObserver PrevRegistrationObserverFunc;
    };

    TAutoPtr<ITabletScheduledEventsGuard> CreateTabletScheduledEventsGuard(const TVector<ui64>& tabletIds, TTestActorRuntime& runtime, const TActorId& sender) {
        return TAutoPtr<ITabletScheduledEventsGuard>(new TTabletScheduledEventsGuard(tabletIds, runtime, sender));
    }

    ui64 GetFreePDiskSize(TTestActorRuntime& runtime, const TActorId& sender) {
        TActorId pdiskServiceId = MakeBlobStoragePDiskID(runtime.GetNodeId(0), 0);
        runtime.Send(new IEventHandle(pdiskServiceId, sender, nullptr));
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<NMon::TEvHttpInfoRes>(handle);
        UNIT_ASSERT(event);
        //Cout << event->Answer << "\n";
        ui64 totalFreeSize = 0;
        for (ui32 i = 0; i < 2; ++i) {
            TString regex = Sprintf(".*sensor=%s:\\s(\\d+).*", i == 0 ? "FreeChunks" : "UntrimmedFreeChunks");
            TRegExBase matcher(regex);
            regmatch_t groups[2] = {};
            matcher.Exec(event->Answer.data(), groups, 0, 2);
            const ui64 freeSize = IntFromString<ui64, 10>(event->Answer.data() + groups[1].rm_so, groups[1].rm_eo - groups[1].rm_so);
            totalFreeSize += freeSize;
        }

        return totalFreeSize;
    };

    NTabletPipe::TClientConfig GetPipeConfigWithRetriesAndFollowers() { // with blackjack and hookers... (c)
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        pipeConfig.AllowFollower = true;
        return pipeConfig;
    }

    void WaitScheduledEvents(TTestActorRuntime &runtime, TDuration delay, const TActorId &sender, ui32 nodeIndex) {
        runtime.Schedule(new IEventHandle(sender, sender, new TEvents::TEvWakeup()), delay, nodeIndex);
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEvent<TEvents::TEvWakeup>(handle);
    }

    /**
     * A special actor, which starts a tablet follower and restarts it, if needed.
     */

}
