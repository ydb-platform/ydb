#include "tablet_helpers.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/scheme/tablet_scheme.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/tx/coordinator/coordinator.h>
#include <ydb/core/tx/mediator/mediator.h>
#include <ydb/core/tx/replication/controller/controller.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/sequenceshard/sequenceshard.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/persqueue/pq_l2_service.h>
#include <ydb/core/util/console.h>

#include <google/protobuf/text_format.h>

#include <util/folder/dirut.h>
#include <util/random/mersenne.h>
#include <library/cpp/regex/pcre/regexp.h>
#include <util/string/printf.h>
#include <util/string/subst.h>
#include <util/system/env.h>
#include <util/system/sanitizers.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/kesus/tablet/tablet.h>
#include <ydb/core/keyvalue/keyvalue.h>
#include <ydb/core/persqueue/pq.h>
#include <ydb/core/sys_view/processor/processor.h>
#include <ydb/core/statistics/aggregator/aggregator.h>
#include <ydb/core/graph/api/shard.h>

#include <ydb/core/testlib/basics/storage.h>
#include <ydb/core/testlib/basics/appdata.h>

const bool SUPPRESS_REBOOTS = false;
const bool ENABLE_REBOOT_DISPATCH_LOG = true;
const bool TRACE_DELAY_TIMING = true;
const bool SUPPRESS_DELAYS = false;
const bool VARIATE_RANDOM_SEED = false;
const ui64 PQ_CACHE_MAX_SIZE_MB = 32;
const TDuration PQ_CACHE_KEEP_TIMEOUT = TDuration::Seconds(10);

static NActors::TTestActorRuntime& AsKikimrRuntime(NActors::TTestActorRuntimeBase& r) {
    try {
        return dynamic_cast<NActors::TTestBasicRuntime&>(r);
    } catch (const std::bad_cast& e) {
        Cerr << e.what() << Endl;
        Y_ABORT("Failed to cast to TTestActorRuntime: %s", e.what());
    }
}

namespace NKikimr {

    class TFakeMediatorTimecastProxy : public TActor<TFakeMediatorTimecastProxy> {
    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::TX_MEDIATOR_TIMECAST_ACTOR;
        }

        TFakeMediatorTimecastProxy()
            : TActor(&TFakeMediatorTimecastProxy::StateFunc)
        {}

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvMediatorTimecast::TEvRegisterTablet, Handle);
            }
        }

        void Handle(TEvMediatorTimecast::TEvRegisterTablet::TPtr &ev, const TActorContext &ctx) {
            const ui64 tabletId = ev->Get()->TabletId;
            auto& entry = Entries[tabletId];
            if (!entry) {
                entry = new TMediatorTimecastSharedEntry();
            }

            ctx.Send(ev->Sender, new TEvMediatorTimecast::TEvRegisterTabletResult(tabletId, new TMediatorTimecastEntry(entry, entry)));
        }

    private:
        THashMap<ui64, TIntrusivePtr<TMediatorTimecastSharedEntry>> Entries;
    };

    void SetupMediatorTimecastProxy(TTestActorRuntime& runtime, ui32 nodeIndex, bool useFake = false)
    {
        runtime.AddLocalService(
            MakeMediatorTimecastProxyID()
            , TActorSetupCmd(useFake ? new TFakeMediatorTimecastProxy() : CreateMediatorTimecastProxy()
                            , TMailboxType::Revolving, 0)
            , nodeIndex);
    }

    void SetupTabletCountersAggregator(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        runtime.AddLocalService(MakeTabletCountersAggregatorID(runtime.GetNodeId(nodeIndex)),
            TActorSetupCmd(CreateTabletCountersAggregator(false), TMailboxType::Revolving, 0), nodeIndex);
    }

    void SetupPQNodeCache(TTestActorRuntime& runtime, ui32 nodeIndex)
    {
        struct NPQ::TCacheL2Parameters l2Params = {PQ_CACHE_MAX_SIZE_MB, PQ_CACHE_KEEP_TIMEOUT};
        runtime.AddLocalService(NPQ::MakePersQueueL2CacheID(),
            TActorSetupCmd(
                NPQ::CreateNodePersQueueL2Cache(l2Params, runtime.GetDynamicCounters(0)),
                TMailboxType::Simple, 0),
            nodeIndex);
    }

    struct TUltimateNodes : public NFake::INode {
        TUltimateNodes(TTestActorRuntime &runtime, const TAppPrepare *app)
            : Runtime(runtime)
        {

            if (runtime.IsRealThreads()) {
                return;
            }

            const auto& domainsInfo = app->Domains;
            if (!domainsInfo || !domainsInfo->Domain) {
                return;
            }

            const TDomainsInfo::TDomain *domain = domainsInfo->GetDomain();
            UseFakeTimeCast |= domain->Mediators.size() == 0;
        }

        void Birth(ui32 node) noexcept override
        {
            SetupMediatorTimecastProxy(Runtime, node, UseFakeTimeCast);
            SetupMonitoringProxy(Runtime, node);
            SetupTabletCountersAggregator(Runtime, node);
            SetupGRpcProxyStatus(Runtime, node);
            SetupNodeWhiteboard(Runtime, node);
            SetupNodeTabletMonitor(Runtime, node);
            SetupPQNodeCache(Runtime, node);
        }

        TTestActorRuntime &Runtime;
        bool UseFakeTimeCast = false;
    };

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
            } else if (event->GetTypeRewrite() == TEvFakeHive::EvNotifyTabletDeleted) {
                auto notifyEv = event->CastAsLocal<TEvFakeHive::TEvNotifyTabletDeleted>();
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

            if (ENABLE_REBOOT_DISPATCH_LOG)
                Cerr << "!Reset pipe\n";

            // Replace the event with PoisonPill in order to kill PipeClient or PipeServer
            TActorId targetActorId = event->GetRecipientRewrite();
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

    TActorId FollowerTablet(TTestActorRuntime &runtime, const TActorId &launcher, TTabletStorageInfo *info, std::function<IActor * (const TActorId &, TTabletStorageInfo *)> op) {
        return runtime.Register(CreateTabletFollower(launcher, info, new TTabletSetupInfo(op, TMailboxType::Simple, 0, TMailboxType::Simple, 0), 0, new TResourceProfiles));
    }

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

    void SetupTabletServices(TTestActorRuntime &runtime, TAppPrepare *app, bool mockDisk, NFake::TStorage storage,
                            NFake::TCaches caches, bool forceFollowers) {
        TAutoPtr<TAppPrepare> dummy;
        if (app == nullptr) {
            dummy = app = new TAppPrepare;
        }
        TUltimateNodes nodes(runtime, app);
        SetupBasicServices(runtime, *app, mockDisk, &nodes, storage, caches, forceFollowers);
    }

    TDomainsInfo::TDomain::TStoragePoolKinds DefaultPoolKinds(ui32 count) {
        TDomainsInfo::TDomain::TStoragePoolKinds storagePoolKinds;

        for (ui32 poolNum = 1; poolNum <= count; ++poolNum) {
            TString poolKind = "pool-kind-" + ToString(poolNum);
            NKikimrBlobStorage::TDefineStoragePool& hddPool = storagePoolKinds[poolKind];
            hddPool.SetBoxId(1);
            hddPool.SetErasureSpecies("none");
            hddPool.SetVDiskKind("Default");
            hddPool.AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::ROT);
            hddPool.SetKind(poolKind);
            hddPool.SetStoragePoolId(poolNum);
            hddPool.SetName("pool-" + ToString(poolNum));
        }

        return storagePoolKinds;
    }

    i64 SetSplitMergePartCountLimit(TTestActorRuntime* runtime, i64 val) {
        TAtomic prev;
        runtime->GetAppData().Icb->SetValue("SchemeShard_SplitMergePartCountLimit", val, prev);
        return prev;
    }

    bool SetAllowServerlessStorageBilling(TTestActorRuntime* runtime, bool isAllow) {
        TAtomic prev;
        runtime->GetAppData().Icb->SetValue("SchemeShard_AllowServerlessStorageBilling", isAllow, prev);
        return prev;
    }

    void SetupChannelProfiles(TAppPrepare &app, ui32 nchannels) {
        auto& poolKinds = app.Domains->GetDomain()->StoragePoolTypes;
        Y_ABORT_UNLESS(!poolKinds.empty());

        TIntrusivePtr<TChannelProfiles> channelProfiles = new TChannelProfiles;

        {//Set unexisted pool type for default profile # 0
            channelProfiles->Profiles.emplace_back();
            auto& profile = channelProfiles->Profiles.back();
            for (ui32 channelIdx = 0; channelIdx < nchannels; ++channelIdx) {
                profile.Channels.emplace_back(TBlobStorageGroupType::ErasureNone, 0, NKikimrBlobStorage::TVDiskKind::Default, poolKinds.begin()->first);
            }
        }

        //add mixed pool profile # 1
        if (poolKinds) {
            channelProfiles->Profiles.emplace_back();
            TChannelProfiles::TProfile &profile = channelProfiles->Profiles.back();
            auto poolIt = poolKinds.begin();

            profile.Channels.emplace_back(TBlobStorageGroupType::ErasureNone, 0, NKikimrBlobStorage::TVDiskKind::Default, poolIt->first);

            if (poolKinds.size() > 1) {
                ++poolIt;
            }
            profile.Channels.emplace_back(TBlobStorageGroupType::ErasureNone, 0, NKikimrBlobStorage::TVDiskKind::Default, poolIt->first);

            if (poolKinds.size() > 2) {
                ++poolIt;

                profile.Channels.emplace_back(TBlobStorageGroupType::ErasureNone, 0, NKikimrBlobStorage::TVDiskKind::Default, poolIt->first);
            }
        }

        //add one pool profile for each pool # 2 .. poolKinds + 2
        for (auto& kind: poolKinds) {
            channelProfiles->Profiles.emplace_back();
            auto& profile = channelProfiles->Profiles.back();
            for (ui32 channelIdx = 0; channelIdx < nchannels; ++channelIdx) {
                profile.Channels.emplace_back(TBlobStorageGroupType::ErasureNone, 0, NKikimrBlobStorage::TVDiskKind::Default, kind.first);
            }
        }

        app.SetChannels(std::move(channelProfiles));
    }

    void SetupBoxAndStoragePool(TTestActorRuntime &runtime, const TActorId& sender, ui32 nGroups) {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();

        //get NodesInfo, nodes hostname and port are interested
        runtime.Send(new IEventHandle(GetNameserviceActorId(), sender, new TEvInterconnect::TEvListNodes));
        TAutoPtr<IEventHandle> handleNodesInfo;
        auto nodesInfo = runtime.GrabEdgeEventRethrow<TEvInterconnect::TEvNodesInfo>(handleNodesInfo);
        auto bsConfigureRequest = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();

        NKikimrBlobStorage::TDefineBox boxConfig;
        boxConfig.SetBoxId(1);

        ui32 nodeId = runtime.GetNodeId(0);
        Y_ABORT_UNLESS(nodesInfo->Nodes[0].NodeId == nodeId);
        auto& nodeInfo = nodesInfo->Nodes[0];

        NKikimrBlobStorage::TDefineHostConfig hostConfig;
        hostConfig.SetHostConfigId(nodeId);
        TString path = TStringBuilder() << runtime.GetTempDir() << "pdisk_1.dat";
        hostConfig.AddDrive()->SetPath(path);
        Cerr << "tablet_helpers.cpp: SetPath # " << path << Endl;
        bsConfigureRequest->Record.MutableRequest()->AddCommand()->MutableDefineHostConfig()->CopyFrom(hostConfig);

        auto &host = *boxConfig.AddHost();
        host.MutableKey()->SetFqdn(nodeInfo.Host);
        host.MutableKey()->SetIcPort(nodeInfo.Port);
        host.SetHostConfigId(hostConfig.GetHostConfigId());
        bsConfigureRequest->Record.MutableRequest()->AddCommand()->MutableDefineBox()->CopyFrom(boxConfig);

        for (const auto& [kind, pool] : runtime.GetAppData().DomainsInfo->GetDomain()->StoragePoolTypes) {
            NKikimrBlobStorage::TDefineStoragePool storagePool(pool);
            storagePool.SetNumGroups(nGroups);
            bsConfigureRequest->Record.MutableRequest()->AddCommand()->MutableDefineStoragePool()->CopyFrom(storagePool);
        }

        runtime.SendToPipe(MakeBSControllerID(), sender, bsConfigureRequest.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handleConfigureResponse;
        auto configureResponse = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(handleConfigureResponse);
        if (!configureResponse->Record.GetResponse().GetSuccess()) {
            Cerr << "\n\n configResponse is #" << configureResponse->Record.DebugString() << "\n\n";
        }
        UNIT_ASSERT(configureResponse->Record.GetResponse().GetSuccess());
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

    class TFakeHive : public TActor<TFakeHive>, public NTabletFlatExecutor::TTabletExecutedFlat {
    public:
        static std::function<IActor* (const TActorId &, TTabletStorageInfo*)> DefaultGetTabletCreationFunc(ui32 type) {
            Y_UNUSED(type);
            return nullptr;
        }

        using TTabletInfo = TFakeHiveTabletInfo;
        using TState = TFakeHiveState;

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::HIVE_ACTOR;
        }

        TFakeHive(const TActorId &tablet, TTabletStorageInfo *info, TState::TPtr state,
                  TGetTabletCreationFunc getTabletCreationFunc)
            : TActor<TFakeHive>(&TFakeHive::StateInit)
            , NTabletFlatExecutor::TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
            , State(state)
            , GetTabletCreationFunc(getTabletCreationFunc)
        {
        }

        void DefaultSignalTabletActive(const TActorContext &) override {
            // must be empty
        }

        void OnActivateExecutor(const TActorContext &ctx) final {
            Become(&TFakeHive::StateWork);
            SignalTabletActive(ctx);

            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] started, primary subdomain " << PrimarySubDomainKey);
        }

        void OnDetach(const TActorContext &ctx) override {
            Die(ctx);
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override {
            Y_UNUSED(ev);
            Die(ctx);
        }

        void StateInit(STFUNC_SIG) {
            StateInitImpl(ev, SelfId());
        }

        void StateWork(STFUNC_SIG) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
                HFunc(TEvHive::TEvConfigureHive, Handle);
                HFunc(TEvHive::TEvCreateTablet, Handle);
                HFunc(TEvHive::TEvAdoptTablet, Handle);
                HFunc(TEvHive::TEvDeleteTablet, Handle);
                HFunc(TEvHive::TEvDeleteOwnerTablets, Handle);
                HFunc(TEvHive::TEvRequestHiveInfo, Handle);
                HFunc(TEvHive::TEvInitiateTabletExternalBoot, Handle);
                HFunc(TEvHive::TEvUpdateTabletsObject, Handle);
                HFunc(TEvFakeHive::TEvSubscribeToTabletDeletion, Handle);
                HFunc(TEvHive::TEvUpdateDomain, Handle);
                HFunc(TEvFakeHive::TEvRequestDomainInfo, Handle);
                HFunc(TEvents::TEvPoisonPill, Handle);
            }
        }

        void BrokenState(STFUNC_SIG) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
            }
        }

        void Handle(TEvHive::TEvConfigureHive::TPtr& ev, const TActorContext& ctx) {
            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] TEvConfigureHive, msg: " << ev->Get()->Record.ShortDebugString());

            const auto& subdomainKey(ev->Get()->Record.GetDomain());
            PrimarySubDomainKey = TSubDomainKey(subdomainKey);

            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] TEvConfigureHive, subdomain set to " << subdomainKey);
            ctx.Send(ev->Sender, new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS, TabletID()));
        }

        void Handle(TEvHive::TEvCreateTablet::TPtr& ev, const TActorContext& ctx) {
            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] TEvCreateTablet, msg: " << ev->Get()->Record.ShortDebugString());
            Cout << "FAKEHIVE " << TabletID() << " TEvCreateTablet " << ev->Get()->Record.ShortDebugString() << Endl;
            NKikimrProto::EReplyStatus status = NKikimrProto::OK;
            const std::pair<ui64, ui64> key(ev->Get()->Record.GetOwner(), ev->Get()->Record.GetOwnerIdx());
            const auto type = ev->Get()->Record.GetTabletType();
            const auto bootMode = ev->Get()->Record.GetTabletBootMode();

            auto logPrefix = TStringBuilder() << "[" << TabletID() << "] TEvCreateTablet"
                << ", Owner " << ev->Get()->Record.GetOwner() << ", OwnerIdx " << ev->Get()->Record.GetOwnerIdx()
                << ", type " << type
                << ", ";

            auto it = State->Tablets.find(key);
            TActorId bootstrapperActorId;
            if (it == State->Tablets.end()) {
                if (bootMode == NKikimrHive::TABLET_BOOT_MODE_EXTERNAL) {
                    // don't boot anything
                    LOG_INFO_S(ctx, NKikimrServices::HIVE, logPrefix << "external boot mode requested");
                } else if (auto x = GetTabletCreationFunc(type)) {
                    bootstrapperActorId = Boot(ctx, type, x, DataGroupErasure);
                } else if (type == TTabletTypes::DataShard) {
                    bootstrapperActorId = Boot(ctx, type, &CreateDataShard, DataGroupErasure);
                } else if (type == TTabletTypes::KeyValue) {
                    bootstrapperActorId = Boot(ctx, type, &CreateKeyValueFlat, DataGroupErasure);
                } else if (type == TTabletTypes::ColumnShard) {
                    bootstrapperActorId = Boot(ctx, type, &CreateColumnShard, DataGroupErasure);
                } else if (type == TTabletTypes::PersQueue) {
                    bootstrapperActorId = Boot(ctx, type, &CreatePersQueue, DataGroupErasure);
                } else if (type == TTabletTypes::PersQueueReadBalancer) {
                    bootstrapperActorId = Boot(ctx, type, &CreatePersQueueReadBalancer, DataGroupErasure);
                } else if (type == TTabletTypes::Coordinator) {
                    bootstrapperActorId = Boot(ctx, type, &CreateFlatTxCoordinator, DataGroupErasure);
                } else if (type == TTabletTypes::Mediator) {
                    bootstrapperActorId = Boot(ctx, type, &CreateTxMediator, DataGroupErasure);
                } else if (type == TTabletTypes::SchemeShard) {
                    bootstrapperActorId = Boot(ctx, type, &CreateFlatTxSchemeShard, DataGroupErasure);
                } else if (type == TTabletTypes::Kesus) {
                    bootstrapperActorId = Boot(ctx, type, &NKesus::CreateKesusTablet, DataGroupErasure);
                } else if (type == TTabletTypes::Hive) {
                    TFakeHiveState::TPtr state = State->AllocateSubHive();
                    bootstrapperActorId = Boot(ctx, type, [=](const TActorId& tablet, TTabletStorageInfo* info) {
                                                   return new TFakeHive(tablet, info, state, &TFakeHive::DefaultGetTabletCreationFunc);
                                               }, DataGroupErasure);
                } else if (type == TTabletTypes::SysViewProcessor) {
                    bootstrapperActorId = Boot(ctx, type, &NSysView::CreateSysViewProcessor, DataGroupErasure);
                } else if (type == TTabletTypes::SequenceShard) {
                    bootstrapperActorId = Boot(ctx, type, &NSequenceShard::CreateSequenceShard, DataGroupErasure);
                } else if (type == TTabletTypes::ReplicationController) {
                    bootstrapperActorId = Boot(ctx, type, &NReplication::CreateController, DataGroupErasure);
                } else if (type == TTabletTypes::PersQueue) {
                    bootstrapperActorId = Boot(ctx, type, &CreatePersQueue, DataGroupErasure);
                } else if (type == TTabletTypes::StatisticsAggregator) {
                    bootstrapperActorId = Boot(ctx, type, &NStat::CreateStatisticsAggregator, DataGroupErasure);
                } else if (type == TTabletTypes::GraphShard) {
                    bootstrapperActorId = Boot(ctx, type, &NGraph::CreateGraphShard, DataGroupErasure);
                } else {
                    status = NKikimrProto::ERROR;
                }

                if (status == NKikimrProto::OK) {
                    ui64 tabletId = State->AllocateTabletId();
                    it = State->Tablets.insert(std::make_pair(key, TTabletInfo(type, tabletId, bootstrapperActorId))).first;
                    State->TabletIdToOwner[tabletId] = key;

                    LOG_INFO_S(ctx, NKikimrServices::HIVE, logPrefix << "boot OK, tablet id " << tabletId);
                } else {
                    LOG_ERROR_S(ctx, NKikimrServices::HIVE, logPrefix << "boot failed, status " << status);
                }
            } else {
                if (it->second.Type != type) {
                    status = NKikimrProto::ERROR;
                }
            }

            if (status == NKikimrProto::OK) {
                auto& boundChannels = ev->Get()->Record.GetBindedChannels();
                it->second.BoundChannels.assign(boundChannels.begin(), boundChannels.end());
                it->second.ChannelsProfile = ev->Get()->Record.GetChannelsProfile();
            }

            ctx.Send(ev->Sender, new TEvHive::TEvCreateTabletReply(status, key.first,
                key.second, it->second.TabletId, TabletID()), 0, ev->Cookie);
        }

        void TraceAdoptingCases(const std::pair<ui64, ui64> prevKey,
                                const std::pair<ui64, ui64> newKey,
                                const TTabletTypes::EType type,
                                const ui64 tabletID,
                                TString& explain,
                                NKikimrProto::EReplyStatus& status)
        {
            auto it = State->Tablets.find(newKey);
            if (it !=  State->Tablets.end()) {
                if (it->second.TabletId != tabletID) {
                    explain = "there is another tablet associated with the (owner; ownerIdx)";
                    status = NKikimrProto::EReplyStatus::RACE;
                    return;
                }

                if (it->second.Type != type) {
                    explain = "there is the tablet with different type associated with the (owner; ownerIdx)";
                    status = NKikimrProto::EReplyStatus::RACE;
                    return;
                }

                explain = "it seems like the tablet already adopted";
                status = NKikimrProto::EReplyStatus::ALREADY;
                return;
            }

            it = State->Tablets.find(prevKey);
            if (it == State->Tablets.end()) {
                explain = "the tablet isn't found";
                status = NKikimrProto::EReplyStatus::NODATA;
                return;
            }

            if (it->second.TabletId != tabletID) {
                explain = "there is another tablet associated with the (prevOwner; prevOwnerIdx)";
                status = NKikimrProto::EReplyStatus::ERROR;
                return;
            }

            if (it->second.Type != type) { // tablet is the same
                explain = "there is the tablet with different type associated with the (preOwner; prevOwnerIdx)";
                status = NKikimrProto::EReplyStatus::ERROR;
                return;
            }

            State->Tablets.emplace(newKey, it->second);
            State->Tablets.erase(prevKey);
            State->TabletIdToOwner[tabletID] = newKey;

            explain = "we did it";
            status = NKikimrProto::OK;
            return;
        }

        void Handle(TEvHive::TEvAdoptTablet::TPtr& ev, const TActorContext& ctx) {
            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] TEvAdoptTablet, msg: " << ev->Get()->Record.ShortDebugString());
            const std::pair<ui64, ui64> prevKey(ev->Get()->Record.GetPrevOwner(), ev->Get()->Record.GetPrevOwnerIdx());
            const std::pair<ui64, ui64> newKey(ev->Get()->Record.GetOwner(), ev->Get()->Record.GetOwnerIdx());
            const TTabletTypes::EType type = ev->Get()->Record.GetTabletType();
            const ui64 tabletID = ev->Get()->Record.GetTabletID();

            TString explain;
            NKikimrProto::EReplyStatus status = NKikimrProto::OK;

            TraceAdoptingCases(prevKey, newKey, type, tabletID, explain, status);

            ctx.Send(ev->Sender, new TEvHive::TEvAdoptTabletReply(status, tabletID, newKey.first,
                newKey.second, explain, TabletID()), 0, ev->Cookie);
        }

        void DeleteTablet(const std::pair<ui64, ui64>& id, const TActorContext &ctx) {
            auto it = State->Tablets.find(id);
            if (it == State->Tablets.end()) {
                return;
            }

            TFakeHiveTabletInfo& tabletInfo = it->second;
            ctx.Send(ctx.SelfID, new TEvFakeHive::TEvNotifyTabletDeleted(tabletInfo.TabletId));

            // Kill the tablet and don't restart it
            TActorId bootstrapperActorId = tabletInfo.BootstrapperActorId;
            ctx.Send(bootstrapperActorId, new TEvBootstrapper::TEvStandBy());

            for (TActorId waiter : tabletInfo.DeletionWaiters) {
                SendDeletionNotification(it->second.TabletId, waiter, ctx);
            }
            State->TabletIdToOwner.erase(it->second.TabletId);
            State->Tablets.erase(it);
        }

        void Handle(TEvHive::TEvDeleteTablet::TPtr &ev, const TActorContext &ctx) {
            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] TEvDeleteTablet, msg: " << ev->Get()->Record.ShortDebugString());
            NKikimrHive::TEvDeleteTablet& rec = ev->Get()->Record;
            Cout << "FAKEHIVE " << TabletID() << " TEvDeleteTablet " << rec.ShortDebugString() << Endl;
            TVector<ui64> deletedIdx;
            for (size_t i = 0; i < rec.ShardLocalIdxSize(); ++i) {
                auto id = std::make_pair<ui64, ui64>(rec.GetShardOwnerId(), rec.GetShardLocalIdx(i));
                deletedIdx.push_back(rec.GetShardLocalIdx(i));
                DeleteTablet(id, ctx);
            }
            ctx.Send(ev->Sender, new TEvHive::TEvDeleteTabletReply(NKikimrProto::OK, TabletID(), rec.GetTxId_Deprecated(), rec.GetShardOwnerId(), deletedIdx));
        }

        void Handle(TEvHive::TEvDeleteOwnerTablets::TPtr &ev, const TActorContext &ctx) {
            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] TEvDeleteOwnerTablets, msg: " << ev->Get()->Record);
            NKikimrHive::TEvDeleteOwnerTablets& rec = ev->Get()->Record;
            Cout << "FAKEHIVE " << TabletID() << " TEvDeleteOwnerTablets " << rec.ShortDebugString() << Endl;
            auto ownerId = rec.GetOwner();
            TVector<ui64> toDelete;

            if (ownerId == 0) {
                ctx.Send(ev->Sender, new TEvHive::TEvDeleteOwnerTabletsReply(NKikimrProto::ERROR, TabletID(), ownerId, rec.GetTxId()));
                return;
            }

            for (auto& item: State->Tablets) {
                auto& id = item.first;

                if (id.first != ownerId) {
                    continue;
                }

                toDelete.push_back(id.second);
            }

            if (toDelete.empty()) {
                ctx.Send(ev->Sender, new TEvHive::TEvDeleteOwnerTabletsReply(NKikimrProto::ALREADY, TabletID(), ownerId, rec.GetTxId()));
                return;
            }

            for (auto& idx: toDelete) {
                std::pair<ui64, ui64> id(ownerId, idx);
                DeleteTablet(id, ctx);
            }

            ctx.Send(ev->Sender, new TEvHive::TEvDeleteOwnerTabletsReply(NKikimrProto::OK, TabletID(), ownerId, rec.GetTxId()));
        }

        void Handle(TEvHive::TEvRequestHiveInfo::TPtr &ev, const TActorContext &ctx) {
            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] TEvRequestHiveInfo, msg: " << ev->Get()->Record.ShortDebugString());
            const auto& record = ev->Get()->Record;
            TAutoPtr<TEvHive::TEvResponseHiveInfo> response = new TEvHive::TEvResponseHiveInfo();

            if (record.HasTabletID()) {
                auto it = State->TabletIdToOwner.find(record.GetTabletID());
                FillTabletInfo(response->Record, record.GetTabletID(), it == State->TabletIdToOwner.end() ? nullptr : State->Tablets.FindPtr(it->second));
            } else {
                response->Record.MutableTablets()->Reserve(State->Tablets.size());
                for (auto it = State->Tablets.begin(); it != State->Tablets.end(); ++it) {
                    if (record.HasTabletType() && record.GetTabletType() != it->second.Type)
                        continue;
                    FillTabletInfo(response->Record, it->second.TabletId, &it->second);
                }
            }

            ctx.Send(ev->Sender, response.Release());
        }

        void Handle(TEvHive::TEvInitiateTabletExternalBoot::TPtr &ev, const TActorContext &ctx) {
            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] TEvInitiateTabletExternalBoot, msg: " << ev->Get()->Record.ShortDebugString());

            ui64 tabletId = ev->Get()->Record.GetTabletID();
            if (!State->TabletIdToOwner.contains(tabletId)) {
                ctx.Send(ev->Sender, new TEvHive::TEvBootTabletReply(NKikimrProto::EReplyStatus::ERROR), 0, ev->Cookie);
                return;
            }

            auto key = State->TabletIdToOwner[tabletId];
            auto it = State->Tablets.find(key);
            Y_ABORT_UNLESS(it != State->Tablets.end());

            THolder<TTabletStorageInfo> tabletInfo(CreateTestTabletInfo(tabletId, it->second.Type));
            ctx.Send(ev->Sender, new TEvLocal::TEvBootTablet(*tabletInfo.Get(), 0), 0, ev->Cookie);
        }

        void Handle(TEvHive::TEvUpdateTabletsObject::TPtr &ev, const TActorContext &ctx) {
            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] TEvUpdateTabletsObject, msg: " << ev->Get()->Record.ShortDebugString());

            // Fake Hive does not care about objects, do nothing


            auto response = std::make_unique<TEvHive::TEvUpdateTabletsObjectReply>(NKikimrProto::OK);
            response->Record.SetTxId(ev->Get()->Record.GetTxId());
            response->Record.SetTxPartId(ev->Get()->Record.GetTxPartId());
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        }

        void Handle(TEvHive::TEvUpdateDomain::TPtr &ev, const TActorContext &ctx) {
            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] TEvUpdateDomain, msg: " << ev->Get()->Record.ShortDebugString());
            
            const TSubDomainKey subdomainKey(ev->Get()->Record.GetDomainKey());
            NHive::TDomainInfo& domainInfo = State->Domains[subdomainKey];
            if (ev->Get()->Record.HasServerlessComputeResourcesMode()) {
                domainInfo.ServerlessComputeResourcesMode = ev->Get()->Record.GetServerlessComputeResourcesMode();
            } else {
                domainInfo.ServerlessComputeResourcesMode.Clear();
            }
            
            auto response = std::make_unique<TEvHive::TEvUpdateDomainReply>();
            response->Record.SetTxId(ev->Get()->Record.GetTxId());
            response->Record.SetOrigin(TabletID());
            ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        }

        void Handle(TEvFakeHive::TEvRequestDomainInfo::TPtr &ev, const TActorContext &ctx) {
            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] TEvRequestDomainInfo, " << ev->Get()->DomainKey);
            auto response = std::make_unique<TEvFakeHive::TEvRequestDomainInfoReply>(State->Domains[ev->Get()->DomainKey]);
            ctx.Send(ev->Sender, response.release());
        }

        void Handle(TEvFakeHive::TEvSubscribeToTabletDeletion::TPtr &ev, const TActorContext &ctx) {
            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] TEvSubscribeToTabletDeletion, " << ev->Get()->TabletId);

            ui64 tabletId = ev->Get()->TabletId;
            auto it = State->TabletIdToOwner.find(tabletId);
            if (it == State->TabletIdToOwner.end()) {
                SendDeletionNotification(tabletId, ev->Sender, ctx);
            } else {
                State->Tablets.FindPtr(it->second)->DeletionWaiters.insert(ev->Sender);
            }
        }

        void SendDeletionNotification(ui64 tabletId, TActorId waiter, const TActorContext& ctx) {
            TAutoPtr<TEvHive::TEvResponseHiveInfo> response = new TEvHive::TEvResponseHiveInfo();
            FillTabletInfo(response->Record, tabletId, nullptr);
            ctx.Send(waiter, response.Release());
        }

        void Handle(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            LOG_INFO_S(ctx, NKikimrServices::HIVE, "[" << TabletID() << "] TEvPoisonPill");
            Y_UNUSED(ev);
            Become(&TThis::BrokenState);
            ctx.Send(Tablet(), new TEvents::TEvPoisonPill);
        }

    private:
        TActorId Boot(const TActorContext& ctx, TTabletTypes::EType tabletType, std::function<IActor* (const TActorId &, TTabletStorageInfo *)> op,
            TBlobStorageGroupType::EErasureSpecies erasure) {
            TIntrusivePtr<TBootstrapperInfo> bi(new TBootstrapperInfo(new TTabletSetupInfo(op, TMailboxType::Simple, 0,
                TMailboxType::Simple, 0)));
            return ctx.ExecutorThread.RegisterActor(CreateBootstrapper(
                CreateTestTabletInfo(State->NextTabletId, tabletType, erasure), bi.Get()));
        }

        void FillTabletInfo(NKikimrHive::TEvResponseHiveInfo& response, ui64 tabletId, const TFakeHiveTabletInfo *info) {
            auto& tabletInfo = *response.AddTablets();
            tabletInfo.SetTabletID(tabletId);
            if (info) {
                tabletInfo.SetTabletType(info->Type);
                tabletInfo.SetState(200); // THive::ReadyToWork

                // TODO: fill other fields when needed
            }
        }

    private:
        TState::TPtr State;
        TGetTabletCreationFunc GetTabletCreationFunc;
        TSubDomainKey PrimarySubDomainKey;
    };

    void BootFakeHive(TTestActorRuntime& runtime, ui64 tabletId, TFakeHiveState::TPtr state,
                      TGetTabletCreationFunc getTabletCreationFunc)
    {
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(tabletId, TTabletTypes::Hive), [=](const TActorId & tablet, TTabletStorageInfo* info) {
            return new TFakeHive(tablet, info, state,
                                 (getTabletCreationFunc == nullptr) ? &TFakeHive::DefaultGetTabletCreationFunc : getTabletCreationFunc);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
            runtime.DispatchEvents(options);
        }
    }

}
