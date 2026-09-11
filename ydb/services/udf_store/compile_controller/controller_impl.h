#pragma once

#include "private_events.h"
#include "schema.h"

#include <ydb/services/udf_store/compile_controller/events.h>
#include <ydb/services/udf_store/metadata_subscription/snapshot.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/defs.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/counters_wasm_compile_controller.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/services/metadata/service.h>

#include <ydb/library/actors/core/actor.h>

#include <deque>
#include <memory>

namespace NKikimr::NUdfStore {

//! Identity of one compilable unit on one platform. The uid is part of it, so
//! a re-upload produces a different gap instead of silently reusing the
//! artifact of the upload it replaced.
struct TGapKey {
    TString Name;
    TString Kind;
    TString Uid;
    TString CpuSpec;

    bool operator==(const TGapKey& other) const {
        return Name == other.Name
            && Kind == other.Kind
            && Uid == other.Uid
            && CpuSpec == other.CpuSpec;
    }

    TString ToString() const {
        return TStringBuilder() << Name << '/' << Kind << '/' << Uid << '@' << CpuSpec;
    }
};

struct TGapKeyHash {
    size_t operator()(const TGapKey& key) const {
        return CombineHashes(
            CombineHashes(THash<TString>()(key.Name), THash<TString>()(key.Kind)),
            CombineHashes(THash<TString>()(key.Uid), THash<TString>()(key.CpuSpec)));
    }
};

class TWasmCompileController
    : public NActors::TActor<TWasmCompileController>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    using Schema = TCompileControllerSchema;

    class TTxBase : public NTabletFlatExecutor::TTransactionBase<TWasmCompileController> {
    public:
        TTxBase(const TString& name, TWasmCompileController* self)
            : TTransactionBase(self)
            , LogPrefix(name)
        {}

    protected:
        const TString LogPrefix;
    };

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::WASM_COMPILE_CONTROLLER_TABLET;
    }

    TWasmCompileController(const NActors::TActorId& tablet, TTabletStorageInfo* info);

    STFUNC(StateInit) {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleConfig);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleConfig);
            hFunc(TEvSubDomain::TEvConfigure, Handle);
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            hFunc(TEvControllerPrivate::TEvReconcileResult, Handle);
            hFunc(TEvControllerPrivate::TEvScheduleTick, Handle);
            hFunc(TEvCompileController::TEvRegister, Handle);
            hFunc(TEvCompileController::TEvHeartbeat, Handle);
            hFunc(TEvCompileController::TEvNeedArtifact, Handle);
            hFunc(TEvCompileController::TEvCompileDone, Handle);
            hFunc(TEvCompileController::TEvCompileFailed, Handle);
            hFunc(TEvTabletPipe::TEvServerConnected, Handle);
            hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
        }
    }

    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext& ctx) override;

private:
    //! What a registered dinode can be asked to do. Liveness lives here rather
    //! than in local DB because it means nothing after a leader change: the
    //! persisted row only records that the platform exists.
    struct TWorkerState {
        ui32 NodeId = 0;
        TString CpuSpec;
        ui32 Capacity = 1;
        NActors::TActorId PipeServer;
        TInstant LastHeartbeat;
        ui32 Inflight = 0;
        bool Alive = false;
    };

    struct TAssignment {
        ui64 AssignmentId = 0;
        ui32 NodeId = 0;
        TInstant Deadline;
        ui64 Generation = 0;
    };

    struct TAttemptState {
        ui32 FailCount = 0;
        TString LastError;
        bool Poisoned = false;
    };

    //! One entry of the `modules` side of the reconcile, taken from the
    //! metadata snapshot rather than from a poll of the table.
    struct TModuleEntry {
        TString Name;
        TString Kind;
        TString Uid;
        TString Manifest;
        TVector<TString> RequiredLibraries;
        THashMap<TString, TString> LibraryUids;
    };

    //! An assignment that is reserved in memory but not handed out yet. The
    //! event only goes to the worker once the row is durable: an exclusive
    //! right that a restart would forget is worse than a slightly later start.
    struct TPendingAssign {
        TGapKey Key;
        TAssignment Assignment;
        std::unique_ptr<TEvCompileController::TEvAssignCompile> Event;
    };

    //! The per-platform half of the numbers below. `cpu_spec` is an arbitrary
    //! string coming from the workers, so it cannot be an enum counter and has
    //! to be a label on a dynamic one.
    struct TPlatformCounters {
        NMonitoring::TDynamicCounters::TCounterPtr WorkersRegistered;
        NMonitoring::TDynamicCounters::TCounterPtr AssignmentsActive;
        NMonitoring::TDynamicCounters::TCounterPtr QueueLength;
        NMonitoring::TDynamicCounters::TCounterPtr ModulesMissingArtifact;
    };

    //! A batch of durable state changes applied by a single TTxFinish.
    struct TStateUpdate {
        //! Why the assignments below are being given up, for the release log.
        TStringBuf Reason = "released";
        TVector<TGapKey> ErasedAssignments;
        TVector<TGapKey> ErasedAttempts;
        TVector<std::pair<TGapKey, TAttemptState>> UpdatedAttempts;
        TVector<TGapKey> ReadyBroadcasts;
    };

public:
    class TTxInitSchema;
    class TTxInit;
    class TTxRegisterWorker;
    class TTxAssign;
    class TTxFinish;

    void RunTxInitSchema(const TActorContext& ctx);
    void RunTxInit(const TActorContext& ctx);
    void RunTxRegisterWorker(ui32 nodeId, const TString& cpuSpec, ui32 capacity);
    void RunTxAssign(TVector<TPendingAssign>&& pending);
    void RunTxFinish(TStateUpdate&& update);
    void SwitchToWork(const TActorContext& ctx);

private:
    void HandleConfig(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr& ev);
    void HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev);
    void Handle(TEvSubDomain::TEvConfigure::TPtr& ev);
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);
    void Handle(TEvControllerPrivate::TEvReconcileResult::TPtr& ev);
    void Handle(TEvControllerPrivate::TEvScheduleTick::TPtr& ev);
    void Handle(TEvCompileController::TEvRegister::TPtr& ev);
    void Handle(TEvCompileController::TEvHeartbeat::TPtr& ev);
    void Handle(TEvCompileController::TEvNeedArtifact::TPtr& ev);
    void Handle(TEvCompileController::TEvCompileDone::TPtr& ev);
    void Handle(TEvCompileController::TEvCompileFailed::TPtr& ev);
    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev);

    void ApplyConfig(const NKikimrConfig::TUdfStoreConfig& config);
    void SubscribeForConfigChanges(const TActorContext& ctx);
    void SubscribeToSnapshot();
    void ScheduleTick();

    void StartReconcile(const TString& cpuSpec);
    void StartReconcileForAllPlatforms();
    void RebuildQueue();
    void ScheduleAssignments();
    void ApplyStateUpdate(TStateUpdate&& update);
    void BroadcastArtifactReady(const TGapKey& key);
    //! Frees the slot in memory. Losing an assignment early only risks a
    //! duplicate compile, which storage keeps idempotent, so this does not
    //! wait for the erase to become durable.
    void ReleaseAssignment(const TGapKey& key, TStringBuf reason);
    void CollectExpiredAssignments(TStateUpdate& update);
    TPlatformCounters& GetPlatformCounters(const TString& cpuSpec);
    void ReportCounters();

    bool HasArtifact(const TString& cpuSpec, const TString& name, const TString& kind, const TString& uid) const;
    bool AreLibrariesReady(const TModuleEntry& entry, const TString& cpuSpec) const;
    const TModuleEntry* FindModule(const TGapKey& key) const;
    const TWorkerState* PickWorker(const TString& cpuSpec) const;
    ui32 ActiveOnCpuSpec(const TString& cpuSpec) const;

    // Budgets, refreshed from the console config subscription.
    ui32 MaxPerDinode = 1;
    ui32 MaxPerCpuSpec = 1;
    ui32 MaxPerTenant = 0;
    ui32 MaxCompileAttempts = 3;
    TDuration AssignmentTimeout = TDuration::Seconds(900);
    TDuration HeartbeatTimeout = TDuration::Seconds(60);

    ui64 NextAssignmentId = 1;

    THashMap<ui32, TWorkerState> Workers;
    THashMap<NActors::TActorId, ui32> NodeByPipeServer;
    THashMap<TString, THashSet<TString>> ArtifactsByCpuSpec;
    THashSet<TString> ReconcileInFlight;

    std::shared_ptr<TSnapshot> CurrentSnapshot;
    TVector<TModuleEntry> Modules;

    std::deque<TGapKey> Queue;
    THashSet<TGapKey, TGapKeyHash> Queued;
    THashMap<TGapKey, TAssignment, TGapKeyHash> Assignments;
    THashMap<TGapKey, TAttemptState, TGapKeyHash> Attempts;

    // Gaps as of the last RebuildQueue, including the ones it refused to queue.
    // Queue length alone would read as zero while the platform is uncovered.
    THashMap<TString, ui32> MissingByCpuSpec;
    ui32 PoisonedGaps = 0;

    TTabletCountersBase* TabletCounters = nullptr;
    TAutoPtr<TTabletCountersBase> TabletCountersPtr;
    NMonitoring::TDynamicCounterPtr PlatformCountersRoot;
    THashMap<TString, TPlatformCounters> PlatformCounters;

    bool SnapshotSubscribed = false;
};

} // namespace NKikimr::NUdfStore
