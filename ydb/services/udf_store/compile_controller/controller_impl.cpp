#include "controller_impl.h"
#include "compile_controller.h"
#include "reconcile_actor.h"

#include <ydb/services/udf_store/metadata_subscription/fetcher.h>
#include <ydb/services/udf_store/metadata_subscription/storage_paths.h>
#include <ydb/services/udf_store/metadata_subscription/wasm_artifact.h>
#include <ydb/services/udf_store/service.h>
#include <ydb/services/udf_store/wasm/manifest.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NUdfStore {

namespace {

constexpr TDuration TickInterval = TDuration::Seconds(5);

//! How long a burst of NeedArtifact hints is collected before it is acted upon.
//! Short enough to stay a latency shortcut over the periodic tick, long enough
//! that a refresh seen by every dinode collapses into one scheduling round.
constexpr TDuration HintCoalesceInterval = TDuration::Seconds(1);

const TString& ModuleKindString() {
    static const TString kind = WasmArtifactKindToString(EWasmArtifactKind::Module);
    return kind;
}

const TString& LibraryKindString() {
    static const TString kind = WasmArtifactKindToString(EWasmArtifactKind::Library);
    return kind;
}

const TString& KindToString(NKikimrUdfStore::EArtifactKind kind) {
    return kind == NKikimrUdfStore::ARTIFACT_KIND_LIBRARY ? LibraryKindString() : ModuleKindString();
}

NKikimrUdfStore::EArtifactKind KindFromString(TStringBuf kind) {
    return kind == LibraryKindString()
        ? NKikimrUdfStore::ARTIFACT_KIND_LIBRARY
        : NKikimrUdfStore::ARTIFACT_KIND_MODULE;
}

TGapKey GapKeyFromProto(const NKikimrUdfStore::TArtifactKey& key) {
    return TGapKey{
        .Name = key.GetName(),
        .Kind = KindToString(key.GetKind()),
        .Uid = key.GetUid(),
        .CpuSpec = key.GetCpuSpec(),
    };
}

void FillProtoKey(NKikimrUdfStore::TArtifactKey& proto, const TGapKey& key) {
    proto.SetName(key.Name);
    proto.SetKind(KindFromString(key.Kind));
    proto.SetUid(key.Uid);
    proto.SetCpuSpec(key.CpuSpec);
}

} // namespace

TWasmCompileController::TWasmCompileController(
    const NActors::TActorId& tablet,
    TTabletStorageInfo* info)
    : NActors::TActor<TWasmCompileController>(&TThis::StateInit)
    , NTabletFlatExecutor::TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
{
    TabletCountersPtr.Reset(new TProtobufTabletCounters<
        ESimpleCounters_descriptor,
        ECumulativeCounters_descriptor,
        EPercentileCounters_descriptor,
        ETxTypes_descriptor
    >());
    TabletCounters = TabletCountersPtr.Get();
}

void TWasmCompileController::OnDetach(const TActorContext& ctx) {
    Cleanup();
    Die(ctx);
}

void TWasmCompileController::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) {
    Cleanup();
    Die(ctx);
}

void TWasmCompileController::Cleanup() {
    // Everything this leader generation owns outside its own mailbox: a
    // subscription the provider would keep feeding, and reconcile actors that
    // would answer into the void.
    UnsubscribeFromSnapshot();
    StopReconciles();
}

void TWasmCompileController::OnActivateExecutor(const TActorContext& ctx) {
    Executor()->RegisterExternalTabletCounters(TabletCountersPtr);
    PlatformCountersRoot = GetServiceCounters(AppData(ctx)->Counters, "udf_store")
        ->GetSubgroup("subsystem", "wasm_compile");

    // TUdfStoreConfig is not part of AppData, so until the console answers the
    // subscription the budgets stay at the proto defaults mirrored below.
    SubscribeForConfigChanges(ctx);
    RunTxInitSchema(ctx);
}

void TWasmCompileController::DefaultSignalTabletActive(const TActorContext&) {
    // Deferred until TTxInit has restored the persisted state, so that the
    // first Register cannot race with a half-loaded queue.
}

void TWasmCompileController::SwitchToWork(const TActorContext& ctx) {
    Become(&TThis::StateWork);
    SignalTabletActive(ctx);
    SubscribeToSnapshot();
    StartReconcileForAllPlatforms();
    ScheduleTick();
    ScheduleReconcileTick();
}

void TWasmCompileController::ApplyConfig(const NKikimrConfig::TUdfStoreConfig& config) {
    MaxPerDinode = Max<ui32>(1, config.GetWasmCompileMaxPerDinode());
    MaxPerCpuSpec = Max<ui32>(1, config.GetWasmCompileMaxPerCpuSpec());
    MaxPerTenant = config.GetWasmCompileMaxPerTenant();
    MaxCompileAttempts = Max<ui32>(1, config.GetWasmMaxCompileAttempts());
    if (const ui32 seconds = config.GetWasmCompileAssignmentTimeoutSeconds()) {
        AssignmentTimeout = TDuration::Seconds(seconds);
    }
    if (const ui32 seconds = config.GetWasmCompileHeartbeatTimeoutSeconds()) {
        HeartbeatTimeout = TDuration::Seconds(seconds);
    }
    if (const ui32 seconds = config.GetWasmCompileAssignGraceSeconds()) {
        AssignGrace = TDuration::Seconds(seconds);
    }
    if (const ui32 seconds = config.GetWasmCompileReconcileIntervalSeconds()) {
        ReconcileInterval = TDuration::Seconds(seconds);
    }
    if (const ui32 seconds = config.GetWasmCompileWorkerRetentionSeconds()) {
        WorkerRetention = TDuration::Seconds(seconds);
    }
}

void TWasmCompileController::SubscribeForConfigChanges(const TActorContext& ctx) {
    ctx.Send(NConsole::MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
        new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
            (ui32)NKikimrConsole::TConfigItem::UdfStoreConfigItem,
        }));
}

void TWasmCompileController::HandleConfig(
    NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&)
{
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TWasmCompileController[" << TabletID() << "]: subscribed for config changes";
}

void TWasmCompileController::HandleConfig(
    NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev)
{
    const auto& record = ev->Get()->Record;
    if (record.GetConfig().HasUdfStoreConfig()) {
        ApplyConfig(record.GetConfig().GetUdfStoreConfig());
        ScheduleAssignments();
    }
    Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);
}

void TWasmCompileController::Handle(TEvSubDomain::TEvConfigure::TPtr& ev) {
    Send(ev->Sender, new TEvSubDomain::TEvConfigureStatus(
        NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS, TabletID()));
}

void TWasmCompileController::SubscribeToSnapshot() {
    if (SnapshotFetcher) {
        return;
    }
    SnapshotFetcher = std::make_shared<TSnapshotsFetcher>();
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvSubscribeExternal(SnapshotFetcher));
}

void TWasmCompileController::UnsubscribeFromSnapshot() {
    if (!SnapshotFetcher) {
        return;
    }
    // Without this the provider keeps this dead actor in its subscriber list
    // and broadcasts every refresh to it, once per leader generation forever.
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvUnsubscribeExternal(SnapshotFetcher));
    SnapshotFetcher.reset();
}

void TWasmCompileController::ScheduleTick() {
    NActors::TActor<TWasmCompileController>::Schedule(
        TickInterval, new TEvControllerPrivate::TEvScheduleTick());
}

void TWasmCompileController::ScheduleReconcileTick() {
    NActors::TActor<TWasmCompileController>::Schedule(
        ReconcileInterval, new TEvControllerPrivate::TEvReconcileTick());
}

void TWasmCompileController::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    auto snapshot = ev->Get()->GetSnapshotPtrAs<TSnapshot>();
    if (!snapshot) {
        return;
    }
    CurrentSnapshot = snapshot;

    Modules.clear();
    auto collect = [&](const TUdfModule& module, const TString& kind) {
        TModuleEntry entry;
        entry.Name = module.GetName();
        entry.Kind = kind;
        entry.Uid = module.GetUid();
        entry.Manifest = module.GetManifest();
        // Only a module declares dependencies; a library has nothing that would
        // parse as a module manifest, so asking would just log a failure per
        // library per snapshot.
        if (kind == ModuleKindString()) {
            try {
                entry.RequiredLibraries = NWasm::ParseManifest(module.GetManifest()).RequiredLibraries;
            } catch (...) {
                // A manifest that does not parse can never be compiled anyway.
                // The dinode reports the parse error when it gets to it; the
                // controller just treats the module as having no dependencies.
                ALS_WARN(NKikimrServices::METADATA_PROVIDER)
                    << "TWasmCompileController[" << TabletID() << "]: cannot read the manifest of "
                    << entry.Name << ": " << CurrentExceptionMessage();
            }
        }
        for (const auto& libraryName : entry.RequiredLibraries) {
            const auto libraryIt = snapshot->GetLibraries().find(libraryName);
            if (libraryIt != snapshot->GetLibraries().end()) {
                entry.LibraryUids[libraryName] = libraryIt->second.GetUid();
            }
        }
        Modules.push_back(std::move(entry));
    };

    for (const auto& [name, library] : snapshot->GetLibraries()) {
        Y_UNUSED(name);
        collect(library, LibraryKindString());
    }
    for (const auto& [name, udf] : snapshot->GetUdfs()) {
        Y_UNUSED(name);
        if (udf.GetType() == EUdfType::WASM) {
            collect(udf, ModuleKindString());
        }
    }

    ModuleIndex.clear();
    ModuleIndex.reserve(Modules.size());
    for (size_t index = 0; index < Modules.size(); ++index) {
        const auto& entry = Modules[index];
        ModuleIndex[MakeArtifactKey(entry.Name, entry.Kind, entry.Uid)] = index;
    }

    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TWasmCompileController[" << TabletID() << "]: snapshot with " << Modules.size()
        << " compilable modules";

    // A fresh upload changes the uid, so what the artifact tables already cover
    // has to be re-read before anything is handed out for the new one.
    StartReconcileForAllPlatforms();
    RebuildQueue();
    ScheduleAssignments();
}

bool TWasmCompileController::IsKnownPlatform(const TString& cpuSpec) const {
    for (const auto& [nodeId, worker] : Workers) {
        Y_UNUSED(nodeId);
        if (worker.CpuSpec == cpuSpec) {
            return true;
        }
    }
    return false;
}

void TWasmCompileController::StartReconcile(const TString& cpuSpec) {
    if (cpuSpec.empty() || ReconcileInFlight.contains(cpuSpec)) {
        return;
    }
    ReconcileInFlight[cpuSpec] =
        Register(CreateReconcileActor(SelfId(), cpuSpec, GetArtifactTablePath(cpuSpec)));
}

void TWasmCompileController::StopReconciles() {
    for (const auto& [cpuSpec, actorId] : ReconcileInFlight) {
        Y_UNUSED(cpuSpec);
        Send(actorId, new NActors::TEvents::TEvPoison());
    }
    ReconcileInFlight.clear();
}

void TWasmCompileController::StartReconcileForAllPlatforms() {
    THashSet<TString> platforms;
    // Workers loaded by TTxInit are not alive yet, but they still tell us which
    // platforms this tenant has, so reconcile can start before anyone connects.
    for (const auto& [nodeId, worker] : Workers) {
        Y_UNUSED(nodeId);
        platforms.insert(worker.CpuSpec);
    }

    // A platform exists as long as some node has a Workers row for it. Once the
    // last one is forgotten, keeping the platform here would mean scanning its
    // artifact table every interval for the lifetime of the tenant.
    TVector<TString> forgotten;
    for (const auto& [cpuSpec, artifacts] : ArtifactsByCpuSpec) {
        Y_UNUSED(artifacts);
        if (!platforms.contains(cpuSpec)) {
            forgotten.push_back(cpuSpec);
        }
    }
    for (const auto& cpuSpec : forgotten) {
        ArtifactsByCpuSpec.erase(cpuSpec);
    }

    for (const auto& cpuSpec : platforms) {
        StartReconcile(cpuSpec);
    }
}

void TWasmCompileController::Handle(TEvControllerPrivate::TEvReconcileResult::TPtr& ev) {
    auto* msg = ev->Get();
    ReconcileInFlight.erase(msg->CpuSpec);
    if (!msg->Success) {
        return;
    }
    ArtifactsByCpuSpec[msg->CpuSpec] = std::move(msg->ArtifactKeys);
    RebuildQueue();
    ScheduleAssignments();
}

void TWasmCompileController::Handle(TEvControllerPrivate::TEvScheduleTick::TPtr&) {
    TStateUpdate update;
    CollectExpiredAssignments(update);
    CollectStaleRows(update);
    ApplyStateUpdate(std::move(update));

    RebuildQueue();
    ScheduleAssignments();
    ScheduleTick();
}

void TWasmCompileController::Handle(TEvControllerPrivate::TEvReconcileTick::TPtr&) {
    StartReconcileForAllPlatforms();
    ScheduleReconcileTick();
}

void TWasmCompileController::Handle(TEvControllerPrivate::TEvHintTick::TPtr&) {
    HintTickScheduled = false;
    for (const auto& cpuSpec : DirtyPlatforms) {
        StartReconcile(cpuSpec);
    }
    DirtyPlatforms.clear();
    RebuildQueue();
    ScheduleAssignments();
}

bool TWasmCompileController::HasArtifact(
    const TString& cpuSpec,
    const TString& name,
    const TString& kind,
    const TString& uid) const
{
    const auto it = ArtifactsByCpuSpec.find(cpuSpec);
    if (it == ArtifactsByCpuSpec.end()) {
        return false;
    }
    return it->second.contains(MakeArtifactKey(name, kind, uid));
}

bool TWasmCompileController::AreLibrariesReady(const TModuleEntry& entry, const TString& cpuSpec) const {
    for (const auto& libraryName : entry.RequiredLibraries) {
        const auto uidIt = entry.LibraryUids.find(libraryName);
        if (uidIt == entry.LibraryUids.end()) {
            return false;
        }
        if (!HasArtifact(cpuSpec, libraryName, LibraryKindString(), uidIt->second)) {
            return false;
        }
    }
    return true;
}

const TWasmCompileController::TModuleEntry* TWasmCompileController::FindModule(const TGapKey& key) const {
    const auto it = ModuleIndex.find(MakeArtifactKey(key.Name, key.Kind, key.Uid));
    return it == ModuleIndex.end() ? nullptr : &Modules[it->second];
}

void TWasmCompileController::RebuildQueue() {
    Queue.clear();
    Queued.clear();
    MissingByCpuSpec.clear();
    PoisonedGaps = 0;

    // Two different sets on purpose. A gap is only assignable on a platform
    // that has someone to compile it, but it is still missing on a platform
    // whose workers all went away - which is exactly when the alarm matters.
    THashSet<TString> livePlatforms;
    THashSet<TString> knownPlatforms;
    for (const auto& [nodeId, worker] : Workers) {
        Y_UNUSED(nodeId);
        knownPlatforms.insert(worker.CpuSpec);
        if (worker.Alive) {
            livePlatforms.insert(worker.CpuSpec);
        }
    }
    for (const auto& [cpuSpec, artifacts] : ArtifactsByCpuSpec) {
        Y_UNUSED(artifacts);
        knownPlatforms.insert(cpuSpec);
    }

    // Libraries go in their own earlier pass: a module is only assignable once
    // the artifacts it links against exist on the very same platform.
    for (const bool librariesPass : {true, false}) {
        for (const auto& entry : Modules) {
            const bool isLibrary = entry.Kind == LibraryKindString();
            if (isLibrary != librariesPass) {
                continue;
            }
            for (const auto& cpuSpec : knownPlatforms) {
                if (HasArtifact(cpuSpec, entry.Name, entry.Kind, entry.Uid)) {
                    continue;
                }
                TGapKey key{
                    .Name = entry.Name,
                    .Kind = entry.Kind,
                    .Uid = entry.Uid,
                    .CpuSpec = cpuSpec,
                };
                MissingByCpuSpec[cpuSpec] += 1;

                if (!livePlatforms.contains(cpuSpec)) {
                    // Nobody can be asked to compile it, but the gap is real and
                    // has to keep showing up in WasmModulesMissingArtifact.
                    continue;
                }

                const auto attemptIt = Attempts.find(key);
                if (attemptIt != Attempts.end() && attemptIt->second.Poisoned) {
                    ++PoisonedGaps;
                    continue;
                }
                if (!isLibrary && !AreLibrariesReady(entry, cpuSpec)) {
                    continue;
                }
                if (Assignments.contains(key)) {
                    continue;
                }
                if (Queued.insert(key).second) {
                    Queue.push_back(std::move(key));
                }
            }
        }
    }
}

void TWasmCompileController::RecountInflight() {
    for (auto& [nodeId, worker] : Workers) {
        Y_UNUSED(nodeId);
        worker.Inflight = 0;
    }
    for (const auto& [key, assignment] : Assignments) {
        Y_UNUSED(key);
        const auto it = Workers.find(assignment.NodeId);
        if (it != Workers.end()) {
            it->second.Inflight += 1;
        }
    }
}

ui32 TWasmCompileController::ActiveOnCpuSpec(const TString& cpuSpec) const {
    ui32 count = 0;
    for (const auto& [key, assignment] : Assignments) {
        Y_UNUSED(assignment);
        if (key.CpuSpec == cpuSpec) {
            ++count;
        }
    }
    return count;
}

const TWasmCompileController::TWorkerState* TWasmCompileController::PickWorker(const TString& cpuSpec) const {
    const TWorkerState* best = nullptr;
    for (const auto& [nodeId, worker] : Workers) {
        Y_UNUSED(nodeId);
        if (!worker.Alive || worker.CpuSpec != cpuSpec) {
            continue;
        }
        if (worker.Inflight >= Min(worker.Capacity, MaxPerDinode)) {
            continue;
        }
        if (!best || worker.Inflight < best->Inflight) {
            best = &worker;
        }
    }
    return best;
}

TWasmCompileController::TPlatformCounters& TWasmCompileController::GetPlatformCounters(const TString& cpuSpec) {
    const auto it = PlatformCounters.find(cpuSpec);
    if (it != PlatformCounters.end()) {
        return it->second;
    }
    auto group = PlatformCountersRoot->GetSubgroup("cpu_spec", cpuSpec);
    TPlatformCounters counters{
        .WorkersRegistered = group->GetCounter("WasmCompileWorkersRegistered", false),
        .AssignmentsActive = group->GetCounter("WasmCompileAssignmentsActive", false),
        .QueueLength = group->GetCounter("WasmCompileQueueLength", false),
        .ModulesMissingArtifact = group->GetCounter("WasmModulesMissingArtifact", false),
    };
    return PlatformCounters.emplace(cpuSpec, std::move(counters)).first->second;
}

void TWasmCompileController::ReportCounters() {
    struct TPlatformNumbers {
        ui64 Workers = 0;
        ui64 Active = 0;
        ui64 Queued = 0;
        ui64 Missing = 0;
    };
    THashMap<TString, TPlatformNumbers> byCpuSpec;

    // Platforms whose last worker left are walked too, otherwise their gauges
    // keep reporting whatever they held at the moment the platform vanished.
    for (const auto& [cpuSpec, counters] : PlatformCounters) {
        Y_UNUSED(counters);
        byCpuSpec[cpuSpec];
    }
    for (const auto& [nodeId, worker] : Workers) {
        Y_UNUSED(nodeId);
        if (worker.Alive) {
            byCpuSpec[worker.CpuSpec].Workers += 1;
        }
    }
    for (const auto& [key, assignment] : Assignments) {
        Y_UNUSED(assignment);
        byCpuSpec[key.CpuSpec].Active += 1;
    }
    for (const auto& key : Queue) {
        byCpuSpec[key.CpuSpec].Queued += 1;
    }
    for (const auto& [cpuSpec, missing] : MissingByCpuSpec) {
        byCpuSpec[cpuSpec].Missing = missing;
    }

    TPlatformNumbers total;
    for (const auto& [cpuSpec, numbers] : byCpuSpec) {
        auto& counters = GetPlatformCounters(cpuSpec);
        counters.WorkersRegistered->Set(numbers.Workers);
        counters.AssignmentsActive->Set(numbers.Active);
        counters.QueueLength->Set(numbers.Queued);
        counters.ModulesMissingArtifact->Set(numbers.Missing);

        total.Workers += numbers.Workers;
        total.Active += numbers.Active;
        total.Queued += numbers.Queued;
        total.Missing += numbers.Missing;
    }

    TabletCounters->Simple()[COUNTER_WORKERS_REGISTERED].Set(total.Workers);
    TabletCounters->Simple()[COUNTER_ASSIGNMENTS_ACTIVE].Set(total.Active);
    TabletCounters->Simple()[COUNTER_QUEUE_LENGTH].Set(total.Queued);
    TabletCounters->Simple()[COUNTER_MODULES_MISSING_ARTIFACT].Set(total.Missing);
    TabletCounters->Simple()[COUNTER_MODULES_POISONED].Set(PoisonedGaps);
}

void TWasmCompileController::ScheduleAssignments() {
    TVector<TPendingAssign> pending;
    std::deque<TGapKey> deferred;

    // The budget is derived here rather than maintained across events: a
    // reconnect, a leader change or a release all used to leave it disagreeing
    // with the assignment table, and every disagreement overshoots MaxPerDinode.
    RecountInflight();

    const TInstant now = TActivationContext::Now();

    while (!Queue.empty()) {
        TGapKey key = std::move(Queue.front());
        Queue.pop_front();

        if (MaxPerTenant != 0 && Assignments.size() >= MaxPerTenant) {
            deferred.push_back(std::move(key));
            continue;
        }
        if (ActiveOnCpuSpec(key.CpuSpec) >= MaxPerCpuSpec) {
            deferred.push_back(std::move(key));
            continue;
        }
        const TWorkerState* worker = PickWorker(key.CpuSpec);
        if (!worker) {
            deferred.push_back(std::move(key));
            continue;
        }
        const TModuleEntry* entry = FindModule(key);
        if (!entry) {
            // The snapshot moved on between RebuildQueue and here; the next
            // reconcile rebuilds the gap with whatever uid is current.
            continue;
        }

        TAssignment assignment{
            .AssignmentId = NextAssignmentId++,
            .NodeId = worker->NodeId,
            .Deadline = now + AssignmentTimeout,
            .Generation = Executor()->Generation(),
            .IssuedAt = now,
        };

        auto event = std::make_unique<TEvCompileController::TEvAssignCompile>();
        auto& record = event->Record;
        FillProtoKey(*record.MutableKey(), key);
        record.SetAssignmentId(assignment.AssignmentId);
        record.SetControllerGeneration(assignment.Generation);
        record.SetDeadlineSeconds(assignment.Deadline.Seconds());
        record.SetManifest(entry->Manifest);
        for (const auto& [libraryName, libraryUid] : entry->LibraryUids) {
            auto& proto = *record.AddLibraryUids();
            proto.SetName(libraryName);
            proto.SetUid(libraryUid);
        }

        ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TWasmCompileController[" << TabletID() << "]: assign " << key.ToString()
            << " to node " << worker->NodeId << " as " << assignment.AssignmentId;

        // Reserved in memory right away so that the rest of this round sees the
        // budget as taken; the event itself waits for the row to be durable.
        Assignments[key] = assignment;
        Workers[worker->NodeId].Inflight += 1;
        Queued.erase(key);
        TabletCounters->Cumulative()[COUNTER_ASSIGNMENTS_ISSUED].Increment(1);

        pending.push_back(TPendingAssign{
            .Key = std::move(key),
            .Assignment = assignment,
            .Event = std::move(event),
        });
    }

    Queue = std::move(deferred);

    if (!pending.empty()) {
        RunTxAssign(std::move(pending));
    }

    // Every path that changes the queue, the workers or the assignments ends
    // here, so this is the one place the gauges have to be refreshed from.
    ReportCounters();
}

void TWasmCompileController::BroadcastArtifactReady(const TGapKey& key) {
    for (const auto& [nodeId, worker] : Workers) {
        if (!worker.Alive || worker.CpuSpec != key.CpuSpec) {
            continue;
        }
        auto ready = std::make_unique<TEvCompileController::TEvArtifactReady>();
        FillProtoKey(*ready->Record.MutableKey(), key);
        Send(MakeServiceId(nodeId), ready.release());
    }
}

void TWasmCompileController::ReleaseAssignment(const TGapKey& key, TStringBuf reason) {
    const auto it = Assignments.find(key);
    if (it == Assignments.end()) {
        return;
    }
    // No decrement here: the next scheduling round recounts the budget from
    // `Assignments`, and decrementing a count this assignment may never have
    // been part of is what used to drive it below the truth.
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TWasmCompileController[" << TabletID() << "]: release " << key.ToString()
        << " (" << reason << ")";
    Assignments.erase(it);
}

void TWasmCompileController::CollectExpiredAssignments(TStateUpdate& update) {
    const TInstant now = TActivationContext::Now();
    update.Reason = "expired";

    for (auto& [nodeId, worker] : Workers) {
        if (worker.Alive && now - worker.LastHeartbeat > HeartbeatTimeout) {
            worker.Alive = false;
            ALS_WARN(NKikimrServices::METADATA_PROVIDER)
                << "TWasmCompileController[" << TabletID() << "]: worker node " << nodeId
                << " missed heartbeats";
        }
    }

    for (const auto& [key, assignment] : Assignments) {
        const auto workerIt = Workers.find(assignment.NodeId);
        const bool workerGone = workerIt == Workers.end() || !workerIt->second.Alive;
        if (workerGone || now > assignment.Deadline) {
            update.ErasedAssignments.push_back(key);
            TabletCounters->Cumulative()[COUNTER_ASSIGNMENT_TIMEOUTS].Increment(1);
        }
    }
}

void TWasmCompileController::CollectStaleRows(TStateUpdate& update) {
    // Only meaningful once a snapshot has arrived: before that every gap would
    // look like one whose module has been dropped.
    if (CurrentSnapshot) {
        for (const auto& [key, attempt] : Attempts) {
            Y_UNUSED(attempt);
            if (!ModuleIndex.contains(MakeArtifactKey(key.Name, key.Kind, key.Uid))) {
                // The upload this attempt counted failures for is gone, either
                // dropped or superseded by a new uid. Nothing will ever ask
                // about it again.
                update.ErasedAttempts.push_back(key);
            }
        }
    }

    const TInstant now = TActivationContext::Now();
    for (const auto& [nodeId, worker] : Workers) {
        if (!worker.Alive && now - worker.LastHeartbeat > WorkerRetention) {
            update.ErasedWorkers.push_back(nodeId);
        }
    }
}

void TWasmCompileController::ApplyStateUpdate(TStateUpdate&& update) {
    if (update.ErasedAssignments.empty()
        && update.ErasedAttempts.empty()
        && update.UpdatedAttempts.empty()
        && update.ReadyBroadcasts.empty()
        && update.ErasedWorkers.empty())
    {
        return;
    }

    for (const auto& key : update.ErasedAssignments) {
        ReleaseAssignment(key, update.Reason);
    }
    for (const auto& key : update.ErasedAttempts) {
        Attempts.erase(key);
    }
    for (const auto& [key, attempt] : update.UpdatedAttempts) {
        Attempts[key] = attempt;
    }
    // After the releases above, so that a forgotten node takes no assignment
    // with it into a state nothing refers to.
    for (const ui32 nodeId : update.ErasedWorkers) {
        const auto it = Workers.find(nodeId);
        if (it == Workers.end()) {
            continue;
        }
        ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TWasmCompileController[" << TabletID() << "]: forgetting node " << nodeId
            << " of cpu_spec " << it->second.CpuSpec;
        NodeByPipeServer.erase(it->second.PipeServer);
        Workers.erase(it);
    }

    RunTxFinish(std::move(update));
}

void TWasmCompileController::Handle(TEvCompileController::TEvRegister::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const ui32 nodeId = record.GetNodeId();

    auto& worker = Workers[nodeId];
    worker.NodeId = nodeId;
    worker.CpuSpec = record.GetCpuSpec();
    worker.Capacity = Max<ui32>(1, record.GetCapacity());
    worker.PipeServer = ev->Recipient;
    worker.LastHeartbeat = TActivationContext::Now();
    worker.Alive = true;
    NodeByPipeServer[ev->Recipient] = nodeId;

    auto response = std::make_unique<TEvCompileController::TEvRegisterResult>();
    response->Record.SetControllerGeneration(Executor()->Generation());
    Send(ev->Sender, response.release(), 0, ev->Cookie);

    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TWasmCompileController[" << TabletID() << "]: node " << nodeId
        << " registered with cpu_spec " << worker.CpuSpec;

    RunTxRegisterWorker(nodeId, worker.CpuSpec, worker.Capacity);
}

void TWasmCompileController::Handle(TEvCompileController::TEvHeartbeat::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto it = Workers.find(record.GetNodeId());
    if (it == Workers.end()) {
        return;
    }

    const TInstant now = TActivationContext::Now();
    auto& worker = it->second;
    worker.LastHeartbeat = now;
    worker.Alive = true;
    worker.PipeServer = ev->Recipient;
    if (record.GetCapacity()) {
        worker.Capacity = record.GetCapacity();
    }

    // Assignments the worker no longer claims can be handed out again even
    // before their deadline: it just told us it is not running them.
    const THashSet<ui64> claimed(
        record.GetActiveAssignmentIds().begin(), record.GetActiveAssignmentIds().end());
    TStateUpdate update;
    update.Reason = "not claimed by the worker";
    for (const auto& [key, assignment] : Assignments) {
        if (assignment.NodeId != worker.NodeId || claimed.contains(assignment.AssignmentId)) {
            continue;
        }
        if (now - assignment.IssuedAt < AssignGrace) {
            // This heartbeat was on the wire before the worker could learn of
            // the assignment, so its silence says nothing yet. Reclaiming here
            // would hand the same gap to a second node while the first starts.
            continue;
        }
        update.ErasedAssignments.push_back(key);
    }

    const bool changed = !update.ErasedAssignments.empty();
    ApplyStateUpdate(std::move(update));
    if (changed) {
        RebuildQueue();
    }
    ScheduleAssignments();
}

void TWasmCompileController::Handle(TEvCompileController::TEvNeedArtifact::TPtr& ev) {
    const TGapKey key = GapKeyFromProto(ev->Get()->Record.GetKey());
    if (!IsKnownPlatform(key.CpuSpec)) {
        // A node always registers before it reports anything, so a cpu_spec no
        // Workers row mentions cannot be a platform this tenant compiles for.
        // Taking it would add an artifact table to scan forever.
        return;
    }

    // Only a hint: it says this platform is worth looking at sooner than the
    // next periodic tick would. Every dinode sends one per gap on every
    // snapshot refresh, so the work it triggers is done once per burst.
    DirtyPlatforms.insert(key.CpuSpec);
    if (!HintTickScheduled) {
        HintTickScheduled = true;
        NActors::TActor<TWasmCompileController>::Schedule(
            HintCoalesceInterval, new TEvControllerPrivate::TEvHintTick());
    }
}

void TWasmCompileController::Handle(TEvCompileController::TEvCompileDone::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const TGapKey key = GapKeyFromProto(record.GetKey());

    const auto it = Assignments.find(key);
    if (it == Assignments.end() || it->second.AssignmentId != record.GetAssignmentId()) {
        return;
    }

    ArtifactsByCpuSpec[key.CpuSpec].insert(MakeArtifactKey(key.Name, key.Kind, key.Uid));
    TabletCounters->Cumulative()[COUNTER_COMPILES_DONE].Increment(1);

    TStateUpdate update;
    update.Reason = "compiled";
    update.ErasedAssignments.push_back(key);
    update.ErasedAttempts.push_back(key);
    update.ReadyBroadcasts.push_back(key);
    ApplyStateUpdate(std::move(update));

    // A finished library unblocks the modules that link against it.
    RebuildQueue();
    ScheduleAssignments();
}

void TWasmCompileController::Handle(TEvCompileController::TEvCompileFailed::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const TGapKey key = GapKeyFromProto(record.GetKey());

    const auto it = Assignments.find(key);
    if (it == Assignments.end() || it->second.AssignmentId != record.GetAssignmentId()) {
        return;
    }

    TStateUpdate update;
    update.Reason = record.GetStale() ? "stale" : "failed";
    update.ErasedAssignments.push_back(key);

    if (record.GetStale()) {
        // A stale report means a re-upload won the race, not that this module
        // is broken, so it must not bring the poison pill any closer.
        ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TWasmCompileController[" << TabletID() << "]: " << key.ToString() << " is stale";
    } else {
        TabletCounters->Cumulative()[COUNTER_COMPILES_FAILED].Increment(1);
        TAttemptState attempt = Attempts.Value(key, TAttemptState{});
        attempt.FailCount += 1;
        attempt.LastError = record.GetError();
        attempt.Poisoned = attempt.FailCount >= MaxCompileAttempts;
        if (attempt.Poisoned) {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                << "TWasmCompileController[" << TabletID() << "]: " << key.ToString()
                << " poisoned after " << attempt.FailCount << " failures: " << attempt.LastError;
        }
        update.UpdatedAttempts.emplace_back(key, std::move(attempt));
    }

    ApplyStateUpdate(std::move(update));
    RebuildQueue();
    ScheduleAssignments();
}

void TWasmCompileController::Handle(TEvTabletPipe::TEvServerConnected::TPtr&) {
    // Nothing to do until the node identifies itself with Register.
}

void TWasmCompileController::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev) {
    const auto nodeIt = NodeByPipeServer.find(ev->Get()->ServerId);
    if (nodeIt == NodeByPipeServer.end()) {
        return;
    }
    const ui32 nodeId = nodeIt->second;
    NodeByPipeServer.erase(nodeIt);

    const auto workerIt = Workers.find(nodeId);
    if (workerIt == Workers.end() || workerIt->second.PipeServer != ev->Get()->ServerId) {
        return;
    }
    // A closed pipe is the fast path to noticing a dead worker; the heartbeat
    // timeout stays as the backstop for one that holds the pipe open but stops
    // making progress.
    workerIt->second.Alive = false;

    TStateUpdate update;
    update.Reason = "worker disconnected";
    for (const auto& [key, assignment] : Assignments) {
        if (assignment.NodeId == nodeId) {
            update.ErasedAssignments.push_back(key);
        }
    }

    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TWasmCompileController[" << TabletID() << "]: node " << nodeId << " disconnected";

    ApplyStateUpdate(std::move(update));
    RebuildQueue();
    ScheduleAssignments();
}

NActors::IActor* CreateWasmCompileController(
    const NActors::TActorId& tablet,
    TTabletStorageInfo* info)
{
    return new TWasmCompileController(tablet, info);
}

} // namespace NKikimr::NUdfStore
