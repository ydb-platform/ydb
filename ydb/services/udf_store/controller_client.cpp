#include "service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NUdfStore {

namespace {

constexpr TDuration ControllerTickInterval = TDuration::Seconds(10);

//! Cookie of the second navigate hop, the one that resolves the database whose
//! controller actually owns this node's compiles.
constexpr ui64 ResolveDomainCookie = 1;

void AddNavigateByPathId(
    NSchemeCache::TSchemeCacheNavigate::TResultSet& items,
    const TPathId& pathId)
{
    auto& entry = items.emplace_back();
    entry.TableId = TTableId(pathId.OwnerId, pathId.LocalPathId);
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    entry.RedirectRequired = false;
    entry.ShowPrivatePath = true;
}

} // namespace

void TUdfStoreService::ResolveCompileController() {
    if (!EnableCompileControllerFlag || ControllerResolveStage == EControllerResolveStage::InFlight) {
        return;
    }

    const TString& database = AppData()->TenantName;
    if (database.empty()) {
        return;
    }

    auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
    navigate->DatabaseName = database;
    auto& entry = navigate->ResultSet.emplace_back();
    entry.Path = SplitPath(database);
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
    entry.RedirectRequired = false;
    entry.ShowPrivatePath = true;

    ControllerResolveStage = EControllerResolveStage::InFlight;
    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
}

void TUdfStoreService::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    std::unique_ptr<NSchemeCache::TSchemeCacheNavigate> navigate(ev->Get()->Request.Release());
    ControllerResolveStage = EControllerResolveStage::Initial;

    if (navigate->ResultSet.empty()) {
        return;
    }
    const auto& entry = navigate->ResultSet.front();
    if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok || !entry.DomainInfo) {
        ALS_WARN(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: failed to resolve the compile controller, status "
            << (ui32)entry.Status;
        return;
    }

    const auto& domainInfo = entry.DomainInfo;
    // A serverless database runs on the shared database's dinodes, so it is
    // that database's controller that schedules this node's compiles.
    if (domainInfo->IsServerless() && ev->Cookie != ResolveDomainCookie) {
        auto second = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        second->DatabaseName = AppData()->DomainsInfo->GetDomain()->Name;
        AddNavigateByPathId(second->ResultSet, domainInfo->ResourcesDomainKey);
        ControllerResolveStage = EControllerResolveStage::InFlight;
        Send(MakeSchemeCacheID(),
            new TEvTxProxySchemeCache::TEvNavigateKeySet(second.release()), 0, ResolveDomainCookie);
        return;
    }

    if (!domainInfo->Params.HasWasmCompileController()) {
        ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: database has no compile controller yet";
        return;
    }

    const ui64 tabletId = domainInfo->Params.GetWasmCompileController();
    if (tabletId == CompileControllerTabletId && CompileControllerPipe) {
        return;
    }
    CompileControllerTabletId = tabletId;
    ControllerResolveStage = EControllerResolveStage::Finished;
    ConnectToCompileController();
}

void TUdfStoreService::ConnectToCompileController() {
    if (!CompileControllerTabletId || CompileControllerPipe) {
        return;
    }
    NTabletPipe::TClientConfig config;
    config.RetryPolicy = {
        .RetryLimitCount = 3,
    };
    CompileControllerPipe = Register(NTabletPipe::CreateClient(
        SelfId(), CompileControllerTabletId, config));
    SendRegister();
}

void TUdfStoreService::SendRegister() {
    if (!CompileControllerPipe) {
        return;
    }
    auto request = std::make_unique<TEvCompileController::TEvRegister>();
    request->Record.SetCpuSpec(LocalCpuSpec);
    request->Record.SetNodeId(SelfId().NodeId());
    request->Record.SetCapacity(CompileCapacity);
    NTabletPipe::SendData(SelfId(), CompileControllerPipe, request.release());
}

void TUdfStoreService::SendHeartbeat() {
    if (!CompileControllerPipe) {
        return;
    }
    auto request = std::make_unique<TEvCompileController::TEvHeartbeat>();
    auto& record = request->Record;
    record.SetCpuSpec(LocalCpuSpec);
    record.SetNodeId(SelfId().NodeId());
    record.SetCapacity(CompileCapacity);
    record.SetInflight(ModuleAssignments.size() + LibraryAssignments.size());
    for (const auto& [name, assignment] : ModuleAssignments) {
        Y_UNUSED(name);
        record.AddActiveAssignmentIds(assignment.AssignmentId);
    }
    for (const auto& [name, assignment] : LibraryAssignments) {
        Y_UNUSED(name);
        record.AddActiveAssignmentIds(assignment.AssignmentId);
    }
    NTabletPipe::SendData(SelfId(), CompileControllerPipe, request.release());
}

void TUdfStoreService::ScheduleControllerTick() {
    Schedule(ControllerTickInterval, new NActors::TEvents::TEvWakeup());
}

void TUdfStoreService::HandleControllerTick() {
    if (!EnableCompileControllerFlag) {
        return;
    }
    if (!CompileControllerPipe) {
        // Covers both a controller that did not exist at startup and a pipe
        // that broke: the tablet may also have moved to another node.
        ResolveCompileController();
    } else {
        SendHeartbeat();
    }
    ScheduleControllerTick();
}

void TUdfStoreService::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
    if (ev->Get()->ClientId != CompileControllerPipe) {
        return;
    }
    if (ev->Get()->Status != NKikimrProto::OK) {
        CompileControllerPipe = TActorId();
        CompileControllerTabletId = 0;
        ControllerResolveStage = EControllerResolveStage::Initial;
        return;
    }
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TUdfStoreService: connected to compile controller " << CompileControllerTabletId;
}

void TUdfStoreService::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
    if (ev->Get()->ClientId != CompileControllerPipe) {
        return;
    }
    CompileControllerPipe = TActorId();
    // The tablet may have moved, so the id is resolved again rather than reused.
    CompileControllerTabletId = 0;
    ControllerResolveStage = EControllerResolveStage::Initial;
    ResolveCompileController();
}

void TUdfStoreService::Handle(TEvCompileController::TEvRegisterResult::TPtr& ev) {
    const ui64 generation = ev->Get()->Record.GetControllerGeneration();
    ControllerGeneration = Max(ControllerGeneration, generation);
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TUdfStoreService: registered with compile controller, generation " << generation;
    // Assignments held from a previous leader are re-declared right away so
    // that it does not wait for the first periodic heartbeat to learn of them.
    SendHeartbeat();
}

void TUdfStoreService::RequestArtifact(const TString& name, const TString& uid, bool isLibrary) {
    if (!CompileControllerPipe) {
        // Nobody else will pick this gap up, because under the flag the snapshot
        // path no longer compiles locally. It is reported again on the next
        // snapshot refresh, but on a database that has not been migrated yet
        // there is no controller to report it to at all.
        ALS_WARN(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: no compile controller to report a gap to, dropping "
            << (isLibrary ? "library " : "module ") << name << " uid " << uid;
        return;
    }
    auto request = std::make_unique<TEvCompileController::TEvNeedArtifact>();
    auto& key = *request->Record.MutableKey();
    key.SetName(name);
    key.SetKind(isLibrary
        ? NKikimrUdfStore::ARTIFACT_KIND_LIBRARY
        : NKikimrUdfStore::ARTIFACT_KIND_MODULE);
    key.SetUid(uid);
    key.SetCpuSpec(LocalCpuSpec);
    NTabletPipe::SendData(SelfId(), CompileControllerPipe, request.release());
}

void TUdfStoreService::ReportCompileResult(
    const TActiveAssignment& assignment,
    bool success,
    bool stale,
    const TString& error)
{
    if (!CompileControllerPipe) {
        return;
    }
    if (success) {
        auto done = std::make_unique<TEvCompileController::TEvCompileDone>();
        done->Record.SetAssignmentId(assignment.AssignmentId);
        *done->Record.MutableKey() = assignment.Key;
        NTabletPipe::SendData(SelfId(), CompileControllerPipe, done.release());
        return;
    }

    auto failed = std::make_unique<TEvCompileController::TEvCompileFailed>();
    failed->Record.SetAssignmentId(assignment.AssignmentId);
    *failed->Record.MutableKey() = assignment.Key;
    failed->Record.SetError(error);
    // A deferred result means a re-upload won the race, not that the module is
    // broken, and the controller must not count it towards the poison pill.
    failed->Record.SetStale(stale);
    NTabletPipe::SendData(SelfId(), CompileControllerPipe, failed.release());
}

void TUdfStoreService::Handle(TEvCompileController::TEvAssignCompile::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto& key = record.GetKey();
    const bool isLibrary = key.GetKind() == NKikimrUdfStore::ARTIFACT_KIND_LIBRARY;
    const TString& name = key.GetName();

    if (key.GetCpuSpec() != LocalCpuSpec) {
        // Object code is only valid on the platform that produced it, so an
        // assignment for another one can only be a stale route.
        ALS_WARN(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: rejecting assignment for cpu_spec " << key.GetCpuSpec()
            << ", local cpu_spec is " << LocalCpuSpec;
        return;
    }

    // An assignment from a replaced leader is not an exclusive right to anything:
    // the current one may have handed the same key to another node already. A
    // higher generation than the known one is the opposite case, a leader whose
    // registration reply has not arrived yet, and is trusted.
    const ui64 generation = record.GetControllerGeneration();
    if (generation < ControllerGeneration) {
        ALS_WARN(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: rejecting assignment from controller generation " << generation
            << ", already registered with generation " << ControllerGeneration;
        return;
    }
    ControllerGeneration = generation;

    auto& assignments = isLibrary ? LibraryAssignments : ModuleAssignments;
    if (assignments.contains(name)) {
        return;
    }
    assignments[name] = TActiveAssignment{
        .AssignmentId = record.GetAssignmentId(),
        .Key = key,
    };

    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TUdfStoreService: assigned to compile " << (isLibrary ? "library" : "module")
        << " '" << name << "' uid " << key.GetUid()
        << ", assignment " << record.GetAssignmentId();

    if (isLibrary) {
        if (!IsLibraryPending(name)) {
            PendingLibraryCompile.push_back(TPendingLibrary{.Name = name});
        }
        if (!LibraryCompileInProgress) {
            FetchNextLibraryCompile();
        }
        return;
    }

    THashMap<TString, TString> libraryUids;
    for (const auto& libraryUid : record.GetLibraryUids()) {
        libraryUids[libraryUid.GetName()] = libraryUid.GetUid();
    }
    if (!IsNamePending(name, EUdfType::WASM)) {
        PendingWasmCompile.push_back(TPendingUdf{
            .Name = name,
            .Uid = key.GetUid(),
            .Type = EUdfType::WASM,
            // The manifest travels with the assignment: this node must compile
            // the upload the controller decided on, not whatever its own
            // snapshot happens to show right now.
            .Manifest = record.GetManifest(),
            .ModuleExtension = GetModuleExtensionFromManifest(record.GetManifest()),
            .LibraryUids = std::move(libraryUids),
        });
    }
    if (!WasmCompileInProgress) {
        FetchNextWasmCompile();
    }
}

void TUdfStoreService::Handle(TEvCompileController::TEvArtifactReady::TPtr& ev) {
    const auto& key = ev->Get()->Record.GetKey();
    if (key.GetKind() == NKikimrUdfStore::ARTIFACT_KIND_LIBRARY) {
        // Modules that link against it become loadable once their own artifact
        // shows up; nothing to do on this node until then.
        return;
    }
    if (!CurrentSnapshot) {
        return;
    }
    const auto* udf = CurrentSnapshot->GetUdfByName(key.GetName());
    if (!udf || udf->GetUid() != key.GetUid()) {
        // The snapshot has not caught up with the compile that just finished;
        // the next refresh enqueues the load anyway. This event only shortens
        // the wait when the snapshot is already current.
        return;
    }
    EnqueueWasmLoadIfNeeded(*udf);
    if (!WasmLoadInProgress) {
        FetchNextWasmLoad();
    }
}

} // namespace NKikimr::NUdfStore
