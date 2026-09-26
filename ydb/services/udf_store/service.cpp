#include "service.h"
#include "metadata_subscription/fetcher.h"
#include "metadata_subscription/storage_paths.h"
#include "cpu_spec.h"
#include "wasm/manifest.h"
#include "wasm/module_catalog.h"

#include <ydb/services/metadata/service.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/common/dynamic_function_registry.h>

#include <library/cpp/json/json_reader.h>
#include <util/folder/path.h>
#include <util/system/fs.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::METADATA_PROVIDER

namespace NKikimr::NUdfStore {

namespace {

bool ManifestLooksValid(TStringBuf manifest) {
    if (manifest.empty()) {
        return false;
    }
    try {
        NWasm::ParseManifest(manifest);
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

TUdfStoreService::TUdfStoreService(
    const NKikimrConfig::TUdfStoreConfig& config,
    TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> functionRegistry)
    : FunctionRegistry(std::move(functionRegistry))
    , KvStorageMedia(config.GetKvStorageMedia())
    , EnableUnsafeNativeUdfFlag(config.GetEnableUnsafeNativeUdf())
    , UnsafeNativeUdfDir(config.GetUnsafeNativeUdfDir())
    , EnableWasmUdfFlag(config.GetEnableWasmUdf())
    , CompileCapacity(Max<ui32>(1, config.GetWasmCompileMaxPerDinode()))
    , WasmCpuSpecOverride(config.GetWasmCpuSpecOverride())
    , LocalCpuSpec(DetectLocalCpuSpec(WasmCpuSpecOverride))
{}

bool TUdfStoreService::IsNamePending(const TString& name, EUdfType type) const {
    auto pred = [&](const TPendingUdf& pending) { return pending.Name == name; };
    if (type == EUdfType::WASM) {
        return std::any_of(PendingWasmCompile.begin(), PendingWasmCompile.end(), pred)
            || std::any_of(PendingWasmLoad.begin(), PendingWasmLoad.end(), pred);
    }
    return std::any_of(PendingNativeUdfs.begin(), PendingNativeUdfs.end(), pred);
}

bool TUdfStoreService::IsLibraryPending(const TString& name) const {
    return std::any_of(PendingLibraryCompile.begin(), PendingLibraryCompile.end(),
        [&](const TPendingLibrary& pending) { return pending.Name == name; });
}

TString TUdfStoreService::GetModuleExtensionFromManifest(TStringBuf manifest) {
    NJson::TJsonValue root;
    if (!NJson::ReadJsonTree(manifest, &root, true) || !root.IsMap()) {
        return "wasm";
    }
    if (root.Has("module_extension")) {
        return root["module_extension"].GetString();
    }
    return "wasm";
}

bool TUdfStoreService::AreLibraryDependenciesReady(
    TStringBuf manifest,
    const TSnapshot* snapshot) const
{
    const TSnapshot* snap = snapshot ? snapshot : CurrentSnapshot.get();
    if (!snap) {
        return false;
    }
    try {
        const auto parsed = NWasm::ParseManifest(manifest);
        for (const auto& libraryName : parsed.RequiredLibraries) {
            const auto* library = snap->GetLibraryByName(libraryName);
            const auto ready = LocallyReadyLibraries.find(libraryName);
            if (!library || ready == LocallyReadyLibraries.end() || ready->second != library->GetUid()) {
                return false;
            }
        }
        return true;
    } catch (...) {
        return false;
    }
}

THashMap<TString, TString> TUdfStoreService::CollectLibraryUids(
    TStringBuf manifest,
    const TSnapshot* snapshot) const
{
    THashMap<TString, TString> uids;
    const TSnapshot* snap = snapshot ? snapshot : CurrentSnapshot.get();
    if (!snap) {
        return uids;
    }
    try {
        const auto parsed = NWasm::ParseManifest(manifest);
        for (const auto& libraryName : parsed.RequiredLibraries) {
            if (const auto* library = snap->GetLibraryByName(libraryName)) {
                uids[libraryName] = library->GetUid();
            }
        }
    } catch (...) {
        // A manifest that does not parse never reaches compile or load anyway.
    }
    return uids;
}

void TUdfStoreService::EnqueueNativeUdfIfNeeded(const TUdfModule& udf) {
    const TString& name = udf.GetName();
    if (udf.GetSize() == 0) {
        YDB_LOG_ERROR("TUdfStoreService: UDF has zero size in metadata, skipping fetch",
            {"name", name});
        return;
    }
    // The name becomes a path under UnsafeNativeUdfDir once the body arrives.
    if (!IsSafeUdfFileName(name)) {
        YDB_LOG_ERROR("TUdfStoreService: UDF is not usable as a file name, skipping fetch",
            {"name", name});
        return;
    }
    if (LoadedUdfs.contains(name) || IsNamePending(name, EUdfType::NATIVE_UNSAFE)) {
        return;
    }
    PendingNativeUdfs.push_back(TPendingUdf{
        .Name = name,
        .Md5 = udf.GetMd5(),
        .ExpectedSize = udf.GetSize(),
        .Type = EUdfType::NATIVE_UNSAFE,
    });
}

void TUdfStoreService::EnqueueWasmCompileIfNeeded(const TUdfModule& udf, const TSnapshot* snapshot) {
    if (!AreLibraryDependenciesReady(udf.GetManifest(), snapshot)) {
        return;
    }
    if (IsNamePending(udf.GetName(), EUdfType::WASM)) {
        return;
    }
    // This node does not decide to compile anything on its own: it reports the
    // gap and waits to be assigned, so that all the nodes of a platform do not
    // start the same LLVM run at once.
    RequestArtifact(udf.GetName(), udf.GetUid(), false);
}

void TUdfStoreService::EnqueueWasmLoadIfNeeded(const TUdfModule& udf, const TSnapshot* snapshot) {
    const TString& name = udf.GetName();
    if (LoadedUdfs.contains(name) || IsNamePending(name, EUdfType::WASM)) {
        return;
    }
    // Same snapshot the refresh is applying: CurrentSnapshot is still the previous
    // one until the handler finishes, and on a cold start it is null. Looking
    // there would leave LibraryUids empty even when sdk is Ready in this refresh.
    if (!AreLibraryDependenciesReady(udf.GetManifest(), snapshot)) {
        return;
    }
    try {
        const auto manifest = NWasm::ParseManifest(udf.GetManifest());
        if (manifest.ModuleName != name) {
            YDB_LOG_ERROR("TUdfStoreService: skipping WASM load for manifest declares",
                {"name", name},
                {"moduleName", manifest.ModuleName});
            return;
        }
    } catch (const std::exception& ex) {
        YDB_LOG_ERROR("TUdfStoreService: skipping WASM load due to invalid",
            {"name", name},
            {"manifest", ex.what()});
        return;
    } catch (...) {
        YDB_LOG_ERROR("TUdfStoreService: skipping WASM load due to unknown manifest parse error",
            {"name", name});
        return;
    }
    PendingWasmLoad.push_back(TPendingUdf{
        .Name = name,
        .Uid = udf.GetUid(),
        .Md5 = udf.GetMd5(),
        .ExpectedSize = udf.GetSize(),
        .Type = EUdfType::WASM,
        .Manifest = udf.GetManifest(),
        .ModuleExtension = GetModuleExtensionFromManifest(udf.GetManifest()),
        .LibraryUids = CollectLibraryUids(udf.GetManifest(), snapshot),
    });
}

void TUdfStoreService::EnqueueLibraryCompileIfNeeded(const TUdfModule& library) {
    if (IsLibraryPending(library.GetName())) {
        return;
    }
    RequestArtifact(library.GetName(), library.GetUid(), true);
}

void TUdfStoreService::ReportGapsUnblockedByLibrary(const TString& libraryName) {
    if (!CurrentSnapshot) {
        return;
    }
    for (const auto& [_, udf] : CurrentSnapshot->GetUdfs()) {
        if (udf.GetType() != EUdfType::WASM)
        {
            continue;
        }
        try {
            const auto manifest = NWasm::ParseManifest(udf.GetManifest());
            const bool dependsOnLibrary = std::any_of(
                manifest.RequiredLibraries.begin(),
                manifest.RequiredLibraries.end(),
                [&](const TString& name) { return name == libraryName; });
            if (!dependsOnLibrary) {
                continue;
            }
        } catch (...) {
            continue;
        }
        EnqueueWasmCompileIfNeeded(udf);
    }
}

void TUdfStoreService::UnloadWasmUdfsDependingOnLibrary(const TString& libraryName) {
    if (!CurrentSnapshot) {
        return;
    }
    for (const auto& [name, udf] : CurrentSnapshot->GetUdfs()) {
        if (udf.GetType() != EUdfType::WASM) {
            continue;
        }
        try {
            const auto manifest = NWasm::ParseManifest(udf.GetManifest());
            const bool dependsOnLibrary = std::any_of(
                manifest.RequiredLibraries.begin(),
                manifest.RequiredLibraries.end(),
                [&](const TString& required) { return required == libraryName; });
            if (!dependsOnLibrary) {
                continue;
            }
        } catch (...) {
            continue;
        }
        LoadedUdfs.erase(name);
        UnloadWasmUdf(name);
        EnqueueWasmLoadIfNeeded(udf);
    }
}

void TUdfStoreService::Bootstrap() {
    GapsWithoutControllerGauge = GetServiceCounters(AppData()->Counters, "udf_store")
        ->GetSubgroup("subsystem", "dinode")
        ->GetCounter("WasmGapsWithoutController", false);

    ModulesTablePath = TUdfModule::GetBehaviour()->GetStorageTablePath();
    ModuleChunksTablePath = GetModuleChunksTablePath();
    ArtifactTablePath = GetArtifactTablePath(LocalCpuSpec);
    ArtifactChunksTablePath = GetArtifactChunksTablePath(LocalCpuSpec);

    Become(&TUdfStoreService::StateMain);
    Register(new TUdfStoreInitializer(SelfId(), KvStorageMedia));
}

void TUdfStoreService::EnsureArtifactTable() {
    Register(new TWasmArtifactTableInitializer(
        SelfId(),
        ArtifactTablePath,
        ArtifactChunksTablePath));
}

void TUdfStoreService::Handle(TEvStoreInitialized::TPtr& ev) {
    KvVolumePath = ev->Get()->KvVolumePath;
    YDB_LOG_INFO("TUdfStoreService: infrastructure initialized",
        {"path", KvVolumePath},
        {"cpuSpec", LocalCpuSpec});
    if (EnableWasmUdfFlag) {
        EnsureArtifactTable();
        return;
    }
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvSubscribeExternal(std::make_shared<TSnapshotsFetcher>()));
}

void TUdfStoreService::Handle(TEvArtifactTableInitialized::TPtr& ev) {
    ArtifactTablePath = ev->Get()->ArtifactTablePath;
    YDB_LOG_INFO("TUdfStoreService: artifact table ready",
        {"artifactTablePath", ArtifactTablePath});
    ResolveCompileController();
    ScheduleControllerTick();
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvSubscribeExternal(std::make_shared<TSnapshotsFetcher>()));
}

void TUdfStoreService::Handle(TEvStoreInitFailed::TPtr& ev) {
    YDB_LOG_ERROR("TUdfStoreService: infrastructure initialization",
        {"failed", ev->Get()->ErrorMessage});
    PassAway();
}

void TUdfStoreService::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    auto snapshot = ev->Get()->GetSnapshotPtrAs<TSnapshot>();
    if (!snapshot) {
        YDB_LOG_ERROR("TUdfStoreService: received non-UDF snapshot");
        return;
    }
    YDB_LOG_INFO("TUdfStoreService: received UDF snapshot");

    for (const auto& [name, library] : snapshot->GetLibraries()) {
        const TUdfModule* existing = CurrentSnapshot
            ? CurrentSnapshot->GetLibraryByName(name)
            : nullptr;
        const bool isNew = !existing;
        if (isNew) {
            YDB_LOG_INFO("TUdfStoreService: library added",
                {"name", name},
                {"md5", library.GetMd5()},
                {"version", library.GetVersion()});
            EnqueueLibraryCompileIfNeeded(library);
        } else if (existing->GetUid() != library.GetUid()
            || existing->GetMd5() != library.GetMd5()
            || existing->GetSize() != library.GetSize()
            || existing->GetVersion() != library.GetVersion())
        {
            // uid is the primary re-upload signal: md5 alone is only a checksum
            // and may stay the same across uploads of identical bytes.
            YDB_LOG_INFO("TUdfStoreService: library changed",
                {"name", name},
                {"oldUid", existing->GetUid()},
                {"newUid", library.GetUid()},
                {"oldMd5", existing->GetMd5()},
                {"newMd5", library.GetMd5()});
            UnloadWasmUdfsDependingOnLibrary(name);
            LocallyReadyLibraries.erase(name);
            RequestArtifact(name, library.GetUid(), true);
        } else {
            EnqueueLibraryCompileIfNeeded(library);
        }
    }

    if (CurrentSnapshot) {
        for (const auto& [name, library] : CurrentSnapshot->GetLibraries()) {
            if (!snapshot->GetLibraryByName(name)) {
                YDB_LOG_INFO("TUdfStoreService: library removed",
                    {"name", name});
                UnloadWasmUdfsDependingOnLibrary(name);
            }
        }
    }

    for (const auto& [name, udf] : snapshot->GetUdfs()) {
        const TUdfModule* existing = CurrentSnapshot ? CurrentSnapshot->GetUdfByName(name) : nullptr;
        const bool isNew = !existing;

        if (isNew) {
            YDB_LOG_INFO("TUdfStoreService: UDF added",
                {"name", name},
                {"uid", udf.GetUid()},
                {"type", udf.GetType()},
                {"size", udf.GetSize()});
        } else if (existing->GetUid() != udf.GetUid()
            || existing->GetMd5() != udf.GetMd5()
            || existing->GetSize() != udf.GetSize()
            || existing->GetVersion() != udf.GetVersion()
            || (udf.GetType() == EUdfType::WASM && existing->GetManifest() != udf.GetManifest()))
        {
            YDB_LOG_INFO("TUdfStoreService: UDF changed",
                {"name", name},
                {"oldUid", existing->GetUid()},
                {"newUid", udf.GetUid()},
                {"oldSize", existing->GetSize()},
                {"newSize", udf.GetSize()});
            LoadedUdfs.erase(name);
            FetchRetryCounts.erase(name);
            if (udf.GetType() == EUdfType::WASM) {
                UnloadWasmUdf(name);
            }
        }
        switch (udf.GetType()) {
            case EUdfType::NATIVE_UNSAFE:
                if (!EnableUnsafeNativeUdfFlag) {
                    YDB_LOG_ERROR("TUdfStoreService: EnableUnsafeNativeUdf is not set, skipping UDF",
                        {"name", name});
                    break;
                }
                if (UnsafeNativeUdfDir.empty()) {
                    YDB_LOG_ERROR("TUdfStoreService: EnableUnsafeNativeUdf is set but UnsafeNativeUdfDir is empty, skipping UDF",
                        {"name", name});
                    break;
                }
                if (!LoadedUdfs.contains(name)) {
                    FetchRetryCounts.erase(name);
                }
                EnqueueNativeUdfIfNeeded(udf);
                break;
            case EUdfType::WASM:
                if (!EnableWasmUdfFlag) {
                    YDB_LOG_ERROR("TUdfStoreService: EnableWasmUdf is not set, skipping WASM UDF",
                        {"name", name});
                    break;
                }
                if (!ManifestLooksValid(udf.GetManifest())) {
                    YDB_LOG_ERROR("TUdfStoreService: WASM UDF has invalid or empty manifest, skipping",
                        {"name", name});
                    break;
                }
                if (!LoadedUdfs.contains(name)) {
                    FetchRetryCounts.erase(name);
                }
                EnqueueWasmCompileIfNeeded(udf, snapshot.get());
                break;
            case EUdfType::LIBRARY:
                break;
        }
    }

    if (CurrentSnapshot) {
        for (const auto& [name, udf] : CurrentSnapshot->GetUdfs()) {
            if (!snapshot->GetUdfByName(name)) {
                YDB_LOG_INFO("TUdfStoreService: UDF removed",
                    {"name", name},
                    {"type", udf.GetType()},
                    {"uid", udf.GetUid()});
                LoadedUdfs.erase(name);
                FetchRetryCounts.erase(name);
                if (udf.GetType() == EUdfType::WASM) {
                    UnloadWasmUdf(name);
                } else if (!UnsafeNativeUdfDir.empty() && IsSafeUdfFileName(name)) {
                    const TFsPath path = TFsPath(UnsafeNativeUdfDir) / name;
                    if (NFs::Exists(path.GetPath())) {
                        NFs::Remove(path.GetPath());
                    }
                }
            }
        }
    }

    CurrentSnapshot = snapshot;

    TVector<TString> staleLibraries;
    for (const auto& [name, uid] : LocallyReadyLibraries) {
        const auto* library = CurrentSnapshot->GetLibraryByName(name);
        if (!library || library->GetUid() != uid) {
            staleLibraries.push_back(name);
        }
    }
    for (const auto& name : staleLibraries) {
        LocallyReadyLibraries.erase(name);
    }

    if (!NativeFetchInProgress) {
        FetchNextNativeBody();
    }
    if (!WasmLoadInProgress) {
        FetchNextWasmLoad();
    }
}

void TUdfStoreService::UnloadWasmUdf(const TString& name) {
    if (auto* dynamicRegistry = NKqp::AsDynamicFunctionRegistry(FunctionRegistry.Get())) {
        dynamicRegistry->RemoveModule(name);
    }
    LoadedUdfs.erase(name);
    NWasm::GetWasmModuleCatalog().Unregister(name);
}

void TUdfStoreService::FetchNextNativeBody() {
    if (PendingNativeUdfs.empty()) {
        NativeFetchInProgress = false;
        return;
    }

    NativeFetchInProgress = true;
    const auto& pending = PendingNativeUdfs.front();

    Register(new TKvBodyReadActor(
        SelfId(),
        pending.Name,
        pending.Md5,
        KvVolumePath,
        UnsafeNativeUdfDir,
        FunctionRegistry,
        pending.ExpectedSize));
}

void TUdfStoreService::FetchNextLibraryCompile() {
    if (PendingLibraryCompile.empty()) {
        LibraryCompileInProgress = false;
        return;
    }

    LibraryCompileInProgress = true;
    const auto& pending = PendingLibraryCompile.front();

    Register(new TWasmLibraryCompileActor(
        SelfId(),
        pending.Name,
        LocalCpuSpec,
        ModulesTablePath,
        ModuleChunksTablePath,
        ArtifactTablePath,
        ArtifactChunksTablePath));
}

void TUdfStoreService::FetchNextWasmCompile() {
    if (PendingWasmCompile.empty()) {
        WasmCompileInProgress = false;
        return;
    }

    WasmCompileInProgress = true;
    const auto& pending = PendingWasmCompile.front();

    Register(new TWasmCompileActor(
        SelfId(),
        pending.Name,
        pending.Manifest,
        LocalCpuSpec,
        ModulesTablePath,
        ModuleChunksTablePath,
        ArtifactTablePath,
        ArtifactChunksTablePath,
        pending.LibraryUids));
}

void TUdfStoreService::FetchNextWasmLoad() {
    if (PendingWasmLoad.empty()) {
        WasmLoadInProgress = false;
        return;
    }

    WasmLoadInProgress = true;
    const auto& pending = PendingWasmLoad.front();

    Register(new TWasmArtifactLoadActor(
        SelfId(),
        pending.Name,
        pending.Manifest,
        pending.Uid,
        ArtifactTablePath,
        ArtifactChunksTablePath,
        pending.LibraryUids,
        FunctionRegistry));
}

void TUdfStoreService::Handle(TEvLibraryCompileResponse::TPtr& ev) {
    const bool fromCompile = !PendingLibraryCompile.empty()
        && PendingLibraryCompile.front().Name == ev->Get()->LibraryName;
    if (!fromCompile) {
        YDB_LOG_WARN("TUdfStoreService: received unexpected TEvLibraryCompileResponse for library",
            {"libraryName", ev->Get()->LibraryName});
        return;
    }

    const TString libraryName = ev->Get()->LibraryName;
    PendingLibraryCompile.pop_front();
    LibraryCompileInProgress = false;

    if (ev->Get()->Deferred) {
        YDB_LOG_INFO("TUdfStoreService: deferred library",
            {"libraryName", libraryName},
            {"errorMessage", ev->Get()->ErrorMessage});
    } else if (ev->Get()->Success) {
        YDB_LOG_INFO("TUdfStoreService: library compiled for cpu_spec",
            {"libraryName", libraryName},
            {"localCpuSpec", LocalCpuSpec});
        if (CurrentSnapshot) {
            if (const auto* library = CurrentSnapshot->GetLibraryByName(libraryName)) {
                LocallyReadyLibraries[libraryName] = library->GetUid();
            }
        }
        ReportGapsUnblockedByLibrary(libraryName);
    } else {
        YDB_LOG_ERROR("TUdfStoreService: failed to compile library",
            {"libraryName", libraryName},
            {"errorMessage", ev->Get()->ErrorMessage});
    }

    const auto assignmentIt = LibraryAssignments.find(libraryName);
    if (assignmentIt != LibraryAssignments.end()) {
        ReportCompileResult(
            assignmentIt->second,
            ev->Get()->Success,
            ev->Get()->Deferred,
            ev->Get()->ErrorMessage);
        LibraryAssignments.erase(assignmentIt);
    }

    FetchNextLibraryCompile();
}

void TUdfStoreService::Handle(TEvWasmCompileResponse::TPtr& ev) {
    const bool fromCompile = !PendingWasmCompile.empty() && PendingWasmCompile.front().Name == ev->Get()->Name;
    if (!fromCompile) {
        YDB_LOG_WARN("TUdfStoreService: received unexpected TEvWasmCompileResponse for UDF",
            {"name", ev->Get()->Name});
        return;
    }

    TPendingUdf pending = std::move(PendingWasmCompile.front());
    PendingWasmCompile.pop_front();
    WasmCompileInProgress = false;

    const TString& name = ev->Get()->Name;
    const auto assignmentIt = ModuleAssignments.find(name);
    const bool assigned = assignmentIt != ModuleAssignments.end();

    if (ev->Get()->Deferred) {
        YDB_LOG_INFO("TUdfStoreService: deferred WASM UDF",
            {"name", name},
            {"errorMessage", ev->Get()->ErrorMessage});
    } else if (ev->Get()->Success) {
        PendingWasmLoad.push_back(std::move(pending));
        YDB_LOG_INFO("TUdfStoreService: WASM UDF compiled for cpu_spec",
            {"name", name},
            {"localCpuSpec", LocalCpuSpec});
    } else if (assigned) {
        // The controller owns the retry budget for a compile; a local retry here
        // would race with whatever it decides to do next.
        YDB_LOG_ERROR("TUdfStoreService: failed to compile assigned WASM UDF",
            {"name", name},
            {"errorMessage", ev->Get()->ErrorMessage});
    } else {
        // Every compile starts from an assignment, so there is nobody to report
        // this to and nobody to schedule a retry: the gap is offered again on
        // the next snapshot refresh.
        YDB_LOG_ERROR("TUdfStoreService: failed to compile WASM UDF outside of an",
            {"name", name},
            {"assignment", ev->Get()->ErrorMessage});
    }

    if (assigned) {
        ReportCompileResult(
            assignmentIt->second,
            ev->Get()->Success,
            ev->Get()->Deferred,
            ev->Get()->ErrorMessage);
        ModuleAssignments.erase(assignmentIt);
    }

    FetchNextWasmCompile();
    if (!WasmLoadInProgress) {
        FetchNextWasmLoad();
    }
}

void TUdfStoreService::Handle(TEvReadBodyResponse::TPtr& ev) {
    // The same name may sit in both queues (a native UDF and a WASM UDF), so
    // match the type too: taking the wrong queue would leave the other fetch
    // marked in progress forever.
    const bool fromNative = ev->Get()->Type != EUdfType::WASM
        && !PendingNativeUdfs.empty() && PendingNativeUdfs.front().Name == ev->Get()->Name;
    const bool fromWasm = ev->Get()->Type == EUdfType::WASM
        && !PendingWasmLoad.empty() && PendingWasmLoad.front().Name == ev->Get()->Name;

    if (!fromNative && !fromWasm) {
        YDB_LOG_WARN("TUdfStoreService: received unexpected TEvReadBodyResponse for UDF with no matching pending fetch",
            {"name", ev->Get()->Name});
        return;
    }

    TPendingUdf pending = fromNative
        ? std::move(PendingNativeUdfs.front())
        : std::move(PendingWasmLoad.front());
    if (fromNative) {
        PendingNativeUdfs.pop_front();
        NativeFetchInProgress = false;
    } else {
        PendingWasmLoad.pop_front();
        WasmLoadInProgress = false;
    }

    if (ev->Get()->Success) {
        LoadedUdfs.insert(pending.Name);
        FetchRetryCounts.erase(pending.Name);
        if (pending.Type == EUdfType::WASM) {
            YDB_LOG_INFO("TUdfStoreService: WASM UDF loaded from artifact table",
                {"name", pending.Name},
                {"artifactTablePath", ArtifactTablePath});
        } else {
            YDB_LOG_INFO("TUdfStoreService: native UDF saved",
                {"name", pending.Name},
                {"unsafeNativeUdfDir", UnsafeNativeUdfDir});
        }
    } else {
        const TString name = pending.Name;
        ui32& retryCount = FetchRetryCounts[name];
        if (retryCount < MaxFetchRetries) {
            ++retryCount;
            YDB_LOG_ERROR("TUdfStoreService: failed to load UDF",
                {"name", name},
                {"retryCount", retryCount},
                {"maxFetchRetries", MaxFetchRetries},
                {"errorMessage", ev->Get()->ErrorMessage});
            if (pending.Type == EUdfType::WASM) {
                PendingWasmLoad.push_back(std::move(pending));
            } else {
                PendingNativeUdfs.push_back(std::move(pending));
            }
        } else {
            YDB_LOG_ERROR("TUdfStoreService: giving up on UDF after",
                {"name", name},
                {"maxFetchRetries", MaxFetchRetries},
                {"retries", ev->Get()->ErrorMessage});
        }
    }

    if (fromWasm) {
        FetchNextWasmLoad();
    } else {
        FetchNextNativeBody();
    }
}

NActors::TActorId MakeServiceId(ui32 nodeId) {
    return NActors::TActorId(nodeId, "SrvcUdfStore");
}

NActors::IActor* CreateService(const NKikimrConfig::TUdfStoreConfig& serviceConfig, TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> functionRegistry) {
    if (!serviceConfig.GetEnabled()) {
        return nullptr;
    }
    return new TUdfStoreService(serviceConfig, std::move(functionRegistry));
}

} // namespace NKikimr::NUdfStore
