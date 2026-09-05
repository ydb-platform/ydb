#include "service.h"
#include "metadata_subscription/fetcher.h"
#include "metadata_subscription/storage_paths.h"
#include "cpu_spec.h"
#include "wasm/manifest.h"
#include "wasm/module_catalog.h"

#include <ydb/services/metadata/service.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/dynamic_function_registry.h>

#include <library/cpp/json/json_reader.h>
#include <util/folder/path.h>
#include <util/system/fs.h>

#include <algorithm>

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
            if (LocallyReadyLibraries.contains(libraryName)) {
                continue;
            }
            const auto* library = snap->GetLibraryByName(libraryName);
            if (!library || library->GetCompileStatus() != ECompileStatus::Ready) {
                return false;
            }
        }
        return true;
    } catch (...) {
        return false;
    }
}

void TUdfStoreService::EnqueueNativeUdfIfNeeded(const TUdfModule& udf) {
    const TString& name = udf.GetName();
    if (udf.GetSize() == 0) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: UDF '" << name << "' has zero size in metadata, skipping fetch";
        return;
    }
    // The name becomes a path under UnsafeNativeUdfDir once the body arrives.
    if (!IsSafeUdfFileName(name)) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: UDF '" << name << "' is not usable as a file name, skipping fetch";
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
    if (udf.GetCompileStatus() == ECompileStatus::Ready
        || udf.GetCompileStatus() == ECompileStatus::Failed)
    {
        return;
    }
    if (!AreLibraryDependenciesReady(udf.GetManifest(), snapshot)) {
        return;
    }
    if (IsNamePending(udf.GetName(), EUdfType::WASM)) {
        return;
    }
    PendingWasmCompile.push_back(TPendingUdf{
        .Name = udf.GetName(),
        .Md5 = udf.GetMd5(),
        .ExpectedSize = udf.GetSize(),
        .Type = EUdfType::WASM,
        .Manifest = udf.GetManifest(),
        .ModuleExtension = GetModuleExtensionFromManifest(udf.GetManifest()),
    });
}

void TUdfStoreService::EnqueueWasmLoadIfNeeded(const TUdfModule& udf) {
    if (udf.GetCompileStatus() != ECompileStatus::Ready) {
        return;
    }
    const TString& name = udf.GetName();
    if (LoadedUdfs.contains(name) || IsNamePending(name, EUdfType::WASM)) {
        return;
    }
    try {
        const auto manifest = NWasm::ParseManifest(udf.GetManifest());
        if (manifest.ModuleName != name) {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: skipping WASM load for name=" << name
                << ": manifest declares module_name=" << manifest.ModuleName;
            return;
        }
    } catch (const std::exception& ex) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: skipping WASM load for name=" << name
            << " due to invalid manifest: " << ex.what();
        return;
    } catch (...) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: skipping WASM load for name=" << name
            << " due to unknown manifest parse error";
        return;
    }
    PendingWasmLoad.push_back(TPendingUdf{
        .Name = name,
        .Md5 = udf.GetMd5(),
        .ExpectedSize = udf.GetSize(),
        .Type = EUdfType::WASM,
        .Manifest = udf.GetManifest(),
        .ModuleExtension = GetModuleExtensionFromManifest(udf.GetManifest()),
    });
}

void TUdfStoreService::EnqueueLibraryCompileIfNeeded(const TUdfModule& library) {
    if (library.GetCompileStatus() == ECompileStatus::Ready
        || library.GetCompileStatus() == ECompileStatus::Failed)
    {
        return;
    }
    if (IsLibraryPending(library.GetName())) {
        return;
    }
    PendingLibraryCompile.push_back(TPendingLibrary{
        .Name = library.GetName(),
    });
}

void TUdfStoreService::RetryPendingWasmCompilesForLibrary(const TString& libraryName) {
    if (!CurrentSnapshot) {
        return;
    }
    for (const auto& [name, udf] : CurrentSnapshot->GetUdfs()) {
        Y_UNUSED(name);
        if (udf.GetType() != EUdfType::WASM
            || udf.GetCompileStatus() == ECompileStatus::Ready
            || udf.GetCompileStatus() == ECompileStatus::Failed)
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
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TUdfStoreService: infrastructure initialized, KV Volume path: " << KvVolumePath
        << ", local cpu_spec: " << LocalCpuSpec;
    if (EnableWasmUdfFlag) {
        EnsureArtifactTable();
        return;
    }
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvSubscribeExternal(std::make_shared<TSnapshotsFetcher>()));
}

void TUdfStoreService::Handle(TEvArtifactTableInitialized::TPtr& ev) {
    ArtifactTablePath = ev->Get()->ArtifactTablePath;
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TUdfStoreService: artifact table ready at " << ArtifactTablePath;
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvSubscribeExternal(std::make_shared<TSnapshotsFetcher>()));
}

void TUdfStoreService::Handle(TEvStoreInitFailed::TPtr& ev) {
    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
        << "TUdfStoreService: infrastructure initialization failed: " << ev->Get()->ErrorMessage;
    PassAway();
}

void TUdfStoreService::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    auto snapshot = ev->Get()->GetSnapshotPtrAs<TSnapshot>();
    if (!snapshot) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: received non-UDF snapshot";
        return;
    }
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TUdfStoreService: received UDF snapshot";

    for (const auto& [name, library] : snapshot->GetLibraries()) {
        const TUdfModule* existing = CurrentSnapshot
            ? CurrentSnapshot->GetLibraryByName(name)
            : nullptr;
        const bool isNew = !existing;
        if (isNew) {
            ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: library added"
                << ", name=" << name
                << ", md5=" << library.GetMd5()
                << ", version=" << library.GetVersion();
            EnqueueLibraryCompileIfNeeded(library);
        } else if (existing->GetMd5() != library.GetMd5()
            || existing->GetVersion() != library.GetVersion())
        {
            ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: library changed"
                << ", name=" << name
                << ", old_md5=" << existing->GetMd5()
                << ", new_md5=" << library.GetMd5();
            UnloadWasmUdfsDependingOnLibrary(name);
            if (!IsLibraryPending(name)) {
                PendingLibraryCompile.push_back(TPendingLibrary{.Name = name});
            }
        } else {
            EnqueueLibraryCompileIfNeeded(library);
        }
    }

    if (CurrentSnapshot) {
        for (const auto& [name, library] : CurrentSnapshot->GetLibraries()) {
            if (!snapshot->GetLibraryByName(name)) {
                ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                    << "TUdfStoreService: library removed: name=" << name;
                UnloadWasmUdfsDependingOnLibrary(name);
            }
        }
    }

    for (const auto& [name, udf] : snapshot->GetUdfs()) {
        const TUdfModule* existing = CurrentSnapshot ? CurrentSnapshot->GetUdfByName(name) : nullptr;
        const bool isNew = !existing;

        if (isNew) {
            ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: UDF added"
                << ", name=" << name
                << ", uid=" << udf.GetUid()
                << ", type=" << udf.GetType()
                << ", size=" << udf.GetSize();
        } else if (existing->GetUid() != udf.GetUid()
            || existing->GetMd5() != udf.GetMd5()
            || existing->GetSize() != udf.GetSize()
            || existing->GetVersion() != udf.GetVersion()
            || (udf.GetType() == EUdfType::WASM && existing->GetManifest() != udf.GetManifest()))
        {
            ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: UDF changed"
                << ", name=" << name
                << ", old_uid=" << existing->GetUid()
                << ", new_uid=" << udf.GetUid()
                << ", old_size=" << existing->GetSize()
                << ", new_size=" << udf.GetSize();
            LoadedUdfs.erase(name);
            FetchRetryCounts.erase(name);
            if (udf.GetType() == EUdfType::WASM) {
                UnloadWasmUdf(name);
            }
        }
        switch (udf.GetType()) {
            case EUdfType::NATIVE_UNSAFE:
                if (!EnableUnsafeNativeUdfFlag) {
                    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                        << "TUdfStoreService: EnableUnsafeNativeUdf is not set,"
                        << " skipping UDF '" << name << "'";
                    break;
                }
                if (UnsafeNativeUdfDir.empty()) {
                    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                        << "TUdfStoreService: EnableUnsafeNativeUdf is set but UnsafeNativeUdfDir is empty,"
                        << " skipping UDF '" << name << "'";
                    break;
                }
                if (!LoadedUdfs.contains(name)) {
                    FetchRetryCounts.erase(name);
                }
                EnqueueNativeUdfIfNeeded(udf);
                break;
            case EUdfType::WASM:
                if (!EnableWasmUdfFlag) {
                    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                        << "TUdfStoreService: EnableWasmUdf is not set,"
                        << " skipping WASM UDF '" << name << "'";
                    break;
                }
                if (!ManifestLooksValid(udf.GetManifest())) {
                    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                        << "TUdfStoreService: WASM UDF '" << name
                        << "' has invalid or empty manifest, skipping";
                    break;
                }
                if (!LoadedUdfs.contains(name)) {
                    FetchRetryCounts.erase(name);
                }
                if (udf.GetCompileStatus() != ECompileStatus::Ready) {
                    EnqueueWasmCompileIfNeeded(udf, snapshot.get());
                } else {
                    EnqueueWasmLoadIfNeeded(udf);
                }
                break;
            case EUdfType::LIBRARY:
                break;
        }
    }

    if (CurrentSnapshot) {
        for (const auto& [name, udf] : CurrentSnapshot->GetUdfs()) {
            if (!snapshot->GetUdfByName(name)) {
                ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                    << "TUdfStoreService: UDF removed"
                    << ": name=" << name
                    << ", type=" << udf.GetType()
                    << ", uid=" << udf.GetUid();
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

    TVector<TString> confirmedLibraries;
    for (const auto& name : LocallyReadyLibraries) {
        const auto* library = CurrentSnapshot->GetLibraryByName(name);
        if (library && library->GetCompileStatus() == ECompileStatus::Ready) {
            confirmedLibraries.push_back(name);
        }
    }
    for (const auto& name : confirmedLibraries) {
        LocallyReadyLibraries.erase(name);
    }

    if (!NativeFetchInProgress) {
        FetchNextNativeBody();
    }
    if (!LibraryCompileInProgress) {
        FetchNextLibraryCompile();
    }
    if (!WasmCompileInProgress) {
        FetchNextWasmCompile();
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
        ArtifactChunksTablePath));
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
        ArtifactTablePath,
        ArtifactChunksTablePath,
        FunctionRegistry));
}

void TUdfStoreService::Handle(TEvLibraryCompileResponse::TPtr& ev) {
    const bool fromCompile = !PendingLibraryCompile.empty()
        && PendingLibraryCompile.front().Name == ev->Get()->LibraryName;
    if (!fromCompile) {
        ALS_WARN(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: received unexpected TEvLibraryCompileResponse for library '"
            << ev->Get()->LibraryName << "'";
        return;
    }

    const TString libraryName = ev->Get()->LibraryName;
    PendingLibraryCompile.pop_front();
    LibraryCompileInProgress = false;

    if (ev->Get()->Success) {
        ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: library '" << libraryName
            << "' compiled for cpu_spec " << LocalCpuSpec;
        LocallyReadyLibraries.insert(libraryName);
        RetryPendingWasmCompilesForLibrary(libraryName);
    } else {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: failed to compile library '" << libraryName
            << "': " << ev->Get()->ErrorMessage;
    }

    FetchNextLibraryCompile();
}

void TUdfStoreService::Handle(TEvWasmCompileResponse::TPtr& ev) {
    const bool fromCompile = !PendingWasmCompile.empty() && PendingWasmCompile.front().Name == ev->Get()->Name;
    if (!fromCompile) {
        ALS_WARN(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: received unexpected TEvWasmCompileResponse for UDF '"
            << ev->Get()->Name << "'";
        return;
    }

    TPendingUdf pending = std::move(PendingWasmCompile.front());
    PendingWasmCompile.pop_front();
    WasmCompileInProgress = false;

    if (ev->Get()->Deferred) {
        ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: deferred WASM UDF '" << ev->Get()->Name
            << "': " << ev->Get()->ErrorMessage;
    } else if (ev->Get()->Success) {
        PendingWasmLoad.push_back(std::move(pending));
        ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: WASM UDF '" << ev->Get()->Name << "' compiled for cpu_spec "
            << LocalCpuSpec;
    } else {
        const TString& name = ev->Get()->Name;
        ui32& retryCount = FetchRetryCounts[name];
        if (retryCount < MaxFetchRetries) {
            ++retryCount;
            PendingWasmCompile.push_back(std::move(pending));
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: failed to compile WASM UDF '" << name
                << "' (retry " << retryCount << "/" << MaxFetchRetries
                << "): " << ev->Get()->ErrorMessage;
        } else {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: giving up on WASM UDF '" << name
                << "' after " << MaxFetchRetries << " compile retries: " << ev->Get()->ErrorMessage;
        }
    }

    FetchNextWasmCompile();
    if (!WasmLoadInProgress) {
        FetchNextWasmLoad();
    }
}

void TUdfStoreService::Handle(TEvReadBodyResponse::TPtr& ev) {
    const bool fromNative = !PendingNativeUdfs.empty() && PendingNativeUdfs.front().Name == ev->Get()->Name;
    const bool fromWasm = !PendingWasmLoad.empty() && PendingWasmLoad.front().Name == ev->Get()->Name;

    if (!fromNative && !fromWasm) {
        ALS_WARN(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: received unexpected TEvReadBodyResponse for UDF '"
            << ev->Get()->Name << "' with no matching pending fetch";
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
            ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: WASM UDF '" << pending.Name
                << "' loaded from artifact table " << ArtifactTablePath;
        } else {
            ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: native UDF '" << pending.Name
                << "' saved to " << UnsafeNativeUdfDir;
        }
    } else {
        const TString name = pending.Name;
        ui32& retryCount = FetchRetryCounts[name];
        if (retryCount < MaxFetchRetries) {
            ++retryCount;
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: failed to load UDF '" << name
                << "' (retry " << retryCount << "/" << MaxFetchRetries
                << "): " << ev->Get()->ErrorMessage;
            if (pending.Type == EUdfType::WASM) {
                PendingWasmLoad.push_back(std::move(pending));
            } else {
                PendingNativeUdfs.push_back(std::move(pending));
            }
        } else {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: giving up on UDF '" << name
                << "' after " << MaxFetchRetries << " retries: " << ev->Get()->ErrorMessage;
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
