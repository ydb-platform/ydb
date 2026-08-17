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

bool TUdfStoreService::IsMd5Pending(const TString& md5, EUdfType type) const {
    if (type == EUdfType::WASM) {
        auto pred = [&](const TPendingUdf& pending) { return pending.Md5 == md5; };
        return std::any_of(PendingWasmCompile.begin(), PendingWasmCompile.end(), pred)
            || std::any_of(PendingWasmLoad.begin(), PendingWasmLoad.end(), pred);
    }
    return std::any_of(PendingNativeUdfs.begin(), PendingNativeUdfs.end(),
        [&](const TPendingUdf& pending) { return pending.Md5 == md5; });
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

void TUdfStoreService::EnqueueNativeUdfIfNeeded(const TString& md5, ui64 expectedSize) {
    if (expectedSize == 0) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: UDF '" << md5 << "' has zero size in metadata, skipping fetch";
        return;
    }
    if (LoadedUdfs.contains(md5) || IsMd5Pending(md5, EUdfType::NATIVE_UNSAFE)) {
        return;
    }
    PendingNativeUdfs.push_back(TPendingUdf{
        .Md5 = md5,
        .ExpectedSize = expectedSize,
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
    if (IsMd5Pending(udf.GetMd5(), EUdfType::WASM)) {
        return;
    }
    PendingWasmCompile.push_back(TPendingUdf{
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
    if (LoadedUdfs.contains(udf.GetMd5()) || IsMd5Pending(udf.GetMd5(), EUdfType::WASM)) {
        return;
    }
    try {
        const auto manifest = NWasm::ParseManifest(udf.GetManifest());
        TVector<TString> conflictingMd5s;
        for (const auto& [md5, moduleName] : LoadedWasmModuleNames) {
            if (moduleName == manifest.ModuleName && md5 != udf.GetMd5()) {
                conflictingMd5s.push_back(md5);
            }
        }
        for (const auto& md5 : conflictingMd5s) {
            ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: unloading previous WASM module '"
                << manifest.ModuleName << "' md5=" << md5
                << " before loading md5=" << udf.GetMd5();
            UnloadWasmUdf(md5);
        }
    } catch (const std::exception& ex) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: skipping WASM load for md5=" << udf.GetMd5()
            << " due to invalid manifest: " << ex.what();
        return;
    } catch (...) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: skipping WASM load for md5=" << udf.GetMd5()
            << " due to unknown manifest parse error";
        return;
    }
    PendingWasmLoad.push_back(TPendingUdf{
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
    for (const auto& [md5, udf] : CurrentSnapshot->GetUdfs()) {
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
    for (const auto& [md5, udf] : CurrentSnapshot->GetUdfs()) {
        if (udf.GetType() != EUdfType::WASM) {
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
        LoadedUdfs.erase(md5);
        UnloadWasmUdf(md5);
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

    for (const auto& [md5, udf] : snapshot->GetUdfs()) {
        const TUdfModule* existing = CurrentSnapshot ? CurrentSnapshot->GetUdfByMd5(md5) : nullptr;
        const bool isNew = !existing;

        if (isNew) {
            ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: UDF added"
                << ", md5=" << md5
                << ", name=" << udf.GetName()
                << ", type=" << udf.GetType()
                << ", size=" << udf.GetSize();
        } else if (existing->GetSize() != udf.GetSize()
            || existing->GetVersion() != udf.GetVersion()
            || (udf.GetType() == EUdfType::WASM && existing->GetManifest() != udf.GetManifest()))
        {
            ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: UDF changed"
                << ", md5=" << md5
                << ", old_size=" << existing->GetSize()
                << ", new_size=" << udf.GetSize();
            LoadedUdfs.erase(md5);
            FetchRetryCounts.erase(md5);
            if (udf.GetType() == EUdfType::WASM) {
                UnloadWasmUdf(md5);
            }
        }
        switch (udf.GetType()) {
            case EUdfType::NATIVE_UNSAFE:
                if (!EnableUnsafeNativeUdfFlag) {
                    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                        << "TUdfStoreService: EnableUnsafeNativeUdf is not set,"
                        << " skipping UDF '" << md5 << "' with name '" << udf.GetName() << "'";
                    break;
                }
                if (UnsafeNativeUdfDir.empty()) {
                    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                        << "TUdfStoreService: EnableUnsafeNativeUdf is set but UnsafeNativeUdfDir is empty,"
                        << " skipping UDF '" << md5 << "' with name '" << udf.GetName() << "'";
                    break;
                }
                if (!LoadedUdfs.contains(md5)) {
                    FetchRetryCounts.erase(md5);
                }
                EnqueueNativeUdfIfNeeded(md5, udf.GetSize());
                break;
            case EUdfType::WASM:
                if (!EnableWasmUdfFlag) {
                    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                        << "TUdfStoreService: EnableWasmUdf is not set,"
                        << " skipping WASM UDF '" << md5 << "' with name '" << udf.GetName() << "'";
                    break;
                }
                if (!ManifestLooksValid(udf.GetManifest())) {
                    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                        << "TUdfStoreService: WASM UDF '" << md5
                        << "' has invalid or empty manifest, skipping";
                    break;
                }
                if (!LoadedUdfs.contains(md5)) {
                    FetchRetryCounts.erase(md5);
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
        for (const auto& [md5, udf] : CurrentSnapshot->GetUdfs()) {
            if (!snapshot->GetUdfByMd5(md5)) {
                ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                    << "TUdfStoreService: UDF removed"
                    << ": md5=" << md5
                    << ", type=" << udf.GetType()
                    << ", name=" << udf.GetName();
                LoadedUdfs.erase(md5);
                FetchRetryCounts.erase(md5);
                if (udf.GetType() == EUdfType::WASM) {
                    UnloadWasmUdf(md5);
                } else if (!UnsafeNativeUdfDir.empty()) {
                    const TFsPath path = TFsPath(UnsafeNativeUdfDir) / md5;
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

void TUdfStoreService::UnloadWasmUdf(const TString& md5) {
    if (auto it = LoadedWasmModuleNames.find(md5); it != LoadedWasmModuleNames.end()) {
        if (auto* dynamicRegistry = NKqp::AsDynamicFunctionRegistry(FunctionRegistry.Get())) {
            dynamicRegistry->RemoveModule(it->second);
        }
        LoadedWasmModuleNames.erase(it);
    }
    LoadedUdfs.erase(md5);
    NWasm::GetWasmModuleCatalog().Unregister(md5);
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
        pending.Md5,
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
        pending.Md5,
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
    const bool fromCompile = !PendingWasmCompile.empty() && PendingWasmCompile.front().Md5 == ev->Get()->Md5;
    if (!fromCompile) {
        ALS_WARN(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: received unexpected TEvWasmCompileResponse for UDF '"
            << ev->Get()->Md5 << "'";
        return;
    }

    TPendingUdf pending = std::move(PendingWasmCompile.front());
    PendingWasmCompile.pop_front();
    WasmCompileInProgress = false;

    if (ev->Get()->Deferred) {
        ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: deferred WASM UDF '" << ev->Get()->Md5
            << "': " << ev->Get()->ErrorMessage;
    } else if (ev->Get()->Success) {
        PendingWasmLoad.push_back(std::move(pending));
        ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: WASM UDF '" << ev->Get()->Md5 << "' compiled for cpu_spec "
            << LocalCpuSpec;
    } else {
        const TString& md5 = ev->Get()->Md5;
        ui32& retryCount = FetchRetryCounts[md5];
        if (retryCount < MaxFetchRetries) {
            ++retryCount;
            PendingWasmCompile.push_back(std::move(pending));
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: failed to compile WASM UDF '" << md5
                << "' (retry " << retryCount << "/" << MaxFetchRetries
                << "): " << ev->Get()->ErrorMessage;
        } else {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: giving up on WASM UDF '" << md5
                << "' after " << MaxFetchRetries << " compile retries: " << ev->Get()->ErrorMessage;
        }
    }

    FetchNextWasmCompile();
    if (!WasmLoadInProgress) {
        FetchNextWasmLoad();
    }
}

void TUdfStoreService::Handle(TEvReadBodyResponse::TPtr& ev) {
    const bool fromNative = !PendingNativeUdfs.empty() && PendingNativeUdfs.front().Md5 == ev->Get()->Name;
    const bool fromWasm = !PendingWasmLoad.empty() && PendingWasmLoad.front().Md5 == ev->Get()->Name;

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
        LoadedUdfs.insert(pending.Md5);
        FetchRetryCounts.erase(pending.Md5);
        if (pending.Type == EUdfType::WASM) {
            try {
                const auto manifest = NWasm::ParseManifest(pending.Manifest);
                LoadedWasmModuleNames[pending.Md5] = manifest.ModuleName;
            } catch (const std::exception& ex) {
                ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                    << "TUdfStoreService: loaded WASM UDF md5=" << pending.Md5
                    << " but failed to record module name: " << ex.what();
            } catch (...) {
                ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                    << "TUdfStoreService: loaded WASM UDF md5=" << pending.Md5
                    << " but failed to record module name due to unknown error";
            }
            ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: WASM UDF '" << pending.Md5
                << "' loaded from artifact table " << ArtifactTablePath;
        } else {
            ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: native UDF '" << pending.Md5
                << "' saved to " << UnsafeNativeUdfDir;
        }
    } else {
        ui32& retryCount = FetchRetryCounts[pending.Md5];
        if (retryCount < MaxFetchRetries) {
            ++retryCount;
            if (pending.Type == EUdfType::WASM) {
                PendingWasmLoad.push_back(std::move(pending));
            } else {
                EnqueueNativeUdfIfNeeded(pending.Md5, pending.ExpectedSize);
            }
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: failed to load UDF '" << pending.Md5
                << "' (retry " << retryCount << "/" << MaxFetchRetries
                << "): " << ev->Get()->ErrorMessage;
        } else {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: giving up on UDF '" << pending.Md5
                << "' after " << MaxFetchRetries << " retries: " << ev->Get()->ErrorMessage;
        }
    }

    if (pending.Type == EUdfType::WASM) {
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
