#pragma once
#include "metadata_subscription/snapshot.h"
#include "kv_body_store.h"
#include "wasm_compartment_actor.h"
#include "wasm_compile_actor.h"
#include "wasm_library_compile_actor.h"
#include "wasm_artifact_load_actor.h"
#include "artifact_table_initializer.h"
#include "store_initializer.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/accessor/accessor.h>

#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NKikimr::NUdfStore {

struct TPendingUdf {
    TString Md5;
    ui64 ExpectedSize = 0;
    EUdfType Type = EUdfType::NATIVE_UNSAFE;
    TString Manifest;
    TString ModuleExtension = "wasm";
};

struct TPendingLibrary {
    TString Name;
};

class TUdfStoreService: public NActors::TActorBootstrapped<TUdfStoreService> {
private:
    using TBase = NActors::TActorBootstrapped<TUdfStoreService>;
    TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;
    TString KvStorageMedia;
    YDB_READONLY_FLAG(EnableUnsafeNativeUdf, false);
    TString KvVolumePath;
    TString UnsafeNativeUdfDir;
    YDB_READONLY_FLAG(EnableWasmUdf, false);
    TString WasmCpuSpecOverride;
    TString LocalCpuSpec;
    TString WasmSourceTablePath;
    TString WasmSourceChunksTablePath;
    TString LibrarySourceTablePath;
    TString LibrarySourceChunksTablePath;
    TString ArtifactTablePath;
    TString ArtifactChunksTablePath;
    TString MetaTablePath;
    std::shared_ptr<TSnapshot> CurrentSnapshot;

    bool NativeFetchInProgress = false;
    bool WasmCompileInProgress = false;
    bool WasmLoadInProgress = false;
    bool LibraryCompileInProgress = false;
    THashSet<TString> LoadedUdfs;
    THashMap<TString, ui32> FetchRetryCounts;

    static constexpr ui32 MaxFetchRetries = 5;

    std::deque<TPendingUdf> PendingNativeUdfs;
    std::deque<TPendingUdf> PendingWasmCompile;
    std::deque<TPendingUdf> PendingWasmLoad;
    std::deque<TPendingLibrary> PendingLibraryCompile;
    THashMap<TString, NActors::TActorId> WasmCompartmentActors;
    THashMap<TString, TString> LoadedWasmModuleNames;

    bool IsMd5Pending(const TString& md5, EUdfType type) const;
    bool IsLibraryPending(const TString& name) const;
    void EnqueueNativeUdfIfNeeded(const TString& md5, ui64 expectedSize);
    void EnqueueWasmCompileIfNeeded(const TUdfMeta& udf);
    void EnqueueWasmLoadIfNeeded(const TUdfMeta& udf);
    void EnqueueLibraryCompileIfNeeded(const TUdfLibrarySource& library);
    bool AreLibraryDependenciesReady(TStringBuf manifest) const;
    void RetryPendingWasmCompilesForLibrary(const TString& libraryName);
    void UnloadWasmUdfsDependingOnLibrary(const TString& libraryName);
    void FetchNextNativeBody();
    void FetchNextWasmCompile();
    void FetchNextWasmLoad();
    void FetchNextLibraryCompile();
    NActors::TActorId GetOrCreateWasmCompartmentActor(const TString& md5);
    void UnloadWasmUdf(const TString& md5);
    static TString GetModuleExtensionFromManifest(TStringBuf manifest);
    void EnsureArtifactTable();

protected:
    void Handle(TEvStoreInitialized::TPtr& ev);
    void Handle(TEvArtifactTableInitialized::TPtr& ev);
    void Handle(TEvStoreInitFailed::TPtr& ev);
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);
    void Handle(TEvReadBodyResponse::TPtr& ev);
    void Handle(TEvWasmCompileResponse::TPtr& ev);
    void Handle(TEvLibraryCompileResponse::TPtr& ev);

public:
    TUdfStoreService(const NKikimrConfig::TUdfStoreConfig& config, TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> functionRegistry);

    void Bootstrap();

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStoreInitialized, Handle);
            hFunc(TEvArtifactTableInitialized, Handle);
            hFunc(TEvStoreInitFailed, Handle);
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            hFunc(TEvReadBodyResponse, Handle);
            hFunc(TEvWasmCompileResponse, Handle);
            hFunc(TEvLibraryCompileResponse, Handle);
            default:
                break;
        }
    }
};

NActors::IActor* CreateService(const NKikimrConfig::TUdfStoreConfig& serviceConfig, TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> functionRegistry);
NActors::TActorId MakeServiceId(ui32 nodeId);

}
