#pragma once
#include "metadata_subscription/snapshot.h"
#include "kv_body_store.h"
#include "wasm/module_catalog.h"
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
    //! Identity of the module: for a WASM UDF the manifest's module_name, i.e.
    //! the name YQL calls it by.
    TString Name;
    //! Content hash of the uploaded body. Used to notice a replace of the same
    //! name and, for native UDFs, to verify the KV download. Not an identity.
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
    TString ModulesTablePath;
    TString ModuleChunksTablePath;
    TString ArtifactTablePath;
    TString ArtifactChunksTablePath;
    std::shared_ptr<TSnapshot> CurrentSnapshot;

    bool NativeFetchInProgress = false;
    bool WasmCompileInProgress = false;
    bool WasmLoadInProgress = false;
    bool LibraryCompileInProgress = false;
    // Names of the modules currently loaded on this node.
    THashSet<TString> LoadedUdfs;
    THashMap<TString, ui32> FetchRetryCounts;
    // Libraries whose compile finished in DB but CurrentSnapshot may still say pending.
    THashSet<TString> LocallyReadyLibraries;

    static constexpr ui32 MaxFetchRetries = 5;

    std::deque<TPendingUdf> PendingNativeUdfs;
    std::deque<TPendingUdf> PendingWasmCompile;
    std::deque<TPendingUdf> PendingWasmLoad;
    std::deque<TPendingLibrary> PendingLibraryCompile;

    bool IsNamePending(const TString& name, EUdfType type) const;
    bool IsLibraryPending(const TString& name) const;
    void EnqueueNativeUdfIfNeeded(const TUdfModule& udf);
    void EnqueueWasmCompileIfNeeded(const TUdfModule& udf, const TSnapshot* snapshot = nullptr);
    void EnqueueWasmLoadIfNeeded(const TUdfModule& udf);
    void EnqueueLibraryCompileIfNeeded(const TUdfModule& library);
    bool AreLibraryDependenciesReady(TStringBuf manifest, const TSnapshot* snapshot = nullptr) const;
    void RetryPendingWasmCompilesForLibrary(const TString& libraryName);
    void UnloadWasmUdfsDependingOnLibrary(const TString& libraryName);
    void FetchNextNativeBody();
    void FetchNextWasmCompile();
    void FetchNextWasmLoad();
    void FetchNextLibraryCompile();
    void UnloadWasmUdf(const TString& name);
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
