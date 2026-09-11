#pragma once
#include "metadata_subscription/snapshot.h"
#include "kv_body_store.h"
#include "wasm/module_catalog.h"
#include "wasm_compile_actor.h"
#include "wasm_library_compile_actor.h"
#include "wasm_artifact_load_actor.h"
#include "artifact_table_initializer.h"
#include "store_initializer.h"
#include "compile_controller/events.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
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
    //! Uid of the upload this entry was queued for, taken from the snapshot.
    //! Artifacts are keyed by it, so the actors need it to find the object code
    //! built from this very upload.
    TString Uid;
    //! Content hash of the uploaded body. Used to notice a replace of the same
    //! name and, for native UDFs, to verify the KV download. Not an identity.
    TString Md5;
    ui64 ExpectedSize = 0;
    EUdfType Type = EUdfType::NATIVE_UNSAFE;
    TString Manifest;
    TString ModuleExtension = "wasm";
    //! Uids of the required_libraries as of the snapshot this entry was queued
    //! from. Library artifacts are keyed by uid too, and neither actor reads
    //! the library rows itself.
    THashMap<TString, TString> LibraryUids;
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
    //! While unset, this node compiles straight from the metadata snapshot as
    //! it did before the controller existed. This is the rollback switch.
    YDB_READONLY_FLAG(EnableCompileController, false);
    ui32 CompileCapacity = 1;
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

    //! A compile this node holds an exclusive right to. The id has to travel
    //! back with the result, and the key tells the controller which gap closed.
    struct TActiveAssignment {
        ui64 AssignmentId = 0;
        NKikimrUdfStore::TArtifactKey Key;
    };

    enum class EControllerResolveStage {
        Initial,
        InFlight,
        Finished,
    };

    ui64 CompileControllerTabletId = 0;
    NActors::TActorId CompileControllerPipe;
    EControllerResolveStage ControllerResolveStage = EControllerResolveStage::Initial;
    //! Highest controller generation this node has heard of. Generations only
    //! grow, so anything below it comes from a leader that has already been
    //! replaced and no longer owns what it hands out. Survives a broken pipe on
    //! purpose: forgetting it would make the node gullible right after a move.
    ui64 ControllerGeneration = 0;
    THashMap<TString, TActiveAssignment> ModuleAssignments;
    THashMap<TString, TActiveAssignment> LibraryAssignments;

    bool IsNamePending(const TString& name, EUdfType type) const;
    bool IsLibraryPending(const TString& name) const;
    void EnqueueNativeUdfIfNeeded(const TUdfModule& udf);
    void EnqueueWasmCompileIfNeeded(const TUdfModule& udf, const TSnapshot* snapshot = nullptr);
    void EnqueueWasmLoadIfNeeded(const TUdfModule& udf);
    void EnqueueLibraryCompileIfNeeded(const TUdfModule& library);
    bool AreLibraryDependenciesReady(TStringBuf manifest, const TSnapshot* snapshot = nullptr) const;
    THashMap<TString, TString> CollectLibraryUids(
        TStringBuf manifest,
        const TSnapshot* snapshot = nullptr) const;
    void RetryPendingWasmCompilesForLibrary(const TString& libraryName);
    void UnloadWasmUdfsDependingOnLibrary(const TString& libraryName);
    void FetchNextNativeBody();
    void FetchNextWasmCompile();
    void FetchNextWasmLoad();
    void FetchNextLibraryCompile();
    void UnloadWasmUdf(const TString& name);
    static TString GetModuleExtensionFromManifest(TStringBuf manifest);
    void EnsureArtifactTable();

    void ResolveCompileController();
    void ConnectToCompileController();
    void SendRegister();
    void SendHeartbeat();
    void ScheduleControllerTick();
    //! Tells the controller about a gap this node noticed. Only a hint: the
    //! controller reconciles on its own, so losing it only costs latency.
    void RequestArtifact(const TString& name, const TString& uid, bool isLibrary);
    void ReportCompileResult(
        const TActiveAssignment& assignment,
        bool success,
        bool stale,
        const TString& error);

protected:
    void Handle(TEvStoreInitialized::TPtr& ev);
    void Handle(TEvArtifactTableInitialized::TPtr& ev);
    void Handle(TEvStoreInitFailed::TPtr& ev);
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);
    void Handle(TEvReadBodyResponse::TPtr& ev);
    void Handle(TEvWasmCompileResponse::TPtr& ev);
    void Handle(TEvLibraryCompileResponse::TPtr& ev);
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev);
    void Handle(TEvCompileController::TEvRegisterResult::TPtr& ev);
    void Handle(TEvCompileController::TEvAssignCompile::TPtr& ev);
    void Handle(TEvCompileController::TEvArtifactReady::TPtr& ev);
    void HandleControllerTick();

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
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvCompileController::TEvRegisterResult, Handle);
            hFunc(TEvCompileController::TEvAssignCompile, Handle);
            hFunc(TEvCompileController::TEvArtifactReady, Handle);
            cFunc(NActors::TEvents::TEvWakeup::EventType, HandleControllerTick);
            default:
                break;
        }
    }
};

NActors::IActor* CreateService(const NKikimrConfig::TUdfStoreConfig& serviceConfig, TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> functionRegistry);
NActors::TActorId MakeServiceId(ui32 nodeId);

}
