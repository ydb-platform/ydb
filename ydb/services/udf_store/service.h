#pragma once
#include "metadata_subscription/snapshot.h"
#include "kv_body_store.h"
#include "store_initializer.h"

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/accessor/accessor.h>

#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NKikimr::NUdfStore {

struct TPendingUdf {
    TString Md5;
    ui64 ExpectedSize = 0; // body size from metadata (must be > 0)
};

// Actor that subscribes to UDF metadata snapshot updates and schedules body fetches
// for NATIVE_UNSAFE UDFs that are not yet loaded on disk.
// Bodies are read from a KV tablet (not from the metadata table) by TKvBodyReadActor,
// which saves them under UnsafeNativeUdfDir/<md5>, verifies size/MD5 and loads them
// into FunctionRegistry. This actor owns the fetch queue, retry policy and loaded set.
// Infrastructure (meta table + KV volume) is initialized by the store initializer actor
// launched from Bootstrap.
class TUdfStoreService: public NActors::TActorBootstrapped<TUdfStoreService> {
private:
    using TBase = NActors::TActorBootstrapped<TUdfStoreService>;
    TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> FunctionRegistry;
    TString KvStorageMedia;
    YDB_READONLY_FLAG(EnableUnsafeNativeUdf, false);
    TString KvVolumePath;
    TString UnsafeNativeUdfDir;
    std::shared_ptr<TSnapshot> CurrentSnapshot;
    

    bool FetchInProgress = false;
    THashSet<TString> LoadedUdfs;
    THashMap<TString, ui32> FetchRetryCounts;

    static constexpr ui32 MaxFetchRetries = 5;

    // Queue of UDFs whose bodies are being fetched from the KV tablet
    std::deque<TPendingUdf> PendingUdfs;

    bool IsMd5Pending(const TString& md5) const;
    void EnqueueUdfIfNeeded(const TString& md5, ui64 expectedSize);
    void FetchNextBody();

protected:
    void Handle(TEvUdfStoreInit::TEvStoreInitialized::TPtr& ev);
    void Handle(TEvUdfStoreInit::TEvStoreInitFailed::TPtr& ev);
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);
    void Handle(TEvKvBodyStore::TEvReadBodyResponse::TPtr& ev);

public:
    TUdfStoreService(const NKikimrConfig::TUdfStoreConfig& config, TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> functionRegistry)
        : FunctionRegistry(std::move(functionRegistry))
        , KvStorageMedia(config.GetKvStorageMedia())
        , EnableUnsafeNativeUdfFlag(config.GetEnableUnsafeNativeUdf())
        , UnsafeNativeUdfDir(config.GetUnsafeNativeUdfDir())
    {}

    void Bootstrap();

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvUdfStoreInit::TEvStoreInitialized, Handle);
            hFunc(TEvUdfStoreInit::TEvStoreInitFailed, Handle);
            hFunc(NProvider::TEvRefreshSubscriberData, Handle);
            hFunc(TEvKvBodyStore::TEvReadBodyResponse, Handle);
            default:
                break;
        }
    }
};

NActors::IActor* CreateService(const NKikimrConfig::TUdfStoreConfig& serviceConfig, TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> functionRegistry);
NActors::TActorId MakeServiceId(ui32 nodeId);

}
