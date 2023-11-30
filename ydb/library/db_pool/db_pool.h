#pragma once

#include "events.h"

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/library/db_pool/protos/config.pb.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/system/mutex.h>

namespace NDbPool {

class TDbPool: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TDbPool>;

    void Cleanup();

    NActors::TActorId GetNextActor();

private:
    friend class TDbPoolMap;

    TDbPool(ui32 sessionsCount, const NYdb::NTable::TTableClient& tableClient, const ::NMonitoring::TDynamicCounterPtr& counters);

    TMutex Mutex;
    TVector<NActors::TActorId> Actors;
    ui32 Index = 0;
    const ::NMonitoring::TDynamicCounterPtr Counters;
};

class TDbPoolMap: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TDbPoolMap>;

    TDbPool::TPtr GetOrCreate(ui32 poolId);

private:
    friend class TDbPoolHolder;

    TDbPoolMap(const NDbPool::TConfig& config,
               NYdb::TDriver driver,
               const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
               const ::NMonitoring::TDynamicCounterPtr& counters);

    void Reset(const NDbPool::TConfig& config);
    TMutex Mutex;
    NDbPool::TConfig Config;
    NYdb::TDriver Driver;
    THashMap<ui32, TDbPool::TPtr> Pools;
public:
    THolder<NYdb::NTable::TTableClient> TableClient;
private:
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    const ::NMonitoring::TDynamicCounterPtr Counters;
};

NYdb::TAsyncStatus ExecDbRequest(TDbPool::TPtr dbPool, std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)> handler);

class TDbPoolHolder: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TDbPoolHolder>;
    TDbPoolHolder(
        const NDbPool::TConfig& config,
        const NYdb::TDriver& driver,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters);

    ~TDbPoolHolder();

    void Reset(const NDbPool::TConfig& config);
    TDbPool::TPtr GetOrCreate(ui32 poolId);
    NYdb::TDriver& GetDriver();
    TDbPoolMap::TPtr Get();

public:
    NYdb::TDriver Driver;
    TDbPoolMap::TPtr Pools;
};

class TDbRequest: public NActors::TActorBootstrapped<TDbRequest> {
    using TFunction = std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)>;
    TDbPool::TPtr DbPool;
    NThreading::TPromise<NYdb::TStatus> Promise;
    TFunction Handler;
public:
    TDbRequest(const TDbPool::TPtr& dbPool, const NThreading::TPromise<NYdb::TStatus>& promise, const TFunction& handler);

    static constexpr char ActorName[] = "YQ_DB_REQUEST";

    STRICT_STFUNC(StateFunc,
        hFunc(TEvents::TEvDbFunctionResponse, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)
    )

    void Bootstrap();
    void Handle(TEvents::TEvDbFunctionResponse::TPtr& ev);
    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr&);
};

} // namespace NDbPool