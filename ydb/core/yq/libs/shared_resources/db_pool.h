#pragma once

#include <ydb/core/yq/libs/config/protos/fq_config.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/core/yq/libs/events/events.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/system/mutex.h>

namespace NYq {

class TDbPool: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TDbPool>;

    void Cleanup();

    NActors::TActorId GetNextActor();

    TString TablePathPrefix;

private:
    friend class TDbPoolMap;

    TDbPool(ui32 sessionsCount, const NYdb::NTable::TTableClient& tableClient, const ::NMonitoring::TDynamicCounterPtr& counters, const TString& tablePathPrefix);

    TMutex Mutex;
    TVector<NActors::TActorId> Actors;
    ui32 Index = 0;
    const ::NMonitoring::TDynamicCounterPtr Counters;
};

enum class EDbPoolId {
    MAIN = 0,
    REFRESH = 1
};

class TDbPoolMap: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TDbPoolMap>;

    TDbPool::TPtr GetOrCreate(EDbPoolId poolId, ui32 sessionsCount, const TString& tablePathPrefix);

private:
    friend class TDbPoolHolder;

    TDbPoolMap(const NYq::NConfig::TDbPoolConfig& config,
               NYdb::TDriver driver,
               const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
               const ::NMonitoring::TDynamicCounterPtr& counters);

    void Reset(const NYq::NConfig::TDbPoolConfig& config);
    TMutex Mutex;
    NYq::NConfig::TDbPoolConfig Config;
    NYdb::TDriver Driver;
    THashMap<EDbPoolId, TDbPool::TPtr> Pools;
    THolder<NYdb::NTable::TTableClient> TableClient;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    const ::NMonitoring::TDynamicCounterPtr Counters;
};

class TDbPoolHolder: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TDbPoolHolder>;
    TDbPoolHolder(
        const NYq::NConfig::TDbPoolConfig& config,
        const NYdb::TDriver& driver,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters);

    ~TDbPoolHolder();

    void Reset(const NYq::NConfig::TDbPoolConfig& config);
    TDbPool::TPtr GetOrCreate(EDbPoolId poolId, ui32 sessionsCount, const TString& tablePathPrefix);
    NYdb::TDriver& GetDriver();
    TDbPoolMap::TPtr Get();

public:
    NYdb::TDriver Driver;
    TDbPoolMap::TPtr Pools;
};

NYdb::TAsyncStatus ExecDbRequest(TDbPool::TPtr dbPool, std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)> handler);

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

} /* NYq */
