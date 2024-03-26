#include "shared_resources.h"

#include <ydb/core/fq/libs/events/events.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/logger/actor.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/public/api/protos/ydb_discovery.pb.h>
#include <ydb/public/sdk/cpp/client/extensions/discovery_mutator/discovery_mutator.h>
#include <ydb/public/sdk/cpp/client/extensions/solomon_stats/pull_client.h>

#include <util/generic/cast.h>
#include <util/generic/strbuf.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/strip.h>
#include <util/system/compiler.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <memory>

namespace NFq {

namespace {

struct TActorSystemPtrMixin {
    NKikimr::TDeferredActorLogBackend::TSharedAtomicActorSystemPtr ActorSystemPtr = std::make_shared<NKikimr::TDeferredActorLogBackend::TAtomicActorSystemPtr>(nullptr);
};

struct TYqSharedResourcesImpl : public TActorSystemPtrMixin, public TYqSharedResources {
    explicit TYqSharedResourcesImpl(
        const NFq::NConfig::TConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters)
        : TYqSharedResources(CreateDriver(config.GetCommon().GetYdbDriverConfig()))
    {
        CreateDbPoolHolder(PrepareDbPoolConfig(config), credentialsProviderFactory, counters);
        AddUnderlayDiscoveryMutator();
    }

    NDbPool::TConfig PrepareDbPoolConfig(const NFq::NConfig::TConfig& config) {
        NDbPool::TConfig dbPoolConfig;
        const auto& storageConfig = config.GetDbPool().GetStorage();
        auto maxSessionCount = config.GetDbPool().GetMaxSessionCount();
        (*dbPoolConfig.MutablePools())[0] = maxSessionCount ? maxSessionCount : 10;
        dbPoolConfig.SetEndpoint(storageConfig.GetEndpoint());
        dbPoolConfig.SetDatabase(storageConfig.GetDatabase());
        dbPoolConfig.SetOAuthFile(storageConfig.GetOAuthFile());
        dbPoolConfig.SetUseLocalMetadataService(storageConfig.GetUseLocalMetadataService());
        dbPoolConfig.SetUseSsl(storageConfig.GetUseSsl());
        dbPoolConfig.SetToken(storageConfig.GetToken());
        return dbPoolConfig;
    }

    void Init(NActors::TActorSystem* actorSystem) override {
        Y_ABORT_UNLESS(!ActorSystemPtr->load(std::memory_order_relaxed), "Double IYqSharedResources init");
        ActorSystemPtr->store(actorSystem, std::memory_order_relaxed);
    }

    void Stop() override {
        CoreYdbDriver.Stop(true);
        // UserSpaceYdbDriver.Stop(true); // For now it points to the same driver as CoreYdbDriver, so don't call Stop
    }

    NYdb::TDriver CreateDriver(const NFq::NConfig::TYdbDriverConfig& config) {
        NYdb::TDriver driver(GetYdbDriverConfig(config));
        if (config.GetMonitoringPort()) {
            NSolomonStatExtension::TSolomonStatPullExtension::TParams params(TString{}, config.GetMonitoringPort(), "yq", "ydb_driver", TString{});
            driver.AddExtension<NSolomonStatExtension::TSolomonStatPullExtension>(params);
        }
        return driver;
    }

    NYdb::TDriverConfig GetYdbDriverConfig(const NFq::NConfig::TYdbDriverConfig& config) {
        NYdb::TDriverConfig cfg;
        if (config.GetNetworkThreadsNum()) {
            cfg.SetNetworkThreadsNum(config.GetNetworkThreadsNum());
        }
        if (config.GetClientThreadsNum()) {
            cfg.SetClientThreadsNum(config.GetClientThreadsNum());
        }
        if (config.GetGrpcMemoryQuota()) {
            cfg.SetGrpcMemoryQuota(config.GetGrpcMemoryQuota());
        }
        cfg.SetDiscoveryMode(NYdb::EDiscoveryMode::Async); // We are in actor system!
        cfg.SetLog(MakeHolder<NKikimr::TDeferredActorLogBackend>(ActorSystemPtr, NKikimrServices::EServiceKikimr::YDB_SDK));
        return cfg;
    }

    void CreateDbPoolHolder(
        const NDbPool::TConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters) {
        DbPoolHolder = MakeIntrusive<NDbPool::TDbPoolHolder>(config, CoreYdbDriver, credentialsProviderFactory, counters);
    }

    void AddUnderlayDiscoveryMutator() {

        auto mutator = [](Ydb::Discovery::ListEndpointsResult* proto, NYdb::TStatus status, const NYdb::IDiscoveryMutatorApi::TAuxInfo& aux) {
            TStringBuf underlayPrefix{"u-"};
            if (!aux.DiscoveryEndpoint.starts_with(underlayPrefix) || !proto) {
                return status;
            }

            for (size_t i = 0; i < proto->endpointsSize(); ++i) {
                Ydb::Discovery::EndpointInfo* endpointInfo = proto->Mutableendpoints(i);
                const TString& address = endpointInfo->address();
                if (address.StartsWith(underlayPrefix)) {
                    continue;
                }
                endpointInfo->set_address(underlayPrefix + address);
            }
            return status;
        };
        UserSpaceYdbDriver.AddExtension<NDiscoveryMutator::TDiscoveryMutator>(NDiscoveryMutator::TDiscoveryMutator::TParams(std::move(mutator)));
    }
};

} // namespace

TYqSharedResources::TPtr CreateYqSharedResourcesImpl(
        const NFq::NConfig::TConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const ::NMonitoring::TDynamicCounterPtr& counters) {
    return MakeIntrusive<TYqSharedResourcesImpl>(config, credentialsProviderFactory, counters);
}

TYqSharedResources::TYqSharedResources(NYdb::TDriver driver)
    : CoreYdbDriver(driver)
    , UserSpaceYdbDriver(std::move(driver))
{
}

TYqSharedResources::TPtr TYqSharedResources::Cast(const IYqSharedResources::TPtr& ptr) {
    return CheckedCast<TYqSharedResources*>(ptr.Get());
}

} // namespace NFq
