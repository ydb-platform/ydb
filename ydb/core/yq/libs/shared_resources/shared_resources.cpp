#include "shared_resources.h"

#include <ydb/core/protos/services.pb.h>
#include <ydb/core/yq/libs/events/events.h>

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>

#include <util/generic/cast.h>
#include <util/generic/strbuf.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/strip.h>
#include <util/system/compiler.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <memory>

namespace NYq {

namespace {

// Log backend that allows us to create shared YDB driver early (before actor system starts),
// but log to actor system.
class TDeferredActorSystemPtrInitActorLogBackend : public TLogBackend {
public:
    using TAtomicActorSystemPtr = std::atomic<NActors::TActorSystem*>;
    using TSharedAtomicActorSystemPtr = std::shared_ptr<TAtomicActorSystemPtr>;

    TDeferredActorSystemPtrInitActorLogBackend(TSharedAtomicActorSystemPtr actorSystem, int logComponent)
        : ActorSystemPtr(std::move(actorSystem))
        , LogComponent(logComponent)
    {
    }

    NActors::NLog::EPriority GetActorLogPriority(ELogPriority priority) {
        switch (priority) {
        case TLOG_EMERG:
            return NActors::NLog::PRI_EMERG;
        case TLOG_ALERT:
            return NActors::NLog::PRI_ALERT;
        case TLOG_CRIT:
            return NActors::NLog::PRI_CRIT;
        case TLOG_ERR:
            return NActors::NLog::PRI_ERROR;
        case TLOG_WARNING:
            return NActors::NLog::PRI_WARN;
        case TLOG_NOTICE:
            return NActors::NLog::PRI_NOTICE;
        case TLOG_INFO:
            return NActors::NLog::PRI_INFO;
        case TLOG_DEBUG:
            return NActors::NLog::PRI_DEBUG;
        default:
            return NActors::NLog::PRI_TRACE;
        }
    }

    void WriteData(const TLogRecord& rec) override {
        NActors::TActorSystem* actorSystem = ActorSystemPtr->load(std::memory_order_relaxed);
        if (Y_LIKELY(actorSystem)) {
            LOG_LOG(*actorSystem, GetActorLogPriority(rec.Priority), LogComponent, TString(rec.Data, rec.Len));
        } else {
            // Not inited. Temporary write to stderr.
            TStringBuilder out;
            out << TStringBuf(rec.Data, rec.Len) << Endl;
            Cerr << out;
        }
    }

    void ReopenLog() override {
    }

protected:
    TSharedAtomicActorSystemPtr ActorSystemPtr;
    const int LogComponent;
};

struct TActorSystemPtrMixin {
    TDeferredActorSystemPtrInitActorLogBackend::TSharedAtomicActorSystemPtr ActorSystemPtr = std::make_shared<TDeferredActorSystemPtrInitActorLogBackend::TAtomicActorSystemPtr>(nullptr);
};

struct TYqSharedResourcesImpl : public TActorSystemPtrMixin, public TYqSharedResources {
    explicit TYqSharedResourcesImpl(
        const NYq::NConfig::TConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const NMonitoring::TDynamicCounterPtr& counters)
        : TYqSharedResources(NYdb::TDriver(GetYdbDriverConfig(config.GetCommon().GetYdbDriverConfig())))
    {
        CreateDbPoolHolder(config.GetDbPool(), credentialsProviderFactory, counters);
    }

    void Init(NActors::TActorSystem* actorSystem) override {
        Y_VERIFY(!ActorSystemPtr->load(std::memory_order_relaxed), "Double IYqSharedResources init");
        ActorSystemPtr->store(actorSystem, std::memory_order_relaxed);
    }

    void Stop() override {
        YdbDriver.Stop(true);
    }

    NYdb::TDriverConfig GetYdbDriverConfig(const NYq::NConfig::TYdbDriverConfig& config) {
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
        cfg.SetLog(MakeHolder<TDeferredActorSystemPtrInitActorLogBackend>(ActorSystemPtr, NKikimrServices::EServiceKikimr::YDB_SDK));
        return cfg;
    }

    void CreateDbPoolHolder(
        const NYq::NConfig::TDbPoolConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const NMonitoring::TDynamicCounterPtr& counters) {
        DbPoolHolder = MakeIntrusive<NYq::TDbPoolHolder>(config, YdbDriver, credentialsProviderFactory, counters);
    }
};

} // namespace

TYqSharedResources::TPtr CreateYqSharedResourcesImpl(
        const NYq::NConfig::TConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const NMonitoring::TDynamicCounterPtr& counters) {
    return MakeIntrusive<TYqSharedResourcesImpl>(config, credentialsProviderFactory, counters);
}

TYqSharedResources::TYqSharedResources(NYdb::TDriver driver)
    : YdbDriver(std::move(driver))
{
}

TYqSharedResources::TPtr TYqSharedResources::Cast(const IYqSharedResources::TPtr& ptr) {
    return CheckedCast<TYqSharedResources*>(ptr.Get());
}

} // namespace NYq
