#include "quota_manager.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/core/yq/libs/shared_resources/shared_resources.h>

#include <ydb/core/protos/services.pb.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_SERVICE, stream)
#define LOG_W(stream) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_SERVICE, stream)
#define LOG_I(stream) \
    LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_SERVICE, stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_SERVICE, stream)

namespace NYq {

NActors::TActorId MakeQuotaServiceActorId() {
    constexpr TStringBuf name = "FQQTASRV";
    return NActors::TActorId(0, name);
}

class TQuotaManagementService : public NActors::TActorBootstrapped<TQuotaManagementService> {
public:
    TQuotaManagementService(
        const NConfig::TQuotasManagerConfig& config,
        const NYq::TYqSharedResources::TPtr& yqSharedResources,
        const NMonitoring::TDynamicCounterPtr& counters,
        std::vector<TQuotaDescription> quotaDesc)
        : Config(config)
        , ServiceCounters(counters->GetSubgroup("subsystem", "QuotaService"))
    {
        Y_UNUSED(yqSharedResources);
        Y_UNUSED(quotaDesc);
    }

    static constexpr char ActorName[] = "FQ_QUOTA_SERVICE";

    void Bootstrap() {
        Become(&TQuotaManagementService::StateFunc);
        LOG_I("STARTED");
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvQuotaService::TQuotaGetRequest, Handle)
    );

    void Handle(TEvQuotaService::TQuotaGetRequest::TPtr& ev) {
        auto response = MakeHolder<TEvQuotaService::TQuotaGetResponse>();
        for (const auto& quota : Config.GetQuotas()) {
            if (quota.GetSubjectType() == ev->Get()->SubjectType && quota.GetSubjectId() == ev->Get()->SubjectId) {
                for (const auto& limit : quota.GetLimit()) {
                    response->Quotas.emplace(limit.GetName(), TQuotaUsage(limit.GetLimit()));
                }
            }
        }
        Send(ev->Sender, response.Release());
    }

    NConfig::TQuotasManagerConfig Config;
    const NMonitoring::TDynamicCounterPtr ServiceCounters;
};

NActors::IActor* CreateQuotaServiceActor(
    const NConfig::TQuotasManagerConfig& config,
    const NYq::TYqSharedResources::TPtr& yqSharedResources,
    const NMonitoring::TDynamicCounterPtr& counters,
    std::vector<TQuotaDescription> quotaDesc) {
        return new TQuotaManagementService(config, yqSharedResources, counters, quotaDesc);
}

} /* NYq */
