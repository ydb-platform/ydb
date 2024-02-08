#include "quota_manager.h"
#include "quota_proxy.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/services/services.pb.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_PROXY, stream)
#define LOG_W(stream) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_PROXY, stream)
#define LOG_I(stream) \
    LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_PROXY, stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_PROXY, stream)
#define LOG_T(stream) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::FQ_QUOTA_PROXY, stream)

namespace NFq {

NActors::TActorId MakeQuotaProxyActorId() {
    constexpr TStringBuf name = "FQ_QPROX";
    return NActors::TActorId(0, name);
}

class TQuotaProxyGetRequestActor : public NActors::TActorBootstrapped<TQuotaProxyGetRequestActor> {
    TEvQuotaService::TQuotaProxyGetRequest::TPtr Ev;

public:
    TQuotaProxyGetRequestActor(TEvQuotaService::TQuotaProxyGetRequest::TPtr& ev) : Ev(ev) {
    }

    void Bootstrap() {
        Become(&TQuotaProxyGetRequestActor::StateFunc);
        Send(NFq::MakeQuotaServiceActorId(SelfId().NodeId()), new TEvQuotaService::TQuotaGetRequest(Ev->Get()->SubjectType, Ev->Get()->SubjectId));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvQuotaService::TQuotaGetResponse, Handle)
    );

    void Handle(TEvQuotaService::TQuotaGetResponse::TPtr& ev) {
        Send(Ev->Sender, new TEvQuotaService::TQuotaProxyGetResponse(ev->Get()->SubjectType, ev->Get()->SubjectId, ev->Get()->Quotas));
        PassAway();
    }
};

class TQuotaProxySetRequestActor : public NActors::TActorBootstrapped<TQuotaProxySetRequestActor> {
    TEvQuotaService::TQuotaProxySetRequest::TPtr Ev;

public:
    TQuotaProxySetRequestActor(TEvQuotaService::TQuotaProxySetRequest::TPtr& ev) : Ev(ev) {
    }

    void Bootstrap() {
        Become(&TQuotaProxySetRequestActor::StateFunc);
        Send(NFq::MakeQuotaServiceActorId(SelfId().NodeId()), new TEvQuotaService::TQuotaSetRequest(Ev->Get()->SubjectType, Ev->Get()->SubjectId, Ev->Get()->Limits));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvQuotaService::TQuotaSetResponse, Handle)
    );

    void Handle(TEvQuotaService::TQuotaSetResponse::TPtr& ev) {
        Send(Ev->Sender, new TEvQuotaService::TQuotaProxySetResponse(ev->Get()->SubjectType, ev->Get()->SubjectId, ev->Get()->Limits));
        PassAway();
    }
};

class TQuotaProxyService : public NActors::TActorBootstrapped<TQuotaProxyService> {
public:
    TQuotaProxyService(
        const NConfig::TQuotasManagerConfig& config,
        const ::NMonitoring::TDynamicCounterPtr& counters)
        : Config(config)
        , ServiceCounters(counters->GetSubgroup("subsystem", "QuotaService"))
    {
    }

    static constexpr char ActorName[] = "FQ_QUOTA_PROXY";

    void Bootstrap() {
        Become(&TQuotaProxyService::StateFunc);
        LOG_I("STARTED");
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvQuotaService::TQuotaProxyGetRequest, Handle)
        hFunc(TEvQuotaService::TQuotaProxySetRequest, Handle)
    );

    void Handle(TEvQuotaService::TQuotaProxyGetRequest::TPtr& ev) {
        if (!ev->Get()->PermissionExists && Config.GetEnablePermissions()) {
            Send(ev->Sender, new TEvQuotaService::TQuotaProxyErrorResponse(grpc::StatusCode::PERMISSION_DENIED, "Permission denied"));
            return;
        }

        Register(new TQuotaProxyGetRequestActor(ev));
    }

    void Handle(TEvQuotaService::TQuotaProxySetRequest::TPtr& ev) {
        if (!ev->Get()->PermissionExists && Config.GetEnablePermissions()) {
            Send(ev->Sender, new TEvQuotaService::TQuotaProxyErrorResponse(grpc::StatusCode::PERMISSION_DENIED, "Permission denied"));
            return;
        }

        Register(new TQuotaProxySetRequestActor(ev));
    }

    NConfig::TQuotasManagerConfig Config;
    const ::NMonitoring::TDynamicCounterPtr ServiceCounters;
};

NActors::IActor* CreateQuotaProxyActor(
    const NConfig::TQuotasManagerConfig& config,
    const ::NMonitoring::TDynamicCounterPtr& counters) {
        return new TQuotaProxyService(config, counters);
}

} /* NFq */
