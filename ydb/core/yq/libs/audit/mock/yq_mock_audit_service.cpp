#include "yq_mock_audit_service.h"

#include <ydb/core/yq/libs/audit/events/events.h>

#include <library/cpp/actors/core/hfunc.h>

namespace NYq {

class TYqMockAuditServiceActor : public NActors::TActor<TYqMockAuditServiceActor> {
public:
    TYqMockAuditServiceActor() : TActor<TYqMockAuditServiceActor>(&TYqMockAuditServiceActor::StateFunc) {}

    static constexpr char ActorName[] = "YQ_MOCK_AUDIT_SERVICE";

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvAuditService::CreateBindingAuditReport, Handle);
        hFunc(TEvAuditService::ModifyBindingAuditReport, Handle);
        hFunc(TEvAuditService::DeleteBindingAuditReport, Handle);
        hFunc(TEvAuditService::CreateConnectionAuditReport, Handle);
        hFunc(TEvAuditService::ModifyConnectionAuditReport, Handle);
        hFunc(TEvAuditService::DeleteConnectionAuditReport, Handle);
        hFunc(TEvAuditService::CreateQueryAuditReport, Handle);
        hFunc(TEvAuditService::ControlQueryAuditReport, Handle);
        hFunc(TEvAuditService::ModifyQueryAuditReport, Handle);
        hFunc(TEvAuditService::DeleteQueryAuditReport, Handle);
    )

    void Handle(TEvAuditService::CreateBindingAuditReport::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void Handle(TEvAuditService::ModifyBindingAuditReport::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void Handle(TEvAuditService::DeleteBindingAuditReport::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void Handle(TEvAuditService::CreateConnectionAuditReport::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void Handle(TEvAuditService::ModifyConnectionAuditReport::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void Handle(TEvAuditService::DeleteConnectionAuditReport::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void Handle(TEvAuditService::CreateQueryAuditReport::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void Handle(TEvAuditService::ControlQueryAuditReport::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void Handle(TEvAuditService::ModifyQueryAuditReport::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void Handle(TEvAuditService::DeleteQueryAuditReport::TPtr& ev) {
        Y_UNUSED(ev);
    }
};

NActors::IActor* CreateMockYqAuditServiceActor(const NConfig::TAuditConfig& config, const ::NMonitoring::TDynamicCounterPtr& counters) {
    Y_UNUSED(config);
    Y_UNUSED(counters);
    return new TYqMockAuditServiceActor();
}

} // namespace NYq
