#include "service.h"

namespace NKikimr::NConveyorComposite {

TProcessGuard TServiceOperator::StartProcess(const ESpecialTaskCategory category, const TString& scopeId, const ui64 externalProcessId,
    const TCPULimitsConfig& cpuLimits, const std::optional<TSchedulerQueryIdentity>& schedulerQueryIdentity, const bool useBatchPool) {
    if (TSelf::IsEnabled() && NActors::TlsActivationContext) {
        auto& context = NActors::TActorContext::AsActorContext();
        const NActors::TActorId& selfId = context.SelfID;
        return TProcessGuard(
            category, scopeId, externalProcessId, cpuLimits, MakeServiceId(selfId.NodeId(), useBatchPool), schedulerQueryIdentity);
    }
    return TProcessGuard(category, scopeId, externalProcessId, cpuLimits, {}, schedulerQueryIdentity);
}

TProcessGuard TScanServiceOperator::StartProcess(const ui64 externalProcessId, const TString& scopeId,
    const TCPULimitsConfig& cpuLimits, const std::optional<TSchedulerQueryIdentity>& schedulerQueryIdentity, const bool useBatchPool) {
    return TServiceOperator::StartProcess(
        ESpecialTaskCategory::Scan, scopeId, externalProcessId, cpuLimits, schedulerQueryIdentity, useBatchPool);
}

}
