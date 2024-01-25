#include "domain_info.h"
#include "hive.h"
#include "hive_impl.h"
#include "leader_tablet_info.h"

#include <ydb/core/util/tuples.h>

namespace NKikimr {
namespace NHive {

TString ETabletStateName(ETabletState value) {
    switch (value) {
    case ETabletState::Unknown: return "Unknown";
    case ETabletState::GroupAssignment: return "GroupAssignment";
    case ETabletState::StoppingInGroupAssignment: return "StoppingInGroupAssignment";
    case ETabletState::Stopping: return "Stopping";
    case ETabletState::Stopped: return "Stopped";
    case ETabletState::ReadyToWork: return "ReadyToWork";
    case ETabletState::BlockStorage: return "BlockStorage";
    case ETabletState::Deleting: return "Deleting";
    default: return Sprintf("%d", static_cast<int>(value));
    }
}

TString EFollowerStrategyName(EFollowerStrategy value) {
    switch (value) {
        case EFollowerStrategy::Unknown: return "Unknown";
        case EFollowerStrategy::Backup: return "Backup";
        case EFollowerStrategy::Read: return "Read";
        default: return Sprintf("%d", static_cast<int>(value));
    }
}

TString EBalancerTypeName(EBalancerType value) {
    switch (value) {
        case EBalancerType::Scatter: return "Scatter";
        case EBalancerType::ScatterCounter: return "Counter";
        case EBalancerType::ScatterCPU: return "CPU";
        case EBalancerType::ScatterMemory: return "Memory";
        case EBalancerType::ScatterNetwork: return "Network";
        case EBalancerType::Emergency: return "Emergency";
        case EBalancerType::SpreadNeighbours: return "Spread";
        case EBalancerType::Manual: return "Manual";
        case EBalancerType::Storage: return "Storage";
    }
}

EResourceToBalance ToResourceToBalance(NMetrics::EResource resource) {
    switch (resource) {
        case NMetrics::EResource::CPU: return EResourceToBalance::CPU;
        case NMetrics::EResource::Memory: return EResourceToBalance::Memory;
        case NMetrics::EResource::Network: return EResourceToBalance::Network;
        case NMetrics::EResource::Counter: return EResourceToBalance::Counter;
    }
}

TResourceNormalizedValues NormalizeRawValues(const TResourceRawValues& values, const TResourceRawValues& maximum) {
    return safe_div(values, maximum);
}

NMetrics::EResource GetDominantResourceType(const TResourceRawValues& values, const TResourceRawValues& maximum) {
    TResourceNormalizedValues normValues = NormalizeRawValues(values, maximum);
    return GetDominantResourceType(normValues);
}

NMetrics::EResource GetDominantResourceType(const TResourceNormalizedValues& normValues) {
    NMetrics::EResource dominant = NMetrics::EResource::Counter;
    auto value = std::get<NMetrics::EResource::Counter>(normValues);
    if (std::get<NMetrics::EResource::CPU>(normValues) > value) {
        dominant = NMetrics::EResource::CPU;
        value = std::get<NMetrics::EResource::CPU>(normValues);
    }
    if (std::get<NMetrics::EResource::Memory>(normValues) > value) {
        dominant = NMetrics::EResource::Memory;
        value = std::get<NMetrics::EResource::Memory>(normValues);
    }
    if (std::get<NMetrics::EResource::Network>(normValues) > value) {
        dominant = NMetrics::EResource::Network;
        value = std::get<NMetrics::EResource::Network>(normValues);
    }
    return dominant;
}

TNodeFilter::TNodeFilter(const THive& hive)
    : Hive(hive)
{}

TArrayRef<const TSubDomainKey> TNodeFilter::GetEffectiveAllowedDomains() const {
    const auto* objectDomainInfo = Hive.FindDomain(ObjectDomain);

    if (!objectDomainInfo) {
        return {AllowedDomains.begin(), AllowedDomains.end()};
    }

    switch (objectDomainInfo->GetNodeSelectionPolicy()) {
        case ENodeSelectionPolicy::Default:
            return {AllowedDomains.begin(), AllowedDomains.end()};
        case ENodeSelectionPolicy::PreferObjectDomain:
            return {&ObjectDomain, 1};
    }
}

} // NHive
} // NKikimr
