#include "hive.h"

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
        case EBalancerType::None: return "???";
        case EBalancerType::Scatter: return "Scatter";
        case EBalancerType::Emergency: return "Emergency";
        case EBalancerType::Manual: return "Manual";
    }
}

TResourceNormalizedValues NormalizeRawValues(const TResourceRawValues& values, const TResourceRawValues& maximum) {
    TResourceNormalizedValues normValues = {};
    if (std::get<NMetrics::EResource::Counter>(maximum) != 0) {
        std::get<NMetrics::EResource::Counter>(normValues) =
                static_cast<double>(std::get<NMetrics::EResource::Counter>(values)) / std::get<NMetrics::EResource::Counter>(maximum);
    }
    if (std::get<NMetrics::EResource::CPU>(maximum) != 0) {
        std::get<NMetrics::EResource::CPU>(normValues) =
                static_cast<double>(std::get<NMetrics::EResource::CPU>(values)) / std::get<NMetrics::EResource::CPU>(maximum);
    }
    if (std::get<NMetrics::EResource::Memory>(maximum) != 0) {
        std::get<NMetrics::EResource::Memory>(normValues) =
                static_cast<double>(std::get<NMetrics::EResource::Memory>(values)) / std::get<NMetrics::EResource::Memory>(maximum);
    }
    if (std::get<NMetrics::EResource::Network>(maximum) != 0) {
        std::get<NMetrics::EResource::Network>(normValues) =
                static_cast<double>(std::get<NMetrics::EResource::Network>(values)) / std::get<NMetrics::EResource::Network>(maximum);
    }
    return normValues;
}

NMetrics::EResource GetDominantResourceType(const TResourceRawValues& values, const TResourceRawValues& maximum) {
    TResourceNormalizedValues normValues = NormalizeRawValues(values, maximum);
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

}
}
