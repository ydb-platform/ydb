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
    : Hive(&hive)
{}

TArrayRef<const TSubDomainKey> TNodeFilter::GetEffectiveAllowedDomains() const {
    const auto* objectDomainInfo = Hive->FindDomain(ObjectDomain);

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

bool TNodeFilter::IsAllowedDataCenter(TDataCenterId dc) const {
    if (AllowedDataCenters.empty()) {
        return true;
    }
    return std::find(AllowedDataCenters.begin(), AllowedDataCenters.end(), dc) != AllowedDataCenters.end();
}

template <typename K, typename V>
std::unordered_map<V, K> MakeReverseMap(const std::unordered_map<K, V>& map) {
    std::unordered_map<V, K> result;
    for (const auto& [k, v] : map) {
        result.emplace(v, k);
    }
    return result;
}

const std::unordered_map<TTabletTypes::EType, TString> TABLET_TYPE_SHORT_NAMES = {{TTabletTypes::SchemeShard, "SS"},
                                                                                  {TTabletTypes::Hive, "H"},
                                                                                  {TTabletTypes::DataShard, "DS"},
                                                                                  {TTabletTypes::ColumnShard, "CS"},
                                                                                  {TTabletTypes::KeyValue, "KV"},
                                                                                  {TTabletTypes::PersQueue, "PQ"},
                                                                                  {TTabletTypes::PersQueueReadBalancer, "PQRB"},
                                                                                  {TTabletTypes::Dummy, "DY"},
                                                                                  {TTabletTypes::Coordinator, "C"},
                                                                                  {TTabletTypes::Mediator, "M"},
                                                                                  {TTabletTypes::BlockStoreVolume, "BV"},
                                                                                  {TTabletTypes::BlockStorePartition2, "BP"},
                                                                                  {TTabletTypes::Kesus, "K"},
                                                                                  {TTabletTypes::SysViewProcessor, "SV"},
                                                                                  {TTabletTypes::FileStore, "FS"},
                                                                                  {TTabletTypes::TestShard, "TS"},
                                                                                  {TTabletTypes::SequenceShard, "SQ"},
                                                                                  {TTabletTypes::ReplicationController, "RC"},
                                                                                  {TTabletTypes::BlobDepot, "BD"},
                                                                                  {TTabletTypes::StatisticsAggregator, "SA"},
                                                                                  {TTabletTypes::GraphShard, "GS"},
                                                                                  {TTabletTypes::NodeBroker, "NB"},
                                                                                  {TTabletTypes::BlockStoreDiskRegistry, "BDR"},
                                                                                  {TTabletTypes::BackupController, "BCT"},
                                                                                 };

const std::unordered_map<TString, TTabletTypes::EType> TABLET_TYPE_BY_SHORT_NAME = MakeReverseMap(TABLET_TYPE_SHORT_NAMES);

} // NHive
} // NKikimr
