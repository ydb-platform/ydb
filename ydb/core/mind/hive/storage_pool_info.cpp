#include "storage_pool_info.h"
#include "hive_impl.h"
#include <library/cpp/random_provider/random_provider.h>

namespace NKikimr {
namespace NHive {

using namespace NKikimrBlobStorage;

TStoragePoolInfo::TStoragePoolInfo(const TString& name, THiveSharedSettings* hive)
    : Settings(hive)
    , Name(name)
{
}

TStorageGroupInfo& TStoragePoolInfo::GetStorageGroup(TStorageGroupId groupId) {
    auto it = Groups.find(groupId);
    if (it == Groups.end()) {
        it = Groups.emplace(std::piecewise_construct, std::tuple<TStorageGroupId>(groupId), std::tuple<const TStoragePoolInfo&, TStorageGroupId>(*this, groupId)).first;
    }
    return it->second;
}

bool TStoragePoolInfo::AcquireAllocationUnit(const TLeaderTabletInfo* tablet, ui32 channel, TStorageGroupId groupId) {
    return GetStorageGroup(groupId).AcquireAllocationUnit(tablet->GetChannel(channel));
}

bool TStoragePoolInfo::ReleaseAllocationUnit(const TLeaderTabletInfo* tablet, ui32 channel, TStorageGroupId groupId) {
    return GetStorageGroup(groupId).ReleaseAllocationUnit(tablet->GetChannel(channel));
}

void TStoragePoolInfo::UpdateStorageGroup(TStorageGroupId groupId, const TEvControllerSelectGroupsResult::TGroupParameters& groupParameters) {
    GetStorageGroup(groupId).UpdateStorageGroup(groupParameters);
}

void TStoragePoolInfo::DeleteStorageGroup(TStorageGroupId groupId) {
    Groups.erase(groupId);
}

template <>
size_t TStoragePoolInfo::SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_ROUND_ROBIN>(const TVector<double>& groupCandidateUsages) {
    Y_ABORT_UNLESS(!groupCandidateUsages.empty());
    return RoundRobinPos++ % groupCandidateUsages.size();
}

template <>
size_t TStoragePoolInfo::SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM>(const TVector<double>& groupCandidateUsages) {
    Y_ABORT_UNLESS(!groupCandidateUsages.empty());
    return TAppData::RandomProvider->GenRand() % groupCandidateUsages.size();
}

template <>
size_t TStoragePoolInfo::SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM>(const TVector<double>& groupCandidateUsages) {
    Y_ABORT_UNLESS(!groupCandidateUsages.empty());
    double sumUsage = 0;
    double maxUsage = 0;
    for (double usage : groupCandidateUsages) {
        sumUsage += usage;
        maxUsage = std::max(maxUsage, usage);
    }
    //maxUsage = std::max(1.0, maxUsage);
    double sumAvail = maxUsage * groupCandidateUsages.size() - sumUsage;
    if (sumAvail > 0) {
        double pos = TAppData::RandomProvider->GenRandReal2() * sumAvail;
        for (size_t i = 0; i < groupCandidateUsages.size(); ++i) {
            double avail = maxUsage - groupCandidateUsages[i];
            if (pos < avail) {
                return i;
            } else {
                pos -= avail;
            }
        }
    }
    return SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM>(groupCandidateUsages);
}

template <>
size_t TStoragePoolInfo::SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_EXACT_MIN>(const TVector<double>& groupCandidateUsages) {
    Y_ABORT_UNLESS(!groupCandidateUsages.empty());
    auto itMin = std::min_element(
                groupCandidateUsages.begin(),
                groupCandidateUsages.end()
                );
    return itMin - groupCandidateUsages.begin();
}

template <>
size_t TStoragePoolInfo::SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM_MIN_7P>(const TVector<double>& groupCandidateUsages) {
    Y_ABORT_UNLESS(!groupCandidateUsages.empty());
    TVector<size_t> groupIndices(groupCandidateUsages.size());
    std::iota(groupIndices.begin(), groupIndices.end(), 0);
    auto itGroup = groupIndices.begin();
    auto itPartition = itGroup;
    size_t percent7 = std::max<size_t>(groupIndices.size() * 7 / 100, 1);
    std::advance(itPartition, percent7);
    std::nth_element(groupIndices.begin(), itPartition, groupIndices.end(), [&groupCandidateUsages](size_t a, size_t b) {
        return groupCandidateUsages[a] < groupCandidateUsages[b];
    });
    std::advance(itGroup, TAppData::RandomProvider->GenRand64() % percent7);
    return *itGroup;
}

const TEvControllerSelectGroupsResult::TGroupParameters* TStoragePoolInfo::FindFreeAllocationUnit(std::function<bool(const TStorageGroupInfo&)> filter,
                                                                                                  std::function<double(const TStorageGroupInfo*)> calculateUsage) {
    if (Groups.empty()) {
        return nullptr;
    }
    TVector<const TStorageGroupInfo*> groupCandidates;
    groupCandidates.reserve(Groups.size());
    for (const auto& [groupId, groupInfo] : Groups) {
        if (filter(groupInfo)) {
            groupCandidates.emplace_back(&groupInfo);
        }
    }
    if (groupCandidates.empty()) {
        return nullptr;
    }
    TVector<double> groupCandidateUsages;
    groupCandidateUsages.reserve(groupCandidates.size());
    std::transform(groupCandidates.begin(), groupCandidates.end(), std::back_inserter(groupCandidateUsages), calculateUsage);
    size_t selectedIndex;
    switch (GetSelectStrategy()) {
    case NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM:
        selectedIndex = SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM>(groupCandidateUsages);
        break;
    case NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_EXACT_MIN:
        selectedIndex = SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_EXACT_MIN>(groupCandidateUsages);
        break;
    case NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM_MIN_7P:
        selectedIndex = SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM_MIN_7P>(groupCandidateUsages);
        break;
    case NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_ROUND_ROBIN:
        selectedIndex = SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_ROUND_ROBIN>(groupCandidateUsages);
        break;
    case NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM:
        selectedIndex = SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM>(groupCandidateUsages);
        break;
    }
    return &(groupCandidates[selectedIndex]->GroupParameters);
}

bool TStoragePoolInfo::IsBalanceByIOPS() const {
    auto balanceStrategy = GetBalanceStrategy();
    return balanceStrategy == NKikimrConfig::THiveConfig::HIVE_STORAGE_BALANCE_STRATEGY_AUTO || balanceStrategy == NKikimrConfig::THiveConfig::HIVE_STORAGE_BALANCE_STRATEGY_IOPS;
}

bool TStoragePoolInfo::IsBalanceByThroughput() const {
    auto balanceStrategy = GetBalanceStrategy();
    return balanceStrategy == NKikimrConfig::THiveConfig::HIVE_STORAGE_BALANCE_STRATEGY_AUTO || balanceStrategy == NKikimrConfig::THiveConfig::HIVE_STORAGE_BALANCE_STRATEGY_THROUGHPUT;
}

bool TStoragePoolInfo::IsBalanceBySize() const {
    auto balanceStrategy = GetBalanceStrategy();
    return balanceStrategy == NKikimrConfig::THiveConfig::HIVE_STORAGE_BALANCE_STRATEGY_AUTO || balanceStrategy == NKikimrConfig::THiveConfig::HIVE_STORAGE_BALANCE_STRATEGY_SIZE;
}

bool TStoragePoolInfo::IsFresh() const {
    return TInstant::Now() < LastUpdate + Settings->GetStoragePoolFreshPeriod();
}

void TStoragePoolInfo::SetAsFresh() {
    LastUpdate = TInstant::Now();
}

void TStoragePoolInfo::Invalidate() {
    LastUpdate = TInstant();
}

THolder<TEvControllerSelectGroups::TGroupParameters> TStoragePoolInfo::BuildRefreshRequest() const {
    THolder<TEvControllerSelectGroups::TGroupParameters> params = MakeHolder<TEvControllerSelectGroups::TGroupParameters>();
    params->MutableStoragePoolSpecifier()->SetName(Name);
    return params;
}

bool TStoragePoolInfo::AddTabletToWait(TTabletId tabletId) {
    bool result = TabletsWaiting.empty();
    TabletsWaiting.emplace_back(tabletId);
    return result;
}

TVector<TTabletId> TStoragePoolInfo::PullWaitingTablets() {
    return std::move(TabletsWaiting);
}

TStoragePoolInfo::TStats TStoragePoolInfo::GetStats() const {
    TStoragePoolInfo::TStats stats = {};
    if (Groups.empty()) {
        return stats;
    }
    using TValue = decltype(Groups)::value_type;
    auto [minIt, maxIt] = std::minmax_element(Groups.begin(), Groups.end(), [](const TValue& lhs, const TValue& rhs) {
        return lhs.second.GetUsage() < rhs.second.GetUsage();
    });
    stats.MinUsage = minIt->second.GetUsage();
    stats.MaxUsage = maxIt->second.GetUsage();
    stats.MinUsageGroupId = minIt->first;
    stats.MaxUsageGroupId = maxIt->first;
    if (stats.MaxUsage > 0) {
        double minUsageToBalance = Settings->GetMinGroupUsageToBalance();
        double minUsage = std::max(stats.MinUsage, minUsageToBalance);
        double maxUsage = std::max(stats.MaxUsage, minUsageToBalance);
        stats.Scatter = (maxUsage - minUsage) / maxUsage;
    }
    return stats;
}

} // NHive
} // NKikimr
