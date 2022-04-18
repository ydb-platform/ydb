#include "storage_pool_info.h"
#include "hive_impl.h"

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
    return GetStorageGroup(groupId).AcquireAllocationUnit(tablet, channel);
}

bool TStoragePoolInfo::ReleaseAllocationUnit(const TLeaderTabletInfo* tablet, ui32 channel, TStorageGroupId groupId) {
    return GetStorageGroup(groupId).ReleaseAllocationUnit(tablet, channel);
}

void TStoragePoolInfo::UpdateStorageGroup(TStorageGroupId groupId, const TEvControllerSelectGroupsResult::TGroupParameters& groupParameters) {
    GetStorageGroup(groupId).UpdateStorageGroup(groupParameters);
}

void TStoragePoolInfo::DeleteStorageGroup(TStorageGroupId groupId) {
    Groups.erase(groupId);
}

template <>
const TEvControllerSelectGroupsResult::TGroupParameters* TStoragePoolInfo::SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_ROUND_ROBIN>(const TVector<const TStorageGroupInfo*>& groupCandidates) {
    Y_VERIFY(!groupCandidates.empty());
    return &(groupCandidates[RoundRobinPos++ % groupCandidates.size()]->GroupParameters);
}

template <>
const TEvControllerSelectGroupsResult::TGroupParameters* TStoragePoolInfo::SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM>(const TVector<const TStorageGroupInfo*>& groupCandidates) {
    Y_VERIFY(!groupCandidates.empty());
    return &(groupCandidates[TAppData::RandomProvider->GenRand() % groupCandidates.size()]->GroupParameters);
}

template <>
const TEvControllerSelectGroupsResult::TGroupParameters* TStoragePoolInfo::SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM>(const TVector<const TStorageGroupInfo*>& groupCandidates) {
    Y_VERIFY(!groupCandidates.empty());
    double sumUsage = 0;
    double maxUsage = 0;
    for (const TStorageGroupInfo* groupInfo : groupCandidates) {
        double usage = groupInfo->GetUsage();
        sumUsage += usage;
        maxUsage = std::max(maxUsage, usage);
    }
    //maxUsage = std::max(1.0, maxUsage);
    double sumAvail = maxUsage * groupCandidates.size() - sumUsage;
    if (sumAvail > 0) {
        double pos = TAppData::RandomProvider->GenRandReal2() * sumAvail;
        for (const TStorageGroupInfo* groupInfo : groupCandidates) {
            double avail = maxUsage - groupInfo->GetUsage();
            if (pos < avail) {
                return &groupInfo->GroupParameters;
            } else {
                pos -= avail;
            }
        }
    }
    return SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM>(groupCandidates);
}

template <>
const TEvControllerSelectGroupsResult::TGroupParameters* TStoragePoolInfo::SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_EXACT_MIN>(const TVector<const TStorageGroupInfo*>& groupCandidates) {
    Y_VERIFY(!groupCandidates.empty());
    auto itMin = std::min_element(
                groupCandidates.begin(),
                groupCandidates.end(),
                [](const TStorageGroupInfo* a, const TStorageGroupInfo* b) {
                    return a->GetUsage() < b->GetUsage();
                });
    return &((*itMin)->GroupParameters);
}

template <>
const TEvControllerSelectGroupsResult::TGroupParameters* TStoragePoolInfo::SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM_MIN_7P>(const TVector<const TStorageGroupInfo*>& groupCandidates) {
    Y_VERIFY(!groupCandidates.empty());
    TVector<const TStorageGroupInfo*> groups(groupCandidates);
    auto itGroup = groups.begin();
    auto itPartition = itGroup;
    size_t percent7 = std::max<size_t>(groups.size() * 7 / 100, 1);
    std::advance(itPartition, percent7);
    std::nth_element(groups.begin(), itPartition, groups.end(), [](const TStorageGroupInfo* a, const TStorageGroupInfo* b) {
        return a->GetUsage() < b->GetUsage();
    });
    std::advance(itGroup, TAppData::RandomProvider->GenRand64() % percent7);
    return &((*itGroup)->GroupParameters);
}

const TEvControllerSelectGroupsResult::TGroupParameters* TStoragePoolInfo::FindFreeAllocationUnit(std::function<bool(const TStorageGroupInfo&)> filter) {
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
    switch (GetSelectStrategy()) {
    case NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM:
        return SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_WEIGHTED_RANDOM>(groupCandidates);
    case NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_EXACT_MIN:
        return SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_EXACT_MIN>(groupCandidates);
    case NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM_MIN_7P:
        return SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM_MIN_7P>(groupCandidates);
    case NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_ROUND_ROBIN:
        return SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_ROUND_ROBIN>(groupCandidates);
    case NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM:
        return SelectGroup<NKikimrConfig::THiveConfig::HIVE_STORAGE_SELECT_STRATEGY_RANDOM>(groupCandidates);
    }
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

} // NHive
} // NKikimr
