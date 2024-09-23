#pragma once

#include "hive.h"
#include "tablet_info.h"
#include "follower_tablet_info.h"
#include <ydb/core/base/channel_profiles.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {
namespace NHive {

struct TTabletCategoryInfo {
    TTabletCategoryId Id;
    std::unordered_set<TLeaderTabletInfo*> Tablets;
    ui64 MaxDisconnectTimeout = 0;
    bool StickTogetherInDC = false;

    TTabletCategoryInfo(TTabletCategoryId id)
        : Id(id)
    {}
};

struct TStoragePoolInfo;

struct TLeaderTabletInfo : TTabletInfo {
protected:
    static TString DEFAULT_STORAGE_POOL_NAME;

public:
    struct TChannel {
        TTabletId TabletId;
        ui32 ChannelId;
        const TChannelBind* ChannelInfo;

        double GetWeight(NKikimrConfig::THiveConfig::EHiveStorageBalanceStrategy metricToBalance) const {
            Y_DEBUG_ABORT_UNLESS(ChannelInfo);
            switch (metricToBalance) {
                case NKikimrConfig::THiveConfig::HIVE_STORAGE_BALANCE_STRATEGY_IOPS:
                    return ChannelInfo->GetIOPS();
                case NKikimrConfig::THiveConfig::HIVE_STORAGE_BALANCE_STRATEGY_THROUGHPUT:
                    return ChannelInfo->GetThroughput();
                default:
                case NKikimrConfig::THiveConfig::HIVE_STORAGE_BALANCE_STRATEGY_SIZE:
                    return ChannelInfo->GetSize();
            }
        }

        bool operator==(const TChannel& other) const {
            return TabletId == other.TabletId && ChannelId == other.ChannelId;
        }
    };

    struct TChannelHistoryEntry {
        ui32 Channel;
        TTabletChannelInfo::THistoryEntry Entry;

        TChannelHistoryEntry(ui32 channel, const TTabletChannelInfo::THistoryEntry& entry)
            : Channel(channel)
            , Entry(entry)
        {
        }
    };

    TTabletId Id;
    ETabletState State;
    TTabletTypes::EType Type;
    TFullObjectId ObjectId;
    TSubDomainKey ObjectDomain;
    TNodeFilter NodeFilter;
    NKikimrHive::TDataCentersPreference DataCentersPreference;
    TIntrusivePtr<TTabletStorageInfo> TabletStorageInfo;
    TChannelsBindings BoundChannels;
    std::bitset<MAX_TABLET_CHANNELS> ChannelProfileNewGroup;
    std::vector<TChannelHistoryEntry> DeletedHistory;
    bool WasAliveSinceCutHistory = false;
    NKikimrHive::TEvReassignTablet::EHiveReassignReason ChannelProfileReassignReason;
    ui32 KnownGeneration;
    TTabletCategoryInfo* Category;
    TList<TFollowerGroup> FollowerGroups;
    TList<TFollowerTabletInfo> Followers;
    TOwnerIdxType::TValueType Owner;
    NKikimrHive::ETabletBootMode BootMode;
    TVector<TActorId> StorageInfoSubscribers;
    TActorId LockedToActor;
    TDuration LockedReconnectTimeout;
    ui64 PendingUnlockSeqNo;

    bool SeizedByChild = false; // transient state for migration - need to delete it later
    bool NeedToReleaseFromParent = false; // transient state for migration - need to delete it later

    TLeaderTabletInfo(TTabletId id, THive& hive)
        : TTabletInfo(ETabletRole::Leader, hive)
        , Id(id)
        , State(ETabletState::Unknown)
        , Type(TTabletTypes::TypeInvalid)
        , ObjectId(0, 0)
        , NodeFilter(hive)
        , ChannelProfileReassignReason(NKikimrHive::TEvReassignTablet::HIVE_REASSIGN_REASON_NO)
        , KnownGeneration(0)
        , Category(nullptr)
        , BootMode(NKikimrHive::TABLET_BOOT_MODE_DEFAULT)
        , PendingUnlockSeqNo(0)
    {}

    bool IsReadyToAssignGroups() const {
        return !SeizedByChild && !NeedToReleaseFromParent && State == ETabletState::GroupAssignment && !BoundChannels.empty();
    }

    bool IsReadyToWork() const {
        return !NeedToReleaseFromParent && State == ETabletState::ReadyToWork && !IsBootingSuppressed();
    }

    bool IsReadyToBoot() const {
        return IsReadyToWork() && TTabletInfo::IsReadyToBoot();
    }

    bool IsReadyToStart(TInstant now) const {
        return IsReadyToWork() && TTabletInfo::IsReadyToStart(now);
    }

    bool IsReadyToBlockStorage() const {
        return State == ETabletState::BlockStorage;
    }

    bool IsStarting() const {
        return IsReadyToWork() && TTabletInfo::IsStarting();
    }

    bool IsStartingOnNode(TNodeId nodeId) const {
        return IsReadyToWork() && TTabletInfo::IsStartingOnNode(nodeId);
    }

    bool IsRunning() const {
        return IsReadyToWork() && TTabletInfo::IsRunning();
    }

    bool IsAlive() const {
        return IsReadyToWork() && TTabletInfo::IsAlive();
    }

    bool IsAliveOnLocal(const TActorId& local) const {
        return IsReadyToWork() && TTabletInfo::IsAliveOnLocal(local);
    }

    bool IsDeleting() const {
        return State == ETabletState::Deleting;
    }

    bool IsSomeoneAliveOnNode(TNodeId nodeId) const;

    bool IsLockedToActor() const {
        return !!LockedToActor;
    }

    bool IsExternalBoot() const {
        return BootMode == NKikimrHive::TABLET_BOOT_MODE_EXTERNAL;
    }

    bool IsBootingSuppressed() const {
        return IsExternalBoot() || IsLockedToActor();
    }

    bool IsReadyToReassignTablet() const {
        return !SeizedByChild && State == ETabletState::ReadyToWork;
    }

    ui32 GetFollowersAliveOnDataCenter(TDataCenterId dataCenterId) const;
    ui32 GetFollowersAliveOnDataCenterExcludingFollower(TDataCenterId dataCenterId, const TTabletInfo& excludingFollower) const;

    TPathId GetTenant() const;

    bool IsAllAlive() const {
        if (!IsAlive())
            return false;
        for (const TTabletInfo& follower : Followers) {
            if (!follower.IsAlive())
                return false;
        }
        return true;
    }

    bool IsSomeoneAlive() const {
        if (IsAlive())
            return true;
        for (const TTabletInfo& follower : Followers) {
            if (follower.IsAlive())
                return true;
        }
        return false;
    }

    bool IsSomeFollowerAlive() const {
        for (const TTabletInfo& follower : Followers) {
            if (follower.IsAlive())
                return true;
        }
        return false;
    }

    bool HaveFollowers() const {
        return !Followers.empty();
    }

    bool IsFollowerPromotableOnNode(TNodeId nodeId) const;
    TFollowerId GetFollowerPromotableOnNode(TNodeId nodeId) const;

    void AssignDomains(const TSubDomainKey& objectDomain, const TVector<TSubDomainKey>& allowedDomains);

    bool TryToBoot() {
        bool boot = false;
        if (IsReadyToBoot()) {
            boot |= InitiateBoot();
        }
        if (HaveFollowers()) {
            boot |= InitiateFollowersBoot();
        }
        return boot;
    }

    bool InitiateAssignTabletGroups();

    bool InitiateFollowersBoot() {
        bool result = false;
        if (IsReadyToWork()) {
            for (TFollowerTabletInfo& follower : Followers) {
                if (follower.IsReadyToBoot()) {
                    result |= follower.InitiateBoot();
                }
            }
        }
        return result;
    }

    void Kill(TSideEffects& sideEffects) {
        for (TFollowerTabletInfo& follower : Followers) {
            follower.SendStopTablet(sideEffects);
        }
        TTabletInfo::SendStopTablet(sideEffects);
    }

    bool InitiateBlockStorage(TSideEffects& sideEffects);
    bool InitiateBlockStorage(TSideEffects& sideEffects, ui32 generation);
    bool InitiateDeleteStorage(TSideEffects& sideEffects);

    void IncreaseGeneration() {
        Y_ABORT_UNLESS(KnownGeneration < Max<ui32>());
        ++KnownGeneration;
    }

    const TTabletInfo* FindTablet(TFollowerId followerId) const { // get leader or follower tablet depending on followerId
        if (followerId == 0) {
            return this; // leader
        }
        auto it = std::find_if(Followers.begin(), Followers.end(), [followerId](const TFollowerTabletInfo& info) -> bool {
            return info.Id == followerId;
        });
        if (it != Followers.end()) {
            return &(*it);
        }
        return nullptr;
    }

    const TTabletInfo& GetTablet(TFollowerId followerId) const { // get leader or follower tablet depending on followerId
        const TTabletInfo* tablet = FindTablet(followerId);
        if (tablet != nullptr) {
            return *tablet;
        }
        return *this; // leader by default
    }

    TTabletInfo* FindTablet(TFollowerId followerId) { // get leader or follower tablet depending on followerId
        return const_cast<TTabletInfo*>(static_cast<const TLeaderTabletInfo*>(this)->FindTablet(followerId));
    }

    TTabletInfo&  GetTablet(TFollowerId followerId) { // get leader or follower tablet depending on followerId
        return const_cast<TTabletInfo&>(static_cast<const TLeaderTabletInfo&>(*this).GetTablet(followerId));
    }

    TFollowerTabletInfo& SpawnFollower(TFollowerGroup& followerGroup) {
        TFollowerTabletInfo& follower = AddFollower(followerGroup);
        follower.BecomeStopped();
        return follower;
    }

    template <template <typename, typename...> class Cont, typename Type, typename... Types>
    static decltype(Type::Id) GenerateId(const Cont<Type, Types...>& items) {
        decltype(Type::Id) id = 0;
        for (const auto& item : items) {
            id = std::max<decltype(Type::Id)>(id, item.Id);
        }
        ++id;
        return id;
    }

    TFollowerId GenerateFollowerId() const {
        return GenerateId(Followers);
    }

    TFollowerTabletInfo& AddFollower(TFollowerGroup& followerGroup, TFollowerId followerId = 0);
    TFollowerGroupId GenerateFollowerGroupId() const;
    TFollowerGroup& AddFollowerGroup(TFollowerGroupId followerGroupId = 0);
    ui32 GetActualFollowerCount(TFollowerGroupId followerGroupId) const;

    TFollowerGroup& GetFollowerGroup(TFollowerGroupId followerGroupId) {
        auto it = std::find(FollowerGroups.begin(), FollowerGroups.end(), followerGroupId);
        Y_ABORT_UNLESS(it != FollowerGroups.end(), "%s", (TStringBuilder()
                    << "TabletId=" << Id
                    << " FollowerGroupId=" << followerGroupId
                    << " FollowerGroupSize=" << FollowerGroups.size()
                    << " FollowersSize=" << Followers.size()).data());
        return *it;
    }

    void NotifyStorageInfo(TCompleteNotifications& notifications) {
        TVector<TActorId> targets;
        targets.swap(StorageInfoSubscribers);
        for (TActorId target : targets) {
            notifications.Send(target, new TEvHive::TEvGetTabletStorageInfoResult(Id, *TabletStorageInfo));
        }
    }

    TActorId SetLockedToActor(const TActorId& actor, const TDuration& timeout);

    TActorId ClearLockedToActor() {
        return SetLockedToActor(TActorId(), TDuration());
    }

    void ActualizeTabletStatistics(TInstant now);

    void ResetTabletGroupsRequests() {
        ChannelProfileNewGroup.reset();
    }

    ui32 GetChannelCount() const {
        return BoundChannels.size();
    }

    TChannel GetChannel(ui32 channelId) const {
        TChannel channel{.TabletId = Id, .ChannelId = channelId, .ChannelInfo = nullptr};
        if (channelId < BoundChannels.size()) {
            channel.ChannelInfo = &BoundChannels[channelId];
        }
        return channel;
    }

    void AcquireAllocationUnits();
    void ReleaseAllocationUnits();
    bool AcquireAllocationUnit(ui32 channelId);
    bool ReleaseAllocationUnit(ui32 channelId);
    const NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters* FindFreeAllocationUnit(ui32 channelId);
    TString GetChannelStoragePoolName(const TTabletChannelInfo& channel) const;
    TString GetChannelStoragePoolName(const TChannelProfiles::TProfile::TChannel& channel) const;
    TString GetChannelStoragePoolName(ui32 channelId) const;
    TStoragePoolInfo& GetStoragePool(ui32 channelId) const;
    void RestoreDeletedHistory();

    void SetType(TTabletTypes::EType type);
};

} // NHive
} // NKikimr

