#include "group_state.h"
#include "syncer.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_write_quorum.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>

#include <map>
#include <set>

namespace NKikimr::NGroupSyncer {

namespace {
std::optional<TMirror3dcGroupConfiguration> GetConfiguration(const TGroupReplicationInfo& info) {
    if (info.GroupState) {
        TMirror3dcGroupConfiguration config{*info.GroupState, info.States};
        return IsValidMirror3dcGroupConfiguration(config) ? std::optional(config) : std::nullopt;
    }
    return std::nullopt;
}

// W expands from 2 in Included to 2+2 in Included and Recovering on these edges.
bool WaitForRecoveringVDiskStartup(EMirror3dcGroupState state) {
    using TGroupState = NKikimrBlobStorage::TGroupState;
    return state == TGroupState::DC_SOLO_RECOVERY_INV1_PENDING || state == TGroupState::DC_SOLO_RECOVERY_INV2_PENDING;
}
} // namespace

class TGroupStateActor : public TActorBootstrapped<TGroupStateActor> {
    const TActorId OwnerId;
    static constexpr ui32 GroupsPerRound = 64;
    static constexpr ui32 MaxRunningRepairs = 16;
    static constexpr TDuration PollInterval = TDuration::Seconds(5);
    enum class EStage {
        ConfirmConfig,
        Repair,
        CheckPromotion,
        Propose,
    };

    struct TGroup {
        TGroupReplicationInfo Info;
        TMirror3dcGroupConfiguration Next;
        TVector<bool> ConfirmedDisks;
        ui32 Pending = 0;
        ui64 ProposalCookie = 0;
        ui64 RepairCookie = 0;
        ui32 RepairNode = 0;
        ui32 NextRepairDisk = 0;
        EStage Stage = EStage::ConfirmConfig;

        bool RequiresStartupConfirmation(ui32 realm) const {
            return WaitForRecoveringVDiskStartup(*Info.GroupState) && IsRecovering(Info.States[realm]);
        }
    };
    struct TQuery {
        ui32 GroupId;
        ui32 Disk;
    };
    std::map<ui32, TGroup> Groups;
    std::map<ui64, TQuery> Queries;
    std::map<ui32, TVector<ui64>> QueryCookiesByGroup;
    std::map<ui64, ui32> Proposals;
    std::map<ui64, ui32> RunningRepairs;
    std::map<ui32, ui32> RepairNodeSubscriptions;
    std::set<ui32> StatusPollGroups;
    std::set<ui32> RepairGroups;
    TVector<ui32> ActiveGroups;
    ui32 Cursor = 0;
    std::optional<ui32> LastStartedRepairGroup;
    ui64 NextCookie = 0;

    void EraseQueries(ui32 groupId) {
        if (const auto it = QueryCookiesByGroup.find(groupId); it != QueryCookiesByGroup.end()) {
            for (ui64 cookie : it->second) {
                Queries.erase(cookie);
            }
            QueryCookiesByGroup.erase(it);
        }
    }

    void ClearRepair(TGroup* group, bool cancel) {
        if (!group->RepairCookie) {
            return;
        }
        if (cancel) {
            std::unique_ptr<TEvRunMirror3dcRepair> request = std::make_unique<TEvRunMirror3dcRepair>();
            request->Record.SetGroupId(group->Info.VDisks.front().Id.GroupID.GetRawId());
            request->Record.SetGeneration(group->Info.Generation);
            request->Record.SetCancel(true);
            Send(MakeBlobStorageNodeWardenID(group->RepairNode), request.release(), 0, group->RepairCookie);
        }
        if (group->RepairNode != SelfId().NodeId() && !--RepairNodeSubscriptions.at(group->RepairNode)) {
            Send(TActivationContext::InterconnectProxy(group->RepairNode), new TEvents::TEvUnsubscribe);
            RepairNodeSubscriptions.erase(group->RepairNode);
        }
        RunningRepairs.erase(group->RepairCookie);
        group->RepairCookie = 0;
    }

    void PassAway() override {
        for (auto& [groupId, group] : Groups) {
            ClearRepair(&group, true);
        }
        TActorBootstrapped::PassAway();
    }

    void CancelRepairsOnNode(ui32 nodeId) {
        TVector<ui32> groups;
        for (const auto& [cookie, groupId] : RunningRepairs) {
            if (Groups.at(groupId).RepairNode == nodeId) {
                groups.push_back(groupId);
            }
        }
        for (ui32 groupId : groups) {
            ClearRepair(&Groups.at(groupId), true);
        }
    }

    void Handle(TEvGroupState::TEvUpdate::TPtr& ev) {
        if (ev->Sender != OwnerId) {
            return;
        }
        const TEvGroupState::TEvUpdate& message = *ev->Get();
        if (auto it = Groups.find(message.GroupId); it != Groups.end() && message.Info
                && it->second.Info.Generation == message.Info->Generation
                && it->second.Info.TargetExcluded == message.Info->TargetExcluded) {
            return;
        }
        EraseQueries(message.GroupId);
        StatusPollGroups.erase(message.GroupId);
        RepairGroups.erase(message.GroupId);
        if (const auto it = Groups.find(message.GroupId); it != Groups.end()) {
            ClearRepair(&it->second, true);
            Proposals.erase(it->second.ProposalCookie);
            Groups.erase(it);
        }
        std::optional<TMirror3dcGroupConfiguration> config = message.Info ? GetConfiguration(*message.Info) : std::nullopt;
        if (config && message.Info->Topology && !message.Info->VDisks.empty()) {
            auto policyStep = message.Info->TargetExcluded ? NextDataCenterTransition(*config, *message.Info->TargetExcluded) : std::nullopt;
            auto next = policyStep ? policyStep : RecoveringComplete(*config);
            if (!next) {
                return;
            }
            TGroup& group = Groups[message.GroupId];
            group.Info = *message.Info;
            group.Next = *next;
            if (policyStep) {
                group.Stage = EStage::Propose;
                RepairGroups.insert(message.GroupId);
                ProposeState(message.GroupId, &group);
                return;
            }
            group.ConfirmedDisks.resize(group.Info.VDisks.size());
            StatusPollGroups.insert(message.GroupId);
        }
    }

    bool HasStatusQuorum(ui32 groupId, const TGroup& group) const {
        const TBlobStorageGroupInfo::TTopology* topology = group.Info.Topology.get();
        TBlobStorageGroupInfo::TGroupVDisks uncertain(topology);
        for (ui32 index = 0; index < group.Info.VDisks.size(); ++index) {
            if (!group.ConfirmedDisks[index]) {
                const TVDiskID& disk = group.Info.VDisks[index].Id;
                if (group.Stage == EStage::ConfirmConfig && group.RequiresStartupConfirmation(disk.FailRealm)) {
                    return false;
                }
                uncertain |= TBlobStorageGroupInfo::TGroupVDisks(topology, TVDiskIdShort(disk));
            }
        }
        if (group.Stage == EStage::CheckPromotion) {
            // Every placement allowed by the next E must include at least one confirmed VDisk.
            return !TMirror3dcWriteQuorumChecker::CanHideEnsuredLayout(group.Next, uncertain);
        }
        TBlobStorageGroupInfo::TDynamicInfo dynamic(TGroupId::FromValue(groupId), group.Info.Generation);
        dynamic.Mirror3dcState = {*group.Info.GroupState, group.Info.States};
        dynamic.ServiceIdForOrderNumber.resize(topology->GetTotalVDisksNum());
        TBlobStorageGroupInfo info(group.Info.Topology, std::move(dynamic), TString(), {}, NPDisk::DEVICE_TYPE_UNKNOWN);
        return TMirror3dcWriteQuorumChecker::CheckFailModelForGroup(info, uncertain);
    }

    void StartRepair(ui32 groupId, TGroup* group) {
        Y_ABORT_UNLESS(!group->RepairCookie && !group->ProposalCookie && group->Stage == EStage::Repair);
        ui32 diskIndex = group->NextRepairDisk;
        while (!group->ConfirmedDisks[diskIndex]) {
            diskIndex = (diskIndex + 1) % group->ConfirmedDisks.size();
        }
        group->NextRepairDisk = (diskIndex + 1) % group->ConfirmedDisks.size();
        group->RepairNode = group->Info.VDisks[diskIndex].ActorId.NodeId();
        group->RepairCookie = ++NextCookie;
        RunningRepairs.emplace(group->RepairCookie, groupId);
        ui32 flags = IEventHandle::FlagTrackDelivery;
        if (group->RepairNode != SelfId().NodeId()) {
            ++RepairNodeSubscriptions[group->RepairNode];
            flags |= IEventHandle::FlagSubscribeOnSession;
        }
        std::unique_ptr<TEvRunMirror3dcRepair> request = std::make_unique<TEvRunMirror3dcRepair>();
        request->Record.SetGroupId(groupId);
        request->Record.SetGeneration(group->Info.Generation);
        Send(MakeBlobStorageNodeWardenID(group->RepairNode), request.release(), flags, group->RepairCookie);
    }

    void ProposeState(ui32 groupId, TGroup* group) {
        if (group->ProposalCookie) {
            return;
        }
        const TMirror3dcGroupConfiguration& next = group->Next;
        Y_ABORT_UNLESS(IsAllowedMirror3dcGroupTransition(*group->Info.GroupState, next.State));
        group->ProposalCookie = ++NextCookie;
        Proposals.emplace(group->ProposalCookie, groupId);
        Send(OwnerId, new TEvGroupState::TEvPropose(groupId, group->Info.Generation, next), 0, group->ProposalCookie);
    }

    void FinishStatusRound(ui32 groupId, TGroup* group) {
        if (!HasStatusQuorum(groupId, *group)) {
            return;
        }
        EraseQueries(groupId);
        group->Pending = 0;
        if (group->Stage == EStage::CheckPromotion || WaitForRecoveringVDiskStartup(*group->Info.GroupState)) {
            ProposeState(groupId, group);
            return;
        }
        StatusPollGroups.erase(groupId);
        group->Stage = EStage::Repair;
        RepairGroups.insert(groupId);
    }

    void RunRepairs() {
        auto it = LastStartedRepairGroup ? RepairGroups.upper_bound(*LastStartedRepairGroup) : RepairGroups.begin();
        for (size_t remaining = RepairGroups.size(); remaining; --remaining) {
            if (it == RepairGroups.end()) {
                it = RepairGroups.begin();
            }
            const ui32 groupId = *it++;
            TGroup& group = Groups.at(groupId);
            if (group.Stage == EStage::Propose) {
                ProposeState(groupId, &group);
            } else if (!group.RepairCookie && !group.ProposalCookie && RunningRepairs.size() < MaxRunningRepairs) {
                StartRepair(groupId, &group);
                LastStartedRepairGroup = groupId;
            }
        }
    }

    void Poll() {
        for (ui32 groupId : ActiveGroups) {
            if (auto it = Groups.find(groupId); it != Groups.end() && it->second.Pending) {
                it->second.Pending = 0;
                FinishStatusRound(groupId, &it->second);
            }
        }
        ActiveGroups.clear();
        Queries.clear();
        QueryCookiesByGroup.clear();
        if (!StatusPollGroups.empty()) {
            auto it = StatusPollGroups.lower_bound(Cursor);
            ui32 count = Min<ui32>(GroupsPerRound, StatusPollGroups.size());
            for (ui32 index = 0; index < count; ++index) {
                if (it == StatusPollGroups.end()) {
                    it = StatusPollGroups.begin();
                }
                ui32 groupId = *it++;
                TGroup& group = Groups.at(groupId);
                if (group.ProposalCookie) {
                    continue;
                }
                const bool checkPromotion = group.Stage == EStage::CheckPromotion;
                if (checkPromotion || WaitForRecoveringVDiskStartup(*group.Info.GroupState)) {
                    // These transitions require a fresh set of replies, including on proposal retries.
                    group.ConfirmedDisks.assign(group.Info.VDisks.size(), false);
                }
                ActiveGroups.push_back(groupId);
                for (ui32 disk = 0; disk < group.Info.VDisks.size(); ++disk) {
                    const TGroupReplicationInfo::TVDisk& vdisk = group.Info.VDisks[disk];
                    ui32 realm = vdisk.Id.FailRealm;
                    bool pollRealm = checkPromotion ? IsReadable(group.Next.Realms[realm]) : !IsExcluded(group.Info.States[realm]);
                    if (pollRealm && !group.ConfirmedDisks[disk]) {
                        ui64 cookie = ++NextCookie;
                        Queries.emplace(cookie, TQuery{groupId, disk});
                        QueryCookiesByGroup[groupId].push_back(cookie);
                        ++group.Pending;
                        Send(vdisk.ActorId, new TEvBlobStorage::TEvVStatus(vdisk.Id), 0, cookie);
                    }
                }
            }
            Cursor = it == StatusPollGroups.end() ? *StatusPollGroups.begin() : *it;
        }
        RunRepairs();
        Schedule(PollInterval, new TEvents::TEvWakeup);
    }

    void Handle(TEvGroupState::TEvProposalResult::TPtr& ev) {
        const auto proposal = Proposals.find(ev->Cookie);
        if (proposal == Proposals.end() || ev->Sender != OwnerId) {
            return;
        }
        if (const auto group = Groups.find(proposal->second); group != Groups.end()
                && group->second.ProposalCookie == ev->Cookie) {
            group->second.ProposalCookie = 0;
        }
        Proposals.erase(proposal);
    }

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr& ev) {
        const auto it = Queries.find(ev->Cookie);
        if (it == Queries.end()) {
            return;
        }
        TQuery query = it->second;
        Queries.erase(it);
        TGroup& group = Groups.at(query.GroupId);
        const NKikimrBlobStorage::TEvVStatusResult& record = ev->Get()->Record;
        TVDiskID reported = record.HasVDiskID() ? VDiskIDFromVDiskID(record.GetVDiskID()) : TVDiskID();
        const TVDiskID& expected = group.Info.VDisks[query.Disk].Id;
        EMirror3dcFailRealmState role = group.Info.States[expected.FailRealm];
        bool allowReadOnly = group.Stage == EStage::CheckPromotion || IsLeaving(role);
        group.ConfirmedDisks[query.Disk] = record.HasVDiskID() && reported == expected
            && record.GetStatus() == NKikimrProto::OK && record.GetJoinedGroup() && record.GetReplicated()
            && (!record.GetIsReadOnly() || allowReadOnly);
        if (!--group.Pending) {
            FinishStatusRound(query.GroupId, &group);
            RunRepairs();
        }
    }

    void Handle(TEvRunMirror3dcRepairResult::TPtr& ev) {
        const auto running = RunningRepairs.find(ev->Cookie);
        if (running == RunningRepairs.end()) {
            return;
        }
        ui32 groupId = running->second;
        TGroup& group = Groups.at(groupId);
        const NKikimrBlobStorage::TEvRunMirror3dcRepairResult& result = ev->Get()->Record;
        bool expectedSender = ev->Sender.NodeId() == group.RepairNode;
        if (!expectedSender || !result.HasStatus() || result.GetGroupId() != groupId
                || result.GetGeneration() != group.Info.Generation) {
            return;
        }
        ClearRepair(&group, false);
        if (result.GetStatus() == NKikimrProto::OK) {
            group.Stage = EStage::CheckPromotion;
            RepairGroups.erase(groupId);
            StatusPollGroups.insert(groupId);
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        const auto it = RunningRepairs.find(ev->Cookie);
        if (it != RunningRepairs.end() && ev->Get()->SourceType == TEvRunMirror3dcRepair::EventType) {
            ClearRepair(&Groups.at(it->second), true);
        }
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        CancelRepairsOnNode(ev->Get()->NodeId);
    }

public:
    explicit TGroupStateActor(const TActorId& ownerId) : OwnerId(ownerId) {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        Poll();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvGroupState::TEvUpdate, Handle)
        hFunc(TEvBlobStorage::TEvVStatusResult, Handle)
        hFunc(TEvGroupState::TEvProposalResult, Handle)
        hFunc(TEvRunMirror3dcRepairResult, Handle)
        hFunc(TEvents::TEvUndelivered, Handle)
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle)
        IgnoreFunc(TEvInterconnect::TEvNodeConnected)
        cFunc(TEvents::TSystem::Wakeup, Poll)
        cFunc(TEvents::TSystem::Poison, PassAway)
    )
};

IActor* CreateGroupStateActor(const TActorId& ownerId) {
    return new TGroupStateActor(ownerId);
}

} // namespace NKikimr::NGroupSyncer
