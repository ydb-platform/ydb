#include "group_state.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_write_quorum.h>
#include <ydb/core/blobstorage/syncer/syncer.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <map>
#include <set>

namespace NKikimr::NBsController {

namespace {
std::optional<TMirror3dcGroupConfiguration> GetConfiguration(const TGroupReplicationInfo& info) {
    if (info.GroupState) {
        TMirror3dcGroupConfiguration config{*info.GroupState, info.States};
        return IsValidMirror3dcGroupConfiguration(config) ? std::optional(config) : std::nullopt;
    }
    return std::nullopt;
}
} // namespace

class TGroupStateActor : public TActorBootstrapped<TGroupStateActor> {
    const TActorId ControllerId;
    static constexpr ui32 GroupsPerRound = 64;
    static constexpr ui32 MaxRunningRepairs = 16;
    static constexpr TDuration PollInterval = TDuration::Seconds(5);
    struct TGroup {
        TGroupReplicationInfo Info;
        TVector<bool> ConfigApplied;
        ui32 Pending = 0;
        ui64 ProposalCookie = 0;
        ui64 RepairCookie = 0;
        ui32 RepairNode = 0;
        ui32 NextRepairDisk = 0;
        bool RepairSucceeded = false;
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
    std::set<ui32> ConfigConfirmationGroups;
    std::set<ui32> RepairGroups;
    TVector<ui32> ActiveGroups;
    ui32 Cursor = 0;
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
            std::unique_ptr<NGroupSyncer::TEvRunMirror3dcRepair> request = std::make_unique<NGroupSyncer::TEvRunMirror3dcRepair>();
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
        const TEvGroupState::TEvUpdate& message = *ev->Get();
        EraseQueries(message.GroupId);
        ConfigConfirmationGroups.erase(message.GroupId);
        RepairGroups.erase(message.GroupId);
        if (const auto it = Groups.find(message.GroupId); it != Groups.end()) {
            ClearRepair(&it->second, true);
            Proposals.erase(it->second.ProposalCookie);
            Groups.erase(it);
        }
        std::optional<TMirror3dcGroupConfiguration> config = message.Info ? GetConfiguration(*message.Info) : std::nullopt;
        if (config && config->State != NKikimrBlobStorage::TGroupState::NORMAL
                && message.Info->Topology && !message.Info->VDisks.empty() && RecoveringComplete(*config)) {
            TGroup& group = Groups[message.GroupId];
            group.Info = *message.Info;
            group.ConfigApplied.resize(group.Info.VDisks.size());
            ConfigConfirmationGroups.insert(message.GroupId);
        }
    }

    bool HasConfigQuorum(ui32 groupId, const TGroup& group) const {
        TBlobStorageGroupInfo::TDynamicInfo dynamic(TGroupId::FromValue(groupId), group.Info.Generation);
        dynamic.Mirror3dcState = {*group.Info.GroupState, group.Info.States};
        dynamic.ServiceIdForOrderNumber.resize(group.Info.Topology->GetTotalVDisksNum());
        TBlobStorageGroupInfo info(group.Info.Topology, std::move(dynamic), TString(), {}, NPDisk::DEVICE_TYPE_UNKNOWN);
        TBlobStorageGroupInfo::TGroupVDisks applied(&info.GetTopology());
        bool allActiveDisksApplied = true;
        for (ui32 index = 0; index < group.Info.VDisks.size(); ++index) {
            const TVDiskID& disk = group.Info.VDisks[index].Id;
            if (info.IsFailRealmExcluded(disk.FailRealm)) {
                continue;
            }
            if (group.ConfigApplied[index]) {
                applied |= TBlobStorageGroupInfo::TGroupVDisks(&info.GetTopology(), TVDiskIdShort(disk));
            } else {
                allActiveDisksApplied = false;
            }
        }
        if (GetMirror3dcGroupLayouts(*group.Info.GroupState)->Ensured == EMirror3dcLayout::LAYOUT_0) {
            return allActiveDisksApplied;
        }
        return TMirror3dcWriteQuorumChecker::CheckFailModelForGroup(info, ~applied);
    }

    void StartRepair(ui32 groupId, TGroup* group) {
        Y_ABORT_UNLESS(!group->RepairCookie && !group->ProposalCookie && !group->RepairSucceeded);
        ui32 diskIndex = group->NextRepairDisk;
        while (!group->ConfigApplied[diskIndex]) {
            diskIndex = (diskIndex + 1) % group->ConfigApplied.size();
        }
        group->NextRepairDisk = (diskIndex + 1) % group->ConfigApplied.size();
        group->RepairNode = group->Info.VDisks[diskIndex].ActorId.NodeId();
        group->RepairCookie = ++NextCookie;
        RunningRepairs.emplace(group->RepairCookie, groupId);
        ui32 flags = IEventHandle::FlagTrackDelivery;
        if (group->RepairNode != SelfId().NodeId()) {
            ++RepairNodeSubscriptions[group->RepairNode];
            flags |= IEventHandle::FlagSubscribeOnSession;
        }
        std::unique_ptr<NGroupSyncer::TEvRunMirror3dcRepair> request = std::make_unique<NGroupSyncer::TEvRunMirror3dcRepair>();
        request->Record.SetGroupId(groupId);
        request->Record.SetGeneration(group->Info.Generation);
        Send(MakeBlobStorageNodeWardenID(group->RepairNode), request.release(), flags, group->RepairCookie);
    }

    void CheckCompletion(ui32 groupId, TGroup* group) {
        if (group->ProposalCookie || !group->RepairSucceeded) {
            return;
        }
        std::optional<TMirror3dcGroupConfiguration> config = GetConfiguration(group->Info);
        std::optional<TMirror3dcGroupConfiguration> next = config ? RecoveringComplete(*config) : std::nullopt;
        if (!next) {
            return;
        }
        Y_ABORT_UNLESS(IsAllowedMirror3dcGroupTransition(config->State, next->State));
        using TDefinition = NKikimrBlobStorage::TDefineGroupState::TGroupDefinition;
        std::unique_ptr<TEvBlobStorage::TEvControllerConfigRequest> request =
            std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        TDefinition* definition = request->Record.MutableRequest()->AddCommand()->MutableDefineGroupState()->AddGroups();
        definition->SetGroupId(groupId);
        definition->SetExpectedGroupGeneration(group->Info.Generation);
        definition->SetGroupState(next->State);
        for (EMirror3dcFailRealmState state : next->Realms) {
            definition->AddFailRealmStates(state);
        }
        group->ProposalCookie = ++NextCookie;
        Proposals.emplace(group->ProposalCookie, groupId);
        Send(ControllerId, request.release(), 0, group->ProposalCookie);
    }

    void ConfirmConfig(ui32 groupId, TGroup* group) {
        if (HasConfigQuorum(groupId, *group)) {
            EraseQueries(groupId);
            group->Pending = 0;
            ConfigConfirmationGroups.erase(groupId);
            RepairGroups.insert(groupId);
        }
    }

    void RunRepairs() {
        for (ui32 groupId : RepairGroups) {
            TGroup& group = Groups.at(groupId);
            if (group.RepairSucceeded) {
                CheckCompletion(groupId, &group);
            } else if (!group.RepairCookie && !group.ProposalCookie && RunningRepairs.size() < MaxRunningRepairs) {
                StartRepair(groupId, &group);
            }
        }
    }

    void Poll() {
        for (ui32 groupId : ActiveGroups) {
            if (auto it = Groups.find(groupId); it != Groups.end() && it->second.Pending) {
                it->second.Pending = 0;
                ConfirmConfig(groupId, &it->second);
            }
        }
        ActiveGroups.clear();
        Queries.clear();
        QueryCookiesByGroup.clear();
        if (!ConfigConfirmationGroups.empty()) {
            auto it = ConfigConfirmationGroups.lower_bound(Cursor);
            ui32 count = Min<ui32>(GroupsPerRound, ConfigConfirmationGroups.size());
            for (ui32 index = 0; index < count; ++index) {
                if (it == ConfigConfirmationGroups.end()) {
                    it = ConfigConfirmationGroups.begin();
                }
                ui32 groupId = *it;
                TGroup& group = Groups.at(groupId);
                ActiveGroups.push_back(groupId);
                for (ui32 disk = 0; disk < group.Info.VDisks.size(); ++disk) {
                    const TGroupReplicationInfo::TVDisk& vdisk = group.Info.VDisks[disk];
                    if (!group.ConfigApplied[disk] && !IsExcluded(group.Info.States[vdisk.Id.FailRealm])) {
                        ui64 cookie = ++NextCookie;
                        Queries.emplace(cookie, TQuery{groupId, disk});
                        QueryCookiesByGroup[groupId].push_back(cookie);
                        ++group.Pending;
                        Send(vdisk.ActorId, new TEvBlobStorage::TEvVStatus(vdisk.Id), 0, cookie);
                    }
                }
                ++it;
            }
            Cursor = it == ConfigConfirmationGroups.end() ? *ConfigConfirmationGroups.begin() : *it;
        }
        RunRepairs();
        Schedule(PollInterval, new TEvents::TEvWakeup);
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        const auto proposal = Proposals.find(ev->Cookie);
        if (proposal == Proposals.end() || ev->Sender != ControllerId) {
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
        group.ConfigApplied[query.Disk] = record.HasVDiskID() && reported == group.Info.VDisks[query.Disk].Id
            && record.GetStatus() == NKikimrProto::OK;
        if (!--group.Pending) {
            ConfirmConfig(query.GroupId, &group);
            RunRepairs();
        }
    }

    void Handle(NGroupSyncer::TEvRunMirror3dcRepairResult::TPtr& ev) {
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
        group.RepairSucceeded = result.GetStatus() == NKikimrProto::OK;
        ClearRepair(&group, false);
        CheckCompletion(groupId, &group);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        const auto it = RunningRepairs.find(ev->Cookie);
        if (it != RunningRepairs.end() && ev->Get()->SourceType == NGroupSyncer::TEvRunMirror3dcRepair::EventType) {
            ClearRepair(&Groups.at(it->second), true);
        }
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        CancelRepairsOnNode(ev->Get()->NodeId);
    }

public:
    explicit TGroupStateActor(const TActorId& controllerId) : ControllerId(controllerId) {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        Poll();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvGroupState::TEvUpdate, Handle)
        hFunc(TEvBlobStorage::TEvVStatusResult, Handle)
        hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle)
        hFunc(NGroupSyncer::TEvRunMirror3dcRepairResult, Handle)
        hFunc(TEvents::TEvUndelivered, Handle)
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle)
        IgnoreFunc(TEvInterconnect::TEvNodeConnected)
        cFunc(TEvents::TSystem::Wakeup, Poll)
        cFunc(TEvents::TSystem::Poison, PassAway)
    )
};

IActor* CreateGroupStateActor(const TActorId& controllerId) {
    return new TGroupStateActor(controllerId);
}

} // namespace NKikimr::NBsController
