#include "group_state.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <algorithm>
#include <map>

namespace NKikimr::NBsController {

namespace {
struct TLayoutRequirements {
    ui32 RequiredRealms;
    ui32 RequiredCopies;
};

TLayoutRequirements GetMinimumLayoutRequirements(EMirror3dcLayout layout) {
    switch (layout) {
        case EMirror3dcLayout::LAYOUT_111_22:
        case EMirror3dcLayout::LAYOUT_11:
            return {2, 1};
        case EMirror3dcLayout::LAYOUT_11_2:
        case EMirror3dcLayout::LAYOUT_1:
            return {1, 1};
        case EMirror3dcLayout::LAYOUT_22:
            return {2, 2};
        case EMirror3dcLayout::LAYOUT_2:
            return {1, 2};
        case EMirror3dcLayout::LAYOUT_111:
            return {3, 1};
        case EMirror3dcLayout::LAYOUT_0:
            return {0, 0};
    }
    Y_ABORT("Unsupported mirror-3-dc layout");
}
} // namespace

std::optional<EMirror3dcFailRealmState> GetReplicatedRealmState(const TFailRealmStates& states, ui32 realm) {
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    if (realm >= 3 || states[realm] == TFailRealm::F) {
        return std::nullopt;
    }
    const std::optional<EMirror3dcGroupState> state = GetMirror3dcGroupState(states[0], states[1], states[2]);
    if (!state) {
        return std::nullopt;
    }
    const TMirror3dcGroupLayouts layouts = *GetMirror3dcGroupLayouts(*state);
    const TLayoutRequirements write = GetMinimumLayoutRequirements(layouts.Write);
    const TLayoutRequirements durability = GetMinimumLayoutRequirements(layouts.Durability);
    const ui32 requiredRealms = Min(write.RequiredRealms, durability.RequiredRealms);
    const ui32 requiredCopies = Min(write.RequiredCopies, durability.RequiredCopies);
    ui32 normalRealmsAfterTransition = states[realm] == TFailRealm::R;
    for (EMirror3dcFailRealmState realmState : states) {
        normalRealmsAfterTransition += realmState != TFailRealm::F && realmState != TFailRealm::R;
    }
    // N0/N1/N2 encode the minimum copy count. A surplus realm may be omitted by future writes.
    const EMirror3dcFailRealmState target = normalRealmsAfterTransition > requiredRealms
        ? TFailRealm::N0 : static_cast<EMirror3dcFailRealmState>(requiredCopies);
    if (states[realm] == target || (target != TFailRealm::N0 && states[realm] == TFailRealm::N2)) {
        return std::nullopt;
    }
    return target;
}

class TGroupStateActor : public TActorBootstrapped<TGroupStateActor> {
    const TActorId ControllerId;
    static constexpr ui32 GroupsPerRound = 64;
    static constexpr TDuration PollInterval = TDuration::Seconds(5);
    struct TGroup {
        TGroupReplicationInfo Info;
        TVector<bool> Replicated;
        ui32 Pending = 0;
        ui64 ProposalCookie = 0;
    };
    struct TQuery {
        ui32 GroupId;
        ui32 Disk;
    };
    std::map<ui32, TGroup> Groups;
    std::map<ui64, TQuery> Queries;
    std::map<ui32, TVector<ui64>> QueryCookiesByGroup;
    std::map<ui64, ui32> Proposals;
    TVector<ui32> ActiveGroups;
    ui32 Cursor = 0;
    ui64 NextCookie = 0;

    void Handle(TEvGroupState::TEvUpdate::TPtr& ev) {
        const TEvGroupState::TEvUpdate& message = *ev->Get();
        if (const auto it = QueryCookiesByGroup.find(message.GroupId); it != QueryCookiesByGroup.end()) {
            for (ui64 cookie : it->second) {
                Queries.erase(cookie);
            }
            QueryCookiesByGroup.erase(it);
        }
        if (const auto it = Groups.find(message.GroupId); it != Groups.end()) {
            Proposals.erase(it->second.ProposalCookie);
        }
        Groups.erase(message.GroupId);
        if (message.Info) {
            bool needsReplication = false;
            for (ui32 realm = 0; realm < 3; ++realm) {
                needsReplication |= GetReplicatedRealmState(message.Info->States, realm).has_value();
            }
            if (needsReplication) {
                TGroup& group = Groups[message.GroupId];
                group.Info = *message.Info;
                group.Replicated.resize(group.Info.VDisks.size());
            }
        }
    }

    void CheckCompletion(ui32 groupId, TGroup* group) {
        if (group->ProposalCookie) {
            return;
        }
        TFailRealmStates states = group->Info.States;
        for (ui32 realm = 0; realm < 3; ++realm) {
            const std::optional<EMirror3dcFailRealmState> target = GetReplicatedRealmState(group->Info.States, realm);
            if (!target) {
                continue;
            }
            bool complete = true;
            ui32 disks = 0;
            for (ui32 index = 0; index < group->Info.VDisks.size(); ++index) {
                if (group->Info.VDisks[index].Id.FailRealm == realm) {
                    ++disks;
                    complete &= group->Replicated[index];
                }
            }
            if (disks && complete) {
                states[realm] = *target;
            }
        }
        const auto it = std::find(states.begin(), states.end(), NKikimrBlobStorage::TGroupInfo::TFailRealm::F);
        if (it == states.end()) {
            using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
            for (ui32 realm = 0; realm < 3; ++realm) {
                if (group->Info.States[realm] == TFailRealm::R && states[realm] != TFailRealm::R) {
                    // This R has confirmed D, even though its resulting state is N0.
                    // Establish 1+1+1 before NORMAL allows writes to omit any realm again.
                    bool everyNormalRealmHasCopy = true;
                    for (ui32 normalRealm = 0; normalRealm < 3; ++normalRealm) {
                        if (normalRealm != realm && states[normalRealm] == TFailRealm::N0) {
                            everyNormalRealmHasCopy = false;
                        }
                    }
                    if (everyNormalRealmHasCopy) {
                        states.fill(TFailRealm::N0);
                    } else {
                        states[realm] = TFailRealm::R;
                    }
                    break;
                }
            }
        }
        if (states != group->Info.States) {
            using TDefinition = NKikimrBlobStorage::TDefineFailRealmStates::TGroupDefinition;
            std::unique_ptr<TEvBlobStorage::TEvControllerConfigRequest> request =
                std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
            NKikimrBlobStorage::TConfigRequest* config = request->Record.MutableRequest();
            config->SetIgnoreGroupFailModelChecks(true);
            config->SetIgnoreDegradedGroupsChecks(true);
            config->SetIgnoreDisintegratedGroupsChecks(true);
            TDefinition* definition = config->AddCommand()->MutableDefineFailRealmStates()->AddGroups();
            definition->SetGroupId(groupId);
            definition->SetExpectedGroupGeneration(group->Info.Generation);
            for (EMirror3dcFailRealmState state : states) {
                definition->AddFailRealmStates(static_cast<TDefinition::EFailRealmState>(state));
            }
            group->ProposalCookie = ++NextCookie;
            Proposals.emplace(group->ProposalCookie, groupId);
            Send(ControllerId, request.release(), 0, group->ProposalCookie);
        }
    }

    void Poll() {
        // Unanswered requests expire with the round; old cookies cannot finish a new round.
        for (ui32 groupId : ActiveGroups) {
            if (auto it = Groups.find(groupId); it != Groups.end() && it->second.Pending) {
                CheckCompletion(groupId, &it->second);
                it->second.Pending = 0;
            }
        }
        ActiveGroups.clear();
        Queries.clear();
        QueryCookiesByGroup.clear();
        if (!Groups.empty()) {
            auto it = Groups.lower_bound(Cursor);
            const ui32 count = Min<ui32>(GroupsPerRound, Groups.size());
            for (ui32 index = 0; index < count; ++index) {
                if (it == Groups.end()) {
                    it = Groups.begin();
                }
                TGroup& group = it->second;
                if (!group.ProposalCookie) {
                    group.Replicated.assign(group.Info.VDisks.size(), false);
                    group.Pending = 0;
                    ActiveGroups.push_back(it->first);
                    for (ui32 disk = 0; disk < group.Info.VDisks.size(); ++disk) {
                        const TGroupReplicationInfo::TVDisk& vdisk = group.Info.VDisks[disk];
                        if (GetReplicatedRealmState(group.Info.States, vdisk.Id.FailRealm)) {
                            const ui64 cookie = ++NextCookie;
                            Queries.emplace(cookie, TQuery{it->first, disk});
                            QueryCookiesByGroup[it->first].push_back(cookie);
                            ++group.Pending;
                            Send(vdisk.ActorId, new TEvBlobStorage::TEvVStatus(vdisk.Id), 0, cookie);
                        }
                    }
                }
                ++it;
            }
            Cursor = it == Groups.end() ? Groups.begin()->first : it->first;
        }
        Schedule(PollInterval, new TEvents::TEvWakeup);
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        const auto proposal = Proposals.find(ev->Cookie);
        if (proposal == Proposals.end() || ev->Sender != ControllerId) {
            return;
        }
        if (const auto group = Groups.find(proposal->second); group != Groups.end()
                && group->second.ProposalCookie == ev->Cookie) {
            // Successful commits also publish a fresh snapshot; rejected commands may retry.
            group->second.ProposalCookie = 0;
        }
        Proposals.erase(proposal);
    }

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr& ev) {
        const auto it = Queries.find(ev->Cookie);
        if (it == Queries.end()) {
            return;
        }
        const TQuery query = it->second;
        Queries.erase(it);
        TGroup& group = Groups.at(query.GroupId);
        const NKikimrBlobStorage::TEvVStatusResult& record = ev->Get()->Record;
        const TVDiskID reportedVDiskId = record.HasVDiskID() ? VDiskIDFromVDiskID(record.GetVDiskID()) : TVDiskID();
        const bool matchingDisk = record.HasVDiskID() && reportedVDiskId == group.Info.VDisks[query.Disk].Id;
        group.Replicated[query.Disk] = matchingDisk && record.GetStatus() == NKikimrProto::OK
            && record.GetJoinedGroup() && record.GetReplicated()
            && record.GetDurabilityReplicated() && !record.GetIsReadOnly();
        if (!--group.Pending) {
            CheckCompletion(query.GroupId, &group);
        }
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
        cFunc(TEvents::TSystem::Wakeup, Poll)
        cFunc(TEvents::TSystem::Poison, PassAway)
    )
};

// Observe the generation applied by each VDisk even when no replication promotion is pending.
class TGroupConfigActor : public TActorBootstrapped<TGroupConfigActor> {
    const TActorId ControllerId;
    std::map<ui32, TGroupReplicationInfo> Groups;
    struct TQuery {
        ui32 GroupId;
        ui32 Generation;
        TGroupReplicationInfo::TVDisk Disk;
    };
    std::map<ui64, TQuery> Queries;
    ui32 Cursor = 0;
    ui64 NextCookie = 0;

    void Handle(TEvGroupState::TEvUpdate::TPtr& ev) {
        const TEvGroupState::TEvUpdate& message = *ev->Get();
        if (message.Info) {
            Groups[message.GroupId] = *message.Info;
        } else {
            Groups.erase(message.GroupId);
        }
    }

    void Poll() {
        Queries.clear();
        if (!Groups.empty()) {
            auto it = Groups.lower_bound(Cursor);
            const ui32 count = Min<ui32>(64, Groups.size());
            for (ui32 index = 0; index < count; ++index) {
                if (it == Groups.end()) {
                    it = Groups.begin();
                }
                for (const TGroupReplicationInfo::TVDisk& disk : it->second.VDisks) {
                    const ui64 cookie = ++NextCookie;
                    Queries.emplace(cookie, TQuery{it->first, it->second.Generation, disk});
                    Send(disk.ActorId, new TEvBlobStorage::TEvVStatus(disk.Id), 0, cookie);
                }
                ++it;
            }
            Cursor = it == Groups.end() ? Groups.begin()->first : it->first;
        }
        Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup);
    }

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr& ev) {
        const auto it = Queries.find(ev->Cookie);
        if (it == Queries.end()) {
            return;
        }
        const TQuery query = it->second;
        Queries.erase(it);
        const NKikimrBlobStorage::TEvVStatusResult& record = ev->Get()->Record;
        if (!record.HasVDiskID()) {
            return;
        }
        const TVDiskID reported = VDiskIDFromVDiskID(record.GetVDiskID());
        if (!reported.SameExceptGeneration(query.Disk.Id)
                || (record.GetStatus() != NKikimrProto::OK && record.GetStatus() != NKikimrProto::RACE)) {
            return;
        }
        Send(ControllerId, new TEvGroupState::TEvConfigObserved(query.GroupId, query.Generation,
            reported, query.Disk.ActorId, query.Disk.ObservationEpoch));
    }

public:
    explicit TGroupConfigActor(const TActorId& controllerId) : ControllerId(controllerId) {}
    void Bootstrap() {
        Become(&TThis::StateWork);
        Poll();
    }
    STRICT_STFUNC(StateWork,
        hFunc(TEvGroupState::TEvUpdate, Handle)
        hFunc(TEvBlobStorage::TEvVStatusResult, Handle)
        cFunc(TEvents::TSystem::Wakeup, Poll)
        cFunc(TEvents::TSystem::Poison, PassAway)
    )
};

IActor* CreateGroupConfigActor(const TActorId& controllerId) {
    return new TGroupConfigActor(controllerId);
}

IActor* CreateGroupStateActor(const TActorId& controllerId) {
    return new TGroupStateActor(controllerId);
}

} // namespace NKikimr::NBsController
