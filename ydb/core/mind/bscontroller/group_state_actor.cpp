#include "group_state.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <map>

namespace NKikimr::NBsController {

std::optional<EMirror3dcFailRealmState> GetReplicatedRealmState(const TFailRealmStates& states, ui32 realm) {
    using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;
    if (realm >= 3 || states[realm] == TFailRealm::F) {
        return std::nullopt;
    }
    const std::optional<EMirror3dcGroupState> state = GetMirror3dcGroupState(states[0], states[1], states[2]);
    if (!state) {
        return std::nullopt;
    }
    const EMirror3dcLayout durability = GetMirror3dcGroupLayouts(*state)->Durability;
    ui32 requiredRealms;
    EMirror3dcFailRealmState target;
    switch (durability) {
        case EMirror3dcLayout::LAYOUT_111: requiredRealms = 3; target = TFailRealm::N1; break;
        case EMirror3dcLayout::LAYOUT_11: requiredRealms = 2; target = TFailRealm::N1; break;
        case EMirror3dcLayout::LAYOUT_22: requiredRealms = 2; target = TFailRealm::N2; break;
        case EMirror3dcLayout::LAYOUT_1: requiredRealms = 1; target = TFailRealm::N1; break;
        case EMirror3dcLayout::LAYOUT_2: requiredRealms = 1; target = TFailRealm::N2; break;
        default: return std::nullopt;
    }
    ui32 normalRealms = 0;
    for (EMirror3dcFailRealmState realmState : states) {
        normalRealms += realmState != TFailRealm::F && realmState != TFailRealm::R;
    }
    if (states[realm] == TFailRealm::R && normalRealms >= requiredRealms) {
        return std::nullopt;
    }
    if (states[realm] == TFailRealm::N2 || states[realm] == target) {
        return std::nullopt;
    }
    return target;
}

class TGroupStateActor : public TActorBootstrapped<TGroupStateActor> {
    const TActorId ControllerId;
    static constexpr ui32 GroupsPerRound = 16;
    static constexpr TDuration PollInterval = TDuration::Seconds(5);
    struct TGroup {
        TGroupReplicationInfo Info;
        TVector<bool> Replicated;
        ui32 Pending = 0;
        bool Proposed = false;
    };
    struct TQuery {
        ui32 GroupId;
        ui32 Disk;
    };
    std::map<ui32, TGroup> Groups;
    std::map<ui64, TQuery> Queries;
    TVector<ui32> ActiveGroups;
    ui32 Cursor = 0;
    ui64 NextCookie = 0;

    void Handle(TEvGroupState::TEvUpdate::TPtr& ev) {
        if (ev->Sender != ControllerId) {
            return;
        }
        const TEvGroupState::TEvUpdate& message = *ev->Get();
        for (auto it = Queries.begin(); it != Queries.end();) {
            it = it->second.GroupId == message.GroupId ? Queries.erase(it) : std::next(it);
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
        if (group->Proposed) {
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
        if (states != group->Info.States) {
            group->Proposed = true;
            Send(ControllerId, new TEvGroupState::TEvReplicationDone(groupId, group->Info.Generation, states));
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
        if (!Groups.empty()) {
            auto it = Groups.lower_bound(Cursor);
            const ui32 count = Min<ui32>(GroupsPerRound, Groups.size());
            for (ui32 index = 0; index < count; ++index) {
                if (it == Groups.end()) {
                    it = Groups.begin();
                }
                TGroup& group = it->second;
                if (!group.Proposed) {
                    group.Replicated.assign(group.Info.VDisks.size(), false);
                    group.Pending = 0;
                    ActiveGroups.push_back(it->first);
                    for (ui32 disk = 0; disk < group.Info.VDisks.size(); ++disk) {
                        const TGroupReplicationInfo::TVDisk& vdisk = group.Info.VDisks[disk];
                        if (GetReplicatedRealmState(group.Info.States, vdisk.Id.FailRealm)) {
                            const ui64 cookie = ++NextCookie;
                            Queries.emplace(cookie, TQuery{it->first, disk});
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

    void Handle(TEvBlobStorage::TEvVStatusResult::TPtr& ev) {
        const auto it = Queries.find(ev->Cookie);
        if (it == Queries.end()) {
            return;
        }
        const TQuery query = it->second;
        Queries.erase(it);
        TGroup& group = Groups.at(query.GroupId);
        const NKikimrBlobStorage::TEvVStatusResult& record = ev->Get()->Record;
        const bool matchingDisk = record.HasVDiskID()
            && VDiskIDFromVDiskID(record.GetVDiskID()) == group.Info.VDisks[query.Disk].Id;
        group.Replicated[query.Disk] = matchingDisk && record.GetStatus() == NKikimrProto::OK
            && record.GetJoinedGroup() && record.GetReplicated() && !record.GetIsReadOnly();
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
        cFunc(TEvents::TSystem::Wakeup, Poll)
        cFunc(TEvents::TSystem::Poison, PassAway)
    )
};

IActor* CreateGroupStateActor(const TActorId& controllerId) {
    return new TGroupStateActor(controllerId);
}

} // namespace NKikimr::NBsController
