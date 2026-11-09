#include "group_state.h"
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <map>

namespace NKikimr::NBsController {

std::optional<EMirror3dcFailRealmState> GetReplicatedRealmState(
        const TMirror3dcGroupConfiguration& config, ui32 realm) {
    bool validRealm = realm < config.Realms.size();
    bool validConfiguration = IsValidMirror3dcGroupConfiguration(config);
    bool replicationTarget = validRealm && IsMirror3dcReplicationTarget(config.State, config.Realms[realm]);
    if (!validRealm || !validConfiguration || !replicationTarget) {
        return std::nullopt;
    }
    std::optional<TMirror3dcGroupConfiguration> next = RecoveringComplete(config);
    return next ? std::optional(next->Realms[realm]) : std::nullopt;
}

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
        std::optional<TMirror3dcGroupConfiguration> config = message.Info ? GetConfiguration(*message.Info) : std::nullopt;
        if (config) {
            bool needsReplication = false;
            for (ui32 realm = 0; realm < 3; ++realm) {
                needsReplication |= GetReplicatedRealmState(*config, realm).has_value();
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
        std::optional<TMirror3dcGroupConfiguration> config = GetConfiguration(group->Info);
        if (!config) {
            return;
        }
        bool hasTarget = false;
        for (ui32 realm = 0; realm < 3; ++realm) {
            if (!GetReplicatedRealmState(*config, realm)) {
                continue;
            }
            hasTarget = true;
            ui32 disks = 0;
            for (ui32 index = 0; index < group->Info.VDisks.size(); ++index) {
                if (group->Info.VDisks[index].Id.FailRealm == realm) {
                    ++disks;
                    if (!group->Replicated[index]) {
                        return;
                    }
                }
            }
            if (!disks) {
                return;
            }
        }
        std::optional<TMirror3dcGroupConfiguration> next = hasTarget ? RecoveringComplete(*config) : std::nullopt;
        if (next && IsAllowedMirror3dcGroupTransition(config->State, next->State)) {
            using TDefinition = NKikimrBlobStorage::TDefineGroupState::TGroupDefinition;
            std::unique_ptr<TEvBlobStorage::TEvControllerConfigRequest> request =
                std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
            NKikimrBlobStorage::TConfigRequest* requestConfig = request->Record.MutableRequest();
            TDefinition* definition = requestConfig->AddCommand()->MutableDefineGroupState()->AddGroups();
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
                        if (GetReplicatedRealmState(*GetConfiguration(group.Info), vdisk.Id.FailRealm)) {
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

IActor* CreateGroupStateActor(const TActorId& controllerId) {
    return new TGroupStateActor(controllerId);
}

} // namespace NKikimr::NBsController
