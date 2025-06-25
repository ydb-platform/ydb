#pragma once

#include "defs.h"

#include "vdisk_mock.h"
#include "events.h"

namespace NKikimr {
namespace NPDisk {
extern const ui64 YdbDefaultPDiskSequence = 0x7e5700007e570000;
}
}

enum class EState {
    INITIAL,
    CONNECTED,
};

struct TEvCheckState : TEventLocal<TEvCheckState, EvCheckState> {
    const EState State;

    TEvCheckState(EState state)
        : State(state)
    {}
};

struct TEvDone : TEventLocal<TEvDone, EvDone> {};
struct TEvUpdateDriveStatus : TEventLocal<TEvUpdateDriveStatus, EvUpdateDriveStatus> {};

class TNodeWardenMock : public TActorBootstrapped<TNodeWardenMock> {
    const ui32 NodeId;
    const ui64 TabletId;
    TActorId PipeClient;
    bool Connected = false;
    EState CurrentState = EState::INITIAL;
    std::multimap<EState, TActorId> Queue;
    std::map<ui32, ui32> Groups;
    std::map<std::tuple<ui32, ui32>, std::unique_ptr<TVDisk>> VDisks;

public:
    TNodeWardenMock(ui32 nodeId, ui64 tabletId)
        : NodeId(nodeId)
        , TabletId(tabletId)
    {}

    void Bootstrap() {
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_NODE, "[%u] Bootstrap", NodeId);
        Connect();
        Become(&TThis::StateFunc);
    }

    void Connect() {
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_NODE, "[%u] Connect", NodeId);
        UNIT_ASSERT(!PipeClient);
        PipeClient = Register(NTabletPipe::CreateClient(SelfId(), TabletId, {}));
    }

    void Handle(TEvCheckState::TPtr ev) {
        auto& msg = *ev->Get();
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_NODE, "[%u] CheckState from %s expected %u current %u",
            NodeId, ev->Sender.ToString().data(), msg.State, CurrentState);
        if (CurrentState == msg.State) {
            LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_NODE, "[%u] sending Done to %s", NodeId, ev->Sender.ToString().data());
            Send(ev->Sender, new TEvDone);
        } else {
            Queue.emplace(msg.State, ev->Sender);
        }
    }

    void SwitchToState(EState state) {
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_NODE, "[%u] State switched from %u to %u", NodeId,
            CurrentState, state);
        CurrentState = state;
        auto r = Queue.equal_range(CurrentState);
        for (auto it = r.first; it != r.second; ++it) {
            LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_NODE, "[%u] sending Done to %s", NodeId, it->second.ToString().data());
            Send(it->second, new TEvDone);
        }
        Queue.erase(r.first, r.second);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
        auto& msg = *ev->Get();
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_NODE, "[%u] ClientConnected Sender# %s Status# %s"
            " ClientId# %s ServerId# %s PipeClient# %s", NodeId, ev->Sender.ToString().data(),
            NKikimrProto::EReplyStatus_Name(msg.Status).data(), msg.ClientId.ToString().data(),
            msg.ServerId.ToString().data(), PipeClient.ToString().data());
        if (ev->Sender == PipeClient) {
            if (msg.Status != NKikimrProto::OK) {
                NTabletPipe::CloseAndForgetClient(SelfId(), PipeClient);
                Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
            } else {
                UNIT_ASSERT(!Connected);
                Connected = true;
                SwitchToState(EState::CONNECTED);
                OnConnected();
            }
        }
    }

    void OnConnected() {
        TVector<ui32> startedDynamicGroups, groupGenerations;
        for (const auto& [groupId, gen] : Groups) {
            startedDynamicGroups.push_back(groupId);
            groupGenerations.push_back(gen);
        }

        auto ev = std::make_unique<TEvBlobStorage::TEvControllerRegisterNode>(NodeId, startedDynamicGroups,
            groupGenerations, TVector<NPDisk::TDriveData>{});
        auto& record = ev->Record;
        for (const auto& [id, vdisk] : VDisks) {
            vdisk->Serialize(record.AddVDiskStatus());
        }
        NTabletPipe::SendData(SelfId(), PipeClient, ev.release());
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
        auto& msg = *ev->Get();
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_NODE, "[%u] ClientDestroyed Sender# %s"
            " ClientId# %s ServerId# %s PipeClient# %s", NodeId, ev->Sender.ToString().data(), msg.ClientId.ToString().data(), msg.ServerId.ToString().data(), PipeClient.ToString().data());
        if (ev->Sender == PipeClient) {
            PipeClient = {};
            Connected = false;
            SwitchToState(EState::INITIAL);
            Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
        }
    }

    void Handle(TEvBlobStorage::TEvControllerNodeServiceSetUpdate::TPtr ev) {
        auto& msg = *ev->Get();
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_NODE, "[%u] NodeServiceSetUpdate", NodeId);

        const auto& ss = msg.Record.GetServiceSet();

        for (const auto& group : ss.GetGroups()) {
            if (group.GetEntityStatus() != NKikimrBlobStorage::DESTROY) {
                Groups.emplace(group.GetGroupID(), group.GetGroupGeneration());
            } else {
                Groups.erase(group.GetGroupID());
            }
        }

        for (const auto& vdisk : ss.GetVDisks()) {
            const auto& location = vdisk.GetVDiskLocation();
            UNIT_ASSERT_VALUES_EQUAL(location.GetNodeID(), NodeId);
            const auto id = std::make_tuple(location.GetPDiskID(), location.GetVDiskSlotID());
            if (vdisk.GetEntityStatus() != NKikimrBlobStorage::DESTROY) {
                const TVDiskID vdiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
                if (const auto it = VDisks.lower_bound(id); it != VDisks.end() && it->first == id) {
                    it->second->UpdateVDiskId(vdiskId);
                } else {
                    VDisks.emplace_hint(it, id, std::make_unique<TVDisk>(vdiskId, location.GetNodeID(),
                        location.GetPDiskID(), location.GetVDiskSlotID(), location.GetPDiskGuid()));
                }
            } else if (const auto it = VDisks.find(id); it != VDisks.end()) {
                it->second->StopActor();
                VDisks.erase(it);
            } else {
                UNIT_ASSERT(false);
            }
        }

        UpdateDriveStatus();
    }

    void UpdateDriveStatus() {
        if (Connected) {
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerUpdateDiskStatus>();
            TInstant nextStatusChange = TInstant::Max();
            const TInstant now = TActivationContext::Now();
            for (auto& [id, vdisk] : VDisks) {
                nextStatusChange = Min(nextStatusChange, vdisk->HandleStatusChange(now));
                vdisk->Report(&ev->Record);
            }
            NTabletPipe::SendData(SelfId(), PipeClient, ev.release());
            if (nextStatusChange != TInstant::Max()) {
                Schedule(nextStatusChange - now, new TEvUpdateDriveStatus);
            }
        }
    }

    void Handle(TEvNodeWardenQueryStorageConfig::TPtr ev) {
        Send(ev->Sender, new TEvNodeWardenStorageConfig(std::make_shared<NKikimrBlobStorage::TStorageConfig>(),
            nullptr, false, nullptr));
    }

    STRICT_STFUNC(StateFunc, {
        hFunc(TEvCheckState, Handle);
        hFunc(TEvTabletPipe::TEvClientConnected, Handle);
        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        hFunc(TEvBlobStorage::TEvControllerNodeServiceSetUpdate, Handle);
        cFunc(EvUpdateDriveStatus, UpdateDriveStatus);
        cFunc(TEvents::TSystem::Wakeup, Connect);
        hFunc(TEvNodeWardenQueryStorageConfig, Handle);
        IgnoreFunc(NStorage::TEvNodeWardenUpdateCache);
        IgnoreFunc(NStorage::TEvNodeWardenQueryCache);
        IgnoreFunc(NStorage::TEvNodeWardenUnsubscribeFromCache);
    })
};
