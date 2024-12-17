#include "node_warden_impl.h"

using namespace NKikimr;
using namespace NStorage;

void TNodeWarden::SendToController(std::unique_ptr<IEventBase> ev, ui64 cookie, TActorId sender) {
    NTabletPipe::SendData(sender ? sender : SelfId(), PipeClientId, ev.release(), cookie);
}

void TNodeWarden::EstablishPipe() {
    const ui64 controllerId = MakeBSControllerID();

    PipeClientId = Register(NTabletPipe::CreateClient(SelfId(), controllerId, NTabletPipe::TClientRetryPolicy{
        .MaxRetryTime = TDuration::Seconds(5),
        .DoFirstRetryInstantly = false,
    }));

    STLOG(PRI_DEBUG, BS_NODE, NW21, "EstablishPipe", (AvailDomainId, AvailDomainId),
        (PipeClientId, PipeClientId), (ControllerId, controllerId));

    for (auto& [key, pdisk] : LocalPDisks) {
        if (pdisk.PDiskMetrics) {
            PDisksWithUnreportedMetrics.PushBack(&pdisk);
        }
    }
    for (auto& [key, vdisk] : LocalVDisks) {
        vdisk.ScrubCookieForController = 0; // invalidate all pending requests to BS_CONTROLLER
        if (vdisk.VDiskMetrics) {
            VDisksWithUnreportedMetrics.PushBack(&vdisk);
        }
    }

    SendRegisterNode();
    SendInitialGroupRequests();
    SendScrubRequests();
    SendDiskMetrics(true);
}

void TNodeWarden::Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();
    if (msg->Status != NKikimrProto::OK) {
        STLOG(PRI_ERROR, BS_NODE, NW71, "TEvTabletPipe::TEvClientConnected", (Status, msg->Status),
            (ClientId, msg->ClientId), (ServerId, msg->ServerId), (TabletId, msg->TabletId),
            (PipeClientId, PipeClientId));
        OnPipeError();
    } else {
        STLOG(PRI_DEBUG, BS_NODE, NW05, "TEvTabletPipe::TEvClientConnected OK", (ClientId, msg->ClientId),
            (ServerId, msg->ServerId), (TabletId, msg->TabletId), (PipeClientId, PipeClientId));
    }
}

void TNodeWarden::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
    TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();
    STLOG(PRI_ERROR, BS_NODE, NW42, "Handle(TEvTabletPipe::TEvClientDestroyed)", (ClientId, msg->ClientId),
        (ServerId, msg->ServerId), (TabletId, msg->TabletId), (PipeClientId, PipeClientId));
    OnPipeError();
}

void TNodeWarden::OnPipeError() {
    for (const auto& [cookie, callback] : ConfigInFlight) {
        callback(nullptr);
    }
    ConfigInFlight.clear();
    EstablishPipe();
}

void TNodeWarden::SendRegisterNode() {
    STLOG(PRI_DEBUG, BS_NODE, NW20, "SendRegisterNode");

    TVector<ui32> startedDynamicGroups, generations;
    for (const auto& [groupId, group] : Groups) {
        if (group.ProxyId && TGroupID(groupId).ConfigurationType() == EGroupConfigurationType::Dynamic &&
                (!group.Info || group.Info->DecommitStatus != NKikimrBlobStorage::TGroupDecommitStatus::DONE)) {
            startedDynamicGroups.push_back(groupId);
            generations.push_back(group.Info ? group.Info->GroupGeneration : 0);
        }
    }

    WorkingLocalDrives = ListLocalDrives();

    auto ev = std::make_unique<TEvBlobStorage::TEvControllerRegisterNode>(LocalNodeId, startedDynamicGroups, generations,
        WorkingLocalDrives);
    FillInVDiskStatus(ev->Record.MutableVDiskStatus(), true);
    ev->Record.SetDeclarativePDiskManagement(true);

    SendToController(std::move(ev));
}

void TNodeWarden::SendInitialGroupRequests() {
    TStackVec<ui32, 32> groupIds;
    for (const auto& [groupId, group] : Groups) {
        if (group.GetGroupRequestPending) {
            groupIds.push_back(groupId);
        }
    }
    if (!groupIds.empty()) {
        STLOG(PRI_DEBUG, BS_NODE, NW22, "SendInitialGroupRequests", (GroupIds, FormatList(groupIds)));
        SendToController(std::make_unique<TEvBlobStorage::TEvControllerGetGroup>(LocalNodeId,
            groupIds.begin(), groupIds.end()));
    }
}
