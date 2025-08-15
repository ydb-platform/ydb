#include "node_warden_impl.h"
#include "node_warden.h"
#include <util/system/fs.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_helpers.h>

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
    SendUnfinishedRequests();
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

    for (const auto& [key, pdisk] : LocalPDisks) {
        if (pdisk.ShredGenerationIssued) {
            auto *item = ev->Record.AddShredStatus();
            item->SetPDiskId(key.PDiskId);
            if (pdisk.Record.HasPDiskGuid()) {
                item->SetPDiskGuid(pdisk.Record.GetPDiskGuid());
            }
            std::visit(TOverloaded{
                [item](const std::monostate&) { item->SetShredInProgress(true); },
                [item](const ui64& generation) { item->SetShredGenerationFinished(generation); },
                [item](const TString& aborted) { item->SetShredAborted(aborted); }
            }, pdisk.ShredState);
        }
    }

    if (!Cfg->ConfigDirPath.empty() && YamlConfig) {
        ev->Record.SetMainConfigVersion(YamlConfig->GetMainConfigVersion());
        ev->Record.SetMainConfigHash(NYaml::GetConfigHash(YamlConfig->GetMainConfig()));
        if (YamlConfig->HasStorageConfigVersion()) {
            ev->Record.SetStorageConfigVersion(YamlConfig->GetStorageConfigVersion());
            ev->Record.SetStorageConfigHash(NYaml::GetConfigHash(YamlConfig->GetStorageConfig()));
        }
    }

    // report working syncers to the controller
    FillInWorkingSyncers(ev->Record.MutableSyncerState());

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
