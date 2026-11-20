#include "node_warden_impl.h"
#include "node_warden.h"
#include <util/system/fs.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_helpers.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

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
    RegisteredAtController = false;
    ++ControllerServiceSetGeneration;
    ControllerServicesDropped = false; // this registration brings full state

    YDB_LOG_DEBUG("EstablishPipe",
        {"marker", "NW21"},
        {"availDomainId", AvailDomainId},
        {"pipeClientId", PipeClientId},
        {"controllerId", controllerId});

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
        YDB_LOG_ERROR("TEvTabletPipe::TEvClientConnected",
            {"marker", "NW71"},
            {"status", msg->Status},
            {"clientId", msg->ClientId},
            {"serverId", msg->ServerId},
            {"tabletId", msg->TabletId},
            {"pipeClientId", PipeClientId});
        OnPipeError();
    } else {
        YDB_LOG_DEBUG("TEvTabletPipe::TEvClientConnected OK",
            {"marker", "NW05"},
            {"clientId", msg->ClientId},
            {"serverId", msg->ServerId},
            {"tabletId", msg->TabletId},
            {"pipeClientId", PipeClientId});
    }
}

void TNodeWarden::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
    TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();
    YDB_LOG_ERROR("Handle(TEvTabletPipe::TEvClientDestroyed)",
        {"marker", "NW42"},
        {"clientId", msg->ClientId},
        {"serverId", msg->ServerId},
        {"tabletId", msg->TabletId},
        {"pipeClientId", PipeClientId});
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
    YDB_LOG_DEBUG("SendRegisterNode",
        {"marker", "NW20"});

    TVector<ui32> subscribedGroups, generations;
    for (const auto& [groupId, group] : Groups) {
        const auto type = TGroupID(groupId).ConfigurationType();
        // A virtual bridge member has no local VSlots from which BSC could recover its subscription.
        // An outstanding lookup may also have lost its reply when the pipe disconnected.
        const bool virtualMember = type == EGroupConfigurationType::Virtual && (!group.Info || group.Info->GetBridgePileId());
        if ((group.ProxyId || group.MustSubscribe) && (type == EGroupConfigurationType::Dynamic || virtualMember) &&
                (!group.Info || group.Info->DecommitStatus != NKikimrBlobStorage::TGroupDecommitStatus::DONE)) {
            subscribedGroups.push_back(groupId);
            generations.push_back(group.Info ? group.Info->GroupGeneration : 0);
        }
    }

    WorkingLocalDrives = ListLocalDrives();

    auto ev = std::make_unique<TEvBlobStorage::TEvControllerRegisterNode>(LocalNodeId, subscribedGroups, generations,
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
    for (const TWorkingSyncer& syncer : WorkingSyncers) {
        FillInWorkingSyncer(ev->Record.MutableSyncerState(), const_cast<TWorkingSyncer&>(syncer), true);
    }

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
        YDB_LOG_DEBUG("SendInitialGroupRequests",
            {"marker", "NW22"},
            {"groupIds", FormatList(groupIds)});
        SendToController(std::make_unique<TEvBlobStorage::TEvControllerGetGroup>(LocalNodeId,
            groupIds.begin(), groupIds.end()));
    }
}
