#include "config.h"

namespace NKikimr::NBsController {

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TUpdateDriveStatus& cmd, TStatus& /*status*/) {
        const auto& host = NormalizeHostKey(cmd.GetHostKey());

        TPDiskId pdiskId;
        if (cmd.GetPDiskId()) {
            if (cmd.GetPath()) {
                throw TExError() << "TUpdateDriveStatus.Path and PDiskId are mutually exclusive";
            }
            pdiskId = TPDiskId(host.GetNodeId(), cmd.GetPDiskId());
            if (!PDisks.Find(pdiskId) || PDisksToRemove.count(pdiskId)) {
                throw TExPDiskNotFound(host, cmd.GetPDiskId(), TString());
            }
        } else {
            const std::optional<TPDiskId> found = FindPDiskByLocation(host.GetNodeId(), cmd.GetPath());
            if (found && !PDisksToRemove.count(*found)) {
                pdiskId = *found;
            } else {
                throw TExPDiskNotFound(host, 0, cmd.GetPath());
            }
        }

        TPDiskInfo *pdisk = PDisks.FindForUpdate(pdiskId);
        bool fitGroups = false;
        const bool wasGoodExpectedStatus = pdisk->HasGoodExpectedStatus();
        if (const auto s = cmd.GetStatus(); s != NKikimrBlobStorage::EDriveStatus::UNKNOWN && s != pdisk->Status) {
            pdisk->Status = s;
            pdisk->StatusTimestamp = Timestamp;
            fitGroups = s == NKikimrBlobStorage::EDriveStatus::BROKEN;
        }
        if (const auto ds = cmd.GetDecommitStatus(); ds != NKikimrBlobStorage::EDecommitStatus::DECOMMIT_UNSET &&
                ds != pdisk->DecommitStatus) {
            pdisk->DecommitStatus = ds;
        }
        if (wasGoodExpectedStatus != pdisk->HasGoodExpectedStatus()) {
            for (const auto& [id, slot] : pdisk->VSlotsOnPDisk) {
                if (slot->Group) {
                    TGroupInfo *group = Groups.FindForUpdate(slot->Group->ID);
                    group->CalculateGroupStatus();
                }
            }
        }

        if (fitGroups) {
            for (const auto& [id, slot] : pdisk->VSlotsOnPDisk) {
                if (slot->Group) {
                    Fit.PoolsAndGroups.emplace(slot->Group->StoragePoolId, slot->Group->ID);
                }
            }
        }

        STLOG(PRI_INFO, BS_CONTROLLER_AUDIT, BSCA01, "UpdateDriveStatus",
            (UniqueId, UniqueId),
            (FQDN, host.GetFqdn()),
            (IcPort, host.GetIcPort()),
            (NodeId, host.GetNodeId()),
            (Path, cmd.GetPath()),
            (Status, cmd.GetStatus()),
            (DecommitStatus, cmd.GetDecommitStatus()));
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TReadDriveStatus& cmd, TStatus& status) {
        const TString& path = cmd.GetPath();

        TPDiskId from = Min<TPDiskId>();
        TPDiskId to = Max<TPDiskId>();

        if (cmd.HasHostKey()) {
            const auto& host = NormalizeHostKey(cmd.GetHostKey());
            const TNodeId& nodeId = host.GetNodeId();
            from = TPDiskId::MinForNode(nodeId);
            to = TPDiskId::MaxForNode(nodeId);
        }

        PDisks.ForEachInRange(from, to, [&](const TPDiskId& pdiskId, const TPDiskInfo& pdiskInfo) {
            if (!path || path == pdiskInfo.Path) {
                NKikimrBlobStorage::TUpdateDriveStatus *item = status.AddDriveStatus();
                NKikimrBlobStorage::THostKey *host = item->MutableHostKey();
                host->SetFqdn(std::get<0>(pdiskInfo.HostId));
                host->SetIcPort(std::get<1>(pdiskInfo.HostId));
                host->SetNodeId(pdiskId.NodeId);
                item->SetPath(pdiskInfo.Path);
                item->SetStatus(pdiskInfo.Status);
                item->SetPDiskId(pdiskId.PDiskId);
                //item->SetSerial(pdiskInfo.ExpectedSerial);
                item->SetStatusChangeTimestamp(pdiskInfo.StatusTimestamp.GetValue());
            }
            return true;
        });
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TAddDriveSerial& cmd,
            TStatus& /*status*/) {

        const auto& serial = cmd.GetSerial();
        auto boxId = cmd.GetBoxId();

        auto driveInfo = DrivesSerials.Find(serial);
        if (driveInfo && driveInfo->LifeStage != NKikimrBlobStorage::TDriveLifeStage::REMOVED) {
            throw TExAlready() << "Device with such serial already exists in BSC database in lifeStage " << driveInfo->LifeStage;
        }

        auto it = NodeIdByDiskSerialNumber.find(serial);
        if (it == NodeIdByDiskSerialNumber.end()) {
            throw TExError() << "Couldn't find node id for disk with serial number" << TErrorParams::DiskSerialNumber(serial);
        }
        auto nodeId = it->second;

        const auto& nodes = Nodes.Get();
        auto nodeIt = nodes.find(nodeId);
        if (nodeIt == nodes.end()) {
            throw TExError() << "Couldn't find node by node id" << TErrorParams::NodeId(nodeId) << " for disk with serial number" << TErrorParams::DiskSerialNumber(serial);
        }

        const auto& nodeInfo = nodeIt->second;
        auto driveIt = nodeInfo.KnownDrives.find(serial);
        if (driveIt == nodeInfo.KnownDrives.end()) {
            throw TExError() << "Couldn't find disk on node" << TErrorParams::NodeId(nodeId) << " by serial number" << TErrorParams::DiskSerialNumber(serial);
        }

        // delete REMOVED entry, if any, but keep its GUID
        auto guid = driveInfo ? std::make_optional(driveInfo->Guid) : std::nullopt;
        if (driveInfo) {
            DrivesSerials.DeleteExistingEntry(serial);
        }

        auto driveInfoMutable = DrivesSerials.ConstructInplaceNewEntry(serial, boxId);
        if (guid) {
            driveInfoMutable->Guid = *guid;
        }
        driveInfoMutable->Kind = cmd.GetKind();
        if (cmd.GetPDiskType() != NKikimrBlobStorage::UNKNOWN_TYPE) {
            driveInfoMutable->PDiskType = cmd.GetPDiskType();
        } else {
            driveInfoMutable->PDiskType = PDiskTypeToPDiskType(driveIt->second.DeviceType);
        }
        TString config;
        if (!cmd.GetPDiskConfig().SerializeToString(&config)) {
            throw TExError() << "Couldn't serialize PDiskConfig for disk with serial number" << TErrorParams::DiskSerialNumber(serial);
        }
        driveInfoMutable->PDiskConfig = config;
        driveInfoMutable->LifeStage = NKikimrBlobStorage::TDriveLifeStage::ADDED;
        driveInfoMutable->NodeId = nodeId;
        driveInfoMutable->Path = driveIt->second.Path;

        Fit.Boxes.insert(boxId);

        STLOG(PRI_INFO, BS_CONTROLLER_AUDIT, BSCA00, "AddDriveSerial", (UniqueId, UniqueId), (Serial, serial),
            (BoxId, boxId));
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TRemoveDriveSerial& cmd,
            TStatus& /*status*/) {

        const auto& serial = cmd.GetSerial();

        auto driveInfo = DrivesSerials.Find(serial);
        if (!driveInfo) {
            throw TExError() << "Couldn't find disk with serial number" << TErrorParams::DiskSerialNumber(serial);
        }

        if (driveInfo->LifeStage == NKikimrBlobStorage::TDriveLifeStage::REMOVED) {
            throw TExError() << "Disk with serial number" << TErrorParams::DiskSerialNumber(serial) << " has already been removed";
        }

        auto driveInfoMutable = DrivesSerials.FindForUpdate(serial);
        driveInfoMutable->NodeId.Clear();
        driveInfoMutable->PDiskId.Clear();
        driveInfoMutable->LifeStage = NKikimrBlobStorage::TDriveLifeStage::REMOVED;
        driveInfoMutable->Path.Clear();

        Fit.Boxes.insert(driveInfo->BoxId);

        STLOG(PRI_INFO, BS_CONTROLLER_AUDIT, BSCA07, "RemoveDriveSerial", (UniqueId, UniqueId), (Serial, serial));
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TForgetDriveSerial& cmd,
            TStatus& /*status*/) {

        const auto& serial = cmd.GetSerial();

        if (auto driveInfo = DrivesSerials.Find(serial)) {
            switch (driveInfo->LifeStage) {
                case NKikimrBlobStorage::TDriveLifeStage::NOT_SEEN:
                    [[fallthrough]];
                case NKikimrBlobStorage::TDriveLifeStage::REMOVED:
                    DrivesSerials.DeleteExistingEntry(serial);
                    break;
                default: {
                    throw TExError() << "Drive not in {NOT_SEEN, REMOVED} lifestage and cannot be forgotten. Remove it first";
                    break;
                }
            }
        } else {
            throw TExAlready() << "Drive is unknown for BS_CONTROLLER and cannot be forgotten";
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TMigrateToSerial& cmd,
            TStatus& /*status*/) {

        const NKikimrBlobStorage::TSerialManagementStage::E newStage = cmd.GetStage();

        switch (newStage) {
        case NKikimrBlobStorage::TSerialManagementStage::DISCOVER_SERIAL:
            break;
        case NKikimrBlobStorage::TSerialManagementStage::CHECK_SERIAL:
            PDisks.ForEach([&](const TPDiskId& pdiskId, const TPDiskInfo& pdiskInfo) {
                if (pdiskInfo.ExpectedSerial && pdiskInfo.LastSeenSerial && pdiskInfo.ExpectedSerial != pdiskInfo.LastSeenSerial) {
                    throw TExError() << "LastSeenSerial doesn't match ExpectedSerial for pdisk"
                        << TErrorParams::NodeId(pdiskId.NodeId) << TErrorParams::PDiskId(pdiskId.PDiskId);
                }
            });
            break;
        case NKikimrBlobStorage::TSerialManagementStage::ONLY_SERIAL:
            PDisks.ForEach([&](const TPDiskId& pdiskId, const TPDiskInfo& pdiskInfo) {
                if (pdiskInfo.ExpectedSerial != pdiskInfo.LastSeenSerial) {
                    throw TExError() << "LastSeenSerial doesn't match ExpectedSerial for pdisk"
                        << TErrorParams::NodeId(pdiskId.NodeId) << TErrorParams::PDiskId(pdiskId.PDiskId);
                }
            });
            break;
        default:
            throw TExError() << "serial management stage is unsupported";
        }

        SerialManagementStage.Unshare() = newStage;
    }
} // NKikimr::NBsController
