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
            const auto it = PDiskLocationMap.find(TPDiskLocation(host.GetNodeId(), cmd.GetPath()));
            if (it != PDiskLocationMap.end() && !PDisksToRemove.count(it->second)) {
                pdiskId = it->second;
            } else {
                throw TExPDiskNotFound(host, 0, cmd.GetPath());
            }
        }

        TPDiskInfo *pdisk = PDisks.FindForUpdate(pdiskId);
        const bool wasGoodExpectedStatus = pdisk->HasGoodExpectedStatus();
        if (const auto s = cmd.GetStatus(); s != NKikimrBlobStorage::EDriveStatus::UNKNOWN && s != pdisk->Status) {
            pdisk->Status = s;
            pdisk->StatusTimestamp = Timestamp;
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
                item->SetSerial(pdiskInfo.ExpectedSerial);
                item->SetStatusChangeTimestamp(pdiskInfo.StatusTimestamp.GetValue());
            }
            return true;
        });
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TAddDriveSerial& cmd,
            TStatus& /*status*/) {

        const TString& newSerial = cmd.GetSerial();

        Schema::DriveSerial::BoxId::Type boxId = cmd.GetBoxId();
        const TDriveSerialInfo *driveInfo = DrivesSerials.Find(newSerial);

        if (driveInfo && driveInfo->LifeStage != NKikimrBlobStorage::TDriveLifeStage::REMOVED) {
            throw TExAlready() << "Device with such serial already exists in BSC database and not in lifeStage REMOVED";
        }

        if (auto it = NodeForSerial.find(newSerial); it != NodeForSerial.end()) {
            // Serial of drive is known, but drive not present in DrivesSerial
            // Check is it defined in HostConfigs
            TNodeId nodeId = it->second;
            const TNodeInfo& nodeInfo = Nodes.Get().at(nodeId);
            TString path = nodeInfo.KnownDrives.at(newSerial).Path;

            TPDiskId from = TPDiskId::MinForNode(nodeId);
            TPDiskId to = TPDiskId::MaxForNode(nodeId);
            std::optional<TPDiskId> updatePDiskId;
            PDisks.ForEachInRange(from, to, [&](const TPDiskId& pdiskId, const TPDiskInfo& pdiskInfo) {
                if (pdiskInfo.Path == path) {
                    updatePDiskId = pdiskId;
                    return false;
                }
                return true;
            });
            if (updatePDiskId) {
                // PDisk is defined through HostConfigs, but there may be fictional row in DrivesSerials
                // if row is present - delete it
                if (driveInfo) {
                    DrivesSerials.DeleteExistingEntry(newSerial);
                    driveInfo = nullptr;
                }
                TPDiskInfo *pdiskInfo = PDisks.FindForUpdate(*updatePDiskId);
                if (pdiskInfo->ExpectedSerial == newSerial) {
                    throw TExAlready() << "Device with such serial already exists in BSC database and is defined through "
                        << "HostConfigs";
                }
                pdiskInfo->ExpectedSerial = newSerial;
                if (pdiskInfo->BoxId != boxId) {
                    throw TExError() << "Drive is defind in host configs, but placed in another box# " << pdiskInfo->BoxId;
                }
                STLOG(PRI_INFO, BS_CONTROLLER_AUDIT, BSCA06, "Set new ExpectedSerial for HostConfigs drive",
                    (UniqueId, UniqueId), (Serial, newSerial), (BoxId, boxId), (PDiskId, *updatePDiskId), (Path, path));
                return;
            }
        }

        {
            // Additional check, may give false negative if ExpectedSerial for pdisk is unknown
            TMaybe<TPDiskId> from;
            TMaybe<TPDiskId> to;
            if (auto it = NodeForSerial.find(newSerial); it != NodeForSerial.end()) {
                from = TPDiskId::MinForNode(it->second);
                to = TPDiskId::MaxForNode(it->second);
            }

            std::optional<TPDiskId> existingPDisk;
            PDisks.ForEachInRange(from, to, [&](const TPDiskId& pdiskId, const TPDiskInfo& pdiskInfo) {
                if (newSerial == pdiskInfo.ExpectedSerial) {
                    existingPDisk = pdiskId;
                    return false;
                }
                return true;
            });
            if (existingPDisk) {
                throw TExAlready() << "Device with such serial already exists in BSC database and is defined in HostConfigs"
                    << " pdiskId# " << *existingPDisk;
            }
        }

        // delete existing entry, if any, but keep its GUID
        std::optional<TMaybe<Schema::DriveSerial::Guid::Type>> guid = driveInfo ? std::make_optional(driveInfo->Guid) : std::nullopt;
        if (driveInfo) {
            DrivesSerials.DeleteExistingEntry(newSerial);
        }

        TDriveSerialInfo *driveInfoNew = DrivesSerials.ConstructInplaceNewEntry(newSerial, boxId);
        if (guid) {
            driveInfoNew->Guid = *guid;
        }

        driveInfoNew->Kind = cmd.GetKind();
        driveInfoNew->PDiskType = cmd.GetPDiskType();
        TString config;
        const bool success = cmd.GetPDiskConfig().SerializeToString(&config);
        Y_VERIFY(success);
        driveInfoNew->PDiskConfig = config;

        STLOG(PRI_INFO, BS_CONTROLLER_AUDIT, BSCA00, "AddDriveSerial", (UniqueId, UniqueId), (Serial, newSerial),
            (BoxId, boxId));
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TRemoveDriveSerial& cmd,
            TStatus& /*status*/) {

        const TString& serial = cmd.GetSerial();

        if (const TDriveSerialInfo *driveInfo = DrivesSerials.Find(serial); !driveInfo) {
            // Drive is defined in HostConfigs
            //

            // Fast search (works only for online nodes)
            std::optional<TNodeId> nodeId;
            if (auto it = NodeForSerial.find(serial); it != NodeForSerial.end()) {
                nodeId = it->second;
            } else {
                // Slow PDisks fullscan
                PDisks.ForEach([&](const TPDiskId& pdiskId, const TPDiskInfo& pdiskInfo) {
                    if (pdiskInfo.ExpectedSerial == serial) {
                        nodeId = pdiskId.NodeId;
                        return false;
                    }
                    return true;
                });
            }
            if (!nodeId) {
                throw TExError() << "Device with such serial is unknown for BSC";
            }

            TPDiskId from = TPDiskId::MinForNode(*nodeId);
            TPDiskId to = TPDiskId::MaxForNode(*nodeId);
            std::optional<TPDiskId> removePDiskId;
            PDisks.ForEachInRange(from, to, [&](const TPDiskId& pdiskId, const TPDiskInfo& pdiskInfo) {
                if (pdiskInfo.ExpectedSerial == serial) {
                    if (pdiskInfo.NumActiveSlots) {
                        throw TExError() << "There are active vdisks on that drive";
                    }
                    if (removePDiskId) {
                        throw TExError() << "has two pdisks defined in HostConfigs with same serial number";
                    }
                    removePDiskId = pdiskId;
                }
                return true;
            });
            if (!removePDiskId) {
                throw TExError() << "The serial was seen in cluster on node# " << *nodeId
                    << " but now there are no pdisks with the serial";
            }
            auto* pdiskUpdate = PDisks.FindForUpdate(*removePDiskId);
            pdiskUpdate->ExpectedSerial = {};
            STLOG(PRI_INFO, BS_CONTROLLER_AUDIT, BSCA08, "Reset ExpectedSerial for HostConfig drive",
                (UniqueId, UniqueId), (Serial, serial), (PDiskId, *removePDiskId));

            // create fictional row in DrivesSerials to be able to reply kAlready for already removed disk
            // even if they are defined through HostConfig
            TDriveSerialInfo *driveInfoNew = DrivesSerials.ConstructInplaceNewEntry(serial, pdiskUpdate->BoxId);
            driveInfoNew->Guid = pdiskUpdate->Guid;
            driveInfoNew->Kind = pdiskUpdate->Kind.Kind();
            driveInfoNew->PDiskType = PDiskTypeToPDiskType(pdiskUpdate->Kind.Type());
            driveInfoNew->PDiskConfig = pdiskUpdate->PDiskConfig;
            driveInfoNew->LifeStage = NKikimrBlobStorage::TDriveLifeStage::REMOVED;
        } else {
            if (driveInfo->LifeStage == NKikimrBlobStorage::TDriveLifeStage::REMOVED) {
                throw TExAlready() << "Drive is already removed";
            }

            if (driveInfo->NodeId && driveInfo->PDiskId) {
                TPDiskId pdiskId(*driveInfo->NodeId, *driveInfo->PDiskId);
                if (auto* pdiskInfo = PDisks.Find(pdiskId)) {
                    if (pdiskInfo->NumActiveSlots) {
                        throw TExError() << "There are active vdisks on that drive";
                    } else {
                        // PDisk will be deleted automatically in FitPDisks
                    }
                }
            }

            TDriveSerialInfo *driveInfoMutable = DrivesSerials.FindForUpdate(serial);
            driveInfoMutable->NodeId.Clear();
            driveInfoMutable->PDiskId.Clear();
            driveInfoMutable->LifeStage = NKikimrBlobStorage::TDriveLifeStage::REMOVED;

            STLOG(PRI_INFO, BS_CONTROLLER_AUDIT, BSCA07, "RemoveDriveSerial", (UniqueId, UniqueId), (Serial, serial));
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TForgetDriveSerial& cmd,
            TStatus& /*status*/) {

        const TString& serial = cmd.GetSerial();

        if (const TDriveSerialInfo *driveInfo = DrivesSerials.Find(serial)) {
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
                TString expected = pdiskInfo.ExpectedSerial;
                if (pdiskInfo.Path && (!expected || expected != pdiskInfo.LastSeenSerial)) {
                    throw TExError() << "pdisk has not ExpectedSerial or ExpectedSerial not equals to LastSeenSerial"
                        << " pdiskId# " << pdiskId << " expected# " << expected.Quote()
                        << " lastSeen# " << pdiskInfo.LastSeenSerial;
                }
            });
            break;
        default:
            throw TExError() << "serial management stage is unsupported";
        }

        SerialManagementStage.Unshare() = newStage;
    }
} // NKikimr::NBsController
