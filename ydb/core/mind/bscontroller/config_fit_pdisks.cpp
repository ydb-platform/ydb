#include "config.h"

namespace NKikimr {
    namespace NBsController {

        TPDiskId FindFirstEmptyPDiskId(const TOverlayMap<TPDiskId, TBlobStorageController::TPDiskInfo>& pdisks,
                TNodeId nodeId) {
            Schema::PDisk::PDiskID::Type nextPDiskID = 1000; // start allocation from this number
            // generate PDisk id; skip generated one if it already exists (e.g. user has added
            // such PDisk by hand)
            TPDiskId pdiskId;
            do {
                pdiskId = TPDiskId(nodeId, nextPDiskID++); // postincrement this number
            } while (pdisks.Find(pdiskId));

            return pdiskId;
        }

        static TString FormatPDiskConfig(const TString& s) {
            NKikimrBlobStorage::TPDiskConfig proto;
            return proto.ParseFromString(s) ? SingleLineProto(proto) : "<error>";
        }

        Schema::PDisk::Guid::Type TBlobStorageController::CheckStaticPDisk(TConfigState &state, TPDiskId pdiskId,
                const TPDiskCategory& category, const TMaybe<Schema::PDisk::PDiskConfig::Type>& pdiskConfig,
                ui32 *staticSlotUsage) {
            const TStaticPDiskInfo& info = state.StaticPDisks.at(pdiskId);

            // create new disk entry; the PDisk with this number MUST NOT exist, otherwise we can
            // have a collision
            if (state.PDisks.Find(pdiskId)) {
                throw TExError() << "PDisk from static config collides with dynamic one"
                    << " PDiskId# " << pdiskId;
            }

            // validate fields
            if (pdiskConfig.GetOrElse(TString()) != info.PDiskConfig) {
                throw TExError() << "PDiskConfig field doesn't match static one"
                    << " pdiskConfig# " << (pdiskConfig ? FormatPDiskConfig(*pdiskConfig) : "(empty)")
                    << " info.PDiskConfig# " << FormatPDiskConfig(info.PDiskConfig);
            } else if (category != info.Category) {
                throw TExError() << "Type/Kind fields do not match static one";
            }

            *staticSlotUsage = info.StaticSlotUsage;
            return info.Guid;
        }

        void TBlobStorageController::AllocatePDiskWithSerial(TConfigState& state, ui32 nodeId, const TSerial& serial,
                TDriveSerialInfo *driveInfo) {
            TPDiskId pdiskId = FindFirstEmptyPDiskId(state.PDisks, nodeId);

            const TNodeInfo& nodeInfo = state.Nodes.Get().at(nodeId);
            const NPDisk::TDriveData& driveData = nodeInfo.KnownDrives.at(serial.Serial);
            TString fsPath = driveData.Path;

            NPDisk::EDeviceType type = PDiskTypeToPDiskType(driveInfo->PDiskType);
            if (type == NPDisk::DEVICE_TYPE_UNKNOWN) {
                type = driveData.DeviceType;
            }
            const TPDiskCategory category(type, driveInfo->Kind);

            if (const auto pdiskId = state.FindPDiskByLocation(nodeId, fsPath)) {
                throw TExError() << "PDisk found in PDisks by specific path, fsPath# " << fsPath.Quote()
                    << " pdiskId# " << *pdiskId;
            }

            ui32 staticSlotUsage = 0;
            auto staticPDiskId = state.FindStaticPDiskByLocation(nodeId, fsPath);
            if (!staticPDiskId) {
                staticPDiskId = state.FindStaticPDiskByLocation(nodeId, serial.Serial);
            }
            if (staticPDiskId) {
                // PDisk is static one, so take it's pdiskId and guid
                // and check that parameters match
                pdiskId = *staticPDiskId;
                driveInfo->Guid = CheckStaticPDisk(state, pdiskId, category, driveInfo->PDiskConfig, &staticSlotUsage);
            }
            // Update FK in DriveSerial table and check for guid
            driveInfo->NodeId = pdiskId.NodeId;
            driveInfo->PDiskId = pdiskId.PDiskId;
            driveInfo->LifeStage = NKikimrBlobStorage::TDriveLifeStage::ALLOCATED;

            // if guid is known, reuse it, else generate new
            if (!driveInfo->Guid) {
                driveInfo->Guid = RandomNumber<Schema::PDisk::Guid::Type>();
            }

            const auto hostId = state.HostRecords->GetHostId(nodeId);
            if (!hostId) {
                throw TExError() << "Unable to find hostId by nodeId# " << nodeId;
            }
            state.PDisks.ConstructInplaceNewEntry(pdiskId, *hostId, TString(), category.GetRaw(),
                *driveInfo->Guid, false, false, 1000, driveInfo->PDiskConfig.GetOrElse(TString()),
                driveInfo->BoxId, DefaultMaxSlots, NKikimrBlobStorage::EDriveStatus::ACTIVE,
                TInstant::Zero(), NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE, serial.Serial,
                TString(), fsPath, staticSlotUsage);

            STLOG(PRI_NOTICE, BS_CONTROLLER, BSCFP01, "Create new pdisk", (PDiskId, pdiskId), (Path, fsPath));
        }

        void TBlobStorageController::ValidatePDiskWithSerial(TConfigState& state, ui32 nodeId, const TSerial& serial,
                const TDriveSerialInfo& driveInfo, std::function<TDriveSerialInfo*()> getMutableItem) {
            // check existing pdisk
            if (!driveInfo.NodeId || !driveInfo.PDiskId) {
                throw TExError() << "Drive is in ALLOCATED stage but has "
                    << " NodeId# " << driveInfo.NodeId
                    << " PDiskId# " << driveInfo.PDiskId;
            }

            const TPDiskId pdiskId(*driveInfo.NodeId, *driveInfo.PDiskId);
            const TPDiskInfo *pdiskInfo = state.PDisks.Find(pdiskId);
            if (!pdiskInfo) {
                throw TExError() << "Unable to find pdisk# " << pdiskId << " from DriveSerial table";
            } else if (pdiskInfo->Path) {
                throw TExError() << "Going to replace existing pdisk with non-empty path# " << pdiskInfo->Path;
            } else if (pdiskInfo->ExpectedSerial != serial.Serial) {
                throw TExError() << "Going to replace existing pdisk with different serial number"
                    << " new sn# " << serial.Serial << " existing serial# " << pdiskInfo->ExpectedSerial;
            } else if (pdiskInfo->Guid != *driveInfo.Guid) {
                throw TExError() << "Going to replace existring pdisk with different guid"
                    << " guid from DrivesSerials# " << *driveInfo.Guid
                    << " guid from PDisks# " << pdiskInfo->Guid;
            }

            const TString& path = serial.Serial;
            if (!state.FindStaticPDiskByLocation(*driveInfo.NodeId, path) && !state.FindPDiskByLocation(*driveInfo.NodeId, path)) {
                throw TExError() << "Drive is in ALLOCATED state and PDisk is created,"
                    " but is not found neither in PDisks, nor in StaticPDisks by specific path";
            }

            if (nodeId && nodeId != *driveInfo.NodeId) {
                // Drive was moved from previous node to new
                TDriveSerialInfo *info = getMutableItem();
                info->LifeStage = NKikimrBlobStorage::TDriveLifeStage::ERROR;
            }

            state.PDisksToRemove.erase(pdiskId);
        }

        void TBlobStorageController::FitPDisksForUserConfig(TConfigState &state) {
            auto pdisksForBoxes = std::exchange(state.Fit.Boxes, {});
            if (pdisksForBoxes.empty()) {
                return;
            }

            // re-fill PDisksToRemove set with all PDisks, we will erase remaining ones from this set a bit later
            state.PDisksToRemove.clear();
            state.PDisks.ForEach([&](const TPDiskId& pdiskId, const TPDiskInfo& pdiskInfo) {
                if (pdisksForBoxes.contains(pdiskInfo.BoxId)) {
                    state.PDisksToRemove.insert(pdiskId);
                }
                return true;
            });

            // Create new pdisks from DriveSerial table

            // Iterate over initial DrivesSerials map since every call to Unshare will invalidate iterators
            state.DrivesSerials.ScanRange({}, {}, [&](const auto& serial, const auto& driveInfo, const auto& getMutableItem) {
                if (!pdisksForBoxes.contains(driveInfo.BoxId)) {
                    return true;
                }
                if (driveInfo.LifeStage == NKikimrBlobStorage::TDriveLifeStage::NOT_SEEN) {
                    // Try to find drive in currently online nodes and create new PDisk
                    if (auto nodeIt = NodeForSerial.find(serial.Serial); nodeIt != NodeForSerial.end()) {
                        AllocatePDiskWithSerial(state, nodeIt->second, serial, getMutableItem());
                    }
                } else if (driveInfo.LifeStage == NKikimrBlobStorage::TDriveLifeStage::ALLOCATED
                        || driveInfo.LifeStage == NKikimrBlobStorage::TDriveLifeStage::ERROR) {
                    const auto it = NodeForSerial.find(serial.Serial);
                    const ui32 nodeId = it != NodeForSerial.end() ? it->second : 0;
                    // TODO(alexvru): check where no entry in NodeForSerial is a valid case
                    ValidatePDiskWithSerial(state, nodeId, serial, driveInfo, getMutableItem);
                }
                return true;
            });

            const auto& hostConfigs = state.HostConfigs.Get();
            const auto& boxes = state.Boxes.Get();
            for (const TBoxId& boxId : pdisksForBoxes) {
                const auto boxIt = boxes.find(boxId);
                if (boxIt == boxes.end()) {
                    continue; // box was deleted
                }
                const TBoxInfo& box = boxIt->second;

                THashSet<TNodeId> usedNodes;
                for (const auto& [hostKey, hostValue] : box.Hosts) {
                    const THostConfigId &hostConfigId = hostValue.HostConfigId;
                    auto it = hostConfigs.find(hostConfigId);
                    if (it == hostConfigs.end()) {
                        throw TExHostConfigNotFound(hostConfigId);
                    }
                    const THostConfigInfo &hostConfig = it->second;

                    const THostId hostId(hostKey.Fqdn, hostKey.IcPort);
                    const auto& nodeId = state.HostRecords->ResolveNodeId(hostKey, hostValue);
                    if (!nodeId) {
                        throw TExHostNotFound(hostKey) << TErrorParams::BoxId(boxId) << TErrorParams::NodeId(*nodeId);
                    } else if (!usedNodes.insert(*nodeId).second) {
                        throw TExError() << "duplicate NodeId" << TErrorParams::BoxId(boxId) << TErrorParams::NodeId(*nodeId)
                            << TErrorParams::Fqdn(hostKey.Fqdn) << TErrorParams::IcPort(hostKey.IcPort);
                    }

                    for (const auto& [drive, driveInfo] : hostConfig.Drives) {
                        TPDiskId pdiskId;
                        const TPDiskCategory category(PDiskTypeToPDiskType(driveInfo.Type), driveInfo.Kind);

                        // check if we already have spawned some PDisk at this location
                        if (const auto found = state.FindPDiskByLocation(*nodeId, drive.Path)) {
                            // yes, we do; find it by id and update some characteristics (that we can update)
                            pdiskId = *found;
                            const TPDiskInfo *pdisk = state.PDisks.Find(pdiskId);
                            Y_VERIFY(pdisk);
                            // update PDisk configuration if needed
                            if (pdisk->Kind != category || pdisk->SharedWithOs != driveInfo.SharedWithOs ||
                                    pdisk->ReadCentric != driveInfo.ReadCentric || pdisk->BoxId != boxId ||
                                    pdisk->PDiskConfig != driveInfo.PDiskConfig.GetOrElse(TString())) {
                                TPDiskInfo *pdisk = state.PDisks.FindForUpdate(pdiskId);
                                pdisk->Kind = category;
                                pdisk->SharedWithOs = driveInfo.SharedWithOs;
                                pdisk->ReadCentric = driveInfo.ReadCentric;
                                pdisk->BoxId = boxId;
                                pdisk->PDiskConfig = driveInfo.PDiskConfig.GetOrElse(TString());
                                pdisk->ExtractConfig(DefaultMaxSlots);
                            }
                        } else {
                            Schema::PDisk::Guid::Type guid;

                            // no, this disk is not in map yet; see if it is mentioned in static configuration
                            ui32 staticSlotUsage = 0;
                            if (const auto found = state.FindStaticPDiskByLocation(*nodeId, drive.Path)) {
                                // yes, take some data from static configuration
                                pdiskId = *found;
                                guid = CheckStaticPDisk(state, pdiskId, category, driveInfo.PDiskConfig, &staticSlotUsage);
                            } else {
                                pdiskId = FindFirstEmptyPDiskId(state.PDisks, *nodeId);
                                guid = RandomNumber<Schema::PDisk::Guid::Type>();
                            }
                            TString path = drive.Path;
                            // try find current serial number for device
                            TString currentSerial;
                            if (auto nodeIt = state.Nodes.Get().find(*nodeId); nodeIt != state.Nodes.Get().end()) {
                                for (const auto& [serial, driveData] : nodeIt->second.KnownDrives) {
                                    if (driveData.Path == path) {
                                        currentSerial = serial;
                                        break;
                                    }
                                }
                            }

                            // emplace PDisk into set
                            state.PDisks.ConstructInplaceNewEntry(pdiskId, hostId, path, category.GetRaw(),
                                guid, driveInfo.SharedWithOs, driveInfo.ReadCentric, 1000,
                                driveInfo.PDiskConfig.GetOrElse(TString()), boxId, DefaultMaxSlots,
                                NKikimrBlobStorage::EDriveStatus::ACTIVE, TInstant::Zero(),
                                NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE,
                                currentSerial, currentSerial, TString(), staticSlotUsage);

                            // insert PDisk into location map
                            STLOG(PRI_NOTICE, BS_CONTROLLER, BSCFP02, "Create new pdisk", (PDiskId, pdiskId),
                                (Path, path));
                        }

                        state.PDisksToRemove.erase(pdiskId);
                    }
                }
            }
            for (const auto& pdiskId : state.PDisksToRemove) {
                STLOG(PRI_NOTICE, BS_CONTROLLER, BSCFP03, "PDisk to remove:", (PDiskId, pdiskId));
            }
            state.CheckConsistency();
        }

        void TBlobStorageController::FitPDisksForNode(TConfigState& state, ui32 nodeId, const std::vector<TSerial>& serials) {
            for (const auto& serial : serials) {
                if (const TDriveSerialInfo *driveInfo = state.DrivesSerials.Find(serial)) {
                    switch (driveInfo->LifeStage) {
                        case NKikimrBlobStorage::TDriveLifeStage::NOT_SEEN:
                            AllocatePDiskWithSerial(state, nodeId, serial, state.DrivesSerials.FindForUpdate(serial));
                            break;

                        case NKikimrBlobStorage::TDriveLifeStage::ALLOCATED:
                        case NKikimrBlobStorage::TDriveLifeStage::ERROR:
                            ValidatePDiskWithSerial(state, nodeId, serial, *driveInfo,
                                [&] { return state.DrivesSerials.FindForUpdate(serial); });
                            break;

                        default:
                            break;
                    }
                }
            }
        }

    } // NBsController
} // NKikimr
