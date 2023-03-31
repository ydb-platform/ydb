#include "config.h"

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr {
    namespace NBsController {

        struct TDiskId {
            ui32 NodeId = 0;
            TString Path;

            bool operator==(const TDiskId& other) const {
                return NodeId == other.NodeId && Path == other.Path;
            }
        };

        struct TDiskInfo {
            ui32 NodeId = 0;
            TBlobStorageController::THostId HostId = {};
            TBoxId BoxId = 0;
            TString Path;
            TString LastSeenPath;
            TString Serial;
            TString LastSeenSerial;
            bool SharedWithOs = false;
            bool ReadCentric = false;
            TPDiskCategory PDiskCategory = {};
            TString PDiskConfig;

            TDiskId GetId() const {
                return {NodeId, Path};
            }
        };

    } // NBsController
} // NKikimr

namespace std {
    template <>
    struct hash<NKikimr::NBsController::TDiskId> {
        size_t operator()(const NKikimr::NBsController::TDiskId& diskId) const {
            return hash<ui32>()(diskId.NodeId) ^ hash<TString>()(diskId.Path);
        }
    };
}

namespace NKikimr {
    namespace NBsController {

        static TPDiskId FindFirstEmptyPDiskId(const TOverlayMap<TPDiskId, TBlobStorageController::TPDiskInfo>& pdisks,
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

        static std::optional<TPDiskId> FindPDisk(const TDiskInfo& disk, const TBlobStorageController::TConfigState& state) {
            auto id = state.FindPDiskByLocation(disk.NodeId, disk.Path);
            if (!id) {
                id = state.FindPDiskByLocation(disk.NodeId, disk.Serial);
            }
            return id;
        }

        static std::optional<TPDiskId> FindStaticPDisk(const TDiskInfo& disk, const TBlobStorageController::TConfigState& state) {
            auto id = state.FindStaticPDiskByLocation(disk.NodeId, disk.Path);
            if (!id) {
                id = state.FindStaticPDiskByLocation(disk.NodeId, disk.Serial);
            }
            return id;
        }

        static void UpdatePDiskIfNeeded(const TPDiskId& pdiskId, const TDiskInfo& disk, ui32 defaultMaxSlots, TBlobStorageController::TConfigState& state) {
            auto pdiskInfo = state.PDisks.Find(pdiskId);
            Y_VERIFY(pdiskInfo != nullptr);
            if (pdiskInfo->Kind != disk.PDiskCategory ||
                pdiskInfo->SharedWithOs != disk.SharedWithOs ||
                pdiskInfo->ReadCentric != disk.ReadCentric ||
                pdiskInfo->BoxId != disk.BoxId ||
                pdiskInfo->PDiskConfig != disk.PDiskConfig)
            {
                // update PDisk configuration
                auto pdiskInfo = state.PDisks.FindForUpdate(pdiskId);
                Y_VERIFY(pdiskInfo != nullptr);
                pdiskInfo->Kind = disk.PDiskCategory;
                pdiskInfo->SharedWithOs = disk.SharedWithOs;
                pdiskInfo->ReadCentric = disk.ReadCentric;
                pdiskInfo->BoxId = disk.BoxId;
                if (pdiskInfo->PDiskConfig != disk.PDiskConfig) {
                    // update PDiskConfig only for nonstatic PDisks
                    if (!NKikimr::NBsController::FindStaticPDisk(disk, state)) {
                        pdiskInfo->PDiskConfig = disk.PDiskConfig;
                    } else {
                        throw TExError() << "Skipping PDiskConfig update for static disk" << TErrorParams::NodeId(disk.NodeId) << TErrorParams::Path(disk.Path);
                    }
                }
                // run ExtractConfig as the very last step
                pdiskInfo->ExtractConfig(defaultMaxSlots);
            }
        }

        // return TString not const TString& to make sure we never use dangling reference
        TString GetDiskPathFromNode(ui32 nodeId, const TString& serialNumber, const TBlobStorageController::TConfigState& state, bool throwOnError = false) {
            if (auto nodeIt = state.Nodes.Get().find(nodeId); nodeIt != state.Nodes.Get().end()) {
                for (const auto& [_, driveData] : nodeIt->second.KnownDrives) {
                    if (serialNumber == driveData.SerialNumber) {
                        return driveData.Path;
                    }
                }
                if (throwOnError) {
                    throw TExError() << "Couldn't find disk's path by serial number " << TErrorParams::DiskSerialNumber(serialNumber);
                }
            } else {
                if (throwOnError) {
                    throw TExError() << "Unknown node id " << TErrorParams::NodeId(nodeId);
                }
            }

            return TString();
        }

        // return TString not const TString& to make sure we never use dangling reference
        TString GetDiskSerialNumberFromNode(ui32 nodeId, const TString& path, const TBlobStorageController::TConfigState& state, bool throwOnError = false) {
            if (auto nodeIt = state.Nodes.Get().find(nodeId); nodeIt != state.Nodes.Get().end()) {
                for (const auto& [_, driveData] : nodeIt->second.KnownDrives) {
                    if (path == driveData.Path) {
                        return driveData.SerialNumber;
                    }
                }
                if (throwOnError) {
                    throw TExError() << "Couldn't find disk's serial number by path " << TErrorParams::Path(path);
                }
            } else {
                if (throwOnError) {
                    throw TExError() << "Unknown node id " << TErrorParams::NodeId(nodeId);
                }
            }

            return TString();
        }

        static std::unordered_map<TDiskId, TDiskInfo> GetDisksFromHostConfig(TBlobStorageController::TConfigState& state, std::set<TBoxId>& relevantBoxes) {
            std::unordered_map<TDiskId, TDiskInfo> disks;

            const auto& hostConfigs = state.HostConfigs.Get();
            const auto& boxes = state.Boxes.Get();
            for (const TBoxId& boxId : relevantBoxes) {
                const auto boxIt = boxes.find(boxId);
                if (boxIt == boxes.end()) {
                    continue; // box was deleted
                }
                const auto& box = boxIt->second;

                THashSet<TNodeId> usedNodes;
                for (const auto& [hostKey, hostValue] : box.Hosts) {
                    const auto& hostConfigId = hostValue.HostConfigId;
                    auto it = hostConfigs.find(hostConfigId);
                    if (it == hostConfigs.end()) {
                        throw TExHostConfigNotFound(hostConfigId);
                    }
                    const auto& hostConfig = it->second;

                    const TBlobStorageController::THostId hostId(hostKey.Fqdn, hostKey.IcPort);
                    const auto& nodeId = state.HostRecords->ResolveNodeId(hostKey, hostValue);
                    if (!nodeId) {
                        throw TExHostNotFound(hostKey) << TErrorParams::BoxId(boxId) << TErrorParams::NodeId(*nodeId);
                    } else if (!usedNodes.insert(*nodeId).second) {
                        throw TExError() << "duplicate NodeId" << TErrorParams::BoxId(boxId) << TErrorParams::NodeId(*nodeId)
                            << TErrorParams::Fqdn(hostKey.Fqdn) << TErrorParams::IcPort(hostKey.IcPort);
                    }

                    for (const auto& [drive, driveInfo] : hostConfig.Drives) {
                        auto serial = GetDiskSerialNumberFromNode(*nodeId, drive.Path, state, /* throwOnError */ false);

                        TDiskInfo disk;
                        disk.BoxId = boxId;
                        disk.HostId = hostId;
                        disk.LastSeenPath = TString();
                        disk.LastSeenSerial = serial;
                        disk.NodeId = *nodeId;
                        disk.Path = drive.Path;
                        disk.PDiskCategory = TPDiskCategory(PDiskTypeToPDiskType(driveInfo.Type), driveInfo.Kind);
                        disk.PDiskConfig = driveInfo.PDiskConfig.GetOrElse(TString());
                        disk.ReadCentric = driveInfo.ReadCentric;
                        disk.Serial = serial;
                        disk.SharedWithOs = driveInfo.SharedWithOs;

                        auto diskId = disk.GetId();
                        auto [_, inserted] = disks.try_emplace(diskId, std::move(disk));
                        if (!inserted) {
                            throw TExError() << "Came across duplicate disk on node: " << TErrorParams::NodeId(diskId.NodeId) << " with path: " << TErrorParams::Path(diskId.Path);
                        }
                    }
                }
            }

            return disks;
        }

        static std::unordered_map<TDiskId, TDiskInfo> GetDisksFromDrivesSerials(TBlobStorageController::TConfigState& state, std::set<TBoxId>& relevantBoxes) {
            std::unordered_map<TDiskId, TDiskInfo> disks;

            state.DrivesSerials.ScanRange({}, {}, [&](const auto& serial, const auto& driveInfo, const auto&) {
                if (!relevantBoxes.contains(driveInfo.BoxId)) {
                    return true;
                }

                if (serial.Serial.empty()) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSCFP04, "Missing disks's serial number");
                    return true;
                }

                auto nodeId = driveInfo.NodeId;
                if (!nodeId) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSCFP05, "Empty node id for disk with serial number.", (SerialNumber, serial.Serial));
                    return true;
                }
                auto hostId = state.HostRecords->GetHostId(*nodeId);
                if (!hostId) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSCFP06, "Couldn't find host id for node.", (NodeId, *nodeId));
                    return true;
                }

                auto path = driveInfo.Path;
                if (!path) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSCFP07, "Couldn't get path for disk with serial number.", (SerialNumber, serial.Serial));
                    return true;
                }

                TDiskInfo disk;
                disk.BoxId = driveInfo.BoxId;
                disk.HostId = *hostId;
                disk.LastSeenPath = GetDiskPathFromNode(*nodeId, serial, state, /* throwOnError */ false);
                disk.LastSeenSerial = serial;
                disk.NodeId = *nodeId;
                disk.Path = *path;
                disk.PDiskCategory = TPDiskCategory(PDiskTypeToPDiskType(driveInfo.PDiskType), driveInfo.Kind);
                disk.PDiskConfig = driveInfo.PDiskConfig.GetOrElse(TString());
                disk.ReadCentric = false;
                disk.Serial = serial;
                disk.SharedWithOs = false;

                auto diskId = disk.GetId();
                auto [_, inserted] = disks.try_emplace(diskId, std::move(disk));
                if (!inserted) {
                    throw TExError() << "Came across duplicate disk on node: " << TErrorParams::NodeId(diskId.NodeId) << " with path: " << TErrorParams::Path(diskId.Path);
                }

                return true;
            });

            return disks;
        }

        static std::unordered_map<TDiskId, TDiskInfo> GetDisksFromDrivesSerialsAndHostConfig(TBlobStorageController::TConfigState& state, std::set<TBoxId>& relevantBoxes) {
            auto disksFromDrivesSerials = GetDisksFromDrivesSerials(state, relevantBoxes);
            auto disksFromHostConfig = GetDisksFromHostConfig(state, relevantBoxes);
            disksFromHostConfig.merge(disksFromDrivesSerials);
            return disksFromHostConfig;
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

        void TBlobStorageController::FitPDisksForUserConfig(TConfigState &state) {
            auto relevantBoxes = std::exchange(state.Fit.Boxes, {});
            if (relevantBoxes.empty()) {
                return;
            }

            // re-fill PDisksToRemove set with all PDisks, we will erase remaining ones from this set a bit later
            state.PDisksToRemove.clear();
            state.PDisks.ForEach([&](const TPDiskId& pdiskId, const TPDiskInfo& pdiskInfo) {
                if (relevantBoxes.contains(pdiskInfo.BoxId)) {
                    state.PDisksToRemove.insert(pdiskId);
                }
                return true;
            });

            auto disks = GetDisksFromDrivesSerialsAndHostConfig(state, relevantBoxes);
            for (const auto& [diskId, disk] : disks) {
                TPDiskId pdiskId;
                // check if we already have spawned some PDisk at this location
                if (auto pdiskIdOptional = NKikimr::NBsController::FindPDisk(disk, state)) {
                    pdiskId = *pdiskIdOptional;
                    // yes, we have; find it by id and update some characteristics (that we can update)
                    UpdatePDiskIfNeeded(pdiskId, disk, DefaultMaxSlots, state);
                } else {
                    // no, this disk is not in map yet; see if it is mentioned in static configuration
                    Schema::PDisk::Guid::Type guid;

                    ui32 staticSlotUsage = 0;
                    if (auto pdiskIdOptional = NKikimr::NBsController::FindStaticPDisk(disk, state)) {
                        // yes, take some data from static configuration
                        pdiskId = *pdiskIdOptional;
                        guid = CheckStaticPDisk(state, pdiskId, disk.PDiskCategory, disk.PDiskConfig, &staticSlotUsage);
                    } else {
                        pdiskId = FindFirstEmptyPDiskId(state.PDisks, disk.NodeId);
                        guid = RandomNumber<Schema::PDisk::Guid::Type>();
                    }

                    // create PDisk
                    state.PDisks.ConstructInplaceNewEntry(pdiskId, disk.HostId, disk.Path,
                            disk.PDiskCategory.GetRaw(), guid, disk.SharedWithOs, disk.ReadCentric,
                            /* nextVslotId */ 1000, disk.PDiskConfig, disk.BoxId, DefaultMaxSlots,
                            NKikimrBlobStorage::EDriveStatus::ACTIVE, /* statusTimestamp */ TInstant::Zero(),
                            NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE,
                            disk.Serial, disk.LastSeenSerial, disk.LastSeenPath, staticSlotUsage);

                    STLOG(PRI_NOTICE, BS_CONTROLLER, BSCFP02, "Create new pdisk", (PDiskId, pdiskId), (Path, disk.Path));
                }

                state.PDisksToRemove.erase(pdiskId);
            }

            for (const auto& pdiskId : state.PDisksToRemove) {
                STLOG(PRI_NOTICE, BS_CONTROLLER, BSCFP03, "PDisk to remove:", (PDiskId, pdiskId));
            }
            state.CheckConsistency();
        }

        void TBlobStorageController::FitPDisksForNode(TConfigState&, ui32, const std::vector<TSerial>&) {
        }

    } // NBsController
} // NKikimr
