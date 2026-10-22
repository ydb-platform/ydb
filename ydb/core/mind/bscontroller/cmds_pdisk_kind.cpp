#include "config.h"

#include <ydb/core/protos/blobstorage_pdisk_kind.pb.h>

namespace NKikimr::NBsController {

    TString TBlobStorageController::GetPDiskKindName(ui64 kindId, const TMap<TString, TPDiskKindInfo>& kinds) {
        for (const auto& [name, kind] : kinds) {
            if (kind.KindId == kindId) {
                return name;
            }
        }
        return {};
    }

    TMaybe<TString> TBlobStorageController::GetSourcePDiskConfig(
            const TPDiskInfo& pdisk, const TMap<THostConfigId, THostConfigInfo>& hosts,
            const TMap<TBoxId, TBoxInfo>& boxes, const TDriveSerialInfo* serial) {
        if (const auto box = boxes.find(pdisk.BoxId); box != boxes.end()) {
            const auto& [fqdn, port] = pdisk.HostId;
            const TBoxInfo::THostKey key(Schema::BoxHostV2::TKey::Type(pdisk.BoxId, fqdn, port));
            if (const auto host = box->second.Hosts.find(key); host != box->second.Hosts.end()) {
                if (const auto config = hosts.find(host->second.HostConfigId); config != hosts.end()) {
                    const THostConfigInfo::TDriveKey driveKey{{config->first, pdisk.Path}};
                    if (const auto drive = config->second.Drives.find(driveKey); drive != config->second.Drives.end()) {
                        return drive->second.PDiskConfig;
                    }
                }
            }
        }
        if (serial && serial->LifeStage == NKikimrBlobStorage::TDriveLifeStage::ADDED_BY_DSTOOL) {
            return serial->PDiskConfig;
        }
        return Nothing();
    }

    ui64 TBlobStorageController::TConfigState::ResolvePDiskKind(const TString& name, ui64 explicitKind) const {
        if (name.empty()) {
            return explicitKind;
        }
        const auto it = PDiskKinds.Get().find(name);
        if (it == PDiskKinds.Get().end()) {
            throw TExError() << "Unknown PDisk kind Name# " << name;
        }
        if (explicitKind && explicitKind != it->second.KindId) {
            throw TExError() << "Explicit Kind# " << explicitKind << " does not match KindName# " << name;
        }
        return it->second.KindId;
    }

    TMaybe<TString> TBlobStorageController::TConfigState::MaterializePDiskConfig(const TString& name,
            const TMaybe<TString>& overrides, TStringBuf context) const {
        NKikimrBlobStorage::TPDiskConfig config;
        if (name) {
            const auto it = PDiskKinds.Get().find(name);
            if (it == PDiskKinds.Get().end()) {
                throw TExError() << "Unknown PDisk kind Name# " << name;
            }
            if (it->second.PDiskConfig) {
                Y_ABORT_UNLESS(config.ParseFromString(*it->second.PDiskConfig));
            }
        }
        if (overrides) {
            NKikimrBlobStorage::TPDiskConfig source;
            Y_ABORT_UNLESS(source.ParseFromString(*overrides));
            // TPDiskConfig is proto2: MergeFrom preserves explicit zero/false and merges submessages.
            config.MergeFrom(source);
        }
        ValidatePDiskConfig(config, context);
        if (!name) {
            return overrides;
        }
        return config.SerializeAsString();
    }

    void TBlobStorageController::TConfigState::PreparePDiskKindUpdates() {
        if (ChangedPDiskKinds.empty()) {
            return;
        }
        for (const auto& [id, host] : HostConfigs.Get()) {
            bool changed = false;
            for (const auto& [key, drive] : host.Drives) {
                if (!drive.KindName || !ChangedPDiskKinds.contains(drive.KindName)) {
                    continue;
                }
                const TString context = TStringBuilder() << "HostConfigId# " << id << " Path# " << key.Path;
                // Validate unused host configs too; only fitting writes materialized settings to PDisks.
                MaterializePDiskConfig(drive.KindName, drive.PDiskConfig, context);
                changed = true;
            }
            if (changed) {
                for (const auto& [boxId, box] : Boxes.Get()) {
                    for (const auto& [key, host] : box.Hosts) {
                        if (host.HostConfigId == id) {
                            Fit.Boxes.insert(boxId);
                            break;
                        }
                    }
                }
            }
        }
        DrivesSerials.ForEach([&](const TSerial& serial, const TDriveSerialInfo& drive) {
            if (!drive.KindName || !ChangedPDiskKinds.contains(drive.KindName)) {
                return;
            }
            MaterializePDiskConfig(drive.KindName, drive.PDiskConfig, serial.Serial);
            if (drive.LifeStage == NKikimrBlobStorage::TDriveLifeStage::ADDED_BY_DSTOOL) {
                Fit.Boxes.insert(drive.BoxId);
            }
        });
        ChangedPDiskKinds.clear();
    }

    void TBlobStorageController::TConfigState::ValidatePDiskKindReferences() const {
        for (const auto& [id, host] : HostConfigs.Get()) {
            for (const auto& [key, drive] : host.Drives) {
                ResolvePDiskKind(drive.KindName, drive.Kind);
            }
        }
        DrivesSerials.ForEach([&](const TSerial&, const TDriveSerialInfo& drive) {
            ResolvePDiskKind(drive.KindName, drive.Kind);
        });
        for (const auto& [id, pool] : StoragePools.Get()) {
            for (const auto& filter : pool.PDiskFilters) {
                if (filter.KindName) {
                    ResolvePDiskKind(*filter.KindName, filter.Kind.GetOrElse(0));
                }
            }
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(
            const NKikimrBlobStorage::TDefinePDiskKind& cmd, TStatus& /*status*/) {
        const auto& name = cmd.GetName();
        if (name.empty()) {
            throw TExError() << "PDisk kind name must not be empty";
        }

        const auto& kinds = PDiskKinds.Get();
        const TString& previousName = cmd.GetPreviousName();
        const TString& lookupName = previousName ? previousName : name;
        const ui64 nextGen = CheckGeneration(cmd, kinds, lookupName);
        const auto it = kinds.find(lookupName);
        if (previousName && previousName != name && (it == kinds.end() || kinds.contains(name))) {
            throw TExError() << "Invalid PDisk kind rename from# " << previousName << " to# " << name;
        }

        TPDiskKindId id;
        if (it != kinds.end()) {
            id = it->second.KindId;
        } else {
            // Named kinds share the numeric namespace with legacy disk kinds and pool filters.
            THashSet<TPDiskKindId> usedIds = ReservedPDiskKindIds;
            for (const auto& [kindName, kind] : kinds) {
                usedIds.insert(kind.KindId);
            }
            for (const auto& [hostConfigId, hostConfig] : HostConfigs.Get()) {
                for (const auto& [key, drive] : hostConfig.Drives) {
                    usedIds.insert(drive.Kind);
                }
            }
            for (const auto& [poolId, pool] : StoragePools.Get()) {
                for (const auto& filter : pool.PDiskFilters) {
                    if (filter.Kind) {
                        usedIds.insert(*filter.Kind);
                    }
                }
            }
            PDisks.ForEach([&](const TPDiskId&, const TPDiskInfo& pdisk) {
                usedIds.insert(TPDiskCategory(pdisk.Kind).Kind());
            });
            DrivesSerials.ForEach([&](const TSerial&, const TDriveSerialInfo& drive) {
                usedIds.insert(drive.Kind);
            });
            for (const auto& [pdiskId, pdisk] : StaticPDisks) {
                usedIds.insert(pdisk.Category.Kind());
            }

            id = 1;
            while (usedIds.contains(id)) {
                ++id;
            }
            // TPDiskCategory stores Kind in 55 bits.
            if (id >= (ui64{1} << 55)) {
                throw TExError() << "No free PDisk kind IDs";
            }
        }

        TPDiskKindInfo kind;
        kind.KindId = id;
        kind.Generation = nextGen;
        if (cmd.HasPDiskConfig()) {
            // A profile may be partial; validate cross-field constraints after applying drive overrides.
            const bool success = cmd.GetPDiskConfig().SerializeToString(&kind.PDiskConfig.ConstructInPlace());
            Y_ABORT_UNLESS(success);
        }

        if (previousName && previousName != name) {
            PDiskKinds.Unshare().erase(previousName);
        }
        PDiskKinds.Unshare()[name] = std::move(kind);
        ChangedPDiskKinds.insert(name);
    }

    void TBlobStorageController::TConfigState::ExecuteStep(
            const NKikimrBlobStorage::TDeletePDiskKind& cmd, TStatus& /*status*/) {
        const auto& name = cmd.GetName();
        if (name.empty()) {
            throw TExError() << "PDisk kind name is empty";
        }
        const auto& kinds = PDiskKinds.Get();
        const auto it = kinds.find(name);
        if (it == kinds.end()) {
            throw TExError() << "PDisk kind not found Name# " << name;
        }
        CheckGeneration(cmd, kinds, name);
        const TPDiskKindId id = it->second.KindId;

        for (const auto& [hostConfigId, hostConfig] : HostConfigs.Get()) {
            for (const auto& [key, drive] : hostConfig.Drives) {
                if (drive.Kind == id) {
                    throw TExError() << "PDisk kind Name# " << name << " is used by drive"
                        << TErrorParams::HostConfigId(hostConfigId) << TErrorParams::Path(key.Path);
                }
            }
        }
        for (const auto& [poolId, pool] : StoragePools.Get()) {
            for (const auto& filter : pool.PDiskFilters) {
                if (filter.Kind && *filter.Kind == id) {
                    throw TExError() << "PDisk kind Name# " << name << " is used by pool filter"
                        << TErrorParams::BoxId(std::get<0>(poolId))
                        << TErrorParams::StoragePoolId(std::get<1>(poolId));
                }
            }
        }
        DrivesSerials.ForEach([&](const TSerial& serial, const TDriveSerialInfo& drive) {
            if (drive.Kind == id) {
                throw TExError() << "PDisk kind Name# " << name << " is used by drive"
                    << TErrorParams::DiskSerialNumber(serial.Serial);
            }
        });
        PDisks.ForEach([&](const TPDiskId& pdiskId, const TPDiskInfo& pdisk) {
            if (pdisk.Kind.Kind() == id) {
                throw TExError() << "PDisk kind Name# " << name << " is used by PDisk"
                    << TErrorParams::NodeId(pdiskId.NodeId) << TErrorParams::PDiskId(pdiskId.PDiskId);
            }
        });
        for (const auto& [pdiskId, pdisk] : StaticPDisks) {
            if (pdisk.Category.Kind() == id) {
                throw TExError() << "PDisk kind Name# " << name << " is used by static PDisk"
                    << TErrorParams::NodeId(pdiskId.NodeId) << TErrorParams::PDiskId(pdiskId.PDiskId);
            }
        }

        PDiskKinds.Unshare().erase(name);
    }

} // namespace NKikimr::NBsController
