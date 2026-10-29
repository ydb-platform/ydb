#include "config.h"

#include <ydb/core/protos/blobstorage_pdisk_kind.pb.h>
#include <util/charset/utf8.h>
#include <util/charset/unidata.h>

namespace NKikimr::NBsController {

    ui64 TBlobStorageController::TConfigState::ResolvePDiskKind(const TString& name, ui64 explicitKind) const {
        // Non-optional proto3 scalars cannot distinguish an absent Kind from zero.
        return ResolvePDiskKind(name, explicitKind ? TMaybe<ui64>(explicitKind) : Nothing());
    }

    ui64 TBlobStorageController::TConfigState::ResolvePDiskKind(const TString& name, TMaybe<ui64> explicitKind) const {
        if (name.empty()) {
            return explicitKind.GetOrElse(0);
        }
        const auto it = PDiskKinds.Get().find(name);
        if (it == PDiskKinds.Get().end()) {
            throw TExError() << "Unknown PDisk kind Name# " << name;
        }
        if (explicitKind && *explicitKind != it->second.KindId) {
            throw TExError() << "Explicit Kind# " << *explicitKind << " does not match KindName# " << name;
        }
        return it->second.KindId;
    }

    TMaybe<TString> TBlobStorageController::MaterializePDiskConfig(const TMap<TString, TPDiskKindInfo>& kinds,
            const TString& name, const TMaybe<TString>& overrides, TStringBuf validationContext) {
        if (!name) {
            return overrides; // legacy drive: nothing to merge
        }
        NKikimrBlobStorage::TPDiskConfig config;
        const auto it = kinds.find(name);
        if (it == kinds.end()) {
            throw TExError() << "Unknown PDisk kind Name# " << name;
        }
        if (it->second.PDiskConfig && !config.ParseFromString(*it->second.PDiskConfig)) {
            throw TExError() << "PDisk kind Name# " << name << " undeserializable PDiskConfig string";
        }
        if (overrides) {
            NKikimrBlobStorage::TPDiskConfig source;
            if (!source.ParseFromString(*overrides)) {
                throw TExError() << validationContext << " undeserializable PDiskConfig string KindName# " << name;
            }
            config = MergePDiskKindConfig(config, source);
        }
        if (validationContext) {
            ValidatePDiskConfig(config, validationContext);
        }
        return config.SerializeAsString();
    }

    TMaybe<TString> TBlobStorageController::TConfigState::MaterializePDiskConfig(const TString& name,
            const TMaybe<TString>& overrides, TStringBuf context) const {
        return TBlobStorageController::MaterializePDiskConfig(PDiskKinds.Get(), name, overrides, context);
    }

    void TBlobStorageController::TConfigState::PreparePDiskKindUpdates(const TString& name) {
        THashSet<THostConfigId> changedHosts;
        for (const auto& [id, host] : HostConfigs.Get()) {
            for (const auto& [key, drive] : host.Drives) {
                if (drive.KindName != name) {
                    continue;
                }
                // Not validated here: the effective settings are checked against the final state of the request in
                // ValidateConfigUpdates, so a later command may still fix a drive override conflicting with the profile.
                HostPDiskConfigUpdates[id][key.Path] = MaterializePDiskConfig(name, drive.PDiskConfig, {});
                changedHosts.insert(id);
            }
        }
        for (const auto& [boxId, box] : Boxes.Get()) {
            for (const auto& [key, host] : box.Hosts) {
                if (changedHosts.contains(host.HostConfigId)) {
                    Fit.Boxes.insert(boxId);
                    break;
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
        if (name.size() > 255) {
            throw TExError() << "PDisk kind name exceeds 255 bytes";
        }
        const auto* pos = reinterpret_cast<const unsigned char*>(name.data());
        const auto* end = pos + name.size();
        while (pos != end) {
            wchar32 ch;
            if (ReadUTF8CharAndAdvance<StrictUTF8::Yes>(ch, pos, end) != RECODE_OK || IsCntrl(ch) || IsWhitespace(ch)) {
                throw TExError() << "PDisk kind name must be valid UTF-8 without whitespace or control characters";
            }
        }

        const auto& kinds = PDiskKinds.Get();
        const ui64 nextGen = CheckGeneration(cmd, kinds, name);
        const auto it = kinds.find(name);
        const bool redefined = it != kinds.end();

        TPDiskKindId id;
        if (it != kinds.end()) {
            id = it->second.KindId;
        } else {
            // Named kinds share the numeric namespace with legacy disk kinds and pool filters.
            if (!UsedPDiskKindIds) {
                auto& usedIds = UsedPDiskKindIds.emplace(ReservedPDiskKindIds);
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
                    if (drive.LifeStage == NKikimrBlobStorage::TDriveLifeStage::ADDED_BY_DSTOOL) {
                        usedIds.insert(drive.Kind);
                    }
                });
                for (const auto& [pdiskId, pdisk] : StaticPDisks) {
                    usedIds.insert(pdisk.Category.Kind());
                }
            }

            id = 1;
            while (UsedPDiskKindIds->contains(id)) {
                ++id;
            }
            // TPDiskCategory stores Kind in 55 bits.
            if (id >= (ui64{1} << 55)) {
                throw TExError() << "No free PDisk kind IDs";
            }
            UsedPDiskKindIds->insert(id);
            PDiskKindNames.Unshare().emplace(id, name);
        }

        TPDiskKindInfo kind;
        kind.KindId = id;
        kind.Generation = nextGen;
        if (cmd.HasPDiskConfig()) {
            // A profile may be partial; validate cross-field constraints after applying drive overrides.
            const bool success = cmd.GetPDiskConfig().SerializeToString(&kind.PDiskConfig.ConstructInPlace());
            Y_ABORT_UNLESS(success);
        }

        PDiskKinds.Unshare()[name] = std::move(kind);
        if (redefined) {
            ChangedPDiskKinds.insert(name);
            PreparePDiskKindUpdates(name);
        }
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
            if (drive.LifeStage == NKikimrBlobStorage::TDriveLifeStage::ADDED_BY_DSTOOL && drive.Kind == id) {
                throw TExError() << "PDisk kind Name# " << name << " is used by drive"
                    << TErrorParams::DiskSerialNumber(serial.Serial);
            }
        });
        PDisks.ForEach([&](const TPDiskId& pdiskId, const TPDiskInfo& pdisk) {
            if (!PDisksToRemove.contains(pdiskId) && pdisk.Kind.Kind() == id) {
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
        PDiskKindNames.Unshare().erase(id);
        ChangedPDiskKinds.erase(name);
    }

} // namespace NKikimr::NBsController
