#include "config.h"

#include <ydb/core/protos/blobstorage_pdisk_kind.pb.h>

namespace NKikimr::NBsController {

    void TBlobStorageController::TConfigState::ExecuteStep(
            const NKikimrBlobStorage::TDefinePDiskKind& cmd, TStatus& /*status*/) {
        const auto& name = cmd.GetName();
        if (name.empty()) {
            throw TExError() << "PDisk kind name must not be empty";
        }

        const auto& kinds = PDiskKinds.Get();
        const ui64 nextGen = CheckGeneration(cmd, kinds, name);
        const auto it = kinds.find(name);

        TPDiskKindId id;
        if (it != kinds.end()) {
            id = it->second.KindId;
        } else {
            // Named kinds share the numeric namespace with legacy disk kinds and pool filters.
            THashSet<TPDiskKindId> usedIds;
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

        PDiskKinds.Unshare()[name] = std::move(kind);
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
