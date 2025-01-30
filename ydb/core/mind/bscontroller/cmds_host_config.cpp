#include "config.h"

namespace NKikimr::NBsController {

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDefineHostConfig& cmd, TStatus& /*status*/) {
        const THostConfigId id = cmd.GetHostConfigId();
        const ui64 nextGen = CheckGeneration(cmd, HostConfigs.Get(), id);

        TMaybe<TString> defaultPDiskConfig;
        if (cmd.HasDefaultHostPDiskConfig()) {
            TString config;
            const bool success = cmd.GetDefaultHostPDiskConfig().SerializeToString(&config);
            Y_ABORT_UNLESS(success);
            defaultPDiskConfig = config;
        }

        THostConfigInfo config;
        config.Name = cmd.GetName();
        config.Generation = nextGen;
        for (const auto &drive : cmd.GetDrive()) {
            THostConfigInfo::TDriveInfo driveInfo;
            driveInfo.Type = drive.GetType();
            driveInfo.SharedWithOs = drive.GetSharedWithOs();
            driveInfo.ReadCentric = drive.GetReadCentric();
            driveInfo.Kind = drive.GetKind();

            if (drive.HasPDiskConfig()) {
                TString config;
                const bool success = drive.GetPDiskConfig().SerializeToString(&config);
                Y_ABORT_UNLESS(success);
                driveInfo.PDiskConfig = config;
            } else {
                driveInfo.PDiskConfig = defaultPDiskConfig;
            }

            Schema::HostConfigDrive::TKey::Type key(id, drive.GetPath());
            const auto [it, inserted] = config.Drives.emplace(std::move(key), std::move(driveInfo));
            if (!inserted) {
                throw TExError() << "duplicate path# " << drive.GetPath();
            }
        }

        auto addDrives = [&](const auto& field, NKikimrBlobStorage::EPDiskType type) {
            THostConfigInfo::TDriveInfo driveInfo;
            driveInfo.Type = type;
            driveInfo.SharedWithOs = false;
            driveInfo.ReadCentric = false;
            driveInfo.Kind = 0;
            driveInfo.PDiskConfig = defaultPDiskConfig;

            for (const auto& path : field) {
                const auto [it, inserted] = config.Drives.emplace(Schema::HostConfigDrive::TKey::Type(id, path), driveInfo);
                if (!inserted) {
                    throw TExError() << "duplicate path# " << path;
                }
            }
        };
        addDrives(cmd.GetRot(), NKikimrBlobStorage::EPDiskType::ROT);
        addDrives(cmd.GetSsd(), NKikimrBlobStorage::EPDiskType::SSD);
        addDrives(cmd.GetNvme(), NKikimrBlobStorage::EPDiskType::NVME);

        auto &hostConfigs = HostConfigs.Unshare();
        hostConfigs[id] = std::move(config);

        for (const auto& [boxId, box] : Boxes.Get()) {
            for (const auto& [hostKey, host] : box.Hosts) {
                if (host.HostConfigId == id) {
                    Fit.Boxes.insert(boxId);
                    break;
                }
            }
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TReadHostConfig& cmd, TStatus& status) {
        TSet<THostConfigId> queryIds;
        if (cmd.HostConfigIdSize()) {
            const auto &ids = cmd.GetHostConfigId();
            queryIds.insert(ids.begin(), ids.end());
        } else {
            for (const auto &kv : HostConfigs.Get()) {
                queryIds.insert(kv.first);
            }
        }

        const auto &hostConfigs = HostConfigs.Get();
        for (const THostConfigId &id : queryIds) {
            auto it = hostConfigs.find(id);
            if (it == hostConfigs.end()) {
                throw TExHostConfigNotFound(id);
            }
            Serialize(status.AddHostConfig(), it->first, it->second);
        }
    }

    void TBlobStorageController::TConfigState::ExecuteStep(const NKikimrBlobStorage::TDeleteHostConfig& cmd, TStatus& /*status*/) {
        const THostConfigId &id = cmd.GetHostConfigId();
        CheckGeneration(cmd, HostConfigs.Get(), id);

        // unshare host configs and find the required entry
        auto &hostConfigs = HostConfigs.Unshare();
        auto it = hostConfigs.find(id);
        if (it == hostConfigs.end()) {
            throw TExHostConfigNotFound(id);
        }

        // check that this configuration is not used in boxes
        for (const auto &kv : Boxes.Get()) {
            for (const auto &host : kv.second.Hosts) {
                if (id == host.second.HostConfigId) {
                    throw TExError() << "HostConfigId# " << id << " is used in BoxId# " << kv.first;
                }
            }
        }

        // remove entry from the table
        hostConfigs.erase(it);
    }

} // NKikimr::NBsController
