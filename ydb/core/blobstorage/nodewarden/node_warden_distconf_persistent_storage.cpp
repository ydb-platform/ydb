#include "node_warden_distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::InvokeForAllDrives(TActorId selfId, const TIntrusivePtr<TNodeWardenConfig>& cfg,
            const TPerDriveCallback& callback) {
        if (const auto& config = cfg->BlobStorageConfig; config.HasAutoconfigSettings()) {
            if (const auto& autoconfigSettings = config.GetAutoconfigSettings(); autoconfigSettings.HasDefineBox()) {
                const auto& defineBox = autoconfigSettings.GetDefineBox();
                std::optional<ui64> hostConfigId;
                for (const auto& host : defineBox.GetHost()) {
                    if (host.GetEnforcedNodeId() == selfId.NodeId()) {
                        hostConfigId.emplace(host.GetHostConfigId());
                        break;
                    }
                }
                if (hostConfigId) {
                    for (const auto& hostConfig : autoconfigSettings.GetDefineHostConfig()) {
                        if (hostConfigId == hostConfig.GetHostConfigId()) {
                            for (const auto& drive : hostConfig.GetDrive()) {
                                callback(drive.GetPath());
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    void TDistributedConfigKeeper::ReadConfig(TActorSystem *actorSystem, TActorId selfId, const TIntrusivePtr<TNodeWardenConfig>& cfg) {
        auto ev = std::make_unique<TEvPrivate::TEvStorageConfigLoaded>();
        InvokeForAllDrives(selfId, cfg, [&](const TString& path) { ReadConfigFromPDisk(*ev, path, cfg->CreatePDiskKey()); });
        actorSystem->Send(new IEventHandle(selfId, {}, ev.release()));
    }

    void TDistributedConfigKeeper::ReadConfigFromPDisk(TEvPrivate::TEvStorageConfigLoaded& msg, const TString& path,
            const NPDisk::TMainKey& key) {
        TRcBuf metadata;
        NKikimrBlobStorage::TPDiskMetadataRecord m;
        switch (ReadPDiskMetadata(path, key, metadata)) {
            case NPDisk::EPDiskMetadataOutcome::OK:
                if (m.ParseFromString(metadata.ExtractUnderlyingContainerOrCopy<TString>()) && m.HasStorageConfig()) {
                    auto *config = m.MutableStorageConfig();
                    if (config->GetFingerprint() == CalculateFingerprint(*config)) {
                        if (!msg.Success || msg.StorageConfig.GetGeneration() < config->GetGeneration()) {
                            msg.StorageConfig.Swap(config);
                            msg.Success = true;
                        }
                    } else {
                        // TODO: invalid record
                    }
                } else {
                    // TODO: invalid record
                }
                break;

            default:
                break;
        }
    }

    void TDistributedConfigKeeper::WriteConfig(TActorSystem *actorSystem, TActorId selfId, const TIntrusivePtr<TNodeWardenConfig>& cfg,
            const NKikimrBlobStorage::TStorageConfig& config) {
        auto ev = std::make_unique<TEvPrivate::TEvStorageConfigStored>();
        InvokeForAllDrives(selfId, cfg, [&](const TString& path) { WriteConfigToPDisk(*ev, config, path, cfg->CreatePDiskKey()); });
        actorSystem->Send(new IEventHandle(selfId, {}, ev.release()));
    }

    void TDistributedConfigKeeper::WriteConfigToPDisk(TEvPrivate::TEvStorageConfigStored& msg,
            const NKikimrBlobStorage::TStorageConfig& config, const TString& path, const NPDisk::TMainKey& key) {
        NKikimrBlobStorage::TPDiskMetadataRecord m;
        m.MutableStorageConfig()->CopyFrom(config);
        TString data;
        const bool success = m.SerializeToString(&data);
        Y_VERIFY(success);
        switch (WritePDiskMetadata(path, key, TRcBuf(std::move(data)))) {
            case NPDisk::EPDiskMetadataOutcome::OK:
                msg.StatusPerPath.emplace_back(path, true);
                break;

            default:
                msg.StatusPerPath.emplace_back(path, false);
                break;
        }
    }

    TString TDistributedConfigKeeper::CalculateFingerprint(const NKikimrBlobStorage::TStorageConfig& config) {
        NKikimrBlobStorage::TStorageConfig temp;
        temp.CopyFrom(config);
        temp.ClearFingerprint();

        TString s;
        const bool success = temp.SerializeToString(&s);
        Y_VERIFY(success);

        auto digest = NOpenSsl::NSha1::Calc(s.data(), s.size());
        return TString(reinterpret_cast<const char*>(digest.data()), digest.size());
    }

    void TDistributedConfigKeeper::Handle(TEvPrivate::TEvStorageConfigLoaded::TPtr ev) {
        (void)ev;
    }

    void TDistributedConfigKeeper::Handle(TEvPrivate::TEvStorageConfigStored::TPtr ev) {
        (void)ev;
    }

} // NKikimr::NStorage

namespace NKikimr {

    struct TVaultRecord {
        TString Path;
        ui64 PDiskGuid;
        TInstant Timestamp;
        TString Record;
    };

    static const TString VaultPath = "/var/tmp/kikimr-storage.bin";

    static bool ReadVault(TFile& file, std::vector<TVaultRecord>& vault) {
        try {
            const TString buffer = TUnbufferedFileInput(file).ReadAll();
            for (TMemoryInput stream(buffer); !stream.Exhausted(); ) {
                TVaultRecord& record = vault.emplace_back();
                ::LoadMany(&stream, record.Path, record.PDiskGuid, record.Timestamp, record.Record);
            }
        } catch (...) {
            return false;
        }

        return true;
    }

    NPDisk::EPDiskMetadataOutcome ReadPDiskMetadata(const TString& path, const NPDisk::TMainKey& key, TRcBuf& metadata) {
        TPDiskInfo info;
        if (!ReadPDiskFormatInfo(path, key, info, true)) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        TFileHandle fh(VaultPath, OpenExisting);
        if (!fh.IsOpen()) {
            return NPDisk::EPDiskMetadataOutcome::NO_METADATA;
        } else if (fh.Flock(LOCK_SH)) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        std::vector<TVaultRecord> vault;
        TFile file(fh.Release());
        if (!ReadVault(file, vault)) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        for (const auto& item : vault) {
            if (item.Path == path && item.PDiskGuid == info.DiskGuid && item.Timestamp == info.Timestamp) {
                metadata = TRcBuf(std::move(item.Record));
                return NPDisk::EPDiskMetadataOutcome::OK;
            }
        }

        return NPDisk::EPDiskMetadataOutcome::NO_METADATA;
    }

    NPDisk::EPDiskMetadataOutcome WritePDiskMetadata(const TString& path, const NPDisk::TMainKey& key, TRcBuf&& metadata) {
        TFileHandle fh(VaultPath, OpenAlways);
        if (!fh.IsOpen() || fh.Flock(LOCK_EX)) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        (void)key;

        std::vector<TVaultRecord> vault;
        TFile file(fh.Release());
        if (!ReadVault(file, vault)) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        bool found = false;
        for (auto& item : vault) {
            if (item.Path == path) {
                item.Record = metadata.ExtractUnderlyingContainerOrCopy<TString>();
                found = true;
                break;
            }
        }
        if (!found) {
            TVaultRecord& record = vault.emplace_back();
            record.Path = path;
            record.Record = metadata.ExtractUnderlyingContainerOrCopy<TString>();
        }

        TStringStream stream;
        for (const auto& item : vault) {
            ::SaveMany(&stream, item.Path, item.PDiskGuid, item.Timestamp, item.Record);
        }
        const TString buffer = stream.Str();

        file.Seek(0, sSet);
        file.Write(buffer.data(), buffer.size());
        file.ShrinkToFit();

        return NPDisk::EPDiskMetadataOutcome::OK;
    }

}
