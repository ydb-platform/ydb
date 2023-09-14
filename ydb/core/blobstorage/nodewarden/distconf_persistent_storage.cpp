#include "distconf.h"

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

    void TDistributedConfigKeeper::ReadConfig(TActorSystem *actorSystem, TActorId selfId, const TIntrusivePtr<TNodeWardenConfig>& cfg,
            const TString& state) {
        auto ev = std::make_unique<TEvPrivate::TEvStorageConfigLoaded>();
        InvokeForAllDrives(selfId, cfg, [&](const TString& path) { ReadConfigFromPDisk(*ev, path, cfg->CreatePDiskKey(), state); });
        actorSystem->Send(new IEventHandle(selfId, {}, ev.release()));
    }

    void TDistributedConfigKeeper::ReadConfigFromPDisk(TEvPrivate::TEvStorageConfigLoaded& msg, const TString& path,
            const NPDisk::TMainKey& key, const TString& state) {
        TRcBuf metadata;
        NKikimrBlobStorage::TPDiskMetadataRecord m;
        switch (ReadPDiskMetadata(path, key, metadata)) {
            case NPDisk::EPDiskMetadataOutcome::OK:
                if (m.ParseFromString(metadata.ExtractUnderlyingContainerOrCopy<TString>())) {
                    if (m.HasProposedStorageConfig()) { // clear incompatible propositions
                        const auto& proposed = m.GetProposedStorageConfig();
                        if (proposed.GetState() != state) {
                            m.ClearProposedStorageConfig();
                        }
                    }

                    if (!msg.Success) {
                        msg.Record.Swap(&m);
                        msg.Success = true;
                    } else {
                        MergeMetadataRecord(&msg.Record, &m);
                    }
                } else {
                    // TODO: invalid record
                }
                break;

            default:
                break;
        }
    }

    void TDistributedConfigKeeper::MergeMetadataRecord(NKikimrBlobStorage::TPDiskMetadataRecord *to,
            NKikimrBlobStorage::TPDiskMetadataRecord *from) {
        // update StorageConfig
        if (!from->HasStorageConfig() || !CheckFingerprint(from->GetStorageConfig())) {
            // can't update StorageConfig from this record
        } else if (!to->HasStorageConfig() || to->GetStorageConfig().GetGeneration() < from->GetStorageConfig().GetGeneration()) {
            to->MutableStorageConfig()->Swap(from->MutableStorageConfig());
        }

        // update ProposedStorageConfig
        if (!from->HasProposedStorageConfig()) {

        } else if (!to->HasProposedStorageConfig()) {
            to->MutableProposedStorageConfig()->Swap(from->MutableProposedStorageConfig());
        } else {
            // TODO: quorum
        }
    }

    void TDistributedConfigKeeper::WriteConfig(TActorSystem *actorSystem, TActorId selfId, const TIntrusivePtr<TNodeWardenConfig>& cfg,
            const NKikimrBlobStorage::TPDiskMetadataRecord& record) {
        THPTimer timer;
        auto ev = std::make_unique<TEvPrivate::TEvStorageConfigStored>();
        InvokeForAllDrives(selfId, cfg, [&](const TString& path) { WriteConfigToPDisk(*ev, record, path, cfg->CreatePDiskKey()); });
        actorSystem->Send(new IEventHandle(selfId, {}, ev.release()));
        STLOGX(*actorSystem, PRI_DEBUG, BS_NODE, NWDC37, "WriteConfig", (Passed, TDuration::Seconds(timer.Passed())));
    }

    void TDistributedConfigKeeper::WriteConfigToPDisk(TEvPrivate::TEvStorageConfigStored& msg,
            const NKikimrBlobStorage::TPDiskMetadataRecord& record, const TString& path, const NPDisk::TMainKey& key) {
        TString data;
        const bool success = record.SerializeToString(&data);
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

    bool TDistributedConfigKeeper::CheckFingerprint(const NKikimrBlobStorage::TStorageConfig& config) {
        return CalculateFingerprint(config) == config.GetFingerprint();
    }

    void TDistributedConfigKeeper::PersistConfig(TPersistCallback callback) {
        TPersistQueueItem& item = PersistQ.emplace_back();

        if (StorageConfig.GetGeneration()) {
            auto *stored = item.Record.MutableStorageConfig();
            stored->CopyFrom(StorageConfig);
        }
        if (ProposedStorageConfig) {
            auto *proposed = item.Record.MutableProposedStorageConfig();
            proposed->SetState(State);
            proposed->MutableStorageConfig()->CopyFrom(*ProposedStorageConfig);
        }

        STLOG(PRI_DEBUG, BS_NODE, NWDC35, "PersistConfig", (Record, item.Record));

        item.Callback = std::move(callback);

        if (PersistQ.size() == 1) {
            auto query = std::bind(&TThis::WriteConfig, TActivationContext::ActorSystem(), SelfId(), Cfg, item.Record);
            Send(MakeIoDispatcherActorId(), new TEvInvokeQuery(std::move(query)));
        }
    }

    void TDistributedConfigKeeper::Handle(TEvPrivate::TEvStorageConfigStored::TPtr ev) {
        ui32 numOk = 0;
        ui32 numError = 0;
        for (const auto& [path, status] : ev->Get()->StatusPerPath) {
            ++(status ? numOk : numError);
        }

        Y_VERIFY(!PersistQ.empty());
        auto& item = PersistQ.front();

        STLOG(PRI_DEBUG, BS_NODE, NWDC36, "TEvStorageConfigStored", (NumOk, numOk), (NumError, numError),
            (Passed, TDuration::Seconds(item.Timer.Passed())));

        if (item.Callback) {
            item.Callback(*ev->Get());
        }
        if (item.Record.HasStorageConfig()) {
            if (const auto& config = item.Record.GetStorageConfig(); config.HasBlobStorageConfig()) {
                if (const auto& bsConfig = config.GetBlobStorageConfig(); bsConfig.HasServiceSet()) {
                    Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvUpdateServiceSet(bsConfig.GetServiceSet()));
                }
            }
            const ui32 selfNodeId = SelfId().NodeId();
            UpdateBound(selfNodeId, SelfNode, item.Record.GetStorageConfig(), nullptr);
        }
        PersistQ.pop_front();

        if (!PersistQ.empty()) {
            auto query = std::bind(&TThis::WriteConfig, TActivationContext::ActorSystem(), SelfId(), Cfg, PersistQ.front().Record);
            Send(MakeIoDispatcherActorId(), new TEvInvokeQuery(std::move(query)));
        }
    }

    void TDistributedConfigKeeper::Handle(TEvPrivate::TEvStorageConfigLoaded::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_NODE, NWDC32, "TEvStorageConfigLoaded", (Success, msg.Success), (Record, msg.Record));
        if (msg.Success) {
            if (msg.Record.HasStorageConfig()) {
                StorageConfig.Swap(msg.Record.MutableStorageConfig());
                Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvUpdateServiceSet(
                    StorageConfig.GetBlobStorageConfig().GetServiceSet()));
            }
            if (msg.Record.HasProposedStorageConfig()) {
                ProposedStorageConfig.emplace();
                ProposedStorageConfig->Swap(msg.Record.MutableProposedStorageConfig()->MutableStorageConfig());
            }
            PersistConfig({}); // recover incorrect replicas
        }
    }

} // NKikimr::NStorage

namespace NKikimr {

    struct TVaultRecord {
        TString Path;
        ui64 PDiskGuid;
        TInstant Timestamp;
        NPDisk::TKey Key;
        TString Record;
    };

    static const TString VaultPath = "/Berkanavt/kikimr/state/storage.bin";

    static bool ReadVault(TFile& file, std::vector<TVaultRecord>& vault) {
        try {
            const TString buffer = TUnbufferedFileInput(file).ReadAll();
            for (TMemoryInput stream(buffer); !stream.Exhausted(); ) {
                TVaultRecord& record = vault.emplace_back();
                ::LoadMany(&stream, record.Path, record.PDiskGuid, record.Timestamp, record.Key, record.Record);
            }
        } catch (...) {
            return false;
        }

        return true;
    }

    NPDisk::EPDiskMetadataOutcome ReadPDiskMetadata(const TString& path, const NPDisk::TMainKey& key, TRcBuf& metadata) {
        TFileHandle fh(VaultPath, OpenExisting);
        if (!fh.IsOpen()) {
            return NPDisk::EPDiskMetadataOutcome::NO_METADATA;
        } else if (fh.Flock(LOCK_SH)) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        TPDiskInfo info;
        if (!ReadPDiskFormatInfo(path, key, info, false)) {
            info.DiskGuid = 0;
            info.Timestamp = TInstant::Max();
        }

        std::vector<TVaultRecord> vault;
        if (TFile file(fh.Release()); !ReadVault(file, vault)) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        auto comp = [](const TVaultRecord& x, const TString& y) { return x.Path < y; };
        auto it = std::lower_bound(vault.begin(), vault.end(), path, comp);

        if (it != vault.end() && it->Path == path && !it->PDiskGuid && it->Timestamp == TInstant::Max() &&
                info.DiskGuid && info.Timestamp != TInstant::Max()) {
            it->PDiskGuid = info.DiskGuid;
            it->Timestamp = info.Timestamp;
            WritePDiskMetadata(path, key, TRcBuf(it->Record));
        }

        if (it != vault.end() && it->Path == path && it->PDiskGuid == info.DiskGuid && it->Timestamp == info.Timestamp &&
                std::find(key.begin(), key.end(), it->Key) != key.end()) {
            metadata = TRcBuf(std::move(it->Record));
            return NPDisk::EPDiskMetadataOutcome::OK;
        }

        return NPDisk::EPDiskMetadataOutcome::NO_METADATA;
    }

    NPDisk::EPDiskMetadataOutcome WritePDiskMetadata(const TString& path, const NPDisk::TMainKey& key, TRcBuf&& metadata) {
        TFileHandle fh(VaultPath, OpenAlways);
        if (!fh.IsOpen() || fh.Flock(LOCK_EX)) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }
        TFile file(fh.Release());

        TPDiskInfo info;
        if (!ReadPDiskFormatInfo(path, key, info, false)) {
            info.DiskGuid = 0;
            info.Timestamp = TInstant::Max();
        }

        std::vector<TVaultRecord> vault;
        if (!ReadVault(file, vault)) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        auto comp = [](const TVaultRecord& x, const TString& y) { return x.Path < y; };
        auto it = std::lower_bound(vault.begin(), vault.end(), path, comp);
        if (it == vault.end() || it->Path != path) {
            it = vault.insert(it, TVaultRecord{.Path = path});
        }
        it->PDiskGuid = info.DiskGuid;
        it->Timestamp = info.Timestamp;
        it->Key = key.back();
        it->Record = metadata.ExtractUnderlyingContainerOrCopy<TString>();

        TStringStream stream;
        for (const auto& item : vault) {
            ::SaveMany(&stream, item.Path, item.PDiskGuid, item.Timestamp, item.Key, item.Record);
        }
        const TString buffer = stream.Str();

        const TString tempPath = VaultPath + ".tmp";
        TFileHandle fh1(tempPath, OpenAlways);
        if (!fh1.IsOpen() || fh1.Write(buffer.data(), buffer.size()) != (i32)buffer.size()) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        if (!NFs::Rename(tempPath, VaultPath)) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        return NPDisk::EPDiskMetadataOutcome::OK;
    }

}
