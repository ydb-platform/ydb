#include "distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::ReadConfig(TActorSystem *actorSystem, TActorId selfId,
            const std::vector<TString>& drives, const TIntrusivePtr<TNodeWardenConfig>& cfg, ui64 cookie) {
        auto ev = std::make_unique<TEvPrivate::TEvStorageConfigLoaded>();
        for (const TString& path : drives) {
            TRcBuf metadata;
            switch (ReadPDiskMetadata(path, cfg->CreatePDiskKey(), metadata)) {
                case NPDisk::EPDiskMetadataOutcome::OK:
                    if (NKikimrBlobStorage::TPDiskMetadataRecord m; m.ParseFromString(metadata.ExtractUnderlyingContainerOrCopy<TString>())) {
                        auto& [p, config] = ev->MetadataPerPath.emplace_back();
                        p = path;
                        config.Swap(&m);
                    }
                    break;

                default:
                    break;
            }
        }
        actorSystem->Send(new IEventHandle(selfId, {}, ev.release(), 0, cookie));
    }

    void TDistributedConfigKeeper::WriteConfig(TActorSystem *actorSystem, TActorId selfId,
            const std::vector<TString>& drives, const TIntrusivePtr<TNodeWardenConfig>& cfg,
            const NKikimrBlobStorage::TPDiskMetadataRecord& record) {
        THPTimer timer;
        auto ev = std::make_unique<TEvPrivate::TEvStorageConfigStored>();
        for (const TString& path : drives) {
            TString data;
            const bool success = record.SerializeToString(&data);
            Y_VERIFY(success);
            switch (WritePDiskMetadata(path, cfg->CreatePDiskKey(), TRcBuf(std::move(data)))) {
                case NPDisk::EPDiskMetadataOutcome::OK:
                    ev->StatusPerPath.emplace_back(path, true);
                    break;

                default:
                    ev->StatusPerPath.emplace_back(path, false);
                    break;
            }
        }
        actorSystem->Send(new IEventHandle(selfId, {}, ev.release()));
    }

    TString TDistributedConfigKeeper::CalculateFingerprint(const NKikimrBlobStorage::TStorageConfig& config) {
        NKikimrBlobStorage::TStorageConfig temp;
        temp.CopyFrom(config);
        UpdateFingerprint(&temp);
        return temp.GetFingerprint();
    }

    void TDistributedConfigKeeper::UpdateFingerprint(NKikimrBlobStorage::TStorageConfig *config) {
        config->ClearFingerprint();

        TString s;
        const bool success = config->SerializeToString(&s);
        Y_VERIFY(success);

        auto digest = NOpenSsl::NSha1::Calc(s.data(), s.size());
        config->SetFingerprint(digest.data(), digest.size());
    }

    bool TDistributedConfigKeeper::CheckFingerprint(const NKikimrBlobStorage::TStorageConfig& config) {
        return CalculateFingerprint(config) == config.GetFingerprint();
    }

    void TDistributedConfigKeeper::PersistConfig(TPersistCallback callback) {
        TPersistQueueItem& item = PersistQ.emplace_back();

        if (StorageConfig && StorageConfig->GetGeneration()) {
            item.Record.MutableCommittedStorageConfig()->CopyFrom(*StorageConfig);
        }

        if (ProposedStorageConfig) {
            item.Record.MutableProposedStorageConfig()->CopyFrom(*ProposedStorageConfig);
        }

        STLOG(PRI_DEBUG, BS_NODE, NWDC35, "PersistConfig", (Record, item.Record));

        std::vector<TString> drives;
        if (item.Record.HasCommittedStorageConfig()) {
            EnumerateConfigDrives(item.Record.GetCommittedStorageConfig(), 0, [&](const auto& /*node*/, const auto& drive) {
                drives.push_back(drive.GetPath());
            });
        }
        if (item.Record.HasProposedStorageConfig()) {
            EnumerateConfigDrives(item.Record.GetProposedStorageConfig(), 0, [&](const auto& /*node*/, const auto& drive) {
                drives.push_back(drive.GetPath());
            });
        }
        std::sort(drives.begin(), drives.end());
        drives.erase(std::unique(drives.begin(), drives.end()), drives.end());

        item.Drives = std::move(drives);
        item.Callback = std::move(callback);

        if (PersistQ.size() == 1) {
            auto query = std::bind(&TThis::WriteConfig, TActivationContext::ActorSystem(), SelfId(), item.Drives, Cfg, item.Record);
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
        PersistQ.pop_front();

        if (!PersistQ.empty()) {
            auto& front = PersistQ.front();
            auto query = std::bind(&TThis::WriteConfig, TActivationContext::ActorSystem(), SelfId(), front.Drives, Cfg,
                front.Record);
            Send(MakeIoDispatcherActorId(), new TEvInvokeQuery(std::move(query)));
        }
    }

    void TDistributedConfigKeeper::Handle(TEvPrivate::TEvStorageConfigLoaded::TPtr ev) {
        auto& msg = *ev->Get();

        STLOG(PRI_DEBUG, BS_NODE, NWDC32, "TEvStorageConfigLoaded");
        if (ev->Cookie) {
            if (const auto it = ScatterTasks.find(ev->Cookie); it != ScatterTasks.end()) {
                TScatterTask& task = it->second;

                THashMap<TStorageConfigMeta, TEvGather::TCollectConfigs::TPersistentConfig*> committed;
                THashMap<TStorageConfigMeta, TEvGather::TCollectConfigs::TPersistentConfig*> proposed;

                auto *res = task.Response.MutableCollectConfigs();
                for (auto& item : *res->MutableCommittedConfigs()) {
                    committed.try_emplace(item.GetConfig(), &item);
                }
                for (auto& item : *res->MutableProposedConfigs()) {
                    proposed.try_emplace(item.GetConfig(), &item);
                }

                for (const auto& [path, m] : msg.MetadataPerPath) {
                    auto addConfig = [&, path = path](const auto& config, auto func, auto& set) {
                        auto& ptr = set[config];
                        if (!ptr) {
                            ptr = (res->*func)();
                            ptr->MutableConfig()->CopyFrom(config);
                        }
                        auto *disk = ptr->AddDisks();
                        SelfNode.Serialize(disk->MutableNodeId());
                        disk->SetPath(path);
                    };

                    if (m.HasCommittedStorageConfig()) {
                        addConfig(m.GetCommittedStorageConfig(), &TEvGather::TCollectConfigs::AddCommittedConfigs, committed);
                    }
                    if (m.HasProposedStorageConfig()) {
                        addConfig(m.GetProposedStorageConfig(), &TEvGather::TCollectConfigs::AddProposedConfigs, proposed);
                    }
                }

                FinishAsyncOperation(it->first);
            }
        } else { // just loaded the initial config, try to acquire newer configuration
            for (const auto& [path, m] : msg.MetadataPerPath) {
                if (m.HasCommittedStorageConfig()) {
                    const auto& config = m.GetCommittedStorageConfig();
                    if (InitialConfig.GetGeneration() < config.GetGeneration()) {
                        InitialConfig.CopyFrom(config);
                    } else if (InitialConfig.GetGeneration() && InitialConfig.GetGeneration() == config.GetGeneration() &&
                            InitialConfig.GetFingerprint() != config.GetFingerprint()) {
                        // TODO: error
                    }
                }
                if (m.HasProposedStorageConfig()) {
                    const auto& proposed = m.GetProposedStorageConfig();
                    // TODO: more checks
                    if (InitialConfig.GetGeneration() < proposed.GetGeneration()) {
                        if (!ProposedStorageConfig) {
                            ProposedStorageConfig.emplace(proposed);
                        } else if (ProposedStorageConfig->GetGeneration() < proposed.GetGeneration()) {
                            ProposedStorageConfig.emplace(proposed);
                        }
                    }
                }
            }

            // generate new list of drives to acquire
            std::vector<TString> drivesToRead;
            EnumerateConfigDrives(InitialConfig, SelfId().NodeId(), [&](const auto& /*node*/, const auto& drive) {
                drivesToRead.push_back(drive.GetPath());
            });
            std::sort(drivesToRead.begin(), drivesToRead.end());

            if (DrivesToRead != drivesToRead) { // re-read configuration as it may cover additional drives
                auto query = std::bind(&TThis::ReadConfig, TActivationContext::ActorSystem(), SelfId(), DrivesToRead, Cfg, 0);
                Send(MakeIoDispatcherActorId(), new TEvInvokeQuery(std::move(query)));
            } else {
                ApplyStorageConfig(InitialConfig);
                Y_VERIFY(DirectBoundNodes.empty()); // ensure we don't have to spread this config
                InitialConfig.Clear();
                StorageConfigLoaded = true;
            }
        }
    }

} // NKikimr::NStorage

namespace NKikimr {

    static const TString VaultPath = "/Berkanavt/kikimr/state/storage.txt";

    static bool ReadVault(TFile& file, NKikimrBlobStorage::TStorageFileContent& vault) {
        TString buffer;
        try {
            buffer = TFileInput(file).ReadAll();
        } catch (...) {
            return false;
        }
        if (!buffer) {
            return true;
        }
        return google::protobuf::util::JsonStringToMessage(buffer, &vault).ok();
    }

    NPDisk::EPDiskMetadataOutcome ReadPDiskMetadata(const TString& path, const NPDisk::TMainKey& key, TRcBuf& metadata) {
        TFileHandle fh(VaultPath, OpenExisting);
        if (!fh.IsOpen()) {
            return NPDisk::EPDiskMetadataOutcome::NO_METADATA;
        } else if (fh.Flock(LOCK_SH)) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        TPDiskInfo info;
        const bool pdiskSuccess = ReadPDiskFormatInfo(path, key, info, false);
        if (!pdiskSuccess) {
            info.DiskGuid = 0;
            info.Timestamp = TInstant::Max();
        }

        NKikimrBlobStorage::TStorageFileContent vault;
        if (TFile file(fh.Release()); !ReadVault(file, vault)) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        NKikimrBlobStorage::TStorageFileContent::TRecord *it = nullptr;
        for (NKikimrBlobStorage::TStorageFileContent::TRecord& item : *vault.MutableRecord()) {
            if (item.GetPath() == path) {
                it = &item;
                break;
            }
        }

        if (it && !it->GetPDiskGuid() && !it->GetTimestamp() && pdiskSuccess) {
            it->SetPDiskGuid(info.DiskGuid);
            it->SetTimestamp(info.Timestamp.GetValue());

            TString s;
            const bool success = it->GetMeta().SerializeToString(&s);
            Y_VERIFY(success);

            WritePDiskMetadata(path, key, TRcBuf(std::move(s)));
        }

        if (it && it->GetPDiskGuid() == info.DiskGuid && it->GetTimestamp() == info.Timestamp.GetValue() &&
                std::find(key.begin(), key.end(), it->GetKey()) != key.end()) {
            TString s;
            const bool success = it->GetMeta().SerializeToString(&s);
            Y_VERIFY(success);
            metadata = TRcBuf(std::move(s));
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
        const bool pdiskSuccess = ReadPDiskFormatInfo(path, key, info, false);
        if (!pdiskSuccess) {
            info.DiskGuid = 0;
            info.Timestamp = TInstant::Max();
        }

        NKikimrBlobStorage::TStorageFileContent vault;
        if (!ReadVault(file, vault)) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        NKikimrBlobStorage::TStorageFileContent::TRecord *it = nullptr;
        for (NKikimrBlobStorage::TStorageFileContent::TRecord& item : *vault.MutableRecord()) {
            if (item.GetPath() == path) {
                it = &item;
                break;
            }
        }

        if (!it) {
            it = vault.AddRecord();
            it->SetPath(path);
        }

        it->SetPDiskGuid(info.DiskGuid);
        it->SetTimestamp(info.Timestamp.GetValue());
        it->SetKey(key.back());
        bool success = it->MutableMeta()->ParseFromString(metadata.ExtractUnderlyingContainerOrCopy<TString>());
        Y_VERIFY(success);

        TString buffer;
        google::protobuf::util::JsonPrintOptions opts;
        opts.add_whitespace = true;
        success = google::protobuf::util::MessageToJsonString(vault, &buffer, opts).ok();
        Y_VERIFY(success);

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
