#include "distconf.h"
#include <google/protobuf/util/json_util.h>


namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::ReadConfig(TActorSystem *actorSystem, TActorId selfId,
            const std::vector<TString>& drives, const TIntrusivePtr<TNodeWardenConfig>& cfg, ui64 cookie) {
        auto ev = std::make_unique<TEvPrivate::TEvStorageConfigLoaded>();
        for (const TString& path : drives) {
            TRcBuf metadata;
            std::optional<ui64> guid;
            switch (auto status = ReadPDiskMetadata(path, cfg->PDiskKey, metadata, &guid)) {
                case NPDisk::EPDiskMetadataOutcome::OK:
                    if (NKikimrBlobStorage::TPDiskMetadataRecord m; m.ParseFromString(metadata.ExtractUnderlyingContainerOrCopy<TString>())) {
                        auto& [p, config, g] = ev->MetadataPerPath.emplace_back();
                        p = path;
                        config.Swap(&m);
                        g = guid;
                    }
                    break;

                default:
                    STLOGX(*actorSystem, PRI_INFO, BS_NODE, NWDC40, "ReadConfig failed to read metadata", (Path, path),
                        (Status, (int)status), (Cookie, cookie));
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
            Y_ABORT_UNLESS(success);
            std::optional<ui64> guid;
            switch (WritePDiskMetadata(path, cfg->PDiskKey, TRcBuf(std::move(data)), &guid)) {
                case NPDisk::EPDiskMetadataOutcome::OK:
                    ev->StatusPerPath.emplace_back(path, true, guid);
                    break;

                default:
                    ev->StatusPerPath.emplace_back(path, false, std::nullopt);
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
        Y_ABORT_UNLESS(success);

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

        std::vector<TString> drives;
        auto processDrive = [&](const auto& /*node*/, const auto& drive) { drives.push_back(drive.GetPath()); };
        if (item.Record.HasCommittedStorageConfig()) {
            EnumerateConfigDrives(item.Record.GetCommittedStorageConfig(), SelfId().NodeId(), processDrive);
        }
        if (item.Record.HasProposedStorageConfig()) {
            EnumerateConfigDrives(item.Record.GetProposedStorageConfig(), SelfId().NodeId(), processDrive);
        }
        std::sort(drives.begin(), drives.end());
        drives.erase(std::unique(drives.begin(), drives.end()), drives.end());

        STLOG(PRI_DEBUG, BS_NODE, NWDC35, "PersistConfig", (Record, item.Record), (Drives, drives));

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
        for (const auto& [path, status, guid] : ev->Get()->StatusPerPath) {
            ++(status ? numOk : numError);
        }

        Y_ABORT_UNLESS(!PersistQ.empty());
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

        STLOG(PRI_DEBUG, BS_NODE, NWDC32, "TEvStorageConfigLoaded", (Cookie, ev->Cookie), (NumItemsRead, msg.MetadataPerPath.size()));
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

                for (const auto& [path, m, guid] : msg.MetadataPerPath) {
                    auto addConfig = [&, path = path, guid = guid](const auto& config, auto func, auto& set) {
                        auto& ptr = set[config];
                        if (!ptr) {
                            ptr = (res->*func)();
                            ptr->MutableConfig()->CopyFrom(config);
                        }
                        auto *disk = ptr->AddDisks();
                        SelfNode.Serialize(disk->MutableNodeId());
                        disk->SetPath(path);
                        if (guid) {
                            disk->SetGuid(*guid);
                        }
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
            for (const auto& [path, m, guid] : msg.MetadataPerPath) {
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
                    if (InitialConfig.GetGeneration() < proposed.GetGeneration() && (
                            !ProposedStorageConfig || ProposedStorageConfig->GetGeneration() < proposed.GetGeneration())) {
                        ProposedStorageConfig.emplace(proposed);
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
                Y_ABORT_UNLESS(DirectBoundNodes.empty()); // ensure we don't have to spread this config
                InitialConfig.Clear();
                StorageConfigLoaded = true;
            }
        }
    }

} // NKikimr::NStorage

namespace NKikimr {

    static const TString VaultLockFile = "/Berkanavt/kikimr/state/storage.lock";
    static const TString VaultPath = "/Berkanavt/kikimr/state/storage.txt";
    static TMutex VaultMutex;

    static bool ReadVault(NKikimrBlobStorage::TStorageFileContent& vault) {
        if (TFileHandle fh(VaultPath, OpenExisting | RdOnly); !fh.IsOpen()) {
            return true;
        } else if (TString buffer = TString::Uninitialized(fh.GetLength())) {
            return fh.Read(buffer.Detach(), buffer.size()) == (i32)buffer.size() &&
                google::protobuf::util::JsonStringToMessage(buffer, &vault).ok();
        } else {
            return true;
        }
    }

    static bool WriteVault(NKikimrBlobStorage::TStorageFileContent& vault) {
        TString buffer;
        const bool success = google::protobuf::util::MessageToJsonString(vault, &buffer).ok();
        Y_ABORT_UNLESS(success);

        const TString tempPath = VaultPath + ".tmp";
        if (TFileHandle fh(tempPath, CreateAlways | WrOnly); !fh.IsOpen()) {
            return false;
        } else if (fh.Flock(LOCK_EX | LOCK_NB)) {
            Y_DEBUG_ABORT("unexpected lock race");
            return false;
        } else if (fh.Write(buffer.data(), buffer.size()) != (i32)buffer.size()) {
            return false;
        } else if (!NFs::Rename(tempPath, VaultPath)) {
            return false;
        }
        return true;
    }

    NPDisk::EPDiskMetadataOutcome ReadPDiskMetadata(const TString& path, const NPDisk::TMainKey& key, TRcBuf& metadata,
            std::optional<ui64> *pdiskGuid) {
        TGuard<TMutex> guard(VaultMutex);
        TFileHandle lock(VaultLockFile, OpenAlways);
        if (!lock.IsOpen() || flock(lock, LOCK_EX) == -1) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        NKikimrBlobStorage::TStorageFileContent vault;
        if (!ReadVault(vault)) {
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
            // no metadata for this disk at all
            return NPDisk::EPDiskMetadataOutcome::NO_METADATA;
        } else if (std::find(key.Keys.begin(), key.Keys.end(), it->GetKey()) == key.Keys.end()) {
            // incorrect key provided, <pretend> to unreadable format
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        TPDiskInfo info;
        const bool pdiskSuccess = ReadPDiskFormatInfo(path, key, info, false);

        if (pdiskSuccess) {
            pdiskGuid->emplace(info.DiskGuid);
        } else {
            pdiskGuid->reset();
        }

        if (it->GetUnformatted() && pdiskSuccess) {
            it->SetPDiskGuid(info.DiskGuid);
            it->SetTimestamp(info.Timestamp.GetValue());
            it->ClearUnformatted();
            if (!WriteVault(vault)) {
                return NPDisk::EPDiskMetadataOutcome::ERROR;
            }
        }

        const bool match = pdiskSuccess
            ? it->GetPDiskGuid() == info.DiskGuid && TInstant::FromValue(it->GetTimestamp()) == info.Timestamp && !it->GetUnformatted()
            : it->GetUnformatted();

        if (match) {
            TString s;
            const bool success = it->GetMeta().SerializeToString(&s);
            Y_ABORT_UNLESS(success);
            metadata = TRcBuf(std::move(s));
            return NPDisk::EPDiskMetadataOutcome::OK;
        }

        return NPDisk::EPDiskMetadataOutcome::NO_METADATA;
    }

    NPDisk::EPDiskMetadataOutcome WritePDiskMetadata(const TString& path, const NPDisk::TMainKey& key, TRcBuf&& metadata,
            std::optional<ui64> *pdiskGuid) {
        TGuard<TMutex> guard(VaultMutex);
        TFileHandle lock(VaultLockFile, OpenAlways);
        if (!lock.IsOpen() || flock(lock, LOCK_EX) == -1) {
            return NPDisk::EPDiskMetadataOutcome::ERROR;
        }

        NKikimrBlobStorage::TStorageFileContent vault;
        if (!ReadVault(vault)) {
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

        TPDiskInfo info;
        const bool pdiskSuccess = ReadPDiskFormatInfo(path, key, info, false);

        if (pdiskSuccess) {
            it->SetPDiskGuid(info.DiskGuid);
            it->SetTimestamp(info.Timestamp.GetValue());
            it->ClearUnformatted();
            pdiskGuid->emplace(info.DiskGuid);
        } else {
            it->SetUnformatted(true);
            pdiskGuid->reset();
        }
        it->SetKey(key.Keys.back());
        const bool success = it->MutableMeta()->ParseFromString(metadata.ExtractUnderlyingContainerOrCopy<TString>());
        Y_ABORT_UNLESS(success);

        return WriteVault(vault)
            ? NPDisk::EPDiskMetadataOutcome::OK
            : NPDisk::EPDiskMetadataOutcome::ERROR;
    }

}
