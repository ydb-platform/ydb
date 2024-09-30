#include "distconf.h"
#include <google/protobuf/util/json_util.h>

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::ReadConfig(ui64 cookie) {
        class TReaderActor : public TActorBootstrapped<TReaderActor> {
            const std::vector<TString> Paths;
            const ui64 Cookie;
            ui32 RepliesPending = 0;
            std::unique_ptr<TEvPrivate::TEvStorageConfigLoaded> Response = std::make_unique<TEvPrivate::TEvStorageConfigLoaded>();
            TActorId ParentId;

        public:
            TReaderActor(std::vector<TString> paths, ui64 cookie)
                : Paths(std::move(paths))
                , Cookie(cookie)
            {}

            void Bootstrap(TActorId parentId) {
                ParentId = parentId;

                STLOG(PRI_DEBUG, BS_NODE, NWDC40, "TReaderActor bootstrap", (Paths, Paths));

                const TActorId nodeWardenId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
                for (size_t index = 0; index < Paths.size(); ++index) {
                    Send(nodeWardenId, new TEvNodeWardenReadMetadata(Paths[index]), 0, index);
                    ++RepliesPending;
                }

                Become(&TThis::StateFunc);
                CheckIfDone();
            }

            void Handle(TEvNodeWardenReadMetadataResult::TPtr ev) {
                auto *msg = ev->Get();
                const size_t index = ev->Cookie;
                Y_ABORT_UNLESS(index < Paths.size());
                const TString& path = Paths[index];
                auto& record = msg->Record;

                STLOG(PRI_DEBUG, BS_NODE, NWDC50, "TReaderActor result", (Path, path), (Outcome, msg->Outcome),
                    (Guid, msg->Guid), (Record, record));

                switch (msg->Outcome) {
                    case NPDisk::EPDiskMetadataOutcome::OK:
                        if (record.HasProposedStorageConfig() && !CheckFingerprint(record.GetProposedStorageConfig())) {
                            Y_DEBUG_ABORT("ProposedStorageConfig metadata fingerprint incorrect");
                            Response->Errors.push_back(path);
                        } else if (record.HasCommittedStorageConfig() && !CheckFingerprint(record.GetCommittedStorageConfig())) {
                            Y_DEBUG_ABORT("CommittedStorageConfig metadata fingerprint incorrect");
                            Response->Errors.push_back(path);
                        } else {
                            Response->MetadataPerPath.emplace_back(path, std::move(record), msg->Guid);
                        }
                        break;

                    case NPDisk::EPDiskMetadataOutcome::NO_METADATA:
                        Response->NoMetadata.emplace_back(path, msg->Guid);
                        break;

                    case NPDisk::EPDiskMetadataOutcome::ERROR:
                        Response->Errors.push_back(path);
                        break;
                }
                --RepliesPending;
                CheckIfDone();
            }

            void CheckIfDone() {
                if (!RepliesPending) {
                    Send(ParentId, Response.release(), 0, Cookie);
                    PassAway();
                }
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvNodeWardenReadMetadataResult, Handle);
            )
        };
        Register(new TReaderActor(DrivesToRead, cookie));
    }

    void TDistributedConfigKeeper::WriteConfig(std::vector<TString> drives, NKikimrBlobStorage::TPDiskMetadataRecord record) {
        class TWriterActor : public TActorBootstrapped<TWriterActor> {
            const std::vector<TString> Drives;
            const NKikimrBlobStorage::TPDiskMetadataRecord Record;
            ui32 RepliesPending = 0;
            std::unique_ptr<TEvPrivate::TEvStorageConfigStored> Response = std::make_unique<TEvPrivate::TEvStorageConfigStored>();
            TActorId ParentId;

        public:
            TWriterActor(std::vector<TString> drives, NKikimrBlobStorage::TPDiskMetadataRecord record)
                : Drives(std::move(drives))
                , Record(std::move(record))
            {}

            void Bootstrap(TActorId parentId) {
                ParentId = parentId;

                STLOG(PRI_DEBUG, BS_NODE, NWDC51, "TWriterActor bootstrap", (Drives, Drives), (Record, Record));

                const TActorId nodeWardenId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
                for (size_t index = 0; index < Drives.size(); ++index) {
                    Send(nodeWardenId, new TEvNodeWardenWriteMetadata(Drives[index], Record), 0, index);
                    ++RepliesPending;
                }

                Become(&TThis::StateFunc);
                CheckIfDone();
            }

            void Handle(TEvNodeWardenWriteMetadataResult::TPtr ev) {
                const size_t index = ev->Cookie;
                Y_ABORT_UNLESS(index < Drives.size());
                const TString& path = Drives[index];
                STLOG(PRI_DEBUG, BS_NODE, NWDC52, "TWriterActor result", (Path, path), (Outcome, ev->Get()->Outcome),
                    (Guid, ev->Get()->Guid));
                Response->StatusPerPath.emplace_back(path, ev->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK, ev->Get()->Guid);
                --RepliesPending;
                CheckIfDone();
            }

            void CheckIfDone() {
                if (!RepliesPending) {
                    Send(ParentId, Response.release());
                    PassAway();
                }
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvNodeWardenWriteMetadataResult, Handle);
            )
        };
        Register(new TWriterActor(std::move(drives), std::move(record)));
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
            WriteConfig(item.Drives, item.Record);
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
            WriteConfig(front.Drives, front.Record);
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
                Y_ABORT_UNLESS(!res->CommittedConfigsSize());
                Y_ABORT_UNLESS(!res->ProposedConfigsSize());

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

                for (const auto& [path, guid] : msg.NoMetadata) {
                    auto *disk = res->AddNoMetadata();
                    SelfNode.Serialize(disk->MutableNodeId());
                    disk->SetPath(path);
                    if (guid) {
                        disk->SetGuid(*guid);
                    }
                }

                for (const auto& path : msg.Errors) {
                    auto *disk = res->AddErrors();
                    SelfNode.Serialize(disk->MutableNodeId());
                    disk->SetPath(path);
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
                ReadConfig();
            } else {
                ApplyStorageConfig(InitialConfig);
                Y_ABORT_UNLESS(DirectBoundNodes.empty()); // ensure we don't have to spread this config
                InitialConfig.Clear();
                StorageConfigLoaded = true;
            }
        }
    }

} // NKikimr::NStorage
