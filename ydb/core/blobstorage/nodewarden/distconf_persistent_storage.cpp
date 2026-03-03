#include "distconf.h"
#include <google/protobuf/util/json_util.h>

#include <library/cpp/openssl/crypto/sha.h>

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::ReadConfig(std::vector<TString> paths, ui64 cookie) {
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
        Register(new TReaderActor(std::move(paths), cookie));
    }

    void TDistributedConfigKeeper::WriteConfig(THashMap<TString, NKikimrBlobStorage::TPDiskMetadataRecord> records) {
        class TWriterActor : public TActorBootstrapped<TWriterActor> {
            THashMap<TString, NKikimrBlobStorage::TPDiskMetadataRecord> Records;
            std::vector<TString> Drives;
            ui32 RepliesPending = 0;
            std::unique_ptr<TEvPrivate::TEvStorageConfigStored> Response = std::make_unique<TEvPrivate::TEvStorageConfigStored>();
            TActorId ParentId;

        public:
            TWriterActor(THashMap<TString, NKikimrBlobStorage::TPDiskMetadataRecord> records)
                : Records(std::move(records))
            {}

            void Bootstrap(TActorId parentId) {
                ParentId = parentId;

                STLOG(PRI_DEBUG, BS_NODE, NWDC51, "TWriterActor bootstrap", (Records, Records));

                const TActorId nodeWardenId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
                for (auto& [path, record] : Records) {
                    Send(nodeWardenId, new TEvNodeWardenWriteMetadata(path, std::move(record)), 0, Drives.size());
                    Drives.push_back(path);
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
        Register(new TWriterActor(std::move(records)));
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

    void TDistributedConfigKeeper::PersistConfig(TPersistCallback callback, const std::vector<TString>& drives) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC35, "PersistConfig", (MetadataByPath, MetadataByPath));

        TPersistQueueItem& item = PersistQ.emplace_back();
        item.Callback = std::move(callback);
        for (const TString& path : drives) {
            if (const auto it = MetadataByPath.find(path); it != MetadataByPath.end()) {
                item.MetadataByPath.insert(*it);
                PathsToRetry.erase(path);
            } else {
                Y_DEBUG_ABORT();
            }
        }

        if (PersistQ.size() == 1) { // this was the first item, we can start writing now
            WriteConfig(std::move(item.MetadataByPath));
        }
    }

    void TDistributedConfigKeeper::Handle(TEvPrivate::TEvStorageConfigStored::TPtr ev) {
        ui32 numOk = 0;
        ui32 numError = 0;
        for (const auto& [path, status, guid] : ev->Get()->StatusPerPath) {
            ++(status ? numOk : numError);
            if (!status) {
                PathsToRetry.insert(path);
            }
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
            WriteConfig(std::move(front.MetadataByPath));
        } else if (!PathsToRetry.empty()) {
            // no pending writes -- retry failed ones
            TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(TEvPrivate::EvRetryPersistConfig,
                0, SelfId(), {}, nullptr, 0));
        }
    }

    void TDistributedConfigKeeper::HandleRetryPersistConfig() {
        PersistConfig({}, {PathsToRetry.begin(), PathsToRetry.end()});
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
        } else {
            std::vector<TString> drivesToRead = PrevDrivesToRead;
            auto processDrive = [&](const auto& /*node*/, const auto& drive) {
                drivesToRead.push_back(drive.GetPath());
            };

            for (const auto& [path, m, guid] : msg.MetadataPerPath) {
                MetadataByPath[path] = m;

                if (m.HasCommittedStorageConfig()) {
                    const auto& config = m.GetCommittedStorageConfig();
                    if (!LocalCommittedStorageConfig || LocalCommittedStorageConfig->GetGeneration() < config.GetGeneration()) {
                        LocalCommittedStorageConfig = std::make_shared<NKikimrBlobStorage::TStorageConfig>(config);
                    }
                    EnumerateConfigDrives(config, SelfId().NodeId(), processDrive);
                }

                if (m.HasProposedStorageConfig()) {
                    const auto& proposed = m.GetProposedStorageConfig();
                    EnumerateConfigDrives(proposed, SelfId().NodeId(), processDrive);
                }
            }

            std::ranges::sort(drivesToRead);
            const auto [begin, end] = std::ranges::unique(drivesToRead);
            drivesToRead.erase(begin, end);

            std::vector<TString> newDrivesToRead;
            std::ranges::set_difference(drivesToRead, PrevDrivesToRead, std::back_inserter(newDrivesToRead));

            // read if got something new
            if (!newDrivesToRead.empty()) {
                ReadConfig(newDrivesToRead);
                PrevDrivesToRead = std::move(drivesToRead);
            } else {
                // apply locally committed storage configuration if any, or else apply startup YAML config
                ApplyStorageConfig(*(LocalCommittedStorageConfig ?: BaseConfig));

                if (LocalCommittedStorageConfig) { // write back committed configuration to all drives that do not have it
                    std::vector<TString> drivesToWrite = GetDrives(*LocalCommittedStorageConfig);

                    // filter out excessive metadata records
                    for (auto it = MetadataByPath.begin(); it != MetadataByPath.end(); ) {
                        if (std::ranges::binary_search(drivesToWrite, it->first)) {
                            ++it;
                        } else {
                            MetadataByPath.erase(it++);
                        }
                    }

                    // remove drives that have the relevant configuration record
                    const auto [begin, end] = std::ranges::remove_if(drivesToWrite, [&](const TString& path) {
                        const auto it = MetadataByPath.find(path);
                        if (it == MetadataByPath.end()) {
                            return false; // this drive did not contain any metadata at all, we have to write it
                        }
                        const NKikimrBlobStorage::TPDiskMetadataRecord& m = it->second;
                        if (!m.HasCommittedStorageConfig()) {
                            return false; // we have to update this drive
                        } else if (const auto& config = m.GetCommittedStorageConfig();
                                config.GetGeneration() < LocalCommittedStorageConfig->GetGeneration()) {
                            return false; // this too
                        } else if (config.GetGeneration() == LocalCommittedStorageConfig->GetGeneration()) {
                            Y_ABORT_UNLESS(config.GetFingerprint() == LocalCommittedStorageConfig->GetFingerprint());
                            return true; // this drive contains relevant record
                        }
                        Y_ABORT(); // impossible case
                    });
                    drivesToWrite.erase(begin, end);

                    for (const TString& path : drivesToWrite) {
                        MetadataByPath[path].MutableCommittedStorageConfig()->CopyFrom(*LocalCommittedStorageConfig);
                    }

                    if (!drivesToWrite.empty()) {
                        PersistConfig({}, drivesToWrite);
                    }
                } else {
                    // no local commit -- drop all metadata records
                    MetadataByPath.clear();
                }

                Y_ABORT_UNLESS(DirectBoundNodes.empty()); // ensure we don't have to spread this config
                StorageConfigLoaded = true;
            }
        }
    }

    std::vector<TString> TDistributedConfigKeeper::GetDrives(const NKikimrBlobStorage::TStorageConfig& config) const {
        std::vector<TString> drives;
        if (BaseConfig->GetSelfManagementConfig().GetEnabled()) {
            auto callback = [&](const auto& /*node*/, const auto& drive) { drives.push_back(drive.GetPath()); };
            EnumerateConfigDrives(config, SelfId().NodeId(), callback);
            std::ranges::sort(drives);
            const auto [begin, end] = std::ranges::unique(drives);
            drives.erase(begin, end);
        }
        return drives;
    }

} // NKikimr::NStorage
