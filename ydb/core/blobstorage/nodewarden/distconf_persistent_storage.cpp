#include "distconf.h"
#include <google/protobuf/util/json_util.h>

#include <library/cpp/openssl/crypto/sha.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

namespace NKikimr::NStorage {

    TActorId TDistributedConfigKeeper::ReadConfig(std::vector<TString> paths, ui64 cookie) {
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

                YDB_LOG_DEBUG("TReaderActor bootstrap",
                    {"marker", "NWDC40"},
                    {"paths", Paths});

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

                YDB_LOG_DEBUG("TReaderActor result",
                    {"marker", "NWDC50"},
                    {"path", path},
                    {"outcome", msg->Outcome},
                    {"guid", msg->Guid},
                    {"record", record});

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
                if (Cookie && RepliesPending) {
                    Response->Complete = false;
                    Send(ParentId, Response.release(), 0, Cookie);
                    Response = std::make_unique<TEvPrivate::TEvStorageConfigLoaded>();
                }
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
                cFunc(TEvents::TSystem::Poison, PassAway);
            )
        };
        return Register(new TReaderActor(std::move(paths), cookie));
    }

    void TDistributedConfigKeeper::WriteConfig(TString path, NKikimrBlobStorage::TPDiskMetadataRecord record,
            ui64 cookie) {
        class TWriterActor : public TActorBootstrapped<TWriterActor> {
            const TString Path;
            std::unique_ptr<TEvNodeWardenWriteMetadata> Request;
            const ui64 Cookie;
            TActorId ParentId;

        public:
            TWriterActor(TString path, NKikimrBlobStorage::TPDiskMetadataRecord record, ui64 cookie)
                : Path(std::move(path))
                , Request(std::make_unique<TEvNodeWardenWriteMetadata>(Path, std::move(record)))
                , Cookie(cookie)
            {}

            void Bootstrap(TActorId parentId) {
                ParentId = parentId;

                YDB_LOG_DEBUG("TWriterActor bootstrap",
                    {"marker", "NWDC51"},
                    {"path", Path},
                    {"record", Request->Record});

                const TActorId nodeWardenId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
                Send(nodeWardenId, Request.release());

                Become(&TThis::StateFunc);
            }

            void Handle(TEvNodeWardenWriteMetadataResult::TPtr ev) {
                YDB_LOG_DEBUG("TWriterActor result",
                    {"marker", "NWDC52"},
                    {"path", Path},
                    {"outcome", ev->Get()->Outcome},
                    {"guid", ev->Get()->Guid});

                auto response = std::make_unique<TEvPrivate::TEvStorageConfigStored>();
                response->StatusPerPath.emplace_back(Path, ev->Get()->Outcome == NPDisk::EPDiskMetadataOutcome::OK,
                    ev->Get()->Guid);
                Send(ParentId, response.release(), 0, Cookie);
                PassAway();
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvNodeWardenWriteMetadataResult, Handle);
            )
        };
        Register(new TWriterActor(std::move(path), std::move(record), cookie));
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

    void TDistributedConfigKeeper::StartPersistWrite(const TString& path) {
        auto& item = PersistQ.at(path).front();
        PathsToRetry.erase(path);
        WriteConfig(path, std::move(item.Metadata), item.Cookie);
    }

    ui64 TDistributedConfigKeeper::PersistConfig(TPersistCallback callback, const std::vector<TString>& drives) {
        YDB_LOG_DEBUG("PersistConfig",
            {"marker", "NWDC35"},
            {"metadataByPath", MetadataByPath});

        ui64 cookie = 0;
        TPersistOperation *operation = nullptr;
        if (callback) {
            cookie = NextPersistCookie++;
            Y_ABORT_UNLESS(cookie);
            operation = &PersistOperations[cookie];
            operation->Callback = std::move(callback);
        }

        const THashSet<TString> paths(drives.begin(), drives.end());
        for (const TString& path : paths) {
            if (const auto it = MetadataByPath.find(path); it != MetadataByPath.end()) {
                auto& queue = PersistQ[path];
                if (!cookie && queue.size() > 1 && !queue.back().Cookie) {
                    // Keep the latest not-started committed save on this side of a proposal barrier.
                    queue.back().Metadata = it->second;
                } else {
                    auto& item = queue.emplace_back();
                    item.Metadata = it->second;
                    item.Cookie = cookie;
                }
                if (operation) {
                    ++operation->RepliesPending;
                }
                if (queue.size() == 1) {
                    StartPersistWrite(path);
                }
            } else {
                Y_DEBUG_ABORT();
            }
        }

        if (operation && !operation->RepliesPending) {
            // Preserve asynchronous completion even when this proposal has no local drives.
            Send(SelfId(), new TEvPrivate::TEvStorageConfigStored, 0, cookie);
        }
        return cookie;
    }

    void TDistributedConfigKeeper::CancelPersistConfig(ui64 cookie) {
        const auto it = PersistOperations.find(cookie);
        if (it == PersistOperations.end()) {
            return;
        }
        TPersistCallback callback = std::move(it->second.Callback);
        PersistOperations.erase(it);

        for (auto& [path, queue] : PersistQ) {
            // The front may still physically write, so retain it and fence its late result by cookie.
            const auto firstPending = std::next(queue.begin());
            queue.erase(std::remove_if(firstPending, queue.end(), [cookie](const auto& item) {
                return item.Cookie == cookie;
            }), queue.end());

            // Removing a proposal barrier may expose two pending committed saves; keep the newer one.
            for (size_t index = 1; index + 1 < queue.size(); ) {
                if (!queue[index].Cookie && !queue[index + 1].Cookie) {
                    queue.erase(queue.begin() + index);
                } else {
                    ++index;
                }
            }
        }

        TEvPrivate::TEvStorageConfigStored result;
        result.Complete = true;
        callback(result);
    }

    void TDistributedConfigKeeper::Handle(TEvPrivate::TEvStorageConfigStored::TPtr ev) {
        ui32 numOk = 0;
        ui32 numError = 0;
        TDuration passed;
        for (const auto& [path, status, guid] : ev->Get()->StatusPerPath) {
            ++(status ? numOk : numError);
            const auto it = PersistQ.find(path);
            Y_ABORT_UNLESS(it != PersistQ.end() && !it->second.empty());
            auto& queue = it->second;
            Y_ABORT_UNLESS(queue.front().Cookie == ev->Cookie);
            passed = Max(passed, TDuration::Seconds(queue.front().Timer.Passed()));
            queue.pop_front();
            if (!queue.empty()) {
                StartPersistWrite(path);
            } else {
                PersistQ.erase(it);
                if (status) {
                    PathsToRetry.erase(path);
                } else {
                    PathsToRetry.insert(path);
                }
            }
        }

        YDB_LOG_DEBUG("TEvStorageConfigStored",
            {"marker", "NWDC36"},
            {"numOk", numOk},
            {"numError", numError},
            {"passed", passed});

        TPersistCallback callback;
        if (const auto it = PersistOperations.find(ev->Cookie); it != PersistOperations.end()) {
            auto& operation = it->second;
            Y_ABORT_UNLESS(operation.RepliesPending >= ev->Get()->StatusPerPath.size());
            operation.RepliesPending -= ev->Get()->StatusPerPath.size();
            ev->Get()->Complete = !operation.RepliesPending;
            if (ev->Get()->Complete) {
                callback = std::move(operation.Callback);
                PersistOperations.erase(it);
            } else {
                callback = operation.Callback;
            }
        }

        if (!PathsToRetry.empty() && !PersistRetryScheduled) {
            PersistRetryScheduled = true;
            TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(TEvPrivate::EvRetryPersistConfig,
                0, SelfId(), {}, nullptr, 0));
        }

        if (callback) {
            callback(*ev->Get());
        }
    }

    void TDistributedConfigKeeper::HandleRetryPersistConfig() {
        PersistRetryScheduled = false;
        PersistConfig({}, {PathsToRetry.begin(), PathsToRetry.end()});
    }

    void TDistributedConfigKeeper::Handle(TEvPrivate::TEvStorageConfigLoaded::TPtr ev) {
        auto& msg = *ev->Get();

        YDB_LOG_DEBUG("TEvStorageConfigLoaded",
            {"marker", "NWDC32"},
            {"cookie", ev->Cookie},
            {"numItemsRead", msg.MetadataPerPath.size()});
        if (ev->Cookie) {
            if (const auto it = ScatterTasks.find(ev->Cookie); it != ScatterTasks.end()
                    && !it->second.LocalContributionExpired) {
                TScatterTask& task = it->second;
                if (msg.Complete) {
                    task.ConfigReaderActor = {};
                }

                THashMap<TStorageConfigMeta, TEvGather::TCollectConfigs::TPersistentConfig*> committed;
                THashMap<TStorageConfigMeta, TEvGather::TCollectConfigs::TPersistentConfig*> proposed;

                auto *res = task.Response.MutableCollectConfigs();
                for (auto& item : *res->MutableCommittedConfigs()) {
                    committed.emplace(item.GetConfig(), &item);
                }
                for (auto& item : *res->MutableProposedConfigs()) {
                    proposed.emplace(item.GetConfig(), &item);
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

                if (msg.Complete) {
                    FinishAsyncOperation(it->first);
                }
            }
        } else {
            std::vector<TString> drivesToRead = PrevDrivesToRead;
            auto processDrive = [&](const auto& /*node*/, const auto& drive) {
                drivesToRead.push_back(drive.GetPath());
            };

            // An empty record means a successful read of an empty disk. Keep it
            // distinct from a failed read, whose existing metadata is still unknown.
            for (const auto& [path, guid] : msg.NoMetadata) {
                MetadataByPath.try_emplace(path);
            }

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
                            // Do not overwrite unread metadata using a local commit:
                            // this drive may contain a newer generation. Let bootstrap
                            // finish so cluster quorum recovery can resolve the config.
                            return true;
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
