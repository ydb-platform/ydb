#include "distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::CheckRootNodeStatus() {
        Y_VERIFY_S(Binding ? RootState == ERootState::INITIAL && !Scepter :
            RootState == ERootState::INITIAL || RootState == ERootState::ERROR_TIMEOUT ? !Scepter :
            static_cast<bool>(Scepter), "Binding# " << (Binding ? Binding->ToString() : "<null>")
            << " RootState# " << RootState << " Scepter# " << (Scepter ? ToString(Scepter->Id) : "<null>"));

        if (Binding || RootState != ERootState::INITIAL) {
            return;
        }

        const bool hasQuorum = HasQuorum();

        if (RootState == ERootState::INITIAL && hasQuorum) { // becoming root node
            Y_ABORT_UNLESS(!Scepter);
            Scepter = std::make_shared<TScepter>();

            auto makeAllBoundNodes = [&] {
                TStringStream s;
                const char *sep = "{";
                for (const auto& [nodeId, _] : AllBoundNodes) {
                    s << std::exchange(sep, " ") << nodeId;
                }
                s << '}';
                return s.Str();
            };
            STLOG(PRI_DEBUG, BS_NODE, NWDC19, "Starting config collection", (Scepter, Scepter->Id),
                (AllBoundNodes, makeAllBoundNodes()));
            RootState = ERootState::COLLECT_CONFIG;
            TEvScatter task;
            task.MutableCollectConfigs();
            IssueScatterTask(TActorId(), std::move(task));
        } else if (Scepter && !hasQuorum) { // unbecoming root node -- lost quorum
            SwitchToError("quorum lost");
        }
    }

    void TDistributedConfigKeeper::SwitchToError(const TString& reason) {
        STLOG(PRI_ERROR, BS_NODE, NWDC38, "SwitchToError", (RootState, RootState), (Reason, reason));
        Scepter.reset();
        RootState = ERootState::ERROR_TIMEOUT;
        ErrorReason = reason;
        const TDuration timeout = TDuration::FromValue(ErrorTimeout.GetValue() * (25 + RandomNumber(51u)) / 50);
        TActivationContext::Schedule(timeout, new IEventHandle(TEvPrivate::EvErrorTimeout, 0, SelfId(), {}, nullptr, 0));
    }

    void TDistributedConfigKeeper::HandleErrorTimeout() {
        STLOG(PRI_DEBUG, BS_NODE, NWDC20, "Error timeout hit");
        Y_ABORT_UNLESS(!Scepter);
        RootState = ERootState::INITIAL;
        ErrorReason = {};
        IssueNextBindRequest();
    }

    void TDistributedConfigKeeper::ProcessGather(TEvGather *res) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC27, "ProcessGather", (RootState, RootState), (Res, *res));

        if (!res) {
            return SwitchToError("leadership lost while executing query");
        }

        switch (res->GetResponseCase()) {
            case TEvGather::kCollectConfigs:
                return RootState == ERootState::COLLECT_CONFIG
                    ? ProcessCollectConfigs(res->MutableCollectConfigs())
                    : SwitchToError("unexpected CollectConfigs response");

            case TEvGather::kProposeStorageConfig:
                return RootState == ERootState::PROPOSE_NEW_STORAGE_CONFIG
                    ? ProcessProposeStorageConfig(res->MutableProposeStorageConfig())
                    : SwitchToError("unexpected ProposeStorageConfig response");

            case TEvGather::RESPONSE_NOT_SET:
                return SwitchToError("response not set");
        }

        SwitchToError("incorrect response from peer");
    }

    bool TDistributedConfigKeeper::HasQuorum() const {
        auto generateConnected = [&](auto&& callback) {
            for (const auto& [nodeId, node] : AllBoundNodes) {
                callback(nodeId);
            }
        };
        return StorageConfig && HasNodeQuorum(*StorageConfig, generateConnected);
    }

    void TDistributedConfigKeeper::ProcessCollectConfigs(TEvGather::TCollectConfigs *res) {
        auto generateSuccessful = [&](auto&& callback) {
            for (const auto& item : res->GetNodes()) {
                for (const auto& node : item.GetNodeIds()) {
                    callback(node);
                }
            }
        };
        const bool nodeQuorum = HasNodeQuorum(*StorageConfig, generateSuccessful);
        STLOG(PRI_DEBUG, BS_NODE, NWDC31, "ProcessCollectConfigs", (RootState, RootState), (NodeQuorum, nodeQuorum), (Res, *res));
        if (!nodeQuorum) {
            return SwitchToError("no node quorum for CollectConfigs");
        }

        // TODO: validate self-assembly UUID

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Pick base config quorum (if we have one)

        struct TBaseConfigInfo {
            NKikimrBlobStorage::TStorageConfig Config;
            THashSet<TNodeIdentifier> HavingNodeIds;
        };
        THashMap<TStorageConfigMeta, TBaseConfigInfo> baseConfigs;
        for (const auto& node : res->GetNodes()) {
            if (node.HasBaseConfig()) {
                const auto& baseConfig = node.GetBaseConfig();
                const auto [it, inserted] = baseConfigs.try_emplace(baseConfig);
                TBaseConfigInfo& r = it->second;
                if (inserted) {
                    r.Config.CopyFrom(baseConfig);
                }
                for (const auto& nodeId : node.GetNodeIds()) {
                    r.HavingNodeIds.emplace(nodeId);
                }
            }
        }
        for (auto it = baseConfigs.begin(); it != baseConfigs.end(); ) { // filter out configs not having node quorum
            TBaseConfigInfo& r = it->second;
            auto generateNodeIds = [&](auto&& callback) {
                for (const auto& nodeId : r.HavingNodeIds) {
                    callback(nodeId);
                }
            };
            if (HasNodeQuorum(r.Config, generateNodeIds)) {
                ++it;
            } else {
                baseConfigs.erase(it++);
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Create quorums for committed and proposed configurations

        struct TDiskConfigInfo {
            NKikimrBlobStorage::TStorageConfig Config;
            THashSet<std::tuple<TNodeIdentifier, TString, std::optional<ui64>>> HavingDisks;
        };
        THashMap<TStorageConfigMeta, TDiskConfigInfo> committedConfigs;
        THashMap<TStorageConfigMeta, TDiskConfigInfo> proposedConfigs;
        for (auto& [field, set] : {
                    std::tie(res->GetCommittedConfigs(), committedConfigs),
                    std::tie(res->GetProposedConfigs(), proposedConfigs)
                }) {
            for (const TEvGather::TCollectConfigs::TPersistentConfig& config : field) {
                const auto [it, inserted] = set.try_emplace(config.GetConfig());
                TDiskConfigInfo& r = it->second;
                if (inserted) {
                    r.Config.CopyFrom(config.GetConfig());
                }
                for (const auto& disk : config.GetDisks()) {
                    r.HavingDisks.emplace(disk.GetNodeId(), disk.GetPath(), disk.HasGuid() ? std::make_optional(disk.GetGuid()) : std::nullopt);
                }
            }
        }
        for (auto& set : {&committedConfigs, &proposedConfigs}) {
            for (auto it = set->begin(); it != set->end(); ) {
                TDiskConfigInfo& r = it->second;

                auto generateSuccessful = [&](auto&& callback) {
                    for (const auto& [node, path, guid] : r.HavingDisks) {
                        callback(node, path, guid);
                    }
                };

                if (HasConfigQuorum(r.Config, generateSuccessful, *Cfg)) {
                    ++it;
                } else {
                    set->erase(it++);
                }
            }
        }

        if (baseConfigs.size() > 1 || committedConfigs.size() > 1 || proposedConfigs.size() > 1) {
            STLOG(PRI_CRIT, BS_NODE, NWDC08, "Multiple nonintersecting node sets have quorum",
                (BaseConfigs.size, baseConfigs.size()), (CommittedConfigs.size, committedConfigs.size()),
                (ProposedConfigs.size, proposedConfigs.size()));
            Y_DEBUG_ABORT_UNLESS(false);
            Halt();
            return;
        }

        NKikimrBlobStorage::TStorageConfig *baseConfig = baseConfigs.empty() ? nullptr : &baseConfigs.begin()->second.Config;
        NKikimrBlobStorage::TStorageConfig *committedConfig = committedConfigs.empty() ? nullptr : &committedConfigs.begin()->second.Config;
        NKikimrBlobStorage::TStorageConfig *proposedConfig = proposedConfigs.empty() ? nullptr : &proposedConfigs.begin()->second.Config;

        if (committedConfig && ApplyStorageConfig(*committedConfig)) { // we have a committed config, apply and spread it
            for (const auto& [nodeId, info] : DirectBoundNodes) {
                SendEvent(nodeId, info, std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId(), &StorageConfig.value()));
            }
        }

        NKikimrBlobStorage::TStorageConfig *configToPropose = nullptr;
        std::optional<NKikimrBlobStorage::TStorageConfig> propositionBase;

        if (proposedConfig) { // we have proposition in progress, resume
            configToPropose = proposedConfig;
        } else if (committedConfig) { // we have committed config, check if we need to update it
            propositionBase.emplace(*committedConfig);
            if (UpdateConfig(committedConfig)) {
                configToPropose = committedConfig;
            }
        } else if (baseConfig && !baseConfig->GetGeneration()) { // we have no committed storage config, but we can create one
            propositionBase.emplace(*baseConfig);
            if (GenerateFirstConfig(baseConfig)) {
                configToPropose = baseConfig;
            }
        }

        STLOG(PRI_DEBUG, BS_NODE, NWDC37, "ProcessCollectConfigs", (BaseConfig, baseConfig), (CommittedConfig, committedConfig),
            (ProposedConfig, proposedConfig), (ConfigToPropose, configToPropose), (PropositionBase, propositionBase));

        if (configToPropose) {
            if (propositionBase) {
                configToPropose->SetGeneration(configToPropose->GetGeneration() + 1);
                configToPropose->MutablePrevConfig()->CopyFrom(*propositionBase);
                configToPropose->MutablePrevConfig()->ClearPrevConfig();
            }
            UpdateFingerprint(configToPropose);

            TEvScatter task;
            auto *propose = task.MutableProposeStorageConfig();
            CurrentProposedStorageConfig.CopyFrom(*configToPropose);
            propose->MutableConfig()->Swap(configToPropose);
            IssueScatterTask(TActorId(), std::move(task));
            RootState = ERootState::PROPOSE_NEW_STORAGE_CONFIG;
        } else {
            RootState = ERootState::RELAX; // nothing to do right now, just relax
        }
    }

    void TDistributedConfigKeeper::ProcessProposeStorageConfig(TEvGather::TProposeStorageConfig *res) {
        auto generateSuccessful = [&](auto&& callback) {
            for (const auto& item : res->GetStatus()) {
                const TNodeIdentifier node(item.GetNodeId());
                for (const auto& drive : item.GetSuccessfulDrives()) {
                    callback(node, drive.GetPath(), drive.HasGuid() ? std::make_optional(drive.GetGuid()) : std::nullopt);
                }
            }
        };

        if (HasConfigQuorum(CurrentProposedStorageConfig, generateSuccessful, *Cfg)) {
            // apply configuration and spread it
            ApplyStorageConfig(CurrentProposedStorageConfig);
            for (const auto& [nodeId, info] : DirectBoundNodes) {
                SendEvent(nodeId, info, std::make_unique<TEvNodeConfigReversePush>(GetRootNodeId(), &StorageConfig.value()));
            }
            CurrentProposedStorageConfig.Clear();
            RootState = ERootState::RELAX;
        } else {
            CurrentProposedStorageConfig.Clear();
            SwitchToError("no quorum for ProposedStorageConfig");
        }
    }

    void TDistributedConfigKeeper::PrepareScatterTask(ui64 cookie, TScatterTask& task) {
        switch (task.Request.GetRequestCase()) {
            case TEvScatter::kCollectConfigs: {
                std::vector<TString> drives;
                EnumerateConfigDrives(*StorageConfig, 0, [&](const auto& /*node*/, const auto& drive) {
                    drives.push_back(drive.GetPath());
                });
                if (ProposedStorageConfig) {
                    EnumerateConfigDrives(*ProposedStorageConfig, 0, [&](const auto& /*node*/, const auto& drive) {
                        drives.push_back(drive.GetPath());
                    });
                }
                std::sort(drives.begin(), drives.end());
                drives.erase(std::unique(drives.begin(), drives.end()), drives.end());
                auto query = std::bind(&TThis::ReadConfig, TActivationContext::ActorSystem(), SelfId(), drives, Cfg, cookie);
                Send(MakeIoDispatcherActorId(), new TEvInvokeQuery(std::move(query)));
                task.AsyncOperationsPending = true;
                break;
            }

            case TEvScatter::kProposeStorageConfig:
                if (ProposedStorageConfigCookie) {
                    auto *status = task.Response.MutableProposeStorageConfig()->AddStatus();
                    SelfNode.Serialize(status->MutableNodeId());
                    status->SetStatus(TEvGather::TProposeStorageConfig::RACE);
                } else {
                    ProposedStorageConfigCookie.emplace(cookie);
                    ProposedStorageConfig.emplace(task.Request.GetProposeStorageConfig().GetConfig());

                    PersistConfig([this, cookie](TEvPrivate::TEvStorageConfigStored& msg) {
                        Y_ABORT_UNLESS(ProposedStorageConfigCookie);
                        Y_ABORT_UNLESS(cookie == ProposedStorageConfigCookie);
                        ProposedStorageConfigCookie.reset();

                        if (auto it = ScatterTasks.find(cookie); it != ScatterTasks.end()) {
                            TScatterTask& task = it->second;

                            auto *status = task.Response.MutableProposeStorageConfig()->AddStatus();
                            SelfNode.Serialize(status->MutableNodeId());
                            status->SetStatus(TEvGather::TProposeStorageConfig::ACCEPTED);
                            for (const auto& [path, ok, guid] : msg.StatusPerPath) {
                                if (ok) {
                                    auto *drive = status->AddSuccessfulDrives();
                                    drive->SetPath(path);
                                    if (guid) {
                                        drive->SetGuid(*guid);
                                    }
                                }
                            }

                            if (StorageConfig && StorageConfig->GetGeneration()) {
                                Y_ABORT_UNLESS(ProposedStorageConfig);
                                
                                // TODO(alexvru): check if this is valid
                                Y_DEBUG_ABORT_UNLESS(StorageConfig->GetGeneration() < ProposedStorageConfig->GetGeneration() || (
                                    StorageConfig->GetGeneration() == ProposedStorageConfig->GetGeneration() &&
                                    StorageConfig->GetFingerprint() == ProposedStorageConfig->GetFingerprint()));

                                const TActorId wardenId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
                                auto ev = std::make_unique<TEvNodeWardenStorageConfig>(*StorageConfig,
                                    StorageConfig->GetGeneration() < ProposedStorageConfig->GetGeneration()
                                        ? &ProposedStorageConfig.value()
                                        : nullptr);
                                Send(wardenId, ev.release(), 0, cookie);
                            } else {
                                FinishAsyncOperation(cookie);
                            }
                        }
                    });

                    task.AsyncOperationsPending = true;
                }
                break;

            case TEvScatter::REQUEST_NOT_SET:
                break;
        }
    }

    void TDistributedConfigKeeper::PerformScatterTask(TScatterTask& task) {
        switch (task.Request.GetRequestCase()) {
            case TEvScatter::kCollectConfigs:
                Perform(task.Response.MutableCollectConfigs(), task.Request.GetCollectConfigs(), task);
                break;

            case TEvScatter::kProposeStorageConfig:
                Perform(task.Response.MutableProposeStorageConfig(), task.Request.GetProposeStorageConfig(), task);
                break;

            case TEvScatter::REQUEST_NOT_SET:
                // unexpected case
                break;
        }
    }

    void TDistributedConfigKeeper::Perform(TEvGather::TCollectConfigs *response,
            const TEvScatter::TCollectConfigs& /*request*/, TScatterTask& task) {
        THashMap<TStorageConfigMeta, TEvGather::TCollectConfigs::TNode*> baseConfigs;

        auto addBaseConfig = [&](const TEvGather::TCollectConfigs::TNode& item) {
            const auto& config = item.GetBaseConfig();
            auto& ptr = baseConfigs[config];
            if (!ptr) {
                ptr = response->AddNodes();
                ptr->MutableBaseConfig()->CopyFrom(config);
            }
            for (const auto& node : item.GetNodeIds()) {
                ptr->AddNodeIds()->CopyFrom(node);
            }
        };

        auto addPerDiskConfig = [&](const TEvGather::TCollectConfigs::TPersistentConfig& item, auto addFunc, auto& set) {
            const auto& config = item.GetConfig();
            auto& ptr = set[config];
            if (!ptr) {
                ptr = (response->*addFunc)();
                ptr->MutableConfig()->CopyFrom(config);
            }
            for (const auto& disk : item.GetDisks()) {
                ptr->AddDisks()->CopyFrom(disk);
            }
        };

        TEvGather::TCollectConfigs::TNode s;
        SelfNode.Serialize(s.AddNodeIds());
        auto *cfg = s.MutableBaseConfig();
        cfg->CopyFrom(BaseConfig);
        addBaseConfig(s);

        THashMap<TStorageConfigMeta, TEvGather::TCollectConfigs::TPersistentConfig*> committedConfigs;
        THashMap<TStorageConfigMeta, TEvGather::TCollectConfigs::TPersistentConfig*> proposedConfigs;

        for (const auto& reply : task.CollectedResponses) {
            if (reply.HasCollectConfigs()) {
                const auto& cc = reply.GetCollectConfigs();
                for (const auto& item : cc.GetNodes()) {
                    addBaseConfig(item);
                }
                for (const auto& item : cc.GetCommittedConfigs()) {
                    addPerDiskConfig(item, &TEvGather::TCollectConfigs::AddCommittedConfigs, committedConfigs);
                }
                for (const auto& item : cc.GetProposedConfigs()) {
                    addPerDiskConfig(item, &TEvGather::TCollectConfigs::AddProposedConfigs, proposedConfigs);
                }
            }
        }
    }

    void TDistributedConfigKeeper::Perform(TEvGather::TProposeStorageConfig *response,
            const TEvScatter::TProposeStorageConfig& /*request*/, TScatterTask& task) {
        for (const auto& reply : task.CollectedResponses) {
            if (reply.HasProposeStorageConfig()) {
                response->MutableStatus()->MergeFrom(reply.GetProposeStorageConfig().GetStatus());
            }
        }
    }

} // NKikimr::NStorage
