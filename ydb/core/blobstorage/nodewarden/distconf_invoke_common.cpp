#include "distconf_invoke.h"
#include "distconf_quorum.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/util/address_classifier.h>

#include <ydb/library/aclib/aclib.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    namespace {
        void SendInvokeResult(std::unique_ptr<IEventHandle> handle, TActorId sessionId) {
            if (sessionId) {
                handle->Rewrite(TEvInterconnect::EvForward, sessionId);
            }
            TActivationContext::Send(handle.release());
        }

        void SendInvokeResult(TActorId recipient, TActorId sender, ui64 cookie, TActorId sessionId,
                              std::unique_ptr<TEvNodeConfigInvokeOnRootResult> response) {
            SendInvokeResult(std::make_unique<IEventHandle>(recipient, sender, response.release(), 0, cookie), sessionId);
        }

        bool IsClusterStateReached(const NKikimrBridge::TClusterState& current, const NKikimrBridge::TClusterState& requested) {
            if (current.GetGeneration() < requested.GetGeneration()) {
                return false;
            }
            if (current.GetPrimaryPile() != requested.GetPrimaryPile() || current.GetPromotedPile() != requested.GetPromotedPile()) {
                return false;
            }
            if (current.PerPileStateSize() != requested.PerPileStateSize()) {
                return false;
            }
            for (size_t i = 0; i < current.PerPileStateSize(); ++i) {
                if (current.GetPerPileState(i) != requested.GetPerPileState(i)) {
                    return false;
                }
            }
            return true;
        }
    }

    TInvokeRequestHandlerActor::TInvokeRequestHandlerActor(TDistributedConfigKeeper *self, TInvokeQuery&& query,
            std::shared_ptr<TFreshConfigQueryGroup> freshConfigQueryGroup)
        : TActor(&TThis::StateFunc)
        , Self(self)
        , LifetimeToken(Self->LifetimeToken)
        , InvokePipelineGeneration(Self->InvokePipelineGeneration)
        , Query(std::move(query))
        , FreshConfigQueryGroup(std::move(freshConfigQueryGroup))
    {}

    TInvokeRequestHandlerActor::TInvokeRequestHandlerActor(TDistributedConfigKeeper *self,
            TInvokeExternalOperation&& query, ui32 hopNodeId)
        : TActor(&TThis::StateFunc)
        , Self(self)
        , LifetimeToken(Self->LifetimeToken)
        , InvokePipelineGeneration(0) // we don't care about pipeline
        , Query(std::move(query))
        , WaitingReplyFromNode(hopNodeId)
        , Subscriptions{{hopNodeId, {}}}
    {}

    void TInvokeRequestHandlerActor::HandleExecuteQuery() {
        YDB_LOG_DEBUG("HandleExecuteQuery",
            {"marker", "NWDC42"},
            {"selfId", SelfId()},
            {"binding", Self->Binding},
            {"rootState", Self->RootState},
            {"errorReason", Self->ErrorReason},
            {"queryInvokePipelineGeneration", InvokePipelineGeneration},
            {"keeperInvokePipelineGeneration", Self->InvokePipelineGeneration});

        if (InvokePipelineGeneration == Self->InvokePipelineGeneration) {
            Y_ABORT_UNLESS(!Self->Binding);
            if (Self->RootState == ERootState::RELAX) {
                Y_ABORT_UNLESS(Self->Scepter); // node must have scepter
                Self->RootState = ERootState::IN_PROGRESS;
            } else {
                Y_ABORT_UNLESS(Self->RootState == ERootState::INITIAL);
                Y_ABORT_UNLESS(!Self->Scepter); // this operation is invoked without a scepter
                InvokedWithoutScepter = true;
            }
            if (FreshConfigQueryGroup) {
                FreshConfigQueryGroup->Started = true;
            }
            ExecuteQuery();
        } else {
            // TEvAbortQuery will come soon (must be already in mailbox)
            Y_ABORT_UNLESS(InvokePipelineGeneration < Self->InvokePipelineGeneration);
        }
    }

    void TInvokeRequestHandlerActor::Handle(TEvPrivate::TEvAbortQuery::TPtr ev) {
        Finish(TResult::RACE, ev->Get()->ErrorReason);
    }

    void TInvokeRequestHandlerActor::Handle(TEvNodeConfigInvokeOnRootResult::TPtr ev) {
        auto *op = std::get_if<TInvokeExternalOperation>(&Query);
        Y_ABORT_UNLESS(op);
        if (ev->HasEvent()) {
            TActivationContext::Send(new IEventHandle(op->Sender, SelfId(), ev->ReleaseBase().Release(), ev->Flags,
                op->Cookie));
        } else {
            TActivationContext::Send(new IEventHandle(ev->Type, ev->Flags, op->Sender, SelfId(), ev->ReleaseChainBuffer(),
                op->Cookie));
        }
        PassAway();
    }

    void TInvokeRequestHandlerActor::Handle(TEvInterconnect::TEvNodeConnected::TPtr ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        const auto it = Subscriptions.find(nodeId);
        Y_ABORT_UNLESS(it != Subscriptions.end());
        Y_ABORT_UNLESS(!it->second || it->second == ev->Sender);
        it->second = ev->Sender;
        SendPendingVStatusQueries(nodeId, ev->Sender);
    }

    void TInvokeRequestHandlerActor::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        const auto it = Subscriptions.find(nodeId);
        Y_ABORT_UNLESS(it != Subscriptions.end());
        Y_ABORT_UNLESS(!it->second || it->second == ev->Sender);
        Subscriptions.erase(it);
        VStatusQueriesAwaitingConnection.erase(nodeId);
        for (auto [begin, end] = NodeToVDisk.equal_range(nodeId); begin != end; ++begin) {
            OnVStatusError(begin->second);
        }
        if (nodeId == WaitingReplyFromNode) {
            if (const auto *op = std::get_if<TInvokeExternalOperation>(&Query);
                    op && op->Command.HasSwitchBridgeClusterState() &&
                    Self->StorageConfig && Self->StorageConfig->HasClusterState() &&
                    IsClusterStateReached(Self->StorageConfig->GetClusterState(),
                                          op->Command.GetSwitchBridgeClusterState().GetNewClusterState())) {
                return Finish(TResult::OK, std::nullopt);
            }
            throw TExRace() << "Hop node disconnected";
        }
    }

    void TInvokeRequestHandlerActor::UnsubscribeInterconnect() {
        for (const auto& [nodeId, proxyId] : Subscriptions) {
            const TActorId actorId = proxyId ? proxyId : TActivationContext::InterconnectProxy(nodeId);
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, actorId, SelfId(), nullptr, 0));
        }
    }

    void TInvokeRequestHandlerActor::ExecuteQuery() {
        std::visit(TOverloaded{
            [&](TInvokeExternalOperation& op) {
                YDB_LOG_DEBUG("ExecuteQuery",
                    {"marker", "NWDC43"},
                    {"selfId", SelfId()},
                    {"command", op.Command});
                switch (op.Command.GetRequestCase()) {
                    case TQuery::kUpdateConfig:
                        return UpdateConfig(op.Command.MutableUpdateConfig());

                    case TQuery::kQueryConfig:
                        return QueryConfig(op.Command.GetQueryConfig());

                    case TQuery::kReassignGroupDisk:
                        return ReassignGroupDisk(op.Command.GetReassignGroupDisk());

                    case TQuery::kStaticVDiskSlain:
                        return StaticVDiskSlain(op.Command.GetStaticVDiskSlain());

                    case TQuery::kDropDonor:
                        return DropDonor(op.Command.GetDropDonor());

                    case TQuery::kReassignStateStorageNode:
                        return ReassignStateStorageNode(op.Command.GetReassignStateStorageNode());

                    case TQuery::kAdvanceGeneration:
                        return AdvanceGeneration();

                    case TQuery::kFetchStorageConfig:
                        return FetchStorageConfig(op.Command.GetFetchStorageConfig());

                    case TQuery::kReplaceStorageConfig:
                        return ReplaceStorageConfig(op.Command.GetReplaceStorageConfig());

                    case TQuery::kBootstrapCluster:
                        return BootstrapCluster(op.Command.GetBootstrapCluster());

                    case TQuery::kSwitchBridgeClusterState:
                        return SwitchBridgeClusterState(op.Command.GetSwitchBridgeClusterState());

                    case TQuery::kReconfigStateStorage:
                        return ReconfigStateStorage(op.Command.GetReconfigStateStorage());

                    case TQuery::kGetStateStorageConfig:
                        return GetStateStorageConfig(op.Command.GetGetStateStorageConfig());

                    case TQuery::kSelfHealStateStorage:
                        return SelfHealStateStorage(op.Command.GetSelfHealStateStorage());

                    case TQuery::kSelfHealNodesStateUpdate:
                        return SelfHealNodesStateUpdate(op.Command.GetSelfHealNodesStateUpdate());

                    case TQuery::kNotifyBridgeSyncFinished:
                        return NotifyBridgeSyncFinished(op.Command.GetNotifyBridgeSyncFinished());

                    case TQuery::kUpdateBridgeGroupInfo:
                        return UpdateBridgeGroupInfo(op.Command.GetUpdateBridgeGroupInfo());

                    case TQuery::kNotifyBridgeSuspended:
                        return NotifyBridgeSuspended(op.Command.GetNotifyBridgeSuspended());

                    case TQuery::kDescendCommittedStorageConfig:
                        return DescendCommittedStorageConfig(op.Command.GetDescendCommittedStorageConfig());

                    case TQuery::kDemandRetroTrace:
                        return DemandRetroTrace(op.Command.GetDemandRetroTrace());

                    case TQuery::kConvertToBridge:
                        return ConvertToBridge(op.Command.GetConvertToBridge());

                    case TQuery::kNotifyBscConversionFinished:
                        return NotifyBscConversionFinished(op.Command.GetNotifyBscConversionFinished());

                    case TQuery::kAddBridgePile:
                        return AddBridgePile(op.Command.GetAddBridgePile());

                    case TQuery::kNotifyBridgePileOperation:
                        return NotifyBridgePileOperation(op.Command.GetNotifyBridgePileOperation());

                    case TQuery::kRemoveBridgePile:
                        return RemoveBridgePile(op.Command.GetRemoveBridgePile());

                    case TQuery::REQUEST_NOT_SET:
                        throw TExError() << "Request field not set";

                    case TQuery::kQueryWorkingRoot:
                        Y_ABORT("QueryWorkingRoot must be handled directly by the keeper");

                    case TQuery::kQueryPileConnectivity:
                        Y_ABORT("QueryPileConnectivity must be handled directly by the keeper");

                    case TQuery::kValidateTargetedProposition:
                        Y_ABORT("ValidateTargetedProposition must be handled directly by the keeper");
                }

                throw TExError() << "Unhandled request";
            },
            [&](TCollectConfigsAndPropose&) {
                YDB_LOG_DEBUG("Starting config collection",
                    {"marker", "NWDC19"});

                TEvScatter task;
                task.MutableCollectConfigs();
                const ui64 recoveryGeneration = Self->ConfigRecoveryGeneration;
                IssueScatterTask(std::move(task), [this, recoveryGeneration](TEvGather *res) {
                    Y_ABORT_UNLESS(Self->StorageConfig); // it can't just disappear
                    Y_ABORT_UNLESS(!Self->CurrentProposition);

                    if (recoveryGeneration != Self->ConfigRecoveryGeneration) {
                        throw TExRace() << "Configuration changed while collecting configs";
                    } else if (!res->HasCollectConfigs()) {
                        throw TExError() << "Incorrect CollectConfigs response";
                    } else if (auto r = Self->ProcessCollectConfigs(res->MutableCollectConfigs(), std::nullopt); r.ErrorReason) {
                        throw TExError() << *r.ErrorReason;
                    } else if (r.IsDistconfDisabledQuorum) {
                        Self->ConfigRecoveryRequired = false;
                        Finish(TResult::OK, std::nullopt);
                    } else if (r.ConfigToPropose) {
                        RecoveryProposition = r.RequiresRecoveryProposition;
                        StartProposition(&r.ConfigToPropose.value(), !r.RequiresRecoveryProposition,
                            r.PropositionBase ? &r.PropositionBase.value() : nullptr, r.AutomaticBootstrap);
                    } else {
                        Finish(TResult::OK, std::nullopt);
                    }
                });
            },
            [&](TProposeConfig& op) {
                StartProposition(&op.Config);
            }
        }, Query);
    }

    void TInvokeRequestHandlerActor::IssueScatterTask(TEvScatter&& task, TGatherCallback callback) {
        const ui64 cookie = NextScatterCookie++;
        const auto [it, inserted] = ScatterTasks.try_emplace(cookie, std::move(callback));
        Y_ABORT_UNLESS(inserted);

        task.SetTaskId(RandomNumber<ui64>());
        task.SetCookie(cookie);
        Self->IssueScatterTask(SelfId(), std::move(task));
    }

    void TInvokeRequestHandlerActor::Handle(TEvNodeConfigGather::TPtr ev) {
        auto& record = ev->Get()->Record;
        YDB_LOG_DEBUG("Handle(TEvNodeConfigGather)",
            {"marker", "NWDC44"},
            {"selfId", SelfId()},
            {"record", record});
        if (record.GetAborted()) {
            throw TExRace() << "Scatter task was aborted due to loss of quorum or other error";
        }

        const auto it = ScatterTasks.find(record.GetCookie());
        Y_ABORT_UNLESS(it != ScatterTasks.end());
        TGatherCallback callback = std::move(it->second);
        ScatterTasks.erase(it);

        // leave it to the end as it may throw exceptions
        callback(&record);
    }

    void TInvokeRequestHandlerActor::UpdateConfig(TQuery::TUpdateConfig *request) {
        RunCommonChecks();
        StartProposition(request->MutableConfig());
    }

    void TInvokeRequestHandlerActor::DemandRetroTrace(const TQuery::TDemandRetroTrace& cmd) {
        for (const auto& proto : cmd.GetTraceId()) {
            NWilson::TTraceId traceId(proto);
            if (traceId) {
                Self->PendingRetroTraceIds.push_back(std::move(traceId));
            }
        }

        if (!std::exchange(Self->RetroTraceBatchFlushScheduled, true)) {
            TActivationContext::Schedule(TDuration::Seconds(Self->RootRetroTraceBatchIntervalSec),
                    new IEventHandle(TEvPrivate::EvFlushRetroTraceBatch, 0, Self->SelfId(), {}, nullptr, 0));
        }

        Finish(TResult::OK, std::nullopt, {}, false);
    }

    void TInvokeRequestHandlerActor::DescendCommittedStorageConfig(const TQuery::TDescendCommittedStorageConfig& request) {
        if (!Self->BridgeInfo) {
            throw TExError() << "Not in bridge mode";
        } else {
            const auto& config = request.GetCommittedStorageConfig();
            if (!Self->CommittedStorageConfig || Self->CommittedStorageConfig->GetGeneration() < config.GetGeneration()) {
                Self->ApplyCommittedStorageConfig(config);
            }
            Finish(TResult::OK, std::nullopt);
        }
    }

    void TInvokeRequestHandlerActor::AdvanceGeneration() {
        RunCommonChecks();
        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        StartProposition(&config);
    }

    void TInvokeRequestHandlerActor::QueryConfig(const TQuery::TQueryConfig& request) {
        auto reply = [this, fresh = request.GetRequireFresh() && !Self->IsBridged()] {
            Finish(TResult::OK, std::nullopt, [&](auto *record) {
                auto *response = record->MutableQueryConfig();
                if (Self->StorageConfig) {
                    response->MutableConfig()->CopyFrom(*Self->StorageConfig);
                }
                response->SetFresh(fresh);
            });
        };
        if (request.GetRequireFresh() && !Self->IsBridged()) {
            CollectConfig([reply = std::move(reply)](TProcessCollectConfigsResult&&) { reply(); });
        } else {
            reply();
        }
    }

    void TInvokeRequestHandlerActor::CollectConfig(std::function<void(TProcessCollectConfigsResult&&)> callback,
                                                 bool allowConflictingGenerationAdvance) {
        if (!Self->StorageConfig || !Self->Scepter) {
            throw TExRace() << "No working root for configuration recovery";
        }
        TEvScatter task;
        task.MutableCollectConfigs();
        const ui64 recoveryGeneration = Self->ConfigRecoveryGeneration;
        IssueScatterTask(std::move(task), [this, recoveryGeneration, callback = std::move(callback), allowConflictingGenerationAdvance]
                                        (TEvGather *res) {
            if (recoveryGeneration != Self->ConfigRecoveryGeneration) {
                throw TExRace() << "Configuration changed while collecting configs";
            }
            if (!res->HasCollectConfigs()) {
                throw TExError() << "Incorrect CollectConfigs response";
            }
            auto result = Self->ProcessCollectConfigs(res->MutableCollectConfigs(), std::nullopt, true,
                                                     allowConflictingGenerationAdvance);
            if (result.ErrorReason || result.IsDistconfDisabledQuorum || !result.RecoveredConfig) {
                throw TExNoQuorum() << result.ErrorReason.value_or("No recoverable configuration");
            }
            if (result.RequiresRecoveryProposition) {
                return StartRecoveryProposition(result);
            }
            if (!result.RequiresGenerationAdvance) {
                Self->ConfigRecoveryRequired = false;
                InvokeOtherActor(*Self, &TDistributedConfigKeeper::ApplyCommittedStorageConfig, *result.RecoveredConfig);
            }
            callback(std::move(result));
        });
    }

    void TInvokeRequestHandlerActor::StartRecoveryProposition(TProcessCollectConfigsResult& result) {
        Y_ABORT_UNLESS(result.RequiresRecoveryProposition && result.ConfigToPropose && result.PropositionBase);
        RecoveryProposition = true;
        StartPropositionImpl(&*result.ConfigToPropose, false, &*result.PropositionBase, false);
    }

    void TInvokeRequestHandlerActor::CollectBridgeConfig(const NKikimrBlobStorage::TStorageConfig& config,
            std::function<void()> callback) {
        TEvScatter task;
        task.MutableCollectConfigs();
        auto quorumConfig = *Self->StorageConfig;
        quorumConfig.MutableClusterState()->CopyFrom(config.GetClusterState());
        IssueScatterTask(std::move(task), [this, quorumConfig = std::move(quorumConfig), callback = std::move(callback)]
                                        (TEvGather *res) {
            if (!res->HasCollectConfigs() || res->GetCollectConfigs().GetMetadataOnly()) {
                throw TExError() << "Configuration bodies are required for bridge configuration changes";
            }
            std::vector<TSuccessfulDisk> successfulDisks;
            const NKikimrBlobStorage::TStorageConfig *committedConfig = nullptr;
            bool committedConflict = false;
            const auto& collected = res->GetCollectConfigs();
            for (auto&& [field, isCommitted] : {
                        std::make_tuple(&collected.GetCommittedConfigs(), true),
                        std::make_tuple(&collected.GetProposedConfigs(), false),
                    }) {
                for (const auto& item : *field) {
                    const auto& config = item.GetConfig();
                    if (!Self->ObserveConfigGeneration(config) || !config.GetGeneration() || !item.DisksSize()) {
                        continue;
                    }
                    for (const auto& disk : item.GetDisks()) {
                        successfulDisks.emplace_back(disk.GetNodeId(), disk.GetPath(),
                            disk.HasGuid() ? std::make_optional(disk.GetGuid()) : std::nullopt);
                    }
                    if (isCommitted) {
                        if (!committedConfig || config.GetGeneration() > committedConfig->GetGeneration()) {
                            committedConfig = &config;
                            committedConflict = false;
                        } else if (config.GetGeneration() == committedConfig->GetGeneration()
                                   && config.GetFingerprint() != committedConfig->GetFingerprint()) {
                            committedConflict = true;
                        }
                    }
                }
            }
            if (committedConflict) {
                throw TExNoQuorum() << "Conflicting committed bridge configurations";
            }
            if (committedConfig) {
                for (const auto *applied : {Self->StorageConfig.get(), Self->CommittedStorageConfig.get(),
                                             Self->LocalCommittedStorageConfig.get()}) {
                    if (applied && applied->GetGeneration() == committedConfig->GetGeneration()
                        && applied->GetFingerprint() != committedConfig->GetFingerprint()) {
                        throw TExNoQuorum() << "Conflicting committed bridge configurations";
                    }
                }
                if (committedConfig->GetGeneration() > Self->StorageConfig->GetGeneration()) {
                    if (const auto error = ValidateConfig(*committedConfig)) {
                        throw TExError() << "Invalid committed bridge configuration: " << *error;
                    }
                    InvokeOtherActor(*Self, &TDistributedConfigKeeper::ApplyCommittedStorageConfig, *committedConfig);
                    throw TExRace() << "Configuration was recovered; retry the configuration update";
                }
            }
            TStringStream err;
            if (!HasConfigQuorum(quorumConfig, successfulDisks, TBridgePileId(), *Self->Cfg, false, &err)) {
                throw TExNoQuorum() << "No quorum for bridge configuration collection:" << err.Str();
            }
            auto result = Self->ProcessCollectConfigs(res->MutableCollectConfigs(), std::nullopt, true, false,
                                                     &quorumConfig);
            if (result.ErrorReason || !result.RecoveredConfig) {
                throw TExNoQuorum() << result.ErrorReason.value_or("No recoverable bridge configuration");
            }
            if (result.RequiresRecoveryProposition) {
                return StartRecoveryProposition(result);
            }
            if (TStorageConfigMeta(*result.RecoveredConfig) != TStorageConfigMeta(*Self->StorageConfig)) {
                if (result.RecoveredConfig->GetGeneration() <= Self->StorageConfig->GetGeneration()) {
                    throw TExNoQuorum() << "The current bridge configuration has not been recovered";
                }
                InvokeOtherActor(*Self, &TDistributedConfigKeeper::ApplyCommittedStorageConfig, *result.RecoveredConfig);
                throw TExRace() << "Configuration was recovered; retry the configuration update";
            }
            callback();
        });
    }

    void TInvokeRequestHandlerActor::StartProposition(NKikimrBlobStorage::TStorageConfig *config, bool mindPrev,
            const NKikimrBlobStorage::TStorageConfig *propositionBase, bool fromBootstrap) {
        if ((Self->IsBridged() || Self->SelfManagementEnabled) && Self->StorageConfig->GetGeneration()
            && !std::holds_alternative<TCollectConfigsAndPropose>(Query)) {
            const TStorageConfigMeta base(*Self->StorageConfig);
            auto savedBase = propositionBase ? std::make_optional(*propositionBase) : std::nullopt;
            const auto *op = std::get_if<TInvokeExternalOperation>(&Query);
            const bool allowConflictingGenerationAdvance = op && op->Command.HasReplaceStorageConfig();
            auto start = [this, config = *config, mindPrev, savedBase = std::move(savedBase), fromBootstrap, base]
                         (bool advancesConflictingGeneration) mutable {
                if (base != TStorageConfigMeta(*Self->StorageConfig)) {
                    throw TExRace() << "Configuration was recovered; retry the configuration update";
                }
                AdvancesConflictingGeneration = advancesConflictingGeneration;
                StartPropositionImpl(&config, mindPrev, savedBase ? &*savedBase : nullptr, fromBootstrap);
            };
            if (Self->IsBridged() && !mindPrev) {
                CollectBridgeConfig(*config, [start = std::move(start)]() mutable { start(false); });
            } else {
                CollectConfig([start = std::move(start)](TProcessCollectConfigsResult&& result) mutable {
                    start(result.RequiresGenerationAdvance);
                }, allowConflictingGenerationAdvance);
            }
        } else {
            StartPropositionImpl(config, mindPrev, propositionBase, fromBootstrap);
        }
    }

    void TInvokeRequestHandlerActor::StartPropositionImpl(NKikimrBlobStorage::TStorageConfig *config, bool mindPrev,
            const NKikimrBlobStorage::TStorageConfig *propositionBase, bool fromBootstrap) {
        if (!Self->HasConnectedNodeQuorum(*config)) {
            // we won't be able to have quorum for this configuration if we start proposing now
            throw TExNoQuorum() << "No quorum to start propose/commit configuration";
        }

        if (const auto *op = std::get_if<TInvokeExternalOperation>(&Query); op && !RecoveryProposition) {
            if (const auto& record = op->Command; record.HasReplaceStorageConfig()) {
                AUDIT_LOG(
                    const auto& replaceConfig = record.GetReplaceStorageConfig();

                    const TString oldConfig = TStringBuilder()
                        << Self->MainConfigYaml
                        << Self->StorageConfigYaml.value_or("");

                    TStringBuilder newConfig;
                    if (replaceConfig.HasYAML()) {
                        newConfig << replaceConfig.GetYAML();
                    } else {
                        newConfig << Self->MainConfigYaml;
                    }
                    if (replaceConfig.HasStorageYAML()) {
                        newConfig << replaceConfig.GetStorageYAML();
                    } else if (replaceConfig.HasSwitchDedicatedStorageSection() && !replaceConfig.GetSwitchDedicatedStorageSection()) {
                        // dedicated storage YAML is switched off by this operation -- no storage config will be set
                    } else if (Self->StorageConfigYaml) {
                        newConfig << *Self->StorageConfigYaml;
                    }

                    NACLib::TUserToken userToken(replaceConfig.GetUserToken());

                    auto wrapEmpty = [](const TString& value) { return value ? value : TString("{none}"); };

                    AUDIT_PART("component", "distconf")
                    AUDIT_PART("remote_address", wrapEmpty(NKikimr::NAddressClassifier::ExtractAddress(replaceConfig.GetPeerName())))
                    AUDIT_PART("subject", wrapEmpty(userToken.GetUserSID()))
                    AUDIT_PART("sanitized_token", wrapEmpty(userToken.GetSanitizedToken()))
                    AUDIT_PART("status", "SUCCESS")
                    AUDIT_PART("reason", TString(), false)
                    AUDIT_PART("operation", "REPLACE CONFIG")
                    AUDIT_PART("old_config", oldConfig)
                    AUDIT_PART("new_config", newConfig)
                );
            }
        }

        if (!propositionBase) {
            propositionBase = Self->StorageConfig.get();
        }

        if (propositionBase && !propositionBase->GetGeneration() && !fromBootstrap) {
            throw TExError() << "First distconf config-changing command has to be BootstrapCluster";
        }

        Y_ABORT_UNLESS(InvokePipelineGeneration == Self->InvokePipelineGeneration);
        auto error = InvokeOtherActor(*Self, &TDistributedConfigKeeper::StartProposition, config, propositionBase,
                                      SelfId(), mindPrev, AdvancesConflictingGeneration, RecoveryProposition);
        if (error) {
            YDB_LOG_DEBUG("Config update validation failed",
                {"marker", "NWDC78"},
                {"selfId", SelfId()},
                {"error", *error},
                {"proposedConfig", *config});
            throw TExError() << "Config update validation failed: " << *error;
        }
    }

    void TInvokeRequestHandlerActor::Handle(TEvPrivate::TEvConfigProposed::TPtr ev) {
        auto& msg = *ev->Get();

        YDB_LOG_DEBUG("OnConfigProposed",
            {"marker", "NWDC64"},
            {"selfId", SelfId()},
            {"errorReason", msg.ErrorReason},
            {"rootState", Self->RootState});

        if (msg.ErrorReason) {
            throw TExError() << "Config proposition failed: " << *msg.ErrorReason;
        } else if (RecoveryProposition && !std::holds_alternative<TCollectConfigsAndPropose>(Query)) {
            Finish(TResult::RACE, "Configuration was recovered; retry the original operation");
        } else if (const auto *op = std::get_if<TInvokeExternalOperation>(&Query);
                   op && op->Command.HasReplaceStorageConfig() && ControllerOp == EControllerOp::OTHER
                   && Self->SelfManagementEnabled) {
            StartCommitConfirmation();
        } else {
            Finish(TResult::OK, std::nullopt, [&](TResult *record) {
                if (ReassignGroupDiskResult) {
                    record->MutableReassignGroupDisk()->CopyFrom(*ReassignGroupDiskResult);
                }
            });
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Query termination and result delivery

    void TInvokeRequestHandlerActor::RunCommonChecks(bool requireScepter) {
        if (!Self->StorageConfig) {
            throw TExError() << "No agreed StorageConfig";
        } else if (auto error = ValidateConfig(*Self->StorageConfig)) {
            throw TExError() << "Current config validation failed: " << *error;
        } else if (requireScepter && !Self->Scepter) {
            throw TExError() << "No scepter";
        }

        if (requireScepter) {
            Y_ABORT_UNLESS(Self->RootState == ERootState::IN_PROGRESS);
        }

        Y_ABORT_UNLESS(!Self->CurrentProposition);
    }

    void TInvokeRequestHandlerActor::Finish(TResult::EStatus status, std::optional<TStringBuf> errorReason,
            const std::function<void(TResult*)>& callback, bool sendResult) {
        TResult record;
        record.SetStatus(status);
        if (errorReason) {
            record.SetErrorReason(errorReason->data(), errorReason->size());
        }
        if (Self->Scepter) {
            auto *s = record.MutableScepter();
            s->SetId(Self->Scepter->Id);
            s->SetNodeId(SelfId().NodeId());
        }
        if (callback) {
            callback(&record);
        }

        YDB_LOG_DEBUG("Finish",
            {"marker", "NWDC61"},
            {"selfId", SelfId()},
            {"record", record});

        std::optional<TString> switchToError; // when set, we will switch distconf keeper to error state with this reason

        std::visit(TOverloaded{
            [&](TInvokeExternalOperation& op) {
                if (sendResult) {
                    auto ev = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
                    record.Swap(&ev->Record);
                    auto response = std::make_unique<IEventHandle>(op.Sender, SelfId(), ev.release(), 0, op.Cookie);
                    if (FreshConfigQueryGroup && (!FreshConfigQueryGroup->AdditionalRequests.empty()
                        || !FreshConfigQueryGroup->RefillRequests.empty())) {
                        auto buffer = response->ReleaseChainBuffer();
                        for (const auto& request : FreshConfigQueryGroup->AdditionalRequests) {
                            SendInvokeResult(std::make_unique<IEventHandle>(TEvNodeConfigInvokeOnRootResult::EventType, 0,
                                request.Sender, SelfId(), buffer, request.Cookie), request.SessionId);
                        }
                        for (const auto& [nodeId, request] : FreshConfigQueryGroup->RefillRequests) {
                            SendInvokeResult(std::make_unique<IEventHandle>(TEvNodeConfigInvokeOnRootResult::EventType, 0,
                                request.Sender, SelfId(), buffer, request.Cookie), request.SessionId);
                        }
                        if (op.Command.GetQueryConfig().GetRefillNodeId()) {
                            return;
                        }
                        response = std::make_unique<IEventHandle>(TEvNodeConfigInvokeOnRootResult::EventType, 0,
                            op.Sender, SelfId(), std::move(buffer), op.Cookie);
                    }
                    SendInvokeResult(std::move(response), op.SessionId);
                }
            },
            [&](TCollectConfigsAndPropose&) {
                if (InvokePipelineGeneration == Self->InvokePipelineGeneration && status == TResult::OK) {
                    Self->ConfigRecoveryScheduled = false;
                    if (Self->ConfigRecoveryRequired) {
                        InvokeOtherActor(*Self, &TDistributedConfigKeeper::ScheduleConfigRecovery);
                    }
                } else if (InvokePipelineGeneration == Self->InvokePipelineGeneration) {
                    // reschedule operation
                    TActivationContext::Schedule(Self->CollectConfigsBackoffTimer.Next(),
                        new IEventHandle(TEvPrivate::EvRetryCollectConfigsAndPropose, 0, Self->SelfId(), {}, nullptr,
                            InvokePipelineGeneration));
                }
            },
            [&](TProposeConfig&) {
                if (status != TResult::OK && InvokePipelineGeneration == Self->InvokePipelineGeneration) {
                    // we were asked to commit config, but error has occured
                    Y_ABORT_UNLESS(errorReason);
                    switchToError.emplace(*errorReason);
                }
            }
        }, Query);

        // terminate this actor
        PassAway();

        // reset root state in keeper actor if this query is still valid and there is no pending proposition
        if (InvokePipelineGeneration == Self->InvokePipelineGeneration && !Self->CurrentProposition && !InvokedWithoutScepter) {
            if (Self->RootState == ERootState::IN_PROGRESS) {
                Self->RootState = ERootState::RELAX;
            } else {
                Y_ABORT_UNLESS(Self->RootState == ERootState::INITIAL); // this operation has been executed without a scepter
            }
        }

        if (switchToError) {
            InvokeOtherActor(*Self, &TDistributedConfigKeeper::SwitchToError, std::move(*switchToError));
        }
    }

    void TInvokeRequestHandlerActor::DetachQuery() {
        Y_ABORT_UNLESS(!LifetimeToken.expired()); // otherwise we shouldn't handle this message at all
        Y_ABORT_UNLESS(!WaitingReplyFromNode); // same thing
        Y_ABORT_UNLESS(!Detached); // can't be called twice

        Detached = true;
        Self->DetachedQueries.insert(SelfId());

        // reset parent state in the same way we would do when we finish processing this query
        if (InvokePipelineGeneration == Self->InvokePipelineGeneration && !Self->CurrentProposition && !InvokedWithoutScepter) {
            if (Self->RootState == ERootState::IN_PROGRESS) {
                Self->RootState = ERootState::RELAX;
            } else {
                Y_ABORT_UNLESS(Self->RootState == ERootState::INITIAL);
            }
        }

        // notify parent actor so it could continue processing queries
        TActivationContext::Send(new IEventHandle(TEvPrivate::EvQueryFinished, 0, Self->SelfId(), SelfId(), nullptr,
            InvokePipelineGeneration));
    }

    void TInvokeRequestHandlerActor::PassAway() {
        if (!LifetimeToken.expired()) {
            if (FreshConfigQueryGroup) {
                for (const auto& [nodeId, request] : FreshConfigQueryGroup->RefillRequests) {
                    const auto it = Self->PendingRefillQueryGroups.find(nodeId);
                    if (it != Self->PendingRefillQueryGroups.end() && it->second.lock() == FreshConfigQueryGroup) {
                        Self->PendingRefillQueryGroups.erase(it);
                    }
                }
            }
            std::erase_if(Self->ConsoleConfigValidationQ, [actorId = SelfId()](const auto& item) {
                return std::get<0>(item) == actorId;
            });
            if (Detached) {
                const size_t n = Self->DetachedQueries.erase(SelfId());
                Y_ABORT_UNLESS(n == 1);
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, MakeBlobStorageNodeWardenID(
                    SelfId().NodeId()), SelfId(), nullptr, 0)); // unsubscribe from receiving config updates
            } else if (!WaitingReplyFromNode) {
                TActivationContext::Send(new IEventHandle(TEvPrivate::EvQueryFinished, 0, Self->SelfId(), SelfId(),
                    nullptr, InvokePipelineGeneration));
            }
        }

        if (ControllerPipeId) {
            NTabletPipe::CloseAndForgetClient(SelfId(), ControllerPipeId);
        }

        UnsubscribeInterconnect();

        TActor::PassAway();
    }

    STFUNC(TInvokeRequestHandlerActor::StateFunc) {
        if (LifetimeToken.expired()) {
            return PassAway();
        }
        if (!WaitingReplyFromNode && !Detached && InvokePipelineGeneration != Self->InvokePipelineGeneration
            && ev->GetTypeRewrite() != TEvPrivate::EvAbortQuery && ev->GetTypeRewrite() != TEvents::TSystem::Poison) {
            return Finish(TResult::RACE, "root operation superseded");
        }
        try {
            STRICT_STFUNC_BODY(
                cFunc(TEvPrivate::EvExecuteQuery, HandleExecuteQuery);
                hFunc(TEvPrivate::TEvAbortQuery, Handle);
                hFunc(TEvNodeConfigInvokeOnRootResult, Handle);
                hFunc(TEvNodeConfigGather, Handle);
                hFunc(TEvPrivate::TEvConfigProposed, Handle);
                hFunc(TEvInterconnect::TEvNodeConnected, Handle);
                hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
                hFunc(TEvBlobStorage::TEvVStatusResult, Handle);
                hFunc(TEvents::TEvUndelivered, Handle);
                hFunc(TEvNodeWardenBaseConfig, Handle);
                cFunc(TEvents::TSystem::Poison, PassAway);
                hFunc(TEvBlobStorage::TEvControllerValidateConfigResponse, Handle);
                hFunc(TEvTabletPipe::TEvClientConnected, Handle);
                hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
                hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
                hFunc(TEvBlobStorage::TEvControllerDistconfResponse, Handle);
                hFunc(TEvNodeWardenStorageConfig, Handle);
                fFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            )
        } catch (const TExError& error) {
            Finish(error.Status, error.what());
            if (error.IsCritical) {
                Y_DEBUG_ABORT("critical error during query processing: %s", error.what());
            }
        }
    }

    void TDistributedConfigKeeper::HandleInvokeOnRoot(TEvNodeConfigInvokeOnRoot::TPtr ev) {
        if (ev->Get()->Record.HasValidateTargetedProposition()) {
            const auto& request = ev->Get()->Record.GetValidateTargetedProposition();
            const ui32 nodeId = ev->Sender.NodeId();
            bool confirmed = false;
            if (!IsBridged() && Scepter && CurrentProposition && IsPropositionCurrent(*CurrentProposition)
                && request.HasConfig()
                && TStorageConfigMeta(request.GetConfig()) == TStorageConfigMeta(CurrentProposition->StorageConfig)) {
                const auto it = ScatterTasks.find(request.GetScatterCookie());
                if (it != ScatterTasks.end()) {
                    const TScatterTask& task = it->second;
                    const auto containsNode = [nodeId](const auto& node) { return node.GetNodeId() == nodeId; };
                    const auto& proposed = CurrentProposition->StorageConfig;
                    confirmed = std::holds_alternative<TScatterTaskOriginFsm>(task.Origin)
                                && task.ScepterCounter == ScepterCounter
                                && task.Request.HasTaskId() && task.Request.GetTaskId() == request.GetTaskId()
                                && task.Request.HasProposeStorageConfig()
                                && TStorageConfigMeta(task.Request.GetProposeStorageConfig().GetConfig())
                                   == TStorageConfigMeta(proposed)
                                && AddedNodesScatterTasks.contains({nodeId, request.GetScatterCookie()})
                                && task.PendingNodes.contains(nodeId)
                                && proposed.HasPrevConfig()
                                && !std::ranges::any_of(proposed.GetPrevConfig().GetAllNodes(), containsNode)
                                && std::ranges::any_of(proposed.GetAllNodes(), containsNode);
                }
            }
            auto response = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
            response->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
            response->Record.MutableValidateTargetedProposition()->SetConfirmed(confirmed);
            if (confirmed) {
                auto *scepter = response->Record.MutableScepter();
                scepter->SetId(Scepter->Id);
                scepter->SetNodeId(SelfId().NodeId());
            }
            SendInvokeResult(ev->Sender, SelfId(), ev->Cookie, ev->InterconnectSession, std::move(response));
            return;
        }
        if (ev->Get()->Record.HasQueryWorkingRoot()) {
            auto response = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
            response->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
            if (Scepter && HasStaticGroupConfig() && QuorumValid && GlobalQuorum) {
                auto *scepter = response->Record.MutableScepter();
                scepter->SetId(Scepter->Id);
                scepter->SetNodeId(SelfId().NodeId());
            }
            SendInvokeResult(ev->Sender, SelfId(), ev->Cookie, ev->InterconnectSession, std::move(response));
            return;
        }
        if (ev->Get()->Record.HasQueryPileConnectivity()) {
            if (!IsSelfStatic && ConnectedToStaticNode && StaticNodeSessionId) {
                // Dynamic nodes subscribe to static nodes; they have no binding subtree of their own.
                ForwardInvokeOnRoot(std::move(ev), ConnectedToStaticNode);
                return;
            }
            if (!IsSelfStatic || !(Binding && Binding->RootNodeId)) {
                auto response = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
                if (!IsSelfStatic) {
                    response->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::NO_QUORUM);
                    response->Record.SetErrorReason("No static node connection for pile connectivity query");
                } else {
                    FillPileConnectivity(response->Record);
                }
                // A component top must answer even while binding or running a stalled proposition.
                SendInvokeResult(ev->Sender, SelfId(), ev->Cookie, ev->InterconnectSession, std::move(response));
                return;
            }
        }
        if (Halted) {
            auto response = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
            response->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR);
            response->Record.SetErrorReason("distconf is halted on this node");
            SendInvokeResult(ev->Sender, SelfId(), ev->Cookie, ev->InterconnectSession, std::move(response));
        } else if (Binding && !Binding->RootNodeId) { // binding is in progess, wait for it to complete
            InvokeOnRootPending.push_back(std::move(ev));
        } else if (Binding) {
            // we have binding, so we have to forward this message to 'hop' node and return answer
            ForwardInvokeOnRoot(std::move(ev), Binding->NodeId);
        } else {
            Invoke(TInvokeExternalOperation{
                .Command = std::move(ev->Get()->Record),
                .Sender = ev->Sender,
                .SessionId = ev->InterconnectSession,
                .Cookie = ev->Cookie,
            });
        }
    }

    void TDistributedConfigKeeper::ForwardInvokeOnRoot(TEvNodeConfigInvokeOnRoot::TPtr ev, ui32 hopNodeId) {
        const TActorId actorId = RegisterWithSameMailbox(new TInvokeRequestHandlerActor(this, {
            .Command = ev->Get()->Record,
            .Sender = ev->Sender,
            .SessionId = ev->InterconnectSession,
            .Cookie = ev->Cookie,
        }, hopNodeId));
        TActivationContext::Send(new IEventHandle(MakeBlobStorageNodeWardenID(hopNodeId), actorId,
            ev->Release().Release(), IEventHandle::FlagSubscribeOnSession));
    }

    void TDistributedConfigKeeper::Invoke(TInvokeQuery&& query) {
        const bool deferExecution = RootState == ERootState::ERROR_TIMEOUT;
        auto& queue = deferExecution ? InvokePending : InvokeQ;
        std::shared_ptr<TFreshConfigQueryGroup> group;
        if (auto *op = std::get_if<TInvokeExternalOperation>(&query); op && !IsBridged()
            && op->Command.HasQueryConfig() && op->Command.GetQueryConfig().GetRequireFresh()) {
            const ui32 refillNodeId = op->Command.GetQueryConfig().GetRefillNodeId();
            if (refillNodeId) {
                const auto it = PendingRefillQueryGroups.find(refillNodeId);
                if (it != PendingRefillQueryGroups.end()) {
                    if (auto pending = it->second.lock()) {
                        auto request = std::exchange(pending->RefillRequests.at(refillNodeId), std::move(*op));
                        auto response = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
                        response->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::RACE);
                        response->Record.SetErrorReason("refill request superseded");
                        SendInvokeResult(request.Sender, SelfId(), request.Cookie, request.SessionId, std::move(response));
                        return;
                    }
                    PendingRefillQueryGroups.erase(it);
                }
            }
            group = PendingFreshConfigQueryGroup.lock();
            if (group && !group->Started && !queue.empty() && queue.back().ActorId == group->ActorId) {
                if (refillNodeId) {
                    group->RefillRequests.emplace(refillNodeId, std::move(*op));
                    PendingRefillQueryGroups[refillNodeId] = group;
                } else {
                    group->AdditionalRequests.push_back(std::move(*op));
                }
                return;
            }
            group = std::make_shared<TFreshConfigQueryGroup>();
            PendingFreshConfigQueryGroup = group;
            if (refillNodeId) {
                group->RefillRequests.emplace(refillNodeId, *op);
                PendingRefillQueryGroups[refillNodeId] = group;
            }
        }
        const TActorId actorId = RegisterWithSameMailbox(new TInvokeRequestHandlerActor(this, std::move(query), group));
        if (group) {
            group->ActorId = actorId;
        }
        queue.push_back(TInvokeOperation{actorId});
        if (!deferExecution && queue.size() == 1) {
            TActivationContext::Send(new IEventHandle(TEvPrivate::EvExecuteQuery, 0, actorId, {}, nullptr, 0));
        }
    }

    void TDistributedConfigKeeper::HandleQueryFinished(STFUNC_SIG) {
        if (ev->Cookie != InvokePipelineGeneration) {
            Y_ABORT_UNLESS(ev->Cookie < InvokePipelineGeneration);
            return; // a race with aborted query
        }

        Y_ABORT_UNLESS(!InvokeQ.empty());
        auto& front = InvokeQ.front();
        Y_ABORT_UNLESS(front.ActorId == ev->Sender);
        if (CurrentProposition) {
            Y_ABORT_UNLESS(CurrentProposition->ActorId == ev->Sender);
            Y_ABORT_UNLESS(!DeadActorWaitingForProposition);
            DeadActorWaitingForProposition = true;
        } else {
            InvokeQ.pop_front();
            if (InvokeQ.empty()) {
                CheckForConfigUpdate();
            } else {
                TActivationContext::Send(new IEventHandle(TEvPrivate::EvExecuteQuery, 0, InvokeQ.front().ActorId, {},
                    nullptr, 0));
            }
        }
    }

    void TDistributedConfigKeeper::OpQueueOnError(const TString& errorReason) {
        PendingFreshConfigQueryGroup.reset();
        PendingRefillQueryGroups.clear();
        for (const auto& item : std::exchange(InvokeQ, {})) {
            Send(item.ActorId, new TEvPrivate::TEvAbortQuery(errorReason));
        }
        DeadActorWaitingForProposition = false;
        ++InvokePipelineGeneration;
    }

} // NKikimr::NStorage
