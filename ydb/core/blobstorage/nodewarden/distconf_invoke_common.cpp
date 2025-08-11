#include "distconf_invoke.h"
#include "distconf_quorum.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/util/address_classifier.h>

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    TInvokeRequestHandlerActor::TInvokeRequestHandlerActor(TDistributedConfigKeeper *self, TInvokeQuery&& query)
        : TActor(&TThis::StateFunc)
        , Self(self)
        , LifetimeToken(Self->LifetimeToken)
        , InvokePipelineGeneration(Self->InvokePipelineGeneration)
        , Query(std::move(query))
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
        STLOG(PRI_DEBUG, BS_NODE, NWDC42, "HandleExecuteQuery",
            (SelfId, SelfId()),
            (Binding, Self->Binding),
            (RootState, Self->RootState),
            (ErrorReason, Self->ErrorReason),
            (Query.InvokePipelineGeneration, InvokePipelineGeneration),
            (Keeper.InvokePipelineGeneration, Self->InvokePipelineGeneration));

        if (InvokePipelineGeneration == Self->InvokePipelineGeneration) {
            Y_ABORT_UNLESS(!Self->Binding);

            switch (auto& state = Self->RootState) {
                case ERootState::INITIAL:
                    state = ERootState::LOCAL_QUORUM_OP;
                    break;

                case ERootState::RELAX:
                    state = ERootState::IN_PROGRESS;
                    break;

                case ERootState::LOCAL_QUORUM_OP:
                case ERootState::IN_PROGRESS:
                case ERootState::ERROR_TIMEOUT:
                    Y_ABORT_S("unexpected RootState# " << state);
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
        const auto it = Subscriptions.find(ev->Get()->NodeId);
        Y_ABORT_UNLESS(it != Subscriptions.end());
        Y_ABORT_UNLESS(!it->second || it->second == ev->Sender);
        it->second = ev->Sender;
    }

    void TInvokeRequestHandlerActor::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        const auto it = Subscriptions.find(nodeId);
        Y_ABORT_UNLESS(it != Subscriptions.end());
        Y_ABORT_UNLESS(!it->second || it->second == ev->Sender);
        Subscriptions.erase(it);
        for (auto [begin, end] = NodeToVDisk.equal_range(nodeId); begin != end; ++begin) {
            OnVStatusError(begin->second);
        }
        if (nodeId == WaitingReplyFromNode) {
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
                STLOG(PRI_DEBUG, BS_NODE, NWDC43, "ExecuteQuery", (SelfId, SelfId()), (Command, op.Command));
                switch (op.Command.GetRequestCase()) {
                    case TQuery::kUpdateConfig:
                        return UpdateConfig(op.Command.MutableUpdateConfig());

                    case TQuery::kQueryConfig:
                        return Finish(TResult::OK, std::nullopt, [&](auto *record) {
                            auto *response = record->MutableQueryConfig();
                            if (Self->StorageConfig) {
                                response->MutableConfig()->CopyFrom(*Self->StorageConfig);
                            }
                        });

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

                    case TQuery::kFetchStorageConfig: {
                        const auto& request = op.Command.GetFetchStorageConfig();
                        return FetchStorageConfig(request.GetMainConfig(), request.GetStorageConfig(),
                            request.GetAddExplicitConfigs(), request.GetAddSectionsForMigrationToV1());
                    }

                    case TQuery::kReplaceStorageConfig:
                        return ReplaceStorageConfig(op.Command.GetReplaceStorageConfig());

                    case TQuery::kBootstrapCluster:
                        return BootstrapCluster(op.Command.GetBootstrapCluster().GetSelfAssemblyUUID());

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

                    case TQuery::REQUEST_NOT_SET:
                        throw TExError() << "Request field not set";
                }

                throw TExError() << "Unhandled request";
            },
            [&](TCollectConfigsAndPropose&) {
                STLOG(PRI_DEBUG, BS_NODE, NWDC19, "Starting config collection");

                TEvScatter task;
                task.MutableCollectConfigs();
                IssueScatterTask(std::move(task), [this](TEvGather *res) {
                    Y_ABORT_UNLESS(Self->StorageConfig); // it can't just disappear
                    Y_ABORT_UNLESS(!Self->CurrentProposition);

                    if (!res->HasCollectConfigs()) {
                        throw TExError() << "Incorrect CollectConfigs response";
                    } else if (auto r = Self->ProcessCollectConfigs(res->MutableCollectConfigs(), std::nullopt); r.ErrorReason) {
                        throw TExError() << *r.ErrorReason;
                    } else if (r.ConfigToPropose) {
                        CheckSyncersAfterCommit = r.CheckSyncersAfterCommit;
                        StartProposition(&r.ConfigToPropose.value(), /*acceptLocalQuorum=*/ true,
                            /*requireScepter=*/ false, /*mindPrev=*/ true,
                            r.PropositionBase ? &r.PropositionBase.value() : nullptr);
                    } else {
                        Finish(TResult::OK, std::nullopt);
                    }
                });
            },
            [&](TProposeConfig& op) {
                CheckSyncersAfterCommit = op.CheckSyncersAfterCommit;
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
        STLOG(PRI_DEBUG, BS_NODE, NWDC44, "Handle(TEvNodeConfigGather)", (SelfId, SelfId()), (Record, record));
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

    void TInvokeRequestHandlerActor::AdvanceGeneration() {
        RunCommonChecks();
        NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
        StartProposition(&config);
    }

    void TInvokeRequestHandlerActor::StartProposition(NKikimrBlobStorage::TStorageConfig *config, bool acceptLocalQuorum,
            bool requireScepter, bool mindPrev, const NKikimrBlobStorage::TStorageConfig *propositionBase) {
        if (auto error = UpdateClusterState(config)) {
            throw TExError() << *error;
        } else if (!Self->HasConnectedNodeQuorum(*config, acceptLocalQuorum)) {
            throw TExError() << "No quorum to start propose/commit configuration";
        } else if (requireScepter && !Self->Scepter) {
            throw TExError() << "No scepter";
        }

        if (const auto *op = std::get_if<TInvokeExternalOperation>(&Query)) {
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

                    AUDIT_PART("component", TString("distconf"))
                    AUDIT_PART("remote_address", wrapEmpty(NKikimr::NAddressClassifier::ExtractAddress(replaceConfig.GetPeerName())))
                    AUDIT_PART("subject", wrapEmpty(userToken.GetUserSID()))
                    AUDIT_PART("sanitized_token", wrapEmpty(userToken.GetSanitizedToken()))
                    AUDIT_PART("status", TString("SUCCESS"))
                    AUDIT_PART("reason", TString(), false)
                    AUDIT_PART("operation", TString("REPLACE CONFIG"))
                    AUDIT_PART("old_config", oldConfig)
                    AUDIT_PART("new_config", newConfig)
                );
            }
        }

        if (!propositionBase) {
            propositionBase = Self->StorageConfig.get();
        }

        Y_ABORT_UNLESS(InvokePipelineGeneration == Self->InvokePipelineGeneration);
        auto error = InvokeOtherActor(*Self, &TDistributedConfigKeeper::StartProposition, config, propositionBase,
            SelfId(), CheckSyncersAfterCommit, mindPrev);
        if (error) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC78, "Config update validation failed", (SelfId, SelfId()),
                (Error, *error), (ProposedConfig, *config));
            throw TExError() << "Config update validation failed: " << *error;
        }
    }

    void TInvokeRequestHandlerActor::Handle(TEvPrivate::TEvConfigProposed::TPtr ev) {
        auto& msg = *ev->Get();

        STLOG(PRI_DEBUG, BS_NODE, NWDC64, "OnConfigProposed", (SelfId, SelfId()), (ErrorReason, msg.ErrorReason),
            (RootState, Self->RootState));

        if (msg.ErrorReason) {
            throw TExError() << "Config proposition failed: " << *msg.ErrorReason;
        } else {
            Finish(TResult::OK, std::nullopt);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Query termination and result delivery

    void TInvokeRequestHandlerActor::RunCommonChecks(bool requireScepter) {
        Y_ABORT_UNLESS(
            Self->RootState == ERootState::LOCAL_QUORUM_OP ||
            Self->RootState == ERootState::IN_PROGRESS
        );

        Y_ABORT_UNLESS(!Self->CurrentProposition);

        if (!Self->StorageConfig) {
            throw TExError() << "No agreed StorageConfig";
        } else if (auto error = ValidateConfig(*Self->StorageConfig)) {
            throw TExError() << "Current config validation failed: " << *error;
        } else if (requireScepter && !Self->Scepter) {
            throw TExError() << "No scepter";
        }
    }

    void TInvokeRequestHandlerActor::Finish(TResult::EStatus status, std::optional<TStringBuf> errorReason,
            const std::function<void(TResult*)>& callback) {
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

        STLOG(PRI_DEBUG, BS_NODE, NWDC61, "Finish", (SelfId, SelfId()), (Record, record));

        std::optional<TString> switchToError; // when set, we will switch distconf keeper to error state with this reason

        std::visit(TOverloaded{
            [&](TInvokeExternalOperation& op) {
                auto ev = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
                record.Swap(&ev->Record);
                auto handle = std::make_unique<IEventHandle>(op.Sender, SelfId(), ev.release(), 0, op.Cookie);
                if (op.SessionId) {
                    handle->Rewrite(TEvInterconnect::EvForward, op.SessionId);
                }
                TActivationContext::Send(handle.release());
            },
            [&](TCollectConfigsAndPropose&) {
                // this is just temporary failure
                // TODO(alexvru): backoff?
            },
            [&](TProposeConfig&) {
                Y_ABORT_UNLESS(InvokePipelineGeneration == Self->InvokePipelineGeneration);
                if (status != TResult::OK) { // we were asked to commit config, but error has occured
                    Y_ABORT_UNLESS(errorReason);
                    switchToError.emplace(*errorReason);
                }
            }
        }, Query);

        // terminate this actor
        PassAway();

        // reset root state in keeper actor if this query is still valid and there is no pending proposition
        if (InvokePipelineGeneration == Self->InvokePipelineGeneration && !Self->CurrentProposition) {
            switch (auto& state = Self->RootState) {
                case ERootState::IN_PROGRESS:
                    state = ERootState::RELAX;
                    break;

                case ERootState::LOCAL_QUORUM_OP:
                    state = Self->Scepter ? ERootState::RELAX : ERootState::INITIAL;
                    break;

                case ERootState::INITIAL:
                case ERootState::ERROR_TIMEOUT:
                case ERootState::RELAX:
                    Y_ABORT_S("unexpected RootState# " << state);
            }
        }

        if (switchToError) {
            InvokeOtherActor(*Self, &TDistributedConfigKeeper::SwitchToError, std::move(*switchToError), true);
        }
    }

    void TInvokeRequestHandlerActor::PassAway() {
        if (!WaitingReplyFromNode && !LifetimeToken.expired()) {
            TActivationContext::Send(new IEventHandle(TEvPrivate::EvQueryFinished, 0, Self->SelfId(), SelfId(), nullptr,
                InvokePipelineGeneration));
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
            )
        } catch (const TExError& error) {
            Finish(error.Status, error.what());
            if (error.IsCritical) {
                Y_DEBUG_ABORT("critical error during query processing: %s", error.what());
            }
        }
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigInvokeOnRoot::TPtr ev) {
        if (Binding) {
            // we have binding, so we have to forward this message to 'hop' node and return answer
            const ui32 hopNodeId = Binding->NodeId;
            const TActorId actorId = RegisterWithSameMailbox(new TInvokeRequestHandlerActor(this, {.Sender = ev->Sender,
                .SessionId = ev->InterconnectSession, .Cookie = ev->Cookie}, hopNodeId));
            TActivationContext::Send(new IEventHandle(MakeBlobStorageNodeWardenID(hopNodeId), actorId,
                ev->Release().Release(), IEventHandle::FlagSubscribeOnSession));
        } else {
            Invoke(TInvokeExternalOperation{
                .Command = std::move(ev->Get()->Record),
                .Sender = ev->Sender,
                .SessionId = ev->InterconnectSession,
                .Cookie = ev->Cookie,
            });
        }
    }

    void TDistributedConfigKeeper::Invoke(TInvokeQuery&& query) {
        const TActorId actorId = RegisterWithSameMailbox(new TInvokeRequestHandlerActor(this, std::move(query)));
        InvokeQ.push_back(TInvokeOperation{actorId});
        if (InvokeQ.size() == 1) {
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
        for (const auto& item : std::exchange(InvokeQ, {})) {
            Send(item.ActorId, new TEvPrivate::TEvAbortQuery(errorReason));
        }
        DeadActorWaitingForProposition = false;
        ++InvokePipelineGeneration;
    }

} // NKikimr::NStorage
