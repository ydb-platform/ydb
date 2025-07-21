#include "distconf_invoke.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/util/address_classifier.h>

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    TInvokeRequestHandlerActor::TInvokeRequestHandlerActor(TDistributedConfigKeeper *self,
            std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>>&& ev)
        : Self(self)
        , LifetimeToken(Self->LifetimeToken)
        , InvokeActorQueueGeneration(Self->InvokeActorQueueGeneration)
        , Event(std::move(ev))
        , Sender(Event->Sender)
        , Cookie(Event->Cookie)
        , RequestSessionId(Event->InterconnectSession)
    {}

    TInvokeRequestHandlerActor::TInvokeRequestHandlerActor(TDistributedConfigKeeper *self)
        : Self(self)
        , LifetimeToken(Self->LifetimeToken)
        , InvokeActorQueueGeneration(Self->InvokeActorQueueGeneration)
        , Cookie()
    {}

    TInvokeRequestHandlerActor::TInvokeRequestHandlerActor(TDistributedConfigKeeper *self,
            NKikimrBlobStorage::TStorageConfig&& config)
        : Self(self)
        , LifetimeToken(Self->LifetimeToken)
        , InvokeActorQueueGeneration(Self->InvokeActorQueueGeneration)
        , Cookie()
        , ReplaceConfig(std::move(config))
    {}

    void TInvokeRequestHandlerActor::Bootstrap(TActorId parentId) {
        if (LifetimeToken.expired()) {
            return FinishWithError(TResult::RACE, "Distributed config keeper terminated");
        }

        STLOG(PRI_DEBUG, BS_NODE, NWDC42, "TInvokeRequestHandlerActor::Bootstrap", (Sender, Sender), (Cookie, Cookie),
            (SelfId, SelfId()), (Binding, Self->Binding), (RootState, Self->RootState));

        ParentId = parentId;
        Become(&TThis::StateFunc);

        if (Self->Binding) { // we aren't the root node
            Y_ABORT_UNLESS(Event);
            if (RequestSessionId) {
                const auto it = Self->DirectBoundNodes.find(Sender.NodeId());
                if (it == Self->DirectBoundNodes.end() || RequestSessionId != it->second.SessionId) {
                    throw TExRace() << "Distconf tree reconfigured during query delivery";
                }
            }
            const ui32 node = Self->Binding->RootNodeId; //Self->Binding->NodeId;
            Send(MakeBlobStorageNodeWardenID(node), Event->Release(), IEventHandle::FlagSubscribeOnSession);
            const auto [it, inserted] = Subscriptions.try_emplace(node);
            Y_ABORT_UNLESS(inserted);
            WaitingReplyFromNode = node;
        } else if (Self->RootState == ERootState::ERROR_TIMEOUT) {
            throw TExError() << Self->ErrorReason;
        } else {
            if (Event) {
                if (const auto& record = Event->Get()->Record; record.HasSwitchBridgeClusterState()) {
                    PrepareSwitchBridgeClusterState(record.GetSwitchBridgeClusterState());
                } else if (record.HasAdvanceClusterStateGeneration()) {
                    PrepareAdvanceClusterStateGeneration(record.GetAdvanceClusterStateGeneration());
                } else if (record.HasMergeUnsyncedPileConfig()) {
                    PrepareMergeUnsyncedPileConfig(record.GetMergeUnsyncedPileConfig());
                }
            }

            const bool scepterless = SwitchBridgeNewConfig &&
                Self->HasConnectedNodeQuorum(*SwitchBridgeNewConfig, SpecificBridgePileIds);
            Y_ABORT_UNLESS(InvokeActorQueueGeneration == Self->InvokeActorQueueGeneration);
            InvokeOtherActor(*Self, &TDistributedConfigKeeper::OpQueueBegin, SelfId(), scepterless);
        }
    }

    void TInvokeRequestHandlerActor::OnError(const TString& errorReason) {
        if (Event || ReplaceConfig) {
            FinishWithError(TResult::RACE, errorReason);
        } else {
            // otherwise this would cause loop (SwitchToError -> OnError -> SwitchToError)
            PassAway();
        }
    }

    void TInvokeRequestHandlerActor::OnNoQuorum() {
        FinishWithError(TResult::NO_QUORUM, "No quorum obtained");
    }

    void TInvokeRequestHandlerActor::OnBeginOperation() {
        BeginRegistered = true;
        Wrap([&] {
            ExecuteQuery();
        });
    }

    void TInvokeRequestHandlerActor::Handle(TEvNodeConfigInvokeOnRootResult::TPtr ev) {
        if (ev->HasEvent()) {
            Finish(Sender, SelfId(), ev->ReleaseBase().Release(), ev->Flags, Cookie);
        } else {
            Finish(ev->Type, ev->Flags, Sender, SelfId(), ev->ReleaseChainBuffer(), Cookie);
        }
    }

    void TInvokeRequestHandlerActor::Handle(TEvInterconnect::TEvNodeConnected::TPtr ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        if (const auto it = Subscriptions.find(nodeId); it != Subscriptions.end()) {
            it->second = ev->Sender;
        }
    }

    void TInvokeRequestHandlerActor::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        Subscriptions.erase(nodeId);
        for (auto [begin, end] = NodeToVDisk.equal_range(nodeId); begin != end; ++begin) {
            OnVStatusError(begin->second);
        }
        if (nodeId == WaitingReplyFromNode) {
            throw TExRace() << "Root node disconnected";
        }
    }

    void TInvokeRequestHandlerActor::UnsubscribeInterconnect() {
        for (auto it = Subscriptions.begin(); it != Subscriptions.end(); ) {
            const TActorId actorId = it->second ? it->second : TActivationContext::InterconnectProxy(it->first);
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, actorId, SelfId(), nullptr, 0));
            Subscriptions.erase(it++);
        }
    }

    void TInvokeRequestHandlerActor::ExecuteQuery() {
        if (!Event) {
            if (ReplaceConfig) {
                StartProposition(&ReplaceConfig.value());
            } else {
                ExecuteInitialRootAction();
            }
            return;
        }

        auto& record = Event->Get()->Record;
        STLOG(PRI_DEBUG, BS_NODE, NWDC43, "ExecuteQuery", (SelfId, SelfId()), (Record, record));
        switch (record.GetRequestCase()) {
            case TQuery::kUpdateConfig:
                return UpdateConfig(record.MutableUpdateConfig());

            case TQuery::kQueryConfig:
                return FinishWithSuccess([&](auto *record) {
                    auto *response = record->MutableQueryConfig();
                    if (Self->StorageConfig) {
                        response->MutableConfig()->CopyFrom(*Self->StorageConfig);
                    }
                    if (Self->CurrentProposition) {
                        // TODO(alexvru): this can't actually happen?
                        response->MutableCurrentProposedStorageConfig()->CopyFrom(Self->CurrentProposition->StorageConfig);
                    }
                });

            case TQuery::kReassignGroupDisk:
                return ReassignGroupDisk(record.GetReassignGroupDisk());

            case TQuery::kStaticVDiskSlain:
                return StaticVDiskSlain(record.GetStaticVDiskSlain());

            case TQuery::kDropDonor:
                return DropDonor(record.GetDropDonor());

            case TQuery::kReassignStateStorageNode:
                return ReassignStateStorageNode(record.GetReassignStateStorageNode());

            case TQuery::kAdvanceGeneration:
                return AdvanceGeneration();

            case TQuery::kFetchStorageConfig: {
                const auto& request = record.GetFetchStorageConfig();
                return FetchStorageConfig(request.GetMainConfig(), request.GetStorageConfig(),
                    request.GetAddExplicitConfigs(), request.GetAddSectionsForMigrationToV1());
            }

            case TQuery::kReplaceStorageConfig:
                return ReplaceStorageConfig(record.GetReplaceStorageConfig());

            case TQuery::kBootstrapCluster:
                return BootstrapCluster(record.GetBootstrapCluster().GetSelfAssemblyUUID());

            case TQuery::kSwitchBridgeClusterState:
                return SwitchBridgeClusterState();

            case TQuery::kReconfigStateStorage:
                return ReconfigStateStorage(record.GetReconfigStateStorage());

            case TQuery::kGetStateStorageConfig:
                return GetStateStorageConfig(record.GetGetStateStorageConfig());

            case TQuery::kSelfHealStateStorage:
                return SelfHealStateStorage(record.GetSelfHealStateStorage());

                case TQuery::kSelfHealNodesStateUpdate:
                return SelfHealNodesStateUpdate(record.GetSelfHealNodesStateUpdate());

            case TQuery::kNotifyBridgeSyncFinished:
                return NotifyBridgeSyncFinished(record.GetNotifyBridgeSyncFinished());

            case TQuery::kMergeUnsyncedPileConfig:
                return MergeUnsyncedPileConfig();

            case TQuery::kNegotiateUnsyncedConnection:
                return NegotiateUnsyncedConnection(record.GetNegotiateUnsyncedConnection());

            case TQuery::kAdvanceClusterStateGeneration:
                return AdvanceClusterStateGeneration();

            case TQuery::REQUEST_NOT_SET:
                throw TExError() << "Request field not set";
        }

        throw TExError() << "Unhandled request";
    }

    void TInvokeRequestHandlerActor::ExecuteInitialRootAction() {
        STLOG(PRI_DEBUG, BS_NODE, NWDC19, "Starting config collection", (Scepter, Self->Scepter->Id));

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
                StartProposition(&r.ConfigToPropose.value(), false, r.PropositionBase ? &r.PropositionBase.value() : nullptr);
            } else {
                FinishWithSuccess();
            }
        });
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

    void TInvokeRequestHandlerActor::StartProposition(NKikimrBlobStorage::TStorageConfig *config, bool forceGeneration,
            const NKikimrBlobStorage::TStorageConfig *propositionBase) {
        if (Self->CurrentProposition) {
            throw TExCriticalError() << "Config proposition request is already in flight";
        }

        if (auto error = UpdateClusterState(config)) {
            throw TExError() << *error;
        }

        if (Event) {
            if (const auto& record = Event->Get()->Record; record.HasReplaceStorageConfig()) {
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

        Y_ABORT_UNLESS(InvokeActorQueueGeneration == Self->InvokeActorQueueGeneration);
        auto error = InvokeOtherActor(*Self, &TDistributedConfigKeeper::StartProposition, config, propositionBase,
            std::move(SpecificBridgePileIds), SelfId(), CheckSyncersAfterCommit, forceGeneration);
        if (error) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC78, "Config update validation failed", (SelfId, SelfId()),
                (Error, *error), (ProposedConfig, *config));
            throw TExError() << "Config update validation failed: " << *error;
        }
    }

    void TInvokeRequestHandlerActor::OnConfigProposed(const std::optional<TString>& errorReason) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC64, "OnConfigProposed", (SelfId, SelfId()), (ErrorReason, errorReason),
            (RootState, Self->RootState));

        if (errorReason) {
            throw TExError() << "Config proposition failed: " << *errorReason;
        } else {
            FinishWithSuccess([&](auto *record) {
                if (MergedConfig) { // copy merged config in case of success, if we have any
                    MergedConfig->Swap(record->MutableMergeUnsyncedPileConfig()->MutableMergedConfig());
                }
            });

            Y_ABORT_UNLESS(InvokeActorQueueGeneration == Self->InvokeActorQueueGeneration);
            InvokeOtherActor(*Self, &TDistributedConfigKeeper::CheckForConfigUpdate);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Query termination and result delivery

    void TInvokeRequestHandlerActor::RunCommonChecks() {
        Y_ABORT_UNLESS(
            Self->RootState == ERootState::SCEPTERLESS_OPERATION ||
            Self->RootState == ERootState::IN_PROGRESS
        );

        Y_ABORT_UNLESS(!Self->CurrentProposition);

        if (!Self->StorageConfig) {
            throw TExError() << "No agreed StorageConfig";
        } else if (auto error = ValidateConfig(*Self->StorageConfig)) {
            throw TExError() << "Current config validation failed: " << *error;
        }
    }

    std::unique_ptr<TEvNodeConfigInvokeOnRootResult> TInvokeRequestHandlerActor::PrepareResult(TResult::EStatus status,
            std::optional<TStringBuf> errorReason) {
        auto ev = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
        auto *record = &ev->Record;
        record->SetStatus(status);
        if (errorReason) {
            record->SetErrorReason(errorReason->data(), errorReason->size());
        }
        if (Self->Scepter) {
            auto *s = record->MutableScepter();
            s->SetId(Self->Scepter->Id);
            s->SetNodeId(SelfId().NodeId());
        }
        return ev;
    }

    void TInvokeRequestHandlerActor::FinishWithError(TResult::EStatus status, const TString& errorReason) {
        if (Event) {
            Finish(Sender, SelfId(), PrepareResult(status, errorReason).release(), 0, Cookie);
        } else if (ReplaceConfig) {
            // this is just temporary failure
            // TODO(alexvru): backoff?
        } else {
            Y_ABORT_UNLESS(InvokeActorQueueGeneration == Self->InvokeActorQueueGeneration);
            InvokeOtherActor(*Self, &TDistributedConfigKeeper::SwitchToError, errorReason);
        }
    }

    void TInvokeRequestHandlerActor::PassAway() {
        TActivationContext::Send(new IEventHandle(TEvPrivate::EvOpQueueEnd, 0, ParentId, SelfId(), nullptr,
            BeginRegistered ? InvokeActorQueueGeneration : 0));
        if (ControllerPipeId) {
            NTabletPipe::CloseAndForgetClient(SelfId(), ControllerPipeId);
        }
        UnsubscribeInterconnect();
        TActorBootstrapped::PassAway();
    }

    STFUNC(TInvokeRequestHandlerActor::StateFunc) {
        Wrap([&] {
            if (LifetimeToken.expired()) {
                throw TExRace() << "Distributed config keeper terminated";
            }
            STRICT_STFUNC_BODY(
                hFunc(TEvNodeConfigInvokeOnRootResult, Handle);
                hFunc(TEvNodeConfigGather, Handle);
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
        });
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigInvokeOnRoot::TPtr ev) {
        std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>> evPtr(ev.Release());
        ChildActors.insert(RegisterWithSameMailbox(new TInvokeRequestHandlerActor(this, std::move(evPtr))));
    }

    void TDistributedConfigKeeper::OpQueueBegin(TActorId actorId, bool scepterless) {
        Y_ABORT_UNLESS(!Binding); // no operation can begin while we are bound
        Y_ABORT_UNLESS(RootState != ERootState::ERROR_TIMEOUT);

        InvokeQ.push_back(TInvokeOperation{actorId, scepterless});
        if (InvokeQ.size() == 1) {
            OpQueueProcessFront();
        }
    }

    void TDistributedConfigKeeper::OpQueueProcessFront() {
        Y_ABORT_UNLESS(!Binding); // no operation can begin when we are bound

        if (InvokeQ.empty()) {
            return;
        }

        // find the actor who issued this request; it is always on the same mailbox
        const auto& item = InvokeQ.front();
        auto *actor = GetInvokeRequestHandlerActor(item.ActorId);
        if (item.ActorId && !actor) {
            // this actor has died and we have to wait for its OpQueueEnd message, which is probably in mailbox
            return;
        }

        void (TInvokeRequestHandlerActor::*pfn)() = nullptr;

        switch (RootState) {
            case ERootState::INITIAL:
                if (item.Scepterless) {
                    // this is scepterless operation and this is root node that doesn't have full quorum
                    RootState = ERootState::SCEPTERLESS_OPERATION;
                    pfn = &TInvokeRequestHandlerActor::OnBeginOperation;
                } else { // this is not scepterless operation and we have no scepter, meaning no quorum
                    pfn = &TInvokeRequestHandlerActor::OnNoQuorum;
                }
                break;

            case ERootState::RELAX:
                RootState = ERootState::IN_PROGRESS;
                pfn = &TInvokeRequestHandlerActor::OnBeginOperation;
                break;

            case ERootState::IN_PROGRESS:
                // maybe system proposition is in flight (triggered by relaxed state)
                break;

            case ERootState::SCEPTERLESS_OPERATION:
            case ERootState::ERROR_TIMEOUT:
                Y_FAIL_S("unexpected state in OpQueueProcessFront# " << RootState);
        }

        if (pfn && actor) {
            Y_ABORT_UNLESS(actor->InvokeActorQueueGeneration == InvokeActorQueueGeneration);
            InvokeOtherActor(*actor, pfn);
        }
    }

    void TDistributedConfigKeeper::HandleOpQueueEnd(STFUNC_SIG) {
        const size_t numErased = ChildActors.erase(ev->Sender);
        Y_ABORT_UNLESS(numErased);

        if (ev->Cookie != InvokeActorQueueGeneration) {
            return; // this is mass error termination, we ignore this -- the queue should be empty by now
        }

        Y_ABORT_UNLESS(!InvokeQ.empty());
        const auto& front = InvokeQ.front();
        Y_ABORT_UNLESS(ev->Sender == front.ActorId);

        if (CurrentProposition && CurrentProposition->ActorId == front.ActorId) {
            DeadActorWaitingForProposition = true;
            return; // transaction is still being proposed, although issuer actor is dead
        }

        switch (RootState) {
            case ERootState::SCEPTERLESS_OPERATION:
                Y_ABORT_UNLESS(front.Scepterless);
                RootState = ERootState::INITIAL;
                break;

            case ERootState::IN_PROGRESS:
                RootState = ERootState::RELAX;
                break;

            default:
                Y_FAIL_S("unexpected state in HandleOpQueueEnd# " << RootState);
        }

        InvokeQ.pop_front();

        OpQueueProcessFront();
    }

    void TDistributedConfigKeeper::OpQueueOnBecomeRoot() {
    }

    void TDistributedConfigKeeper::OpQueueOnUnbecomeRoot() {
        OpQueueOnError("Scepter lost during query execution");
    }

    void TDistributedConfigKeeper::OpQueueOnError(const TString& errorReason) {
        for (const auto& item : std::exchange(InvokeQ, {})) {
            if (auto *actor = GetInvokeRequestHandlerActor(item.ActorId)) {
                Y_ABORT_UNLESS(actor->InvokeActorQueueGeneration == InvokeActorQueueGeneration);
                InvokeOtherActor(*actor, &TInvokeRequestHandlerActor::OnError, errorReason);
            }
        }
        DeadActorWaitingForProposition = false;

        // increment generation to just dismiss any incoming OpQueueEnd's
        ++InvokeActorQueueGeneration;
    }

    TInvokeRequestHandlerActor *TDistributedConfigKeeper::GetInvokeRequestHandlerActor(TActorId actorId) {
        return static_cast<TInvokeRequestHandlerActor*>(TlsActivationContext->Mailbox.FindActor(actorId.LocalId()));
    }

} // NKikimr::NStorage
