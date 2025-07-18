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
            return FinishWithError(TResult::RACE, "distributed config keeper terminated");
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
                    return FinishWithError(TResult::RACE, "distconf tree reconfigured during query delivery");
                }
            }
            const ui32 node = Self->Binding->RootNodeId; //Self->Binding->NodeId;
            Send(MakeBlobStorageNodeWardenID(node), Event->Release(), IEventHandle::FlagSubscribeOnSession);
            const auto [it, inserted] = Subscriptions.try_emplace(node);
            Y_ABORT_UNLESS(inserted);
            WaitingReplyFromNode = node;
        } else if (Self->RootState == ERootState::ERROR_TIMEOUT) {
            FinishWithError(TResult::ERROR, Self->ErrorReason);
        } else {
            if (Event && Self->Cfg->BridgeConfig) {
                if (const auto& record = Event->Get()->Record; record.HasSwitchBridgeClusterState()) {
                    const auto& cmd = record.GetSwitchBridgeClusterState();
                    const auto& newClusterState = cmd.GetNewClusterState();

                    for (ui32 bridgePileId : cmd.GetSpecificBridgePileIds()) {
                        SpecificBridgePileIds.insert(TBridgePileId::FromValue(bridgePileId));
                    }

                    if (const auto& error = ValidateSwitchBridgeClusterState(newClusterState)) {
                        return FinishWithError(TResult::ERROR, *error);
                    }

                    SwitchBridgeNewConfig.emplace(GetSwitchBridgeNewConfig(newClusterState));
                } else if (record.HasAdvanceClusterStateGeneration()) {
                    if (!PrepareAdvanceClusterStateGeneration(record.GetAdvanceClusterStateGeneration())) {
                        return;
                    }
                } else if (record.HasMergeUnsyncedPileConfig()) {
                    if (!PrepareMergeUnsyncedPileConfig(record.GetMergeUnsyncedPileConfig())) {
                        return;
                    }
                }
            }

            const bool scepterless = SwitchBridgeNewConfig &&
                Self->HasConnectedNodeQuorum(*SwitchBridgeNewConfig, SpecificBridgePileIds);
            InvokeOtherActor(*Self, &TDistributedConfigKeeper::OpQueueBegin, SelfId(), scepterless);
        }
    }

    void TInvokeRequestHandlerActor::OnError(const TString& errorReason) {
        FinishWithError(TResult::RACE, errorReason);
    }

    void TInvokeRequestHandlerActor::OnNoQuorum() {
        throw TExNoQuorum() << "no quorum obtained";
    }

    void TInvokeRequestHandlerActor::OnBeginOperation() {
        BeginRegistered = true;
        try {
            ExecuteQuery();
        } catch (const TExError& error) {
            FinishWithError(error.Status, error.what());
            if (error.IsCritical) {
                Y_DEBUG_ABORT("critical error during query processing: %s", error.what());
            }
        }
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
        if (nodeId == WaitingReplyFromNode) {
            FinishWithError(TResult::ERROR, "root node disconnected");
        }
        for (auto [begin, end] = NodeToVDisk.equal_range(nodeId); begin != end; ++begin) {
            OnVStatusError(begin->second);
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

            case TQuery::kQueryConfig: {
                auto ev = PrepareResult(TResult::OK, std::nullopt);
                auto *record = &ev->Record;
                auto *response = record->MutableQueryConfig();
                if (Self->StorageConfig) {
                    response->MutableConfig()->CopyFrom(*Self->StorageConfig);
                }
                if (Self->CurrentProposition) {
                    response->MutableCurrentProposedStorageConfig()->CopyFrom(Self->CurrentProposition->StorageConfig);
                }
                return Finish(Sender, SelfId(), ev.release(), 0, Cookie);
            }

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

            case TQuery::kNotifyBridgeSyncFinished:
                return NotifyBridgeSyncFinished(record.GetNotifyBridgeSyncFinished());

            case TQuery::kMergeUnsyncedPileConfig:
                return MergeUnsyncedPileConfig();

            case TQuery::kNegotiateUnsyncedConnection:
                return NegotiateUnsyncedConnection(record.GetNegotiateUnsyncedConnection());

            case TQuery::kAdvanceClusterStateGeneration:
                return AdvanceClusterStateGeneration();

            case TQuery::REQUEST_NOT_SET:
                return FinishWithError(TResult::ERROR, "Request field not set");
        }

        FinishWithError(TResult::ERROR, "unhandled request");
    }

    void TInvokeRequestHandlerActor::ExecuteInitialRootAction() {
        STLOG(PRI_DEBUG, BS_NODE, NWDC19, "Starting config collection", (Scepter, Self->Scepter->Id));

        TEvScatter task;
        task.MutableCollectConfigs();
        IssueScatterTask(std::move(task), [this](TEvGather *res) -> std::optional<TString> {
            Y_ABORT_UNLESS(Self->StorageConfig); // it can't just disappear
            Y_ABORT_UNLESS(!Self->CurrentProposition);

            if (!res->HasCollectConfigs()) {
                return "incorrect CollectConfigs response";
            } else if (auto r = Self->ProcessCollectConfigs(res->MutableCollectConfigs(), std::nullopt); r.ErrorReason) {
                return *r.ErrorReason;
            } else if (r.ConfigToPropose) {
                StartProposition(&r.ConfigToPropose.value(), false, r.PropositionBase ? &r.PropositionBase.value() : nullptr);
            } else {
                Finish(Sender, SelfId(), PrepareResult(TResult::OK, std::nullopt).release(), 0, Cookie);
            }
            return std::nullopt;
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
            return FinishWithError(TResult::ERROR, "scatter task was aborted due to loss of quorum or other error");
        }

        const auto it = ScatterTasks.find(record.GetCookie());
        Y_ABORT_UNLESS(it != ScatterTasks.end());
        TGatherCallback callback = std::move(it->second);
        ScatterTasks.erase(it);

        if (auto error = callback(&record)) {
            FinishWithError(TResult::ERROR, std::move(*error));
        }
    }

    void TInvokeRequestHandlerActor::UpdateConfig(TQuery::TUpdateConfig *request) {
        if (!RunCommonChecks()) {
            return;
        }
        StartProposition(request->MutableConfig());
    }

    void TInvokeRequestHandlerActor::AdvanceGeneration() {
        if (RunCommonChecks()) {
            NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;
            StartProposition(&config);
        }
    }

    void TInvokeRequestHandlerActor::StartProposition(NKikimrBlobStorage::TStorageConfig *config, bool forceGeneration,
            const NKikimrBlobStorage::TStorageConfig *propositionBase) {
        if (Self->CurrentProposition) {
            return FinishWithError(TResult::ERROR, "Config proposition request is already in flight");
        }

        if (auto error = UpdateClusterState(config)) {
            return FinishWithError(TResult::ERROR, *error);
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

        auto error = InvokeOtherActor(*Self, &TDistributedConfigKeeper::StartProposition, config, propositionBase,
            std::move(SpecificBridgePileIds), SelfId(), CheckSyncersAfterCommit, forceGeneration);
        if (error) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC78, "Config update validation failed", (SelfId, SelfId()),
                (Error, *error), (ProposedConfig, *config));
            return FinishWithError(TResult::ERROR, TStringBuilder() << "Config update validation failed: " << *error);
        }
    }

    void TInvokeRequestHandlerActor::OnConfigProposed(const std::optional<TString>& errorReason) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC64, "OnConfigProposed", (SelfId, SelfId()), (ErrorReason, errorReason),
            (RootState, Self->RootState));

        if (errorReason) {
            FinishWithError(TResult::ERROR, TStringBuilder() << "Config proposition failed: " << *errorReason);
        } else {
            auto ev = PrepareResult(TResult::OK, std::nullopt);
            if (MergedConfig) { // copy merged config in case of success, if we have any
                MergedConfig->Swap(ev->Record.MutableMergeUnsyncedPileConfig()->MutableMergedConfig());
            }
            Finish(Sender, SelfId(), ev.release(), 0, Cookie);

            InvokeOtherActor(*Self, &TDistributedConfigKeeper::CheckForConfigUpdate);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Query termination and result delivery

    bool TInvokeRequestHandlerActor::RunCommonChecks() {
        Y_ABORT_UNLESS(
            Self->RootState == ERootState::SCEPTERLESS_OPERATION ||
            Self->RootState == ERootState::IN_PROGRESS
        );

        Y_ABORT_UNLESS(!Self->CurrentProposition);

        if (!Self->StorageConfig) {
            FinishWithError(TResult::ERROR, "no agreed StorageConfig");
        } else if (auto error = ValidateConfig(*Self->StorageConfig)) {
            FinishWithError(TResult::ERROR, TStringBuilder() << "current config validation failed: " << *error);
        } else {
            return true;
        }

        return false;
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
        if (LifetimeToken.expired()) {
            return FinishWithError(TResult::ERROR, "distributed config keeper terminated");
        }
        try {
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
        } catch (const TExError& error) {
            FinishWithError(error.Status, error.what());
            if (error.IsCritical) {
                Y_DEBUG_ABORT("critical error during query processing: %s", error.what());
            }
        }
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
                break;
        }

        if (pfn && actor) {
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
        OpQueueOnError("scepter lost during query execution");
    }

    void TDistributedConfigKeeper::OpQueueOnError(const TString& errorReason) {
        // increment generation to just dismiss any incoming OpQueueEnd's
        ++InvokeActorQueueGeneration;

        for (const auto& item : std::exchange(InvokeQ, {})) {
            if (auto *actor = GetInvokeRequestHandlerActor(item.ActorId)) {
                InvokeOtherActor(*actor, &TInvokeRequestHandlerActor::OnError, errorReason);
            }
        }
        DeadActorWaitingForProposition = false;
    }

    TInvokeRequestHandlerActor *TDistributedConfigKeeper::GetInvokeRequestHandlerActor(TActorId actorId) {
        return static_cast<TInvokeRequestHandlerActor*>(TlsActivationContext->Mailbox.FindActor(actorId.LocalId()));
    }

} // NKikimr::NStorage
