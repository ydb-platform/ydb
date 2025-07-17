#include "distconf_invoke.h"

#include <ydb/core/audit/audit_log.h>
#include <ydb/core/util/address_classifier.h>

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    TInvokeRequestHandlerActor::TInvokeRequestHandlerActor(TDistributedConfigKeeper *self,
            std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>>&& ev)
        : Self(self)
        , LifetimeToken(Self->LifetimeToken)
        , Scepter(Self->Scepter)
        , ScepterCounter(Self->ScepterCounter)
        , Event(std::move(ev))
        , Sender(Event->Sender)
        , Cookie(Event->Cookie)
        , RequestSessionId(Event->InterconnectSession)
    {}

    void TInvokeRequestHandlerActor::Bootstrap(TActorId parentId) {
        if (LifetimeToken.expired()) {
            return FinishWithError(TResult::RACE, "distributed config keeper terminated");
        }

        STLOG(PRI_DEBUG, BS_NODE, NWDC42, "TInvokeRequestHandlerActor::Bootstrap", (Sender, Sender), (Cookie, Cookie),
            (SelfId, SelfId()), (Binding, Self->Binding), (RootState, Self->RootState));

        ParentId = parentId;
        Become(&TThis::StateFunc);

        if (const auto& record = Event->Get()->Record; record.HasSwitchBridgeClusterState() && Self->Cfg->BridgeConfig) {
            const auto& cmd = record.GetSwitchBridgeClusterState();
            const auto& newClusterState = cmd.GetNewClusterState();

            for (ui32 bridgePileId : cmd.GetSpecificBridgePileIds()) {
                SpecificBridgePileIds.insert(TBridgePileId::FromValue(bridgePileId));
            }

            if (const auto& error = ValidateSwitchBridgeClusterState(newClusterState)) {
                return FinishWithError(TResult::ERROR, *error);
            }

            SwitchBridgeNewConfig.emplace(GetSwitchBridgeNewConfig(newClusterState));
        }

        if (Self->ScepterlessOperationInProgress) {
            FinishWithError(TResult::RACE, "an operation is already in progress");
        } else if (Self->Binding) {
            if (RequestSessionId) {
                FinishWithError(TResult::RACE, "no double-hop invokes allowed");
            } else {
                const ui32 root = Self->Binding->RootNodeId;
                Send(MakeBlobStorageNodeWardenID(root), Event->Release(), IEventHandle::FlagSubscribeOnSession);
                const auto [it, inserted] = Subscriptions.try_emplace(root);
                Y_ABORT_UNLESS(inserted);
                WaitingReplyFromNode = root;
            }
        } else if (!Scepter.expired() || // we have either scepter, or quorum for reduced set of nodes to execute this command
                (SwitchBridgeNewConfig && Self->HasConnectedNodeQuorum(*SwitchBridgeNewConfig, SpecificBridgePileIds))) {
            if (Scepter.expired()) {
                Self->ScepterlessOperationInProgress = IsScepterlessOperation = true;
            }
            ExecuteQuery();
        } else {
            FinishWithError(TResult::NO_QUORUM, "no quorum obtained");
        }
    }

    bool TInvokeRequestHandlerActor::IsScepterExpired() const {
        return Self->ScepterCounter != ScepterCounter;
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

            case TQuery::REQUEST_NOT_SET:
                return FinishWithError(TResult::ERROR, "Request field not set");

            case TQuery::kReconfigStateStorage:
                return ReconfigStateStorage(record.GetReconfigStateStorage());

            case TQuery::kGetStateStorageConfig:
                return GetStateStorageConfig(record.GetGetStateStorageConfig());

            case TQuery::kNotifyBridgeSyncFinished:
                return NotifyBridgeSyncFinished(record.GetNotifyBridgeSyncFinished());
        }

        FinishWithError(TResult::ERROR, "unhandled request");
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
            config.SetGeneration(config.GetGeneration() + 1);
            StartProposition(&config);
        }
    }

    void TInvokeRequestHandlerActor::StartProposition(NKikimrBlobStorage::TStorageConfig *config, bool updateFields) {
        if (Self->CurrentProposition) {
            return FinishWithError(TResult::ERROR, "Config proposition request is already in flight");
        }

        if (updateFields) {
            if (auto error = UpdateClusterState(config)) {
                return FinishWithError(TResult::ERROR, *error);
            }
        }

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

        auto error = InvokeOtherActor(*Self, &TDistributedConfigKeeper::StartProposition, config, &*Self->StorageConfig,
            std::move(SpecificBridgePileIds), SelfId(), CheckSyncersAfterCommit);
        if (error) {
            STLOG(PRI_DEBUG, BS_NODE, NWDC78, "Config update validation failed", (SelfId, SelfId()),
                (Error, *error), (ProposedConfig, *config));
            return FinishWithError(TResult::ERROR, TStringBuilder() << "Config update validation failed: " << *error);
        }

        Self->RootState = ERootState::IN_PROGRESS; // forbid any concurrent activity
    }

    void TInvokeRequestHandlerActor::Handle(TEvPrivate::TEvConfigProposed::TPtr ev) {
        if (std::exchange(WaitingForOtherProposition, false)) {
            // try to restart query
            Bootstrap(ParentId);
        } else if (ev->Get()->ErrorReason) {
            FinishWithError(TResult::ERROR, TStringBuilder() << "Config proposition failed: " << *ev->Get()->ErrorReason);
        } else {
            Finish(Sender, SelfId(), PrepareResult(TResult::OK, std::nullopt).release(), 0, Cookie);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Query termination and result delivery

    bool TInvokeRequestHandlerActor::RunCommonChecks() {
        if (!Self->StorageConfig) {
            FinishWithError(TResult::ERROR, "no agreed StorageConfig");
        } else if (Self->CurrentProposition) {
            Self->CurrentProposition->ActorIds.push_back(SelfId());
            WaitingForOtherProposition = true;
        } else if (Self->RootState != (IsScepterlessOperation ? ERootState::INITIAL : ERootState::RELAX)) {
            FinishWithError(TResult::RACE, "something going on with default FSM");
        } else if (auto error = ValidateConfig(*Self->StorageConfig)) {
            FinishWithError(TResult::ERROR, TStringBuilder() << "current config validation failed: " << *error);
        } else if (IsScepterExpired()) {
            FinishWithError(TResult::RACE, "scepter lost during query execution");
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
        if (auto scepter = Scepter.lock()) {
            auto *s = record->MutableScepter();
            s->SetId(scepter->Id);
            s->SetNodeId(SelfId().NodeId());
        }
        return ev;
    }

    void TInvokeRequestHandlerActor::FinishWithError(TResult::EStatus status, const TString& errorReason) {
        Finish(Sender, SelfId(), PrepareResult(status, errorReason).release(), 0, Cookie);
    }

    void TInvokeRequestHandlerActor::PassAway() {
        if (IsScepterlessOperation && !IsScepterExpired()) {
            Self->ScepterlessOperationInProgress = false;
        }
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Gone, 0, ParentId, SelfId(), nullptr, 0));
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
            hFunc(TEvPrivate::TEvConfigProposed, Handle);
        )
    }

    void TDistributedConfigKeeper::Handle(TEvNodeConfigInvokeOnRoot::TPtr ev) {
        std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>> evPtr(ev.Release());
        ChildActors.insert(RegisterWithSameMailbox(new TInvokeRequestHandlerActor(this, std::move(evPtr))));
    }

} // NKikimr::NStorage
