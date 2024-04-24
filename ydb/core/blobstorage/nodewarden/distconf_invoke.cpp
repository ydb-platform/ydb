#include "distconf.h"
#include "node_warden_impl.h"

namespace NKikimr::NStorage {

    class TDistributedConfigKeeper::TInvokeRequestHandlerActor : public TActorBootstrapped<TInvokeRequestHandlerActor> {
        TDistributedConfigKeeper* const Self;
        const std::weak_ptr<TLifetimeToken> LifetimeToken;
        const std::weak_ptr<TScepter> Scepter;
        std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>> Event;
        const TActorId Sender;
        const ui64 Cookie;
        const TActorId RequestSessionId;

        TActorId ParentId;

        TActorId InterconnectSessionId;
        ui32 ConnectedPeerNodeId = 0;

        using TQuery = NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot;
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

    public:
        TInvokeRequestHandlerActor(TDistributedConfigKeeper *self, std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>>&& ev)
            : Self(self)
            , LifetimeToken(Self->LifetimeToken)
            , Scepter(Self->Scepter)
            , Event(std::move(ev))
            , Sender(Event->Sender)
            , Cookie(Event->Cookie)
            , RequestSessionId(Event->InterconnectSession)
        {}

        void Bootstrap(TActorId parentId) {
            if (LifetimeToken.expired()) {
                return FinishWithError(TResult::ERROR, "distributed config keeper terminated");
            }

            STLOG(PRI_DEBUG, BS_NODE, NWDC42, "TInvokeRequestHandlerActor::Bootstrap", (Sender, Sender), (Cookie, Cookie),
                (SelfId, SelfId()), (Binding, Self->Binding), (RootState, Self->RootState));

            ParentId = parentId;
            Become(&TThis::StateFunc);

            if (auto scepter = Scepter.lock()) {
                // remove unnecessary subscription, if any
                UnsubscribeInterconnect();
                ExecuteQuery();
            } else if (Self->Binding) {
                if (RequestSessionId) {
                    FinishWithError(TResult::ERROR, "no double-hop invokes allowed");
                } else if (Self->Binding->RootNodeId != ConnectedPeerNodeId) { // subscribe to session first
                    Send(TActivationContext::InterconnectProxy(Self->Binding->RootNodeId), new TEvInterconnect::TEvConnectNode);
                    UnsubscribeInterconnect();
                } else { // session is already established, forward event to peer node
                    Y_ABORT_UNLESS(Event);
                    auto ev = IEventHandle::Forward(std::exchange(Event, {}), MakeBlobStorageNodeWardenID(ConnectedPeerNodeId));
                    ev->Rewrite(TEvInterconnect::EvForward, InterconnectSessionId);
                    TActivationContext::Send(ev.release());
                }
            } else {
                FinishWithError(TResult::NO_QUORUM, "no quorum obtained");
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Interconnect machinery

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr ev) {
            // remember actor id of interconnect session to unsubcribe later
            InterconnectSessionId = ev->Sender;
            ConnectedPeerNodeId = ev->Get()->NodeId;
            // restart query from the beginning
            Bootstrap(ParentId);
        }

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr /*ev*/) {
            FinishWithError(TResult::ERROR, "root node disconnected");
        }

        void UnsubscribeInterconnect() {
            if (const TActorId actorId = std::exchange(InterconnectSessionId, {})) {
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, actorId, SelfId(), nullptr, 0));
                ConnectedPeerNodeId = 0;
            }
        }

        void Handle(TEvNodeConfigInvokeOnRootResult::TPtr ev) {
            if (ev->HasEvent()) {
                Finish(Sender, SelfId(), ev->ReleaseBase().Release(), ev->Flags, Cookie);
            } else {
                Finish(ev->Type, ev->Flags, Sender, SelfId(), ev->ReleaseChainBuffer(), Cookie);
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Query execution logic

        void ExecuteQuery() {
            auto& record = Event->Get()->Record;
            STLOG(PRI_DEBUG, BS_NODE, NWDC43, "ExecuteQuery", (SelfId, SelfId()), (Record, record));
            switch (record.GetRequestCase()) {
                case TQuery::kUpdateConfig: {
                    auto *request = record.MutableUpdateConfig();

                    if (!RunCommonChecks()) {
                        return;
                    }

                    auto *config = request->MutableConfig();

                    if (auto error = ValidateConfig(*Self->StorageConfig)) {
                        return FinishWithError(TResult::ERROR, TStringBuilder() << "current config validation failed: " << *error);
                    } else if (auto error = ValidateConfigUpdate(*Self->StorageConfig, *config)) {
                        return FinishWithError(TResult::ERROR, TStringBuilder() << "config validation failed: " << *error);
                    }

                    return StartProposition(config);
                }

                case TQuery::kQueryConfig: {
                    auto ev = PrepareResult(TResult::OK, std::nullopt);
                    auto *record = &ev->Record;
                    auto *response = record->MutableQueryConfig();
                    if (Self->StorageConfig) {
                        response->MutableConfig()->CopyFrom(*Self->StorageConfig);
                    }
                    if (Self->CurrentProposedStorageConfig) {
                        response->MutableCurrentProposedStorageConfig()->CopyFrom(*Self->CurrentProposedStorageConfig);
                    }
                    return Finish(Sender, SelfId(), ev.release(), 0, Cookie);
                }

                case TQuery::kReassignGroupDisk:
                    return ReassignGroupDisk(record.GetReassignGroupDisk());

                case TQuery::kStaticVDiskSlain:
                    return StaticVDiskSlain(record.GetStaticVDiskSlain());

                case TQuery::kDropDonor:
                    return DropDonor(record.GetDropDonor());

                case TQuery::REQUEST_NOT_SET:
                    return FinishWithError(TResult::ERROR, "Request field not set");
            }

            FinishWithError(TResult::ERROR, "unhandled request");
        }

        void Handle(TEvNodeConfigGather::TPtr ev) {
            auto& record = ev->Get()->Record;
            STLOG(PRI_DEBUG, BS_NODE, NWDC44, "Handle(TEvNodeConfigGather)", (SelfId, SelfId()), (Record, record));
            switch (record.GetResponseCase()) {
                case TEvGather::kProposeStorageConfig: {
                    std::unique_ptr<TEvNodeConfigInvokeOnRootResult> ev;
                    if (auto error = Self->ProcessProposeStorageConfig(record.MutableProposeStorageConfig())) {
                        ev = PrepareResult(TResult::ERROR, *error);
                    } else {
                        ev = PrepareResult(TResult::OK, std::nullopt);
                    }
                    return Finish(Sender, SelfId(), ev.release(), 0, Cookie);
                }

                default:
                    return FinishWithError(TResult::ERROR, "unexpected Response case in resulting TEvGather");
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Reassign group disk logic

        void ReassignGroupDisk(const NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot::TReassignGroupDisk& /*cmd*/) {
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryBaseConfig);
        }

        void Handle(TEvNodeWardenBaseConfig::TPtr ev) {
            const auto& record = Event->Get()->Record;
            const auto& cmd = record.GetReassignGroupDisk();

            if (!RunCommonChecks()) {
                return;
            }

            NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

            const auto& vdiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());

            if (!config.HasBlobStorageConfig()) {
                return FinishWithError(TResult::ERROR, "no BlobStorageConfig defined");
            }
            const auto& bsConfig = config.GetBlobStorageConfig();

            if (!bsConfig.HasServiceSet()) {
                return FinishWithError(TResult::ERROR, "no ServiceSet defined");
            }
            const auto& ss = bsConfig.GetServiceSet();

            if (!bsConfig.HasAutoconfigSettings()) {
                return FinishWithError(TResult::ERROR, "no AutoconfigSettings defined");
            }
            const auto& settings = bsConfig.GetAutoconfigSettings();

            THashMap<TVDiskIdShort, NBsController::TPDiskId> replacedDisks;
            NBsController::TGroupMapper::TForbiddenPDisks forbid;
            for (const auto& vdisk : ss.GetVDisks()) {
                const TVDiskID currentVDiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
                if (!currentVDiskId.SameExceptGeneration(vdiskId)) {
                    continue;
                }
                if (currentVDiskId == vdiskId) {
                    NBsController::TPDiskId pdiskId;
                    if (cmd.HasPDiskId()) {
                        const auto& target = cmd.GetPDiskId();
                        pdiskId = {target.GetNodeId(), target.GetPDiskId()};
                    }
                    replacedDisks.emplace(vdiskId, pdiskId);
                } else {
                    Y_DEBUG_ABORT_UNLESS(vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY ||
                        vdisk.HasDonorMode());
                    const auto& loc = vdisk.GetVDiskLocation();
                    forbid.emplace(loc.GetNodeID(), loc.GetPDiskID());
                }
            }

            for (const auto& group : ss.GetGroups()) {
                if (group.GetGroupID() == vdiskId.GroupID) {
                    try {
                        Self->AllocateStaticGroup(&config, vdiskId.GroupID, vdiskId.GroupGeneration + 1,
                            TBlobStorageGroupType((TBlobStorageGroupType::EErasureSpecies)group.GetErasureSpecies()),
                            settings.GetGeometry(), settings.GetPDiskFilter(), replacedDisks, forbid, 0,
                            &ev->Get()->BaseConfig, cmd.GetConvertToDonor());
                    } catch (const TExConfigError& ex) {
                        STLOG(PRI_NOTICE, BS_NODE, NW49, "ReassignGroupDisk failed to allocate group", (Config, config),
                            (BaseConfig, ev->Get()->BaseConfig),
                            (Error, ex.what()));
                        return FinishWithError(TResult::ERROR, TStringBuilder() << "failed to allocate group: " << ex.what());
                    }

                    config.SetGeneration(config.GetGeneration() + 1);
                    return StartProposition(&config);
                }
            }

            return FinishWithError(TResult::ERROR, TStringBuilder() << "group not found");
        }

        void StaticVDiskSlain(const NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot::TStaticVDiskSlain& cmd) {
            HandleDropDonorAndSlain(VDiskIDFromVDiskID(cmd.GetVDiskId()), cmd.GetVSlotId(), false);
        }

        void DropDonor(const NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot::TDropDonor& cmd) {
            HandleDropDonorAndSlain(VDiskIDFromVDiskID(cmd.GetVDiskId()), cmd.GetVSlotId(), true);
        }

        void HandleDropDonorAndSlain(TVDiskID vdiskId, const NKikimrBlobStorage::TVSlotId& vslotId, bool isDropDonor) {
            if (!RunCommonChecks()) {
                return;
            }

            NKikimrBlobStorage::TStorageConfig config = *Self->StorageConfig;

            if (!config.HasBlobStorageConfig()) {
                return FinishWithError(TResult::ERROR, "no BlobStorageConfig defined");
            }
            auto *bsConfig = config.MutableBlobStorageConfig();

            if (!bsConfig->HasServiceSet()) {
                return FinishWithError(TResult::ERROR, "no ServiceSet defined");
            }
            auto *ss = bsConfig->MutableServiceSet();

            bool changes = false;
            ui32 pdiskUsageCount = 0;

            ui32 actualGroupGeneration = 0;
            for (const auto& group : ss->GetGroups()) {
                if (group.GetGroupID() == vdiskId.GroupID) {
                    actualGroupGeneration = group.GetGroupGeneration();
                    break;
                }
            }
            Y_ABORT_UNLESS(0 < actualGroupGeneration && vdiskId.GroupGeneration < actualGroupGeneration);

            for (size_t i = 0; i < ss->VDisksSize(); ++i) {
                if (const auto& vdisk = ss->GetVDisks(i); vdisk.HasVDiskID() && vdisk.HasVDiskLocation()) {
                    const TVDiskID currentVDiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());
                    if (!currentVDiskId.SameExceptGeneration(vdiskId) ||
                            vdisk.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
                        continue;
                    }

                    if (isDropDonor && !vdisk.HasDonorMode()) {
                        Y_ABORT_UNLESS(currentVDiskId.GroupGeneration == actualGroupGeneration);
                        auto *m = ss->MutableVDisks(i);
                        if (vdiskId.GroupGeneration) { // drop specific donor
                            for (size_t k = 0; k < m->DonorsSize(); ++k) {
                                const auto& donor = m->GetDonors(k);
                                const auto& loc = donor.GetVDiskLocation();
                                if (VDiskIDFromVDiskID(donor.GetVDiskId()) == vdiskId && loc.GetNodeID() == vslotId.GetNodeId() &&
                                        loc.GetPDiskID() == vslotId.GetPDiskId() && loc.GetVDiskSlotID() == vslotId.GetVSlotId()) {
                                    m->MutableDonors()->DeleteSubrange(k, 1);
                                    changes = true;
                                    break;
                                }
                            }
                        } else { // drop all of them
                            m->ClearDonors();
                            changes = true;
                        }
                        continue;
                    }

                    const auto& loc = vdisk.GetVDiskLocation();
                    if (loc.GetNodeID() != vslotId.GetNodeId() || loc.GetPDiskID() != vslotId.GetPDiskId()) {
                        continue;
                    }
                    ++pdiskUsageCount;

                    if (loc.GetVDiskSlotID() != vslotId.GetVSlotId()) {
                        continue;
                    }

                    Y_ABORT_UNLESS(currentVDiskId.GroupGeneration < actualGroupGeneration);

                    if (!isDropDonor) {
                        --pdiskUsageCount;
                        ss->MutableVDisks()->DeleteSubrange(i--, 1);
                        changes = true;
                    } else if (vdisk.HasDonorMode()) {
                        if (currentVDiskId == vdiskId || vdiskId.GroupGeneration == 0) {
                            auto *m = ss->MutableVDisks(i);
                            m->ClearDonorMode();
                            m->SetEntityStatus(NKikimrBlobStorage::EEntityStatus::DESTROY);
                            changes = true;
                        }
                    }
                }
            }

            if (!isDropDonor && !pdiskUsageCount) {
                for (size_t i = 0; i < ss->PDisksSize(); ++i) {
                    if (const auto& pdisk = ss->GetPDisks(i); pdisk.HasNodeID() && pdisk.HasPDiskID() &&
                            pdisk.GetNodeID() == vslotId.GetNodeId() && pdisk.GetPDiskID() == vslotId.GetPDiskId()) {
                        ss->MutablePDisks()->DeleteSubrange(i--, 1);
                        changes = true;
                        break;
                    }
                }
            }

            if (!changes) {
                return Finish(Sender, SelfId(), PrepareResult(TResult::OK, std::nullopt).release(), 0, Cookie);
            }

            config.SetGeneration(config.GetGeneration() + 1);
            StartProposition(&config);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Configuration proposition

        void StartProposition(NKikimrBlobStorage::TStorageConfig *config) {
            config->MutablePrevConfig()->CopyFrom(*Self->StorageConfig);
            config->MutablePrevConfig()->ClearPrevConfig();
            UpdateFingerprint(config);

            if (auto error = ValidateConfigUpdate(*Self->StorageConfig, *config)) {
                STLOG(PRI_DEBUG, BS_NODE, NW51, "proposed config validation failed", (Error, *error),
                    (Config, config));
                return FinishWithError(TResult::ERROR, TStringBuilder() << "config validation failed: " << *error);
            }

            Self->CurrentProposedStorageConfig.emplace();
            Self->CurrentProposedStorageConfig->Swap(config);

            TEvScatter task;
            auto *propose = task.MutableProposeStorageConfig();
            propose->MutableConfig()->CopyFrom(*Self->CurrentProposedStorageConfig);

            return Self->IssueScatterTask(SelfId(), std::move(task));
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Query termination and result delivery

        bool RunCommonChecks() {
            if (!Self->StorageConfig) {
                FinishWithError(TResult::ERROR, "no agreed StorageConfig");
            } else if (Self->CurrentProposedStorageConfig) {
                FinishWithError(TResult::ERROR, "config proposition request in flight");
            } else if (Self->RootState != ERootState::RELAX) {
                FinishWithError(TResult::ERROR, "something going on with default FSM");
            } else if (auto error = ValidateConfig(*Self->StorageConfig)) {
                FinishWithError(TResult::ERROR, TStringBuilder() << "current config validation failed: " << *error);
            } else {
                return true;
            }
            return false;
        }

        std::unique_ptr<TEvNodeConfigInvokeOnRootResult> PrepareResult(TResult::EStatus status,
                std::optional<std::reference_wrapper<const TString>> errorReason) {
            auto ev = std::make_unique<TEvNodeConfigInvokeOnRootResult>();
            auto *record = &ev->Record;
            record->SetStatus(status);
            if (errorReason) {
                record->SetErrorReason(*errorReason);
            }
            if (auto scepter = Scepter.lock()) {
                auto *s = record->MutableScepter();
                s->SetId(scepter->Id);
                s->SetNodeId(SelfId().NodeId());
            }
            return ev;
        }

        void FinishWithError(TResult::EStatus status, const TString& errorReason) {
            Finish(Sender, SelfId(), PrepareResult(status, errorReason).release(), 0, Cookie);
        }

        template<typename... TArgs>
        void Finish(TArgs&&... args) {
            auto handle = std::make_unique<IEventHandle>(std::forward<TArgs>(args)...);
            if (RequestSessionId) { // deliver response through interconnection session the request arrived from
                handle->Rewrite(TEvInterconnect::EvForward, RequestSessionId);
            }
            TActivationContext::Send(handle.release());
            PassAway();
        }

        void PassAway() override {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Gone, 0, ParentId, SelfId(), nullptr, 0));
            UnsubscribeInterconnect();
            TActorBootstrapped::PassAway();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvNodeConfigInvokeOnRootResult, Handle);
            hFunc(TEvNodeConfigGather, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            hFunc(TEvNodeWardenBaseConfig, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        )
    };

    void TDistributedConfigKeeper::Handle(TEvNodeConfigInvokeOnRoot::TPtr ev) {
        std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>> evPtr(ev.Release());
        ChildActors.insert(RegisterWithSameMailbox(new TInvokeRequestHandlerActor(this, std::move(evPtr))));
    }

} // NKikimr::NStorage
