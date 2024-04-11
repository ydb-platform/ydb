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
            STLOG(PRI_DEBUG, BS_NODE, NWDC43, "ExecuteQuery", (SelfId, SelfId()));

            auto& record = Event->Get()->Record;
            switch (record.GetRequestCase()) {
                case TQuery::kUpdateConfig: {
                    auto *request = record.MutableUpdateConfig();

                    if (!request->HasConfig()) {
                        return FinishWithError(TResult::ERROR, "Config field is not filled in");
                    } else if (!Self->StorageConfig) {
                        return FinishWithError(TResult::ERROR, "no agreed StorageConfig");
                    } else if (Self->CurrentProposedStorageConfig) {
                        return FinishWithError(TResult::ERROR, "config proposition request in flight");
                    } else if (Self->RootState != ERootState::RELAX) {
                        return FinishWithError(TResult::ERROR, "something going on with default FSM");
                    }

                    auto *config = request->MutableConfig();

                    if (auto error = ValidateConfig(*Self->StorageConfig)) {
                        return FinishWithError(TResult::ERROR, TStringBuilder() << "current config validation failed: " << *error);
                    } else if (auto error = ValidateConfigUpdate(*Self->StorageConfig, *config)) {
                        return FinishWithError(TResult::ERROR, TStringBuilder() << "config validation failed: " << *error);
                    }

                    config->MutablePrevConfig()->CopyFrom(*Self->StorageConfig);
                    config->MutablePrevConfig()->ClearPrevConfig();
                    UpdateFingerprint(config);

                    Self->CurrentProposedStorageConfig.emplace();
                    Self->CurrentProposedStorageConfig->Swap(config);

                    TEvScatter task;
                    auto *propose = task.MutableProposeStorageConfig();
                    propose->MutableConfig()->CopyFrom(*Self->CurrentProposedStorageConfig);

                    return Self->IssueScatterTask(SelfId(), std::move(task));
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
        // Query termination and result delivery

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
            cFunc(TEvents::TSystem::Poison, PassAway);
        )
    };

    void TDistributedConfigKeeper::Handle(TEvNodeConfigInvokeOnRoot::TPtr ev) {
        std::unique_ptr<TEventHandle<TEvNodeConfigInvokeOnRoot>> evPtr(ev.Release());
        ChildActors.insert(RegisterWithSameMailbox(new TInvokeRequestHandlerActor(this, std::move(evPtr))));
    }

} // NKikimr::NStorage
