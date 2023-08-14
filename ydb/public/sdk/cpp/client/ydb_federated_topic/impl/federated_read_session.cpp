#include "federated_read_session.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/log_lazy.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/topic_impl.h>

#include <library/cpp/threading/future/future.h>
#include <util/generic/guid.h>

namespace NYdb::NFederatedTopic {

NTopic::TTopicClientSettings FromFederated(const TFederatedTopicClientSettings& settings);

TFederatedReadSession::TFederatedReadSession(const TFederatedReadSessionSettings& settings,
                                             std::shared_ptr<TGRpcConnectionsImpl> connections,
                                             const TFederatedTopicClientSettings& clientSetttings,
                                             std::shared_ptr<TFederatedDbObserver> observer)
    : Settings(settings)
    , Connections(std::move(connections))
    , SubClientSetttings(FromFederated(clientSetttings))
    , Observer(std::move(observer))
    , AsyncInit(Observer->WaitForFirstState())
    , FederationState(nullptr)
    , SessionId(CreateGuidAsString())
{
}

void TFederatedReadSession::Start() {
    AsyncInit.Subscribe([self = shared_from_this()](const auto& f){
        Y_UNUSED(f);
        with_lock(self->Lock) {
            self->FederationState = self->Observer->GetState();
            self->OnFederatedStateUpdateImpl();
        }
    });
}

void TFederatedReadSession::OpenSubSessionsImpl() {
    for (const auto& db : FederationState->DbInfos) {
        // TODO check if available
        NTopic::TTopicClientSettings settings = SubClientSetttings;
        settings
            .Database(db->path())
            .DiscoveryEndpoint(db->endpoint());
        auto subclient = make_shared<NTopic::TTopicClient::TImpl>(Connections, settings);
        auto subsession = subclient->CreateReadSession(Settings);
        SubSessions.emplace_back(subsession, db);
    }
    SubsessionIndex = 0;
}

void TFederatedReadSession::OnFederatedStateUpdateImpl() {
    if (!FederationState->Status.IsSuccess()) {
        CloseImpl();
        return;
    }
    // 1) compare old info and new info;
    //    result: list of subsessions to open + list of subsessions to close
    // 2) OpenSubSessionsImpl, CloseSubSessionsImpl
    OpenSubSessionsImpl();
    // 3) TODO LATER reschedule OnFederatedStateUpdate
}

NThreading::TFuture<void> TFederatedReadSession::WaitEvent() {
    // TODO override with read session settings timeout
    return AsyncInit.Apply([self = shared_from_this()](const NThreading::TFuture<void>) {
        if (self->Closing) {
            return NThreading::MakeFuture();
        }
        std::vector<NThreading::TFuture<void>> waiters;
        with_lock(self->Lock) {
            Y_VERIFY(!self->SubSessions.empty(), "SubSessions empty in discovered state");
            for (const auto& sub : self->SubSessions) {
                waiters.emplace_back(sub.Session->WaitEvent());
            }
        }
        return NThreading::WaitAny(std::move(waiters));
    });
}

TVector<TReadSessionEvent::TEvent> TFederatedReadSession::GetEvents(bool block, TMaybe<size_t> maxEventsCount, size_t maxByteSize) {
    if (block) {
        WaitEvent().Wait();
    }
    with_lock(Lock) {
        if (Closing) {
            // TODO correct conversion
            return {NTopic::TSessionClosedEvent(FederationState->Status.GetStatus(), {})};
        }
        // TODO!!! handle aborting or closing state
        //         via handler on SessionClosedEvent {
        //    cancel all subsessions, empty SubSessions, set aborting
        // }
        if (SubSessions.empty()) {
            return {};
        }
    }
    TVector<TReadSessionEvent::TEvent> result;
    with_lock(Lock) {
        do {
            auto sub = SubSessions[SubsessionIndex];
            // TODO remove copy
            for (auto&& ev : sub.Session->GetEvents(false, maxEventsCount, maxByteSize)) {
                result.push_back(Federate(ev, sub.DbInfo));
            }
            SubsessionIndex = (SubsessionIndex + 1) % SubSessions.size();
        }
        while (block && result.empty());
    }
    return result;
}

TMaybe<TReadSessionEvent::TEvent> TFederatedReadSession::GetEvent(bool block, size_t maxByteSize) {
    auto events = GetEvents(block, 1, maxByteSize);
    return events.empty() ? Nothing() : TMaybe<TReadSessionEvent::TEvent>{std::move(events.front())};
}

void TFederatedReadSession::CloseImpl() {
    Closing = true;
}

bool TFederatedReadSession::Close(TDuration timeout) {
    bool result = true;
    for (const auto& sub : SubSessions) {
        // TODO substract from user timeout
        result = sub.Session->Close(timeout);
    }
    return result;
}

}  // namespace NYdb::NFederatedTopic
