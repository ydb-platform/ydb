#include "federated_read_session.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/log_lazy.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/topic_impl.h>

#include <library/cpp/threading/future/future.h>
#include <util/generic/guid.h>

namespace NYdb::NFederatedTopic {

NTopic::TTopicClientSettings FromFederated(const TFederatedTopicClientSettings& settings);

template <typename TEvent, typename TFederatedEvent>
typename std::function<void(TEvent&)> WrapFederatedHandler(std::function<void(TFederatedEvent&)> outerHandler, std::shared_ptr<TDbInfo> db) {
    if (outerHandler) {
        return [outerHandler, db = std::move(db)](TEvent& ev) {
            auto fev = Federate(std::move(ev), db);
            return outerHandler(fev);
        };
    }
    return {};
}

NTopic::TReadSessionSettings FromFederated(const TFederatedReadSessionSettings& settings, const std::shared_ptr<TDbInfo>& db) {
    NTopic::TReadSessionSettings SubsessionSettings = settings;
    SubsessionSettings.EventHandlers_.MaxMessagesBytes(settings.EventHandlers_.MaxMessagesBytes_);
    SubsessionSettings.EventHandlers_.HandlersExecutor(settings.EventHandlers_.HandlersExecutor_);

#define MAYBE_CONVERT_HANDLER(type, name) \
    SubsessionSettings.EventHandlers_.name( \
        WrapFederatedHandler<NTopic::type, type>(settings.FederatedEventHandlers_.name##_, db) \
    );

    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TDataReceivedEvent, DataReceivedHandler);
    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TCommitOffsetAcknowledgementEvent, CommitOffsetAcknowledgementHandler);
    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TStartPartitionSessionEvent, StartPartitionSessionHandler);
    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TStopPartitionSessionEvent, StopPartitionSessionHandler);
    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TPartitionSessionStatusEvent, PartitionSessionStatusHandler);
    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TPartitionSessionClosedEvent, PartitionSessionClosedHandler);
    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TEvent, CommonHandler);

#undef MAYBE_CONVERT_HANDLER

    SubsessionSettings.EventHandlers_.SessionClosedHandler(settings.FederatedEventHandlers_.SessionClosedHandler_);

    if (settings.FederatedEventHandlers_.SimpleDataHandlers_.DataHandler) {
        SubsessionSettings.EventHandlers_.SimpleDataHandlers(
            WrapFederatedHandler<NTopic::TReadSessionEvent::TDataReceivedEvent, TReadSessionEvent::TDataReceivedEvent>(
                settings.FederatedEventHandlers_.SimpleDataHandlers_.DataHandler, db),
            settings.FederatedEventHandlers_.SimpleDataHandlers_.CommitDataAfterProcessing,
            settings.FederatedEventHandlers_.SimpleDataHandlers_.GracefulStopAfterCommit);
    }

    return SubsessionSettings;
}

TFederatedReadSessionImpl::TFederatedReadSessionImpl(const TFederatedReadSessionSettings& settings,
                                                     std::shared_ptr<TGRpcConnectionsImpl> connections,
                                                     const TFederatedTopicClientSettings& clientSetttings,
                                                     std::shared_ptr<TFederatedDbObserver> observer)
    : Settings(settings)
    , Connections(std::move(connections))
    , SubClientSetttings(FromFederated(clientSetttings))
    , Observer(std::move(observer))
    , AsyncInit(Observer->WaitForFirstState())
    , FederationState(nullptr)
    , Log(Connections->GetLog())
    , SessionId(CreateGuidAsString())
{
}

void TFederatedReadSessionImpl::Start() {
    AsyncInit.Subscribe([selfCtx = SelfContext](const auto& f){
        Y_UNUSED(f);
        if (auto self = selfCtx->LockShared()) {
            with_lock(self->Lock) {
                if (self->Closing) {
                    return;
                }
                self->FederationState = self->Observer->GetState();
                self->OnFederatedStateUpdateImpl();
            }
        }
    });
}

void TFederatedReadSessionImpl::OpenSubSessionsImpl(const std::vector<std::shared_ptr<TDbInfo>>& dbInfos) {
    for (const auto& db : dbInfos) {
        NTopic::TTopicClientSettings settings = SubClientSetttings;
        settings
            .Database(db->path())
            .DiscoveryEndpoint(db->endpoint());
        auto subclient = make_shared<NTopic::TTopicClient::TImpl>(Connections, settings);
        auto subsession = subclient->CreateReadSession(FromFederated(Settings, db));
        SubSessions.emplace_back(subsession, db);
    }
    SubsessionIndex = 0;
}

void TFederatedReadSessionImpl::OnFederatedStateUpdateImpl() {
    if (!FederationState->Status.IsSuccess()) {
        CloseImpl();
        return;
    }
    if (Settings.IsReadMirroredEnabled()) {
        Y_ABORT_UNLESS(Settings.GetDatabasesToReadFrom().size() == 1);
        auto dbToReadFrom = *Settings.GetDatabasesToReadFrom().begin();

        std::vector<TString> dcNames = GetAllFederationLocations();
        auto topics = Settings.Topics_;
        for (const auto& topic : topics) {
            for (const auto& dc : dcNames) {
                if (AsciiEqualsIgnoreCase(dc, dbToReadFrom)) {
                    continue;
                }
                auto mirroredTopic = topic;
                mirroredTopic.PartitionIds_.clear();
                mirroredTopic.Path(topic.Path_ + "-mirrored-from-" + dc);
                Settings.AppendTopics(mirroredTopic);
            }
        }
    }

    std::vector<std::shared_ptr<TDbInfo>> databases;

    for (const auto& db : FederationState->DbInfos) {
        if (IsDatabaseEligibleForRead(db)) {
            databases.push_back(db);
        }
    }

    if (databases.empty()) {
        CloseImpl();
        return;
    }

    OpenSubSessionsImpl(databases);
}

std::vector<TString> TFederatedReadSessionImpl::GetAllFederationLocations() {
    std::vector<TString> result;
    for (const auto& db : FederationState->DbInfos) {
        result.push_back(db->location());
    }
    return result;
}

bool TFederatedReadSessionImpl::IsDatabaseEligibleForRead(const std::shared_ptr<TDbInfo>& db) {
    if (db->status() != TDbInfo::Status::DatabaseInfo_Status_AVAILABLE &&
        db->status() != TDbInfo::Status::DatabaseInfo_Status_READ_ONLY) {
        return false;
    }

    if (Settings.GetDatabasesToReadFrom().empty()) {
        return true;
    }

    for (const auto& dbFromSettings : Settings.GetDatabasesToReadFrom()) {
        if (AsciiEqualsIgnoreCase(db->name(), dbFromSettings) ||
            AsciiEqualsIgnoreCase(db->id(), dbFromSettings)) {
            return true;
        }
        if (dbFromSettings == "_local" &&
            AsciiEqualsIgnoreCase(FederationState->SelfLocation, db->location())) {
            return true;
        }
    }
    return false;
}

NThreading::TFuture<void> TFederatedReadSessionImpl::WaitEvent() {
    // TODO override with read session settings timeout
    return AsyncInit.Apply([selfCtx = SelfContext](const NThreading::TFuture<void>) {
        if (auto self = selfCtx->LockShared()) {
            std::vector<NThreading::TFuture<void>> waiters;
            with_lock(self->Lock) {
                if (self->Closing) {
                    return NThreading::MakeFuture();
                }
                Y_ABORT_UNLESS(!self->SubSessions.empty(), "SubSessions empty in discovered state");
                for (const auto& sub : self->SubSessions) {
                    waiters.emplace_back(sub.Session->WaitEvent());
                }
            }
            return NThreading::WaitAny(std::move(waiters));
        }
        return NThreading::MakeFuture();
    });
}

TVector<TReadSessionEvent::TEvent> TFederatedReadSessionImpl::GetEvents(bool block, TMaybe<size_t> maxEventsCount, size_t maxByteSize) {
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
            for (auto&& ev : sub.Session->GetEvents(false, maxEventsCount, maxByteSize)) {
                result.push_back(Federate(std::move(ev), sub.DbInfo));
            }
            SubsessionIndex = (SubsessionIndex + 1) % SubSessions.size();
        }
        while (block && result.empty());
    }
    return result;
}

void TFederatedReadSessionImpl::CloseImpl() {
    Closing = true;
}

bool TFederatedReadSessionImpl::Close(TDuration timeout) {
    with_lock(Lock) {
        Closing = true;

        bool result = true;
        for (const auto& sub : SubSessions) {
            // TODO substract from user timeout
            result = sub.Session->Close(timeout);
        }
        return result;
    }
}

}  // namespace NYdb::NFederatedTopic
