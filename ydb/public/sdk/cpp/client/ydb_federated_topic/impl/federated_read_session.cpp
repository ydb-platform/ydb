#include "federated_read_session.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/common/log_lazy.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/topic_impl.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <library/cpp/threading/future/future.h>
#include <util/generic/guid.h>

namespace NYdb::NFederatedTopic {

NTopic::TTopicClientSettings FromFederated(const TFederatedTopicClientSettings& settings);

template <typename TEvent, typename TFederatedEvent>
typename std::function<void(TEvent&)> WrapFederatedHandler(std::function<void(TFederatedEvent&)> outerHandler, std::shared_ptr<TDbInfo> db, std::shared_ptr<TEventFederator> federator) {
    if (outerHandler) {
        return [outerHandler, db = std::move(db), federator = std::move(federator)](TEvent& ev) {
            auto fev = federator->LocateFederate(ev, std::move(db));
            return outerHandler(fev);
        };
    }
    return {};
}

NTopic::TReadSessionSettings FromFederated(const TFederatedReadSessionSettings& settings, const std::shared_ptr<TDbInfo>& db, std::shared_ptr<TEventFederator> federator) {
    NTopic::TReadSessionSettings SubsessionSettings = settings;
    SubsessionSettings.EventHandlers_.MaxMessagesBytes(settings.EventHandlers_.MaxMessagesBytes_);
    SubsessionSettings.EventHandlers_.HandlersExecutor(settings.EventHandlers_.HandlersExecutor_);

    if (settings.FederatedEventHandlers_.SimpleDataHandlers_.DataHandler) {
        SubsessionSettings.EventHandlers_.SimpleDataHandlers(
            WrapFederatedHandler<NTopic::TReadSessionEvent::TDataReceivedEvent, TReadSessionEvent::TDataReceivedEvent>(
                settings.FederatedEventHandlers_.SimpleDataHandlers_.DataHandler, db, federator),
            settings.FederatedEventHandlers_.SimpleDataHandlers_.CommitDataAfterProcessing,
            settings.FederatedEventHandlers_.SimpleDataHandlers_.GracefulStopAfterCommit);
    }

#define MAYBE_CONVERT_HANDLER(type, name)                                                                       \
    if (settings.FederatedEventHandlers_.name##_) {                                                             \
        SubsessionSettings.EventHandlers_.name(                                                                 \
            WrapFederatedHandler<NTopic::type, type>(settings.FederatedEventHandlers_.name##_, db, federator)   \
        );                                                                                                      \
    }

    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TDataReceivedEvent, DataReceivedHandler);
    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TCommitOffsetAcknowledgementEvent, CommitOffsetAcknowledgementHandler);
    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TStartPartitionSessionEvent, StartPartitionSessionHandler);
    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TStopPartitionSessionEvent, StopPartitionSessionHandler);
    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TEndPartitionSessionEvent, EndPartitionSessionHandler);
    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TPartitionSessionStatusEvent, PartitionSessionStatusHandler);
    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TPartitionSessionClosedEvent, PartitionSessionClosedHandler);
    MAYBE_CONVERT_HANDLER(TReadSessionEvent::TEvent, CommonHandler);

#undef MAYBE_CONVERT_HANDLER

    SubsessionSettings.EventHandlers_.SessionClosedHandler(settings.FederatedEventHandlers_.SessionClosedHandler_);

    return SubsessionSettings;
}

TFederatedReadSessionImpl::TFederatedReadSessionImpl(const TFederatedReadSessionSettings& settings,
                                                     std::shared_ptr<TGRpcConnectionsImpl> connections,
                                                     const TFederatedTopicClientSettings& clientSettings,
                                                     std::shared_ptr<TFederatedDbObserver> observer,
                                                     std::shared_ptr<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>> codecs)
    : Settings(settings)
    , Connections(std::move(connections))
    , SubClientSetttings(FromFederated(clientSettings))
    , ProvidedCodecs(std::move(codecs))
    , Observer(std::move(observer))
    , AsyncInit(Observer->WaitForFirstState())
    , FederationState(nullptr)
    , EventFederator(std::make_shared<TEventFederator>())
    , Log(Connections->GetLog())
    , SessionId(CreateGuidAsString())
{
}

TStringBuilder TFederatedReadSessionImpl::GetLogPrefix() const {
     return TStringBuilder() << GetDatabaseLogPrefix(SubClientSetttings.Database_.GetOrElse("")) << "[" << SessionId << "] ";
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
    {
        TStringBuilder log(GetLogPrefix());
        log << "Open read subsessions to databases: ";
        bool first = true;
        for (const auto& db : dbInfos) {
            if (first) first = false; else log << ", ";
            log << "{ name: " << db->name()
                << ", endpoint: " << db->endpoint()
                << ", path: " << db->path() << " }";
        }
        LOG_LAZY(Log, TLOG_INFO, log);
    }
    for (const auto& db : dbInfos) {
        NTopic::TTopicClientSettings settings = SubClientSetttings;
        settings
            .Database(db->path())
            .DiscoveryEndpoint(db->endpoint());
        auto subclient = make_shared<NTopic::TTopicClient::TImpl>(Connections, settings);
        auto subsession = subclient->CreateReadSession(FromFederated(Settings, db, EventFederator));
        SubSessions.emplace_back(subsession, db);
    }
    SubsessionIndex = 0;
}

void TFederatedReadSessionImpl::OnFederatedStateUpdateImpl() {
    if (!FederationState->Status.IsSuccess()) {
        LOG_LAZY(Log, TLOG_ERR, GetLogPrefix() << "Federated state update failed. FederationState: " << *FederationState);
        CloseImpl();
        return;
    }

    EventFederator->SetFederationState(FederationState);

    if (Settings.IsReadMirroredEnabled()) {
        Y_ABORT_UNLESS(Settings.GetDatabasesToReadFrom().size() == 1);
        auto dbToReadFrom = *Settings.GetDatabasesToReadFrom().begin();

        std::vector<TString> dbNames = GetAllFederationDatabaseNames();
        auto topics = Settings.Topics_;
        for (const auto& topic : topics) {
            for (const auto& dbName : dbNames) {
                if (AsciiEqualsIgnoreCase(dbName, dbToReadFrom)) {
                    continue;
                }
                auto mirroredTopic = topic;
                mirroredTopic.PartitionIds_.clear();
                mirroredTopic.Path(topic.Path_ + "-mirrored-from-" + dbName);
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
        // TODO: investigate here, why empty list?
        // Reason (and returned status) could be BAD_REQUEST or UNAVAILABLE.
        LOG_LAZY(Log, TLOG_ERR, GetLogPrefix() << "No available databases to read.");
        auto issues = FederationState->Status.GetIssues();
        TStringBuilder issue;
        issue << "Requested databases {";
        bool first = true;
        for (auto const& dbFromSettings : Settings.GetDatabasesToReadFrom()) {
            if (first) first = false; else issue << ",";
            issue << " " << dbFromSettings;
        }
        issue << " } not found. Available databases {";
        first = true;
        for (auto const& db : FederationState->DbInfos) {
            if (db->status() == Ydb::FederationDiscovery::DatabaseInfo_Status_AVAILABLE) {
                if (first) first = false; else issue << ",";
                issue << " { name: " << db->name()
                      << ", endpoint: " << db->endpoint()
                      << ", path: " << db->path() << " }";
            }
        }
        issue << " }";
        issues.AddIssue(issue);
        FederationState->Status = TStatus(EStatus::BAD_REQUEST, std::move(issues));
        CloseImpl();
        return;
    }

    OpenSubSessionsImpl(databases);
}

std::vector<TString> TFederatedReadSessionImpl::GetAllFederationDatabaseNames() {
    std::vector<TString> result;
    for (const auto& db : FederationState->DbInfos) {
        result.push_back(db->name());
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
            return {NTopic::TSessionClosedEvent(FederationState->Status.GetStatus(), NYql::TIssues(FederationState->Status.GetIssues()))};
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
                result.push_back(EventFederator->LocateFederate(std::move(ev), sub.DbInfo));
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
