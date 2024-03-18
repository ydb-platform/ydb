#include "federated_write_session.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/log_lazy.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/topic_impl.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <library/cpp/threading/future/future.h>

#include <algorithm>

namespace NYdb::NFederatedTopic {

constexpr TDuration UPDATE_FEDERATION_STATE_DELAY = TDuration::Seconds(10);

bool DatabasesAreSame(std::shared_ptr<TDbInfo> lhs, std::shared_ptr<TDbInfo> rhs) {
    if (!lhs || !rhs) {
        return false;
    }
    return lhs->path() == rhs->path() && lhs->endpoint() == rhs->endpoint();
}

NTopic::TTopicClientSettings FromFederated(const TFederatedTopicClientSettings& settings);

TFederatedWriteSession::TFederatedWriteSession(const TFederatedWriteSessionSettings& settings,
                                             std::shared_ptr<TGRpcConnectionsImpl> connections,
                                             const TFederatedTopicClientSettings& clientSetttings,
                                             std::shared_ptr<TFederatedDbObserver> observer,
                                             std::shared_ptr<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>> codecs)
    : Settings(settings)
    , Connections(std::move(connections))
    , SubClientSetttings(FromFederated(clientSetttings))
    , ProvidedCodecs(std::move(codecs))
    , Observer(std::move(observer))
    , AsyncInit(Observer->WaitForFirstState())
    , FederationState(nullptr)
    , Log(Connections->GetLog())
    , ClientEventsQueue(std::make_shared<NTopic::TWriteSessionEventsQueue>(Settings))
    , BufferFreeSpace(Settings.MaxMemoryUsage_)
{
}

TStringBuilder TFederatedWriteSession::GetLogPrefix() const {
     return TStringBuilder() << GetDatabaseLogPrefix(SubClientSetttings.Database_.GetOrElse("")) << "[" << SessionId << "] ";
}

void TFederatedWriteSession::Start() {
    // TODO validate settings?
    Settings.EventHandlers_.HandlersExecutor_->Start();
    with_lock(Lock) {
        ClientEventsQueue->PushEvent(NTopic::TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()});
        ClientHasToken = true;
    }

    AsyncInit.Subscribe([self = shared_from_this()](const auto& f){
        Y_UNUSED(f);
        with_lock(self->Lock) {
            self->FederationState = self->Observer->GetState();
            self->OnFederatedStateUpdateImpl();
        }
    });
}

void TFederatedWriteSession::OpenSubSessionImpl(std::shared_ptr<TDbInfo> db) {
    if (Subsession) {
        PendingToken.Clear();
        Subsession->Close(TDuration::Zero());
    }
    NTopic::TTopicClientSettings clientSettings = SubClientSetttings;
    clientSettings
        .Database(db->path())
        .DiscoveryEndpoint(db->endpoint());
    auto subclient = make_shared<NTopic::TTopicClient::TImpl>(Connections, clientSettings);
    subclient->SetProvidedCodecs(ProvidedCodecs);

    auto handlers = NTopic::TWriteSessionSettings::TEventHandlers()
        .HandlersExecutor(Settings.EventHandlers_.HandlersExecutor_)
        .ReadyToAcceptHander([self = shared_from_this()](NTopic::TWriteSessionEvent::TReadyToAcceptEvent& ev){
            TDeferredWrite deferred(self->Subsession);
            with_lock(self->Lock) {
                Y_ABORT_UNLESS(self->PendingToken.Empty());
                self->PendingToken = std::move(ev.ContinuationToken);
                self->PrepareDeferredWrite(deferred);
            }
            deferred.DoWrite();
        })
        .AcksHandler([self = shared_from_this()](NTopic::TWriteSessionEvent::TAcksEvent& ev){
            with_lock(self->Lock) {
                Y_ABORT_UNLESS(ev.Acks.size() <= self->OriginalMessagesToGetAck.size());
                for (size_t i = 0; i < ev.Acks.size(); ++i) {
                    self->BufferFreeSpace += self->OriginalMessagesToGetAck.front().Data.size();
                    self->OriginalMessagesToGetAck.pop_front();
                }
                self->ClientEventsQueue->PushEvent(std::move(ev));
                if (self->BufferFreeSpace > 0 && !self->ClientHasToken) {
                    self->ClientEventsQueue->PushEvent(NTopic::TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()});
                    self->ClientHasToken = true;
                }
            }
        })
        .SessionClosedHandler([self = shared_from_this()](const NTopic::TSessionClosedEvent & ev){
            with_lock(self->Lock) {
                self->ClientEventsQueue->PushEvent(ev);
            }
        });

    NTopic::TWriteSessionSettings wsSettings = Settings;
    wsSettings
        // .MaxMemoryUsage(Settings.MaxMemoryUsage_)  // to fix if split not by half on creation
        .EventHandlers(handlers);

    Subsession = subclient->CreateWriteSession(wsSettings);
    CurrentDatabase = db;
}

std::shared_ptr<TDbInfo> TFederatedWriteSession::SelectDatabaseImpl() {
    std::vector<std::shared_ptr<TDbInfo>> availableDatabases;
    ui64 totalWeight = 0;

    for (const auto& db : FederationState->DbInfos) {
        if (db->status() != TDbInfo::Status::DatabaseInfo_Status_AVAILABLE) {
            continue;
        }

        if (Settings.PreferredDatabase_ && (AsciiEqualsIgnoreCase(db->name(), *Settings.PreferredDatabase_) ||
                                            AsciiEqualsIgnoreCase(db->id(), *Settings.PreferredDatabase_))) {
            return db;
        } else if (AsciiEqualsIgnoreCase(FederationState->SelfLocation, db->location())) {
            return db;
        } else {
            availableDatabases.push_back(db);
            totalWeight += db->weight();
        }
    }

    if (availableDatabases.empty() || totalWeight == 0) {
        // close session, return error
        return nullptr;
    }

    std::sort(availableDatabases.begin(), availableDatabases.end(), [](const std::shared_ptr<TDbInfo>& lhs, const std::shared_ptr<TDbInfo>& rhs){
        return lhs->weight() > rhs->weight()
               || lhs->weight() == rhs->weight() && lhs->name() < rhs->name();
    });

    ui64 hashValue = THash<TString>()(Settings.Path_);
    hashValue = CombineHashes(hashValue, THash<TString>()(Settings.ProducerId_));
    hashValue %= totalWeight;

    ui64 borderWeight = 0;
    for (const auto& db : availableDatabases) {
        borderWeight += db->weight();
        if (hashValue < borderWeight) {
            return db;
        }
    }
    Y_UNREACHABLE();
}

void TFederatedWriteSession::OnFederatedStateUpdateImpl() {
    if (!FederationState->Status.IsSuccess()) {
        CloseImpl(FederationState->Status.GetStatus(), NYql::TIssues(FederationState->Status.GetIssues()));
        return;
    }

    Y_ABORT_UNLESS(!FederationState->DbInfos.empty());

    auto preferrableDb = SelectDatabaseImpl();

    if (!preferrableDb) {
        CloseImpl(EStatus::UNAVAILABLE,
                  NYql::TIssues{NYql::TIssue("Fail to select database: no available database with positive weight")});
        return;
    }

    if (!DatabasesAreSame(preferrableDb, CurrentDatabase)) {
        LOG_LAZY(Log, TLOG_INFO, GetLogPrefix()
            << "Start federated write session to database '" << preferrableDb->name()
            << "' (previous was " << (CurrentDatabase ? CurrentDatabase->name() : "<empty>") << ")"
            << " FederationState: " << *FederationState);
        OpenSubSessionImpl(preferrableDb);
    }

    ScheduleFederatedStateUpdateImpl(UPDATE_FEDERATION_STATE_DELAY);
}

void TFederatedWriteSession::ScheduleFederatedStateUpdateImpl(TDuration delay) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    auto cb = [self = shared_from_this()](bool ok) {
        if (ok) {
            with_lock(self->Lock) {
                if (self->Closing) {
                    return;
                }
                self->FederationState = self->Observer->GetState();
                self->OnFederatedStateUpdateImpl();
            }
        }
    };

    UpdateStateDelayContext = Connections->CreateContext();
    if (!UpdateStateDelayContext) {
        Closing = true;
        // TODO log DRIVER_IS_STOPPING_DESCRIPTION
        return;
    }
    Connections->ScheduleCallback(delay,
                                  std::move(cb),
                                  UpdateStateDelayContext);
}

NThreading::TFuture<void> TFederatedWriteSession::WaitEvent() {
    return ClientEventsQueue->WaitEvent();
}

TVector<NTopic::TWriteSessionEvent::TEvent> TFederatedWriteSession::GetEvents(bool block, TMaybe<size_t> maxEventsCount) {
    return ClientEventsQueue->GetEvents(block, maxEventsCount);
}

TMaybe<NTopic::TWriteSessionEvent::TEvent> TFederatedWriteSession::GetEvent(bool block) {
    auto events = GetEvents(block, 1);
    return events.empty() ? Nothing() : TMaybe<NTopic::TWriteSessionEvent::TEvent>{std::move(events.front())};
}

NThreading::TFuture<ui64> TFederatedWriteSession::GetInitSeqNo() {
    return NThreading::MakeFuture<ui64>(0u);
}

void TFederatedWriteSession::Write(NTopic::TContinuationToken&& token, TStringBuf data, TMaybe<ui64> seqNo,
                                   TMaybe<TInstant> createTimestamp) {
    NTopic::TWriteMessage message{std::move(data)};
    if (seqNo.Defined())
        message.SeqNo(*seqNo);
    if (createTimestamp.Defined())
        message.CreateTimestamp(*createTimestamp);
    return WriteInternal(std::move(token), std::move(message));
}

void TFederatedWriteSession::Write(NTopic::TContinuationToken&& token, NTopic::TWriteMessage&& message) {
    return WriteInternal(std::move(token), std::move(message));
}

void TFederatedWriteSession::WriteEncoded(NTopic::TContinuationToken&& token, TStringBuf data, NTopic::ECodec codec,
                                          ui32 originalSize, TMaybe<ui64> seqNo, TMaybe<TInstant> createTimestamp) {
    auto message = NTopic::TWriteMessage::CompressedMessage(std::move(data), codec, originalSize);
    if (seqNo.Defined())
        message.SeqNo(*seqNo);
    if (createTimestamp.Defined())
        message.CreateTimestamp(*createTimestamp);
    return WriteInternal(std::move(token), std::move(message));
}

void TFederatedWriteSession::WriteEncoded(NTopic::TContinuationToken&& token, NTopic::TWriteMessage&& message) {
    return WriteInternal(std::move(token), std::move(message));
}

void TFederatedWriteSession::WriteInternal(NTopic::TContinuationToken&&, NTopic::TWriteMessage&& message) {
    ClientHasToken = false;
    if (!message.CreateTimestamp_.Defined()) {
        message.CreateTimestamp_ = TInstant::Now();
    }

    {
        TDeferredWrite deferred(Subsession);
        with_lock(Lock) {
            BufferFreeSpace -= message.Data.size();
            OriginalMessagesToPassDown.emplace_back(std::move(message));

            PrepareDeferredWrite(deferred);
        }
        deferred.DoWrite();
    }
    if (BufferFreeSpace > 0) {
        ClientEventsQueue->PushEvent(NTopic::TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()});
        ClientHasToken = true;
    }
}

bool TFederatedWriteSession::PrepareDeferredWrite(TDeferredWrite& deferred) {
    if (PendingToken.Empty()) {
        return false;
    }
    if (OriginalMessagesToPassDown.empty()) {
        return false;
    }
    OriginalMessagesToGetAck.push_back(OriginalMessagesToPassDown.front());
    deferred.Token.ConstructInPlace(std::move(*PendingToken));
    deferred.Message.ConstructInPlace(std::move(OriginalMessagesToPassDown.front()));
    OriginalMessagesToPassDown.pop_front();
    PendingToken.Clear();
    return true;
}

void TFederatedWriteSession::CloseImpl(EStatus statusCode, NYql::TIssues&& issues) {
    Closing = true;
    if (Subsession) {
        Subsession->Close(TDuration::Zero());
    }
    ClientEventsQueue->Close(TSessionClosedEvent(statusCode, std::move(issues)));
}

bool TFederatedWriteSession::Close(TDuration timeout) {
    if (Subsession) {
        return Subsession->Close(timeout);
    }
    return true;
}

}  // namespace NYdb::NFederatedTopic
