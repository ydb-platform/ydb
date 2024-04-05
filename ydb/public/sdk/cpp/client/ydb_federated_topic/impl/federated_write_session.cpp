#include "federated_write_session.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/log_lazy.h>
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

TFederatedWriteSessionImpl::TFederatedWriteSessionImpl(
    const TFederatedWriteSessionSettings& settings,
    std::shared_ptr<TGRpcConnectionsImpl> connections,
    const TFederatedTopicClientSettings& clientSetttings,
    std::shared_ptr<TFederatedDbObserver> observer,
    std::shared_ptr<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>> codecs
)
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

TStringBuilder TFederatedWriteSessionImpl::GetLogPrefix() const {
     return TStringBuilder() << GetDatabaseLogPrefix(SubClientSetttings.Database_.GetOrElse("")) << "[" << SessionId << "] ";
}

void TFederatedWriteSessionImpl::Start() {
    // TODO validate settings?
    Settings.EventHandlers_.HandlersExecutor_->Start();
    with_lock(Lock) {
        ClientEventsQueue->PushEvent(NTopic::TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()});
        ClientHasToken = true;
    }

    AsyncInit.Subscribe([selfCtx = SelfContext](const auto& f){
        Y_UNUSED(f);
        if (auto self = selfCtx->LockShared()) {
            with_lock(self->Lock) {
                if (!self->Closing) {
                    self->FederationState = self->Observer->GetState();
                    self->OnFederatedStateUpdateImpl();
                }
            }
        }
    });
}

void TFederatedWriteSessionImpl::OpenSubSessionImpl(std::shared_ptr<TDbInfo> db) {
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
        .ReadyToAcceptHandler([selfCtx = SelfContext](NTopic::TWriteSessionEvent::TReadyToAcceptEvent& ev) {
            if (auto self = selfCtx->LockShared()) {
                TDeferredWrite deferred(self->Subsession);
                with_lock(self->Lock) {
                    Y_ABORT_UNLESS(self->PendingToken.Empty());
                    self->PendingToken = std::move(ev.ContinuationToken);
                    self->PrepareDeferredWrite(deferred);
                }
                deferred.DoWrite();
            }
        })
        .AcksHandler([selfCtx = SelfContext](NTopic::TWriteSessionEvent::TAcksEvent& ev) {
            if (auto self = selfCtx->LockShared()) {
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
            }
        })
        .SessionClosedHandler([selfCtx = SelfContext](const NTopic::TSessionClosedEvent & ev) {
            if (auto self = selfCtx->LockShared()) {
                with_lock(self->Lock) {
                    if (!self->Closing) {
                        self->CloseImpl(ev);
                    }
                }
            }
        });

    NTopic::TWriteSessionSettings wsSettings = Settings;
    wsSettings
        // .MaxMemoryUsage(Settings.MaxMemoryUsage_)  // to fix if split not by half on creation
        .EventHandlers(handlers);

    Subsession = subclient->CreateWriteSession(wsSettings);
    CurrentDatabase = db;
}

std::pair<std::shared_ptr<TDbInfo>, EStatus> SelectDatabaseByHash(
    NTopic::TFederatedWriteSessionSettings const& settings,
    std::vector<std::shared_ptr<TDbInfo>> const& dbInfos
) {
    ui64 totalWeight = 0;
    std::vector<std::shared_ptr<TDbInfo>> available;

    for (const auto& db : dbInfos) {
        if (db->status() == TDbInfo::Status::DatabaseInfo_Status_AVAILABLE) {
            available.push_back(db);
            totalWeight += db->weight();
        }
    }

    if (available.empty() || totalWeight == 0) {
        return {nullptr, EStatus::NOT_FOUND};
    }

    std::sort(available.begin(), available.end(), [](auto const& lhs, auto const& rhs) {
        return lhs->weight() > rhs->weight()
               || lhs->weight() == rhs->weight() && lhs->name() < rhs->name();
    });

    ui64 hashValue = THash<TString>()(settings.Path_);
    hashValue = CombineHashes(hashValue, THash<TString>()(settings.ProducerId_));
    hashValue %= totalWeight;

    ui64 borderWeight = 0;
    for (auto const& db : available) {
        borderWeight += db->weight();
        if (hashValue < borderWeight) {
            return {db, EStatus::SUCCESS};
        }
    }
    Y_UNREACHABLE();
}

std::pair<std::shared_ptr<TDbInfo>, EStatus> SelectDatabase(
    NTopic::TFederatedWriteSessionSettings const& settings,
    std::vector<std::shared_ptr<TDbInfo>> const& dbInfos, TString const& selfLocation
) {
    /* Logic of the function should follow this table:

    | PreferredDb | Preferred state | Local state | AllowFallback | Return      |
    |-------------+-----------------+-------------+---------------+-------------|
    | set         | not found       | -           | any           | NOT_FOUND   |
    | set         | available       | -           | any           | preferred   |
    | set         | unavailable     | -           | false         | UNAVAILABLE |
    | set         | unavailable     | -           | true          | by hash     |
    | unset       | -               | not found   | false         | NOT_FOUND   |
    | unset       | -               | not found   | true          | by hash     |
    | unset       | -               | available   | any           | local       |
    | unset       | -               | unavailable | false         | UNAVAILABLE |
    | unset       | -               | unavailable | true          | by hash     |
    */

    decltype(begin(dbInfos)) it;
    if (settings.PreferredDatabase_) {
        it = std::find_if(begin(dbInfos), end(dbInfos), [&preferred = settings.PreferredDatabase_](auto const& db) {
            return AsciiEqualsIgnoreCase(*preferred, db->name()) || AsciiEqualsIgnoreCase(*preferred, db->id());
        });
        if (it == end(dbInfos)) {
            return {nullptr, EStatus::NOT_FOUND};
        }
    } else {
        it = std::find_if(begin(dbInfos), end(dbInfos), [&selfLocation](auto const& db) {
            return AsciiEqualsIgnoreCase(selfLocation, db->location());
        });
        if (it == end(dbInfos)) {
            if (!settings.AllowFallback_) {
                return {nullptr, EStatus::NOT_FOUND};
            }
            return SelectDatabaseByHash(settings, dbInfos);
        }
    }

    auto db = *it;
    if (db->status() == TDbInfo::Status::DatabaseInfo_Status_AVAILABLE) {
        return {db, EStatus::SUCCESS};
    }
    if (!settings.AllowFallback_) {
        return {nullptr, EStatus::UNAVAILABLE};
    }
    return SelectDatabaseByHash(settings, dbInfos);
}

std::pair<std::shared_ptr<TDbInfo>, EStatus> TFederatedWriteSessionImpl::SelectDatabaseImpl() {
    return SelectDatabase(Settings, FederationState->DbInfos, FederationState->SelfLocation);
}

void TFederatedWriteSessionImpl::OnFederatedStateUpdateImpl() {
    if (!FederationState->Status.IsSuccess()) {
        CloseImpl(FederationState->Status.GetStatus(), NYql::TIssues(FederationState->Status.GetIssues()));
        return;
    }

    Y_ABORT_UNLESS(!FederationState->DbInfos.empty());

    auto [preferrableDb, status] = SelectDatabaseImpl();

    if (!preferrableDb) {
        if (!RetryState) {
            RetryState = Settings.RetryPolicy_->CreateRetryState();
        }
        if (auto delay = RetryState->GetNextRetryDelay(status)) {
            ScheduleFederatedStateUpdateImpl(*delay);
        } else {
            CloseImpl(status, NYql::TIssues{NYql::TIssue("Failed to select database: no available database")});
        }
        return;
    }
    RetryState.reset();

    if (!DatabasesAreSame(preferrableDb, CurrentDatabase)) {
        LOG_LAZY(Log, TLOG_INFO, GetLogPrefix()
            << "Start federated write session to database '" << preferrableDb->name()
            << "' (previous was " << (CurrentDatabase ? CurrentDatabase->name() : "<empty>") << ")"
            << " FederationState: " << *FederationState);
        OpenSubSessionImpl(preferrableDb);
    }

    ScheduleFederatedStateUpdateImpl(UPDATE_FEDERATION_STATE_DELAY);
}

void TFederatedWriteSessionImpl::ScheduleFederatedStateUpdateImpl(TDuration delay) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    auto cb = [selfCtx = SelfContext](bool ok) {
        if (ok) {
            if (auto self = selfCtx->LockShared()) {
                with_lock(self->Lock) {
                    if (self->Closing) {
                        return;
                    }
                    self->FederationState = self->Observer->GetState();
                    self->OnFederatedStateUpdateImpl();
                }
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

NThreading::TFuture<void> TFederatedWriteSessionImpl::WaitEvent() {
    return ClientEventsQueue->WaitEvent();
}

TVector<NTopic::TWriteSessionEvent::TEvent> TFederatedWriteSessionImpl::GetEvents(bool block, TMaybe<size_t> maxEventsCount) {
    return ClientEventsQueue->GetEvents(block, maxEventsCount);
}

TMaybe<NTopic::TWriteSessionEvent::TEvent> TFederatedWriteSessionImpl::GetEvent(bool block) {
    auto events = GetEvents(block, 1);
    return events.empty() ? Nothing() : TMaybe<NTopic::TWriteSessionEvent::TEvent>{std::move(events.front())};
}

NThreading::TFuture<ui64> TFederatedWriteSessionImpl::GetInitSeqNo() {
    return NThreading::MakeFuture<ui64>(0u);
}

void TFederatedWriteSessionImpl::Write(NTopic::TContinuationToken&& token, TStringBuf data, TMaybe<ui64> seqNo,
                                   TMaybe<TInstant> createTimestamp) {
    NTopic::TWriteMessage message{std::move(data)};
    if (seqNo.Defined())
        message.SeqNo(*seqNo);
    if (createTimestamp.Defined())
        message.CreateTimestamp(*createTimestamp);
    return WriteInternal(std::move(token), std::move(message));
}

void TFederatedWriteSessionImpl::Write(NTopic::TContinuationToken&& token, NTopic::TWriteMessage&& message) {
    return WriteInternal(std::move(token), TWrappedWriteMessage(std::move(message)));
}

void TFederatedWriteSessionImpl::WriteEncoded(NTopic::TContinuationToken&& token, TStringBuf data, NTopic::ECodec codec,
                                          ui32 originalSize, TMaybe<ui64> seqNo, TMaybe<TInstant> createTimestamp) {
    auto message = NTopic::TWriteMessage::CompressedMessage(std::move(data), codec, originalSize);
    if (seqNo.Defined())
        message.SeqNo(*seqNo);
    if (createTimestamp.Defined())
        message.CreateTimestamp(*createTimestamp);
    return WriteInternal(std::move(token), TWrappedWriteMessage(std::move(message)));
}

void TFederatedWriteSessionImpl::WriteEncoded(NTopic::TContinuationToken&& token, NTopic::TWriteMessage&& message) {
    return WriteInternal(std::move(token), TWrappedWriteMessage(std::move(message)));
}

void TFederatedWriteSessionImpl::WriteInternal(NTopic::TContinuationToken&&, TWrappedWriteMessage&& wrapped) {
    ClientHasToken = false;
    if (!wrapped.Message.CreateTimestamp_.Defined()) {
        wrapped.Message.CreateTimestamp_ = TInstant::Now();
    }

    {
        TDeferredWrite deferred(Subsession);
        with_lock(Lock) {
            BufferFreeSpace -= wrapped.Message.Data.size();
            OriginalMessagesToPassDown.emplace_back(std::move(wrapped));

            PrepareDeferredWrite(deferred);
        }
        deferred.DoWrite();
    }
    if (BufferFreeSpace > 0) {
        ClientEventsQueue->PushEvent(NTopic::TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()});
        ClientHasToken = true;
    }
}

bool TFederatedWriteSessionImpl::PrepareDeferredWrite(TDeferredWrite& deferred) {
    if (PendingToken.Empty()) {
        return false;
    }
    if (OriginalMessagesToPassDown.empty()) {
        return false;
    }
    OriginalMessagesToGetAck.push_back(std::move(OriginalMessagesToPassDown.front()));
    OriginalMessagesToPassDown.pop_front();
    deferred.Token.ConstructInPlace(std::move(*PendingToken));
    deferred.Message.ConstructInPlace(std::move(OriginalMessagesToGetAck.back().Message));
    PendingToken.Clear();
    return true;
}

void TFederatedWriteSessionImpl::CloseImpl(EStatus statusCode, NYql::TIssues&& issues, TDuration timeout) {
    CloseImpl(TPlainStatus(statusCode, std::move(issues)), timeout);
}

void TFederatedWriteSessionImpl::CloseImpl(NTopic::TSessionClosedEvent const& ev, TDuration timeout) {
    if (Closing) {
        return;
    }
    Closing = true;
    if (Subsession) {
        Subsession->Close(timeout);
    }
    ClientEventsQueue->Close(ev);
    NTopic::Cancel(UpdateStateDelayContext);
}

bool TFederatedWriteSessionImpl::Close(TDuration timeout) {
    with_lock (Lock) {
        CloseImpl(EStatus::SUCCESS, {}, timeout);
    }
    return true;
}

}  // namespace NYdb::NFederatedTopic
