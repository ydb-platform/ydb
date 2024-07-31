#include "federated_write_session.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/common/log_lazy.h>
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
    return lhs->name() == rhs->name() && lhs->path() == rhs->path() && lhs->endpoint() == rhs->endpoint();
}

NTopic::TTopicClientSettings FromFederated(const TFederatedTopicClientSettings& settings);

TFederatedWriteSessionImpl::TFederatedWriteSessionImpl(
    const TFederatedWriteSessionSettings& settings,
    std::shared_ptr<TGRpcConnectionsImpl> connections,
    const TFederatedTopicClientSettings& clientSettings,
    std::shared_ptr<TFederatedDbObserver> observer,
    std::shared_ptr<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>> codecs,
    NTopic::IExecutor::TPtr subsessionHandlersExecutor
)
    : Settings(settings)
    , Connections(std::move(connections))
    , SubclientSettings(FromFederated(clientSettings))
    , ProvidedCodecs(std::move(codecs))
    , SubsessionHandlersExecutor(subsessionHandlersExecutor)
    , Observer(std::move(observer))
    , AsyncInit(Observer->WaitForFirstState())
    , FederationState(nullptr)
    , Log(Connections->GetLog())
    , ClientEventsQueue(std::make_shared<NTopic::TWriteSessionEventsQueue>(Settings))
    , BufferFreeSpace(Settings.MaxMemoryUsage_)
    , HasBeenClosed(NThreading::NewPromise())
{
}

TStringBuilder TFederatedWriteSessionImpl::GetLogPrefixImpl() const {
     return TStringBuilder() << GetDatabaseLogPrefix(SubclientSettings.Database_.GetOrElse("")) << "[" << SessionId << "] ";
}


bool TFederatedWriteSessionImpl::MessageQueuesAreEmptyImpl() const {
    Y_ABORT_UNLESS(Lock.IsLocked());
    return OriginalMessagesToGetAck.empty() && OriginalMessagesToPassDown.empty();
}

void TFederatedWriteSessionImpl::IssueTokenIfAllowed() {
    // The session should not issue tokens after it has transitioned to CLOSING or CLOSE state.
    // A user may have one spare token, so at most one additional message
    // could be written to the internal queue after the transition.
    bool issue = false;
    with_lock(Lock) {
        if (BufferFreeSpace > 0 && !ClientHasToken && SessionState < State::CLOSING) {
            ClientHasToken = true;
            issue = true;
        }
    }
    if (issue) {
        ClientEventsQueue->PushEvent(NTopic::TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()});
    }
}

std::shared_ptr<NTopic::IWriteSession> TFederatedWriteSessionImpl::UpdateFederationStateImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());
    // Even after the user has called the Close method, transitioning the session to the CLOSING state,
    // we keep updating the federation state, as the session may still have some messages to send in its queues,
    // and for that we need to know the current state of the federation.
    if (SessionState < State::CLOSED) {
        FederationState = Observer->GetState();
        return OnFederationStateUpdateImpl();
    }
    return {};
}

void TFederatedWriteSessionImpl::Start() {
    // TODO validate settings?
    with_lock(Lock) {
        if (SessionState != State::CREATED) {
            return;
        }
        SessionState = State::WORKING;
        Settings.EventHandlers_.HandlersExecutor_->Start();
    }

    IssueTokenIfAllowed();

    AsyncInit.Subscribe([selfCtx = SelfContext](const auto& f) {
        Y_UNUSED(f);
        if (auto self = selfCtx->LockShared()) {
            with_lock(self->Lock) {
                self->UpdateFederationStateImpl();
            }
        }
    });
}

std::shared_ptr<NTopic::IWriteSession> TFederatedWriteSessionImpl::OpenSubsessionImpl(std::shared_ptr<TDbInfo> db) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    ++SubsessionGeneration;

    std::shared_ptr<NTopic::IWriteSession> oldSubsession;

    if (Subsession) {
        PendingToken.Clear();
        std::swap(oldSubsession, Subsession);
    }

    auto clientSettings = SubclientSettings;
    clientSettings
        .Database(db->path())
        .DiscoveryEndpoint(db->endpoint());
    auto subclient = make_shared<NTopic::TTopicClient::TImpl>(Connections, clientSettings);

    auto handlers = NTopic::TWriteSessionSettings::TEventHandlers()
        .HandlersExecutor(SubsessionHandlersExecutor)
        .ReadyToAcceptHandler([selfCtx = SelfContext, generation = SubsessionGeneration](NTopic::TWriteSessionEvent::TReadyToAcceptEvent& ev) {
            if (auto self = selfCtx->LockShared()) {
                with_lock(self->Lock) {
                    if (generation != self->SubsessionGeneration) {
                        return;
                    }

                    Y_ABORT_UNLESS(self->PendingToken.Empty());
                    self->PendingToken = std::move(ev.ContinuationToken);
                    self->MaybeWriteImpl();
                }
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

                    if (self->MessageQueuesAreEmptyImpl() && self->MessageQueuesHaveBeenEmptied.Initialized() && !self->MessageQueuesHaveBeenEmptied.HasValue()) {
                        self->MessageQueuesHaveBeenEmptied.SetValue();
                    }
                }

                self->ClientEventsQueue->PushEvent(std::move(ev));
                self->IssueTokenIfAllowed();
            }
        })
        .SessionClosedHandler([selfCtx = SelfContext, generation = SubsessionGeneration](const NTopic::TSessionClosedEvent & ev) {
            if (ev.IsSuccess()) {
                // The subsession was closed by the federated write session itself while creating a new subsession.
                // In this case we get SUCCESS status and don't need to propagate it further.
                return;
            }
            if (auto self = selfCtx->LockShared()) {
                with_lock (self->Lock) {
                    if (generation != self->SubsessionGeneration) {
                        return;
                    }
                    self->CloseImpl(ev);
                }
            }
        });

    NTopic::TWriteSessionSettings wsSettings = Settings;
    wsSettings
        // .MaxMemoryUsage(Settings.MaxMemoryUsage_)  // to fix if split not by half on creation
        .EventHandlers(handlers);

    Subsession = subclient->CreateWriteSession(wsSettings);
    CurrentDatabase = db;

    return oldSubsession;
}

std::pair<std::shared_ptr<TDbInfo>, EStatus> SelectDatabaseByHashImpl(
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

    std::sort(available.begin(), available.end(), [](auto const& lhs, auto const& rhs) { return lhs->name() < rhs->name(); });

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

std::pair<std::shared_ptr<TDbInfo>, EStatus> SelectDatabaseImpl(
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
            return SelectDatabaseByHashImpl(settings, dbInfos);
        }
    }

    auto db = *it;
    if (db->status() == TDbInfo::Status::DatabaseInfo_Status_AVAILABLE) {
        return {db, EStatus::SUCCESS};
    }
    if (!settings.AllowFallback_) {
        return {nullptr, EStatus::UNAVAILABLE};
    }
    return SelectDatabaseByHashImpl(settings, dbInfos);
}

std::shared_ptr<NTopic::IWriteSession> TFederatedWriteSessionImpl::OnFederationStateUpdateImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());
    if (!FederationState->Status.IsSuccess()) {
        // The observer became stale, it won't try to get federation state anymore due to retry policy,
        // so there's no reason to keep the write session alive.
        CloseImpl(FederationState->Status.GetStatus(), NYql::TIssues(FederationState->Status.GetIssues()));
        return {};
    }

    Y_ABORT_UNLESS(!FederationState->DbInfos.empty());

    auto [preferrableDb, status] = SelectDatabaseImpl(Settings, FederationState->DbInfos, FederationState->SelfLocation);

    if (!preferrableDb) {
        if (!RetryState) {
            RetryState = Settings.RetryPolicy_->CreateRetryState();
        }
        if (auto delay = RetryState->GetNextRetryDelay(status)) {
            LOG_LAZY(Log, TLOG_NOTICE, GetLogPrefixImpl() << "Retry to update federation state in " << delay);
            ScheduleFederationStateUpdateImpl(*delay);
        } else {
            TString message = "Failed to select database: no available database";
            LOG_LAZY(Log, TLOG_ERR, GetLogPrefixImpl() << message << ". Status: " << status);
            CloseImpl(status, NYql::TIssues{NYql::TIssue(message)});
        }
        return {};
    }
    RetryState.reset();

    std::shared_ptr<NTopic::IWriteSession> oldSubsession;
    if (!DatabasesAreSame(preferrableDb, CurrentDatabase)) {
        LOG_LAZY(Log, TLOG_INFO, GetLogPrefixImpl()
            << "Start federated write session to database '" << preferrableDb->name()
            << "' (previous was " << (CurrentDatabase ? CurrentDatabase->name() : "<empty>") << ")"
            << " FederationState: " << *FederationState);
        oldSubsession = OpenSubsessionImpl(preferrableDb);
    }

    ScheduleFederationStateUpdateImpl(UPDATE_FEDERATION_STATE_DELAY);

    return oldSubsession;
}

void TFederatedWriteSessionImpl::ScheduleFederationStateUpdateImpl(TDuration delay) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    auto cb = [selfCtx = SelfContext](bool ok) {
        if (ok) {
            if (auto self = selfCtx->LockShared()) {
                std::shared_ptr<NTopic::IWriteSession> old;
                with_lock(self->Lock) {
                    old = self->UpdateFederationStateImpl();
                }
                if (old) {
                    old->Close(TDuration::Zero());
                }
            }
        }
    };

    UpdateStateDelayContext = Connections->CreateContext();
    if (!UpdateStateDelayContext) {
        CloseImpl(EStatus::TRANSPORT_UNAVAILABLE, NYql::TIssues{NYql::TIssue("Could not update federation state")});
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
    with_lock(Lock) {
        ClientHasToken = false;
        if (!wrapped.Message.CreateTimestamp_.Defined()) {
            wrapped.Message.CreateTimestamp_ = TInstant::Now();
        }
        BufferFreeSpace -= wrapped.Message.Data.size();
        OriginalMessagesToPassDown.emplace_back(std::move(wrapped));
        MaybeWriteImpl();
    }

    IssueTokenIfAllowed();
}

bool TFederatedWriteSessionImpl::MaybeWriteImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());
    if (PendingToken.Empty()) {
        return false;
    }
    if (OriginalMessagesToPassDown.empty()) {
        return false;
    }
    OriginalMessagesToGetAck.push_back(std::move(OriginalMessagesToPassDown.front()));
    OriginalMessagesToPassDown.pop_front();
    Subsession->Write(std::move(*PendingToken), std::move(OriginalMessagesToGetAck.back().Message));
    PendingToken.Clear();
    return true;
}

void TFederatedWriteSessionImpl::CloseImpl(EStatus statusCode, NYql::TIssues&& issues) {
    CloseImpl(TPlainStatus(statusCode, std::move(issues)));
}

void TFederatedWriteSessionImpl::CloseImpl(NTopic::TSessionClosedEvent const& ev) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    if (SessionState == State::CLOSED) {
        return;
    }
    SessionState = State::CLOSED;
    NTopic::Cancel(UpdateStateDelayContext);
    if (!HasBeenClosed.HasValue()) {
        HasBeenClosed.SetValue();
    }
    {
        auto unguard = Unguard(Lock);
        ClientEventsQueue->Close(ev);
    }
}

bool TFederatedWriteSessionImpl::Close(TDuration timeout) {
    with_lock(Lock) {
        if (SessionState == State::CLOSED) {
            return MessageQueuesAreEmptyImpl();
        }
        SessionState = State::CLOSING;
        if (!MessageQueuesHaveBeenEmptied.Initialized()) {
            MessageQueuesHaveBeenEmptied = NThreading::NewPromise();
            if (MessageQueuesAreEmptyImpl()) {
                MessageQueuesHaveBeenEmptied.SetValue();
            }
        }
    }

    TVector<NThreading::TFuture<void>> futures{MessageQueuesHaveBeenEmptied.GetFuture(), HasBeenClosed.GetFuture()};
    NThreading::WaitAny(futures).Wait(timeout);

    with_lock(Lock) {
        CloseImpl(EStatus::SUCCESS, NYql::TIssues{});
        return MessageQueuesAreEmptyImpl();
    }
}

}  // namespace NYdb::NFederatedTopic
