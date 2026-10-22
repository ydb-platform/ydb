#include "federated_write_session.h"

#include <ydb/public/sdk/cpp/src/client/topic/common/log_lazy.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/simple_blocking_helpers.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/topic_impl.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <library/cpp/threading/future/future.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fatal_error_handlers/handlers.h>

#include <algorithm>
#include <exception>

namespace NYdb::inline Dev::NFederatedTopic {

namespace {

bool IsContractViolation(const std::exception_ptr& error) {
    if (!error) {
        return false;
    }
    try {
        std::rethrow_exception(error);
    } catch (const TContractViolation&) {
        return true;
    } catch (...) {
        return false;
    }
}

}  // namespace

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
    std::shared_ptr<std::unordered_map<NTopic::ECodec, std::unique_ptr<NTopic::ICodec>>> codecs,
    IExecutor::TPtr subsessionHandlersExecutor
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
     return TStringBuilder() << GetDatabaseLogPrefix(SubclientSettings.Database_.value_or("")) << "[" << SessionId << "] ";
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
    with_lock(Lock){
        if (SessionState != State::CREATED) {
            return;
        }
        SessionState = State::WORKING;
        Settings.EventHandlers_.HandlersExecutor_->Start();
    }

    IssueTokenIfAllowed();

    AsyncInit.Subscribe([selfCtx = SelfContext](const auto& f) {
        Y_UNUSED(f);
        std::shared_ptr<TFederatedWriteSessionImpl> pinned;
        {
            auto self = selfCtx->LockShared();
            if (!self) {
                return;
            }
            pinned = selfCtx->TryGet();
            with_lock(self->Lock) {
                self->UpdateFederationStateImpl();
            }
            self->DispatchPendingInitSeqNoArm();
        }
        if (pinned) {
            pinned->PublishPendingClose();
        }
    });
}

std::shared_ptr<NTopic::IWriteSession> TFederatedWriteSessionImpl::OpenSubsessionImpl(std::shared_ptr<TDbInfo> db) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    ++SubsessionGeneration;

    std::shared_ptr<NTopic::IWriteSession> oldSubsession;

    if (Subsession) {
        PendingToken.reset();
        std::swap(oldSubsession, Subsession);
    }

    auto clientSettings = SubclientSettings;
    clientSettings
        .Database(db->path())
        .DiscoveryEndpoint(db->endpoint());
    auto subclient = std::make_shared<NTopic::TTopicClient::TImpl>(Connections, clientSettings);

    auto handlers = NTopic::TWriteSessionSettings::TEventHandlers()
        .HandlersExecutor(SubsessionHandlersExecutor)
        .ReadyToAcceptHandler([selfCtx = SelfContext, generation = SubsessionGeneration](NTopic::TWriteSessionEvent::TReadyToAcceptEvent& ev) {
            if (auto self = selfCtx->LockShared()) {
                with_lock(self->Lock) {
                    if (generation != self->SubsessionGeneration) {
                        return;
                    }

                    Y_ABORT_UNLESS(!self->PendingToken.has_value(), "Continuation token is required");
                    self->PendingToken = std::move(ev.ContinuationToken);
                    self->MaybeWriteImpl();
                }
            }
        })
        .AcksHandler([selfCtx = SelfContext, generation = SubsessionGeneration](NTopic::TWriteSessionEvent::TAcksEvent& ev) {
            if (auto self = selfCtx->LockShared()) {
                with_lock(self->Lock) {
                    if (generation != self->SubsessionGeneration) {
                        return;
                    }

                    Y_ABORT_UNLESS(ev.Acks.size() <= self->OriginalMessagesToGetAck.size());

                    for (size_t i = 0; i < ev.Acks.size(); ++i) {
                        self->BufferFreeSpace += self->OriginalMessagesToGetAck.front().Data.size();
                        self->OriginalMessagesToGetAck.pop_front();
                    }

                    if (self->MessageQueuesAreEmptyImpl() && self->MessageQueuesHaveBeenEmptied.Initialized() && !self->MessageQueuesHaveBeenEmptied.HasValue()) {
                        self->MessageQueuesHaveBeenEmptied.SetValue();
                    }
                }

                self->ClientEventsQueue->PushEvent(ev);
                self->IssueTokenIfAllowed();
            }
        })
        .SessionClosedHandler([selfCtx = SelfContext, generation = SubsessionGeneration](const NTopic::TSessionClosedEvent & ev) {
            if (ev.IsSuccess()) {
                // The subsession was closed by the federated write session itself while creating a new subsession.
                // In this case we get SUCCESS status and don't need to propagate it further.
                return;
            }
            std::shared_ptr<TFederatedWriteSessionImpl> pinned;
            {
                auto self = selfCtx->LockShared();
                if (!self) {
                    return;
                }
                pinned = selfCtx->TryGet();
                with_lock(self->Lock) {
                    if (generation != self->SubsessionGeneration) {
                        pinned.reset();
                        return;
                    }
                    self->BeginClose(ev);
                }
            }
            if (pinned) {
                pinned->PublishPendingClose();
            }
        });

    {
        // Unacknowledged messages should be resent.
        for (auto& msg : OriginalMessagesToPassDown) {
            OriginalMessagesToGetAck.emplace_back(std::move(msg));
        }
        OriginalMessagesToPassDown = std::move(OriginalMessagesToGetAck);
    }

    NTopic::TWriteSessionSettings wsSettings = Settings;
    wsSettings
        // .MaxMemoryUsage(Settings.MaxMemoryUsage_)  // to fix if split not by half on creation
        .EventHandlers(handlers);

    Subsession = subclient->CreateWriteSession(wsSettings);
    CurrentDatabase = db;
    MaybePrepareInitSeqNoArm();

    return oldSubsession;
}

std::pair<std::shared_ptr<TDbInfo>, EStatus> SelectDatabaseByHashImpl(
    NTopic::TFederatedWriteSessionSettings const& settings,
    std::vector<std::shared_ptr<TDbInfo>> const& dbInfos
) {
    uint64_t totalWeight = 0;
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

    size_t hashValue = THash<std::string>()(settings.Path_);
    hashValue = CombineHashes(hashValue, THash<std::string>()(settings.ProducerId_));
    hashValue %= totalWeight;

    uint64_t borderWeight = 0;
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
    std::vector<std::shared_ptr<TDbInfo>> const& dbInfos, std::string const& selfLocation
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
        BeginClose(NTopic::TSessionClosedEvent(FederationState->Status.GetStatus(), NYdb::NIssue::TIssues(FederationState->Status.GetIssues())));
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
            std::string message = "Failed to select database: no available database";
            LOG_LAZY(Log, TLOG_ERR, GetLogPrefixImpl() << message << ". Status: " << status);
            BeginClose(NTopic::TSessionClosedEvent(status, NYdb::NIssue::TIssues{NYdb::NIssue::TIssue(message)}));
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
            std::shared_ptr<TFederatedWriteSessionImpl> pinned;
            {
                auto self = selfCtx->LockShared();
                if (!self) {
                    return;
                }
                pinned = selfCtx->TryGet();
                std::shared_ptr<NTopic::IWriteSession> old;
                with_lock(self->Lock) {
                    old = self->UpdateFederationStateImpl();
                }
                self->DispatchPendingInitSeqNoArm();
                if (old) {
                    old->Close(TDuration::Zero());
                }
            }
            if (pinned) {
                pinned->PublishPendingClose();
            }
        }
    };

    UpdateStateDelayContext = Connections->CreateContext();
    if (!UpdateStateDelayContext) {
        BeginClose(NTopic::TSessionClosedEvent(EStatus::TRANSPORT_UNAVAILABLE, NYdb::NIssue::TIssues{NYdb::NIssue::TIssue("Could not update federation state")}));
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

std::vector<NTopic::TWriteSessionEvent::TEvent> TFederatedWriteSessionImpl::GetEvents(bool block, std::optional<size_t> maxEventsCount) {
    return ClientEventsQueue->GetEvents(block, maxEventsCount);
}

std::optional<NTopic::TWriteSessionEvent::TEvent> TFederatedWriteSessionImpl::GetEvent(bool block) {
    auto events = GetEvents(block, 1);
    return events.empty() ? std::nullopt : std::optional<NTopic::TWriteSessionEvent::TEvent>{std::move(events.front())};
}

void TFederatedWriteSessionImpl::MaybePrepareInitSeqNoArm() {
    Y_ABORT_UNLESS(Lock.IsLocked());
    if (!InitSeqNoPromise.Initialized() || !Subsession) {
        return;
    }
    if (InitSeqNoArmedGeneration == SubsessionGeneration) {
        return;
    }

    const auto current = InitSeqNoPromise.GetFuture();
    // The public promise is one-shot. A replacement subsession still needs
    // GetInitSeqNo() so ValidateSeqNo locks manual mode, as on a direct session.
    const bool publish = !InitSeqNoSettled && !current.HasValue() && !current.HasException();

    InitSeqNoArmedGeneration = SubsessionGeneration;
    if (!publish) {
        try {
            Subsession->GetInitSeqNo();
        } catch (...) {
            // Already settled. A contract error means this subsession is in auto
            // mode; a later manual write fails there. Do not reopen the promise.
        }
        return;
    }

    PendingInitSeqNoGeneration = SubsessionGeneration;
    PendingInitSeqNoError = nullptr;
    try {
        // Called before this subsession accepts writes, so ValidateSeqNo still
        // sees an unset mode and locks manual SeqNo the same way a direct topic
        // session does. Must not subscribe here: a ready future would re-enter Lock.
        PendingInitSeqNoFuture = Subsession->GetInitSeqNo();
    } catch (...) {
        PendingInitSeqNoFuture = {};
        PendingInitSeqNoError = std::current_exception();
    }
    HasPendingInitSeqNoArm = true;
}

void TFederatedWriteSessionImpl::TInitSeqNoCompletion::Apply() {
    if (!Promise.Initialized()) {
        return;
    }
    if (Value.has_value()) {
        Promise.TrySetValue(*Value);
    } else if (Error) {
        Promise.TrySetException(Error);
    }
}

bool TFederatedWriteSessionImpl::IsDeduplicationEnabled() const {
    if (Settings.DeduplicationEnabled_.has_value()) {
        return *Settings.DeduplicationEnabled_;
    }
    // TWriteSessionImpl::InitWriter enables deduplication only when an id is set.
    return !Settings.ProducerId_.empty() || !Settings.MessageGroupId_.empty();
}

void TFederatedWriteSessionImpl::DispatchPendingInitSeqNoArm() {
    bool hasArm = false;
    size_t generation = 0;
    NThreading::TFuture<uint64_t> future;
    std::exception_ptr error;
    with_lock(Lock) {
        if (!HasPendingInitSeqNoArm) {
            return;
        }
        hasArm = true;
        generation = PendingInitSeqNoGeneration;
        future = std::move(PendingInitSeqNoFuture);
        error = std::move(PendingInitSeqNoError);
        HasPendingInitSeqNoArm = false;
        PendingInitSeqNoError = nullptr;
    }
    if (!hasArm) {
        return;
    }

    // Subscribe can run inline when the subsession future is already ready, and this
    // method is reachable from a discovery callback that still holds LockShared.
    // The subsession promise itself is completed on the read callback stack
    // (TWriteSessionImpl::OnReadDone). Public subscribers must run on neither:
    // a subscriber that drops the session would destroy that subsession reentrantly,
    // and LockShared would deadlock the context destructor.
    auto connections = Connections;
    auto selfCtx = SelfContext;
    connections->PostToResponseQueue([selfCtx, generation, future = std::move(future), error = std::move(error), connections]() mutable {
        auto apply = [selfCtx, generation, error = std::move(error), connections](const NThreading::TFuture<uint64_t>& ready) mutable {
            TInitSeqNoCompletion completion;
            {
                auto self = selfCtx->LockShared();
                if (self) {
                    with_lock(self->Lock) {
                        completion = self->ClaimInitSeqNo(generation, ready, std::move(error));
                    }
                }
            }
            connections->PostToResponseQueue([completion = std::move(completion)]() mutable {
                completion.Apply();
            });
        };
        if (error || !future.Initialized()) {
            apply(future);
            return;
        }
        future.Subscribe(std::move(apply));
    });
}

TFederatedWriteSessionImpl::TInitSeqNoCompletion TFederatedWriteSessionImpl::ClaimInitSeqNo(
    size_t generation,
    const NThreading::TFuture<uint64_t>& future,
    std::exception_ptr error
) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    TInitSeqNoCompletion completion;
    if (SessionState == State::CLOSED || InitSeqNoSettled || generation != SubsessionGeneration || !InitSeqNoPromise.Initialized()) {
        return completion;
    }
    const auto current = InitSeqNoPromise.GetFuture();
    if (current.HasValue() || current.HasException()) {
        InitSeqNoSettled = true;
        return completion;
    }
    if (error) {
        InitSeqNoSettled = true;
        completion.Promise = InitSeqNoPromise;
        completion.Error = std::move(error);
        return completion;
    }
    if (!future.Initialized()) {
        return completion;
    }
    try {
        completion.Value = future.GetValue();
    } catch (...) {
        completion.Error = std::current_exception();
    }
    InitSeqNoSettled = true;
    completion.Promise = InitSeqNoPromise;
    return completion;
}

NThreading::TFuture<uint64_t> TFederatedWriteSessionImpl::GetInitSeqNo() {
    if (!IsDeduplicationEnabled()) {
        NYdb::ThrowFatalError("Cannot call GetInitSeqNo when deduplication is disabled");
    }

    NThreading::TFuture<uint64_t> result;
    std::exception_ptr contractError;
    with_lock(Lock) {
        if (!InitSeqNoPromise.Initialized()) {
            InitSeqNoPromise = NThreading::NewPromise<uint64_t>();
            if (SessionState == State::CLOSED) {
                InitSeqNoPromise.SetException("session closed");
            }
        }
        result = InitSeqNoPromise.GetFuture();
        if (!result.HasValue() && !result.HasException()) {
            MaybePrepareInitSeqNoArm();
            if (IsContractViolation(PendingInitSeqNoError)) {
                // TWriteSessionImpl::GetInitSeqNo throws before handing out a future.
                // Drop the arm so the next call throws again instead of hanging.
                contractError = PendingInitSeqNoError;
                HasPendingInitSeqNoArm = false;
                PendingInitSeqNoError = nullptr;
                PendingInitSeqNoFuture = {};
                InitSeqNoArmedGeneration = 0;
            }
        }
    }
    if (contractError) {
        std::rethrow_exception(contractError);
    }
    DispatchPendingInitSeqNoArm();
    return result;
}

NThreading::TFuture<bool> TFederatedWriteSessionImpl::Flush() {
    std::lock_guard guard(Lock);
    if (SessionState == State::CLOSED) {
        return NThreading::MakeFuture(false);
    }

    TWrappedWriteMessage* message = nullptr;
    if (!OriginalMessagesToPassDown.empty()) {
        message = &OriginalMessagesToPassDown.back();
    } else if (!OriginalMessagesToGetAck.empty()) {
        message = &OriginalMessagesToGetAck.back();
    } else {
        return NThreading::MakeFuture(true);
    }

    if (!message->FlushPromise.Initialized()) {
        message->InitFlushPromise(Connections);
    }

    return message->FlushPromise.GetFuture();
}

void TFederatedWriteSessionImpl::Write(NTopic::TContinuationToken&& token, std::string_view data, std::optional<uint64_t> seqNo,
                                   std::optional<TInstant> createTimestamp) {
    NTopic::TWriteMessage message{std::move(data)};
    if (seqNo.has_value())
        message.SeqNo(*seqNo);
    if (createTimestamp.has_value())
        message.CreateTimestamp(*createTimestamp);
    return WriteInternal(std::move(token), std::move(message));
}

void TFederatedWriteSessionImpl::Write(NTopic::TContinuationToken&& token, NTopic::TWriteMessage&& message) {
    return WriteInternal(std::move(token), TWrappedWriteMessage(std::move(message)));
}

void TFederatedWriteSessionImpl::WriteEncoded(NTopic::TContinuationToken&& token, std::string_view data, NTopic::ECodec codec,
                                          ui32 originalSize, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp) {
    auto message = NTopic::TWriteMessage::CompressedMessage(std::move(data), codec, originalSize);
    if (seqNo.has_value())
        message.SeqNo(*seqNo);
    if (createTimestamp.has_value())
        message.CreateTimestamp(*createTimestamp);
    return WriteInternal(std::move(token), TWrappedWriteMessage(std::move(message)));
}

void TFederatedWriteSessionImpl::WriteEncoded(NTopic::TContinuationToken&& token, NTopic::TWriteMessage&& message) {
    return WriteInternal(std::move(token), TWrappedWriteMessage(std::move(message)));
}

void TFederatedWriteSessionImpl::WriteInternal(NTopic::TContinuationToken&&, TWrappedWriteMessage&& wrapped) {
    with_lock(Lock) {
        ClientHasToken = false;
        if (!wrapped.Message.CreateTimestamp_.has_value()) {
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
    if (!PendingToken.has_value()) {
        return false;
    }
    if (OriginalMessagesToPassDown.empty()) {
        return false;
    }
    OriginalMessagesToGetAck.push_back(std::move(OriginalMessagesToPassDown.front()));
    OriginalMessagesToPassDown.pop_front();
    Subsession->Write(std::move(*PendingToken), std::move(OriginalMessagesToGetAck.back().Message));
    PendingToken.reset();
    return true;
}

void TFederatedWriteSessionImpl::BeginClose(const NTopic::TSessionClosedEvent& event) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    if (SessionState == State::CLOSED) {
        return;
    }
    SessionState = State::CLOSED;
    NTopic::Cancel(UpdateStateDelayContext);

    for (auto& message : OriginalMessagesToPassDown) {
        message.CompleteFlush(false);
    }
    for (auto& message : OriginalMessagesToGetAck) {
        message.CompleteFlush(false);
    }

    if (!HasBeenClosed.HasValue()) {
        HasBeenClosed.SetValue();
    }
    if (!InitSeqNoSettled && InitSeqNoPromise.Initialized()) {
        const auto current = InitSeqNoPromise.GetFuture();
        if (!current.HasValue() && !current.HasException()) {
            InitSeqNoSettled = true;
            PendingCloseInitSeqNo = InitSeqNoPromise;
        }
    }
    PendingCloseEvent = event;
    PendingCloseActive = true;
}

void TFederatedWriteSessionImpl::PublishPendingClose() {
    std::optional<NTopic::TSessionClosedEvent> event;
    NThreading::TPromise<uint64_t> initSeqNo;
    with_lock(Lock) {
        if (!PendingCloseActive) {
            return;
        }
        event = std::move(PendingCloseEvent);
        initSeqNo = std::move(PendingCloseInitSeqNo);
        PendingCloseEvent.reset();
        PendingCloseActive = false;
    }
    if (!event) {
        return;
    }
    // Ready before Close(): a WaitEvent subscriber invoked inline must observe this future.
    if (initSeqNo.Initialized()) {
        initSeqNo.TrySetException(std::make_exception_ptr(yexception() << "session closed"));
    }
    ClientEventsQueue->Close(*event);
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

    std::vector<NThreading::TFuture<void>> futures{MessageQueuesHaveBeenEmptied.GetFuture(), HasBeenClosed.GetFuture()};
    NThreading::WaitAny(futures).Wait(timeout);

    bool queuesEmpty = false;
    with_lock(Lock) {
        BeginClose(NTopic::TSessionClosedEvent(EStatus::SUCCESS, NYdb::NIssue::TIssues{}));
        queuesEmpty = MessageQueuesAreEmptyImpl();
    }
    PublishPendingClose();
    return queuesEmpty;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSimpleBlockingFederatedWriteSession

TSimpleBlockingFederatedWriteSession::TSimpleBlockingFederatedWriteSession(
        const TFederatedWriteSessionSettings& settings,
        std::shared_ptr<TGRpcConnectionsImpl> connections,
        const TFederatedTopicClientSettings& clientSettings,
        std::shared_ptr<TFederatedDbObserver> observer,
        std::shared_ptr<std::unordered_map<NTopic::ECodec, std::unique_ptr<NTopic::ICodec>>> codecs,
        IExecutor::TPtr subsessionHandlersExecutor
) {
    TFederatedWriteSessionSettings subSettings = settings;
    auto& log = connections->GetLog();
    if (settings.EventHandlers_.AcksHandler_) {
        LOG_LAZY(log, TLOG_WARNING, "TSimpleBlockingFederatedWriteSession: Cannot use AcksHandler, resetting.");
        subSettings.EventHandlers_.AcksHandler({});
    }
    if (settings.EventHandlers_.ReadyToAcceptHandler_) {
        LOG_LAZY(log, TLOG_WARNING, "TSimpleBlockingFederatedWriteSession: Cannot use ReadyToAcceptHandler, resetting.");
        subSettings.EventHandlers_.ReadyToAcceptHandler({});
    }
    if (settings.EventHandlers_.SessionClosedHandler_) {
        LOG_LAZY(log, TLOG_WARNING, "TSimpleBlockingFederatedWriteSession: Cannot use SessionClosedHandler, resetting.");
        subSettings.EventHandlers_.SessionClosedHandler({});
    }
    if (settings.EventHandlers_.CommonHandler_) {
        LOG_LAZY(log, TLOG_WARNING, "TSimpleBlockingFederatedWriteSession: Cannot use CommonHandler, resetting.");
        subSettings.EventHandlers_.CommonHandler({});
    }

    Writer = std::make_shared<TFederatedWriteSession>(
        subSettings, std::move(connections), clientSettings, std::move(observer), std::move(codecs), std::move(subsessionHandlersExecutor));
    Writer->Start();
}

uint64_t TSimpleBlockingFederatedWriteSession::GetInitSeqNo() {
    return Writer->GetInitSeqNo().GetValueSync();
}

bool TSimpleBlockingFederatedWriteSession::Write(
        std::string_view data, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp, const TDuration& blockTimeout
) {
    auto message = NTopic::TWriteMessage(std::move(data))
        .SeqNo(seqNo)
        .CreateTimestamp(createTimestamp);
    return Write(std::move(message), nullptr, blockTimeout);
}

bool TSimpleBlockingFederatedWriteSession::Write(
        NTopic::TWriteMessage&& message, TTransactionBase* tx, const TDuration& blockTimeout
) {
    if (tx || message.GetTxPtr()) {
        ythrow yexception() << "transactions are not supported";
    }
    auto continuationToken = WaitForToken(blockTimeout);
    if (continuationToken.has_value()) {
        Writer->Write(std::move(*continuationToken), std::move(message));
        return true;
    }
    return false;
}

std::optional<NTopic::TContinuationToken> TSimpleBlockingFederatedWriteSession::WaitForToken(const TDuration& timeout) {
    return NTopic::NDetail::WaitForToken(*Writer, Closed, timeout);
}

NTopic::TWriterCounters::TPtr TSimpleBlockingFederatedWriteSession::GetCounters() {
    ythrow yexception() << "GetCounters is not yet implemented for federated write sessions";
}

bool TSimpleBlockingFederatedWriteSession::IsAlive() const {
    return !Closed.load();
}

bool TSimpleBlockingFederatedWriteSession::Close(TDuration closeTimeout) {
    Closed.store(true);
    return Writer->Close(closeTimeout);
}

TSimpleBlockingFederatedWriteSession::~TSimpleBlockingFederatedWriteSession() {
    if (!Closed.load()) {
        Close(TDuration::Zero());
    }
}

}  // namespace NYdb::NFederatedTopic
