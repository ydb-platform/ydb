#include "federated_read_session.h"

#include <ydb/public/sdk/cpp/src/client/topic/common/log_lazy.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/topic_impl.h>


#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <library/cpp/threading/future/future.h>
#include <util/generic/guid.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <set>

namespace NYdb::inline Dev::NFederatedTopic {

NTopic::TTopicClientSettings FromFederated(const TFederatedTopicClientSettings& settings);

template <typename TEvent, typename TFederatedEvent>
typename std::function<void(TEvent&)> WrapFederatedHandler(std::function<void(TFederatedEvent&)> outerHandler, std::shared_ptr<TDbInfo> db, std::shared_ptr<TEventFederator> federator, std::shared_ptr<std::atomic<bool>> retired) {
    if (outerHandler) {
        return [outerHandler, db = std::move(db), federator = std::move(federator), retired = std::move(retired)](TEvent& ev) {
            if (auto fev = federator->LocateFederate(ev, db, *retired)) {
                outerHandler(*fev);
            }
        };
    }
    return {};
}

NTopic::TReadSessionSettings FromFederated(const TFederatedReadSessionSettings& settings, const std::shared_ptr<TDbInfo>& db, std::shared_ptr<TEventFederator> federator, std::shared_ptr<std::atomic<bool>> retired) {
    NTopic::TReadSessionSettings SubsessionSettings = settings;
    SubsessionSettings.EventHandlers_.MaxMessagesBytes(settings.EventHandlers_.MaxMessagesBytes_);
    SubsessionSettings.EventHandlers_.HandlersExecutor(settings.EventHandlers_.HandlersExecutor_);

    if (settings.FederatedEventHandlers_.SimpleDataHandlers_.DataHandler) {
        SubsessionSettings.EventHandlers_.SimpleDataHandlers(
            WrapFederatedHandler<NTopic::TReadSessionEvent::TDataReceivedEvent, TReadSessionEvent::TDataReceivedEvent>(
                settings.FederatedEventHandlers_.SimpleDataHandlers_.DataHandler, db, federator, retired),
            settings.FederatedEventHandlers_.SimpleDataHandlers_.CommitDataAfterProcessing,
            settings.FederatedEventHandlers_.SimpleDataHandlers_.GracefulStopAfterCommit);
    }

#define MAYBE_CONVERT_HANDLER(type, name)                                                                                \
    if (settings.FederatedEventHandlers_.name##_) {                                                                      \
        SubsessionSettings.EventHandlers_.name(                                                                          \
            WrapFederatedHandler<NTopic::type, type>(settings.FederatedEventHandlers_.name##_, db, federator, retired)   \
        );                                                                                                               \
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

    std::function<void(const NTopic::TSessionClosedEvent&)> sessionClosedHandler;
    if (auto handler = settings.FederatedEventHandlers_.SessionClosedHandler_) {
        sessionClosedHandler = [handler, retired](const NTopic::TSessionClosedEvent& ev) {
            if (!*retired) {
                handler(ev);
            }
        };
    }
    SubsessionSettings.EventHandlers_.SessionClosedHandler(sessionClosedHandler);

    return SubsessionSettings;
}

TFederatedReadSessionImpl::TFederatedReadSessionImpl(const TFederatedReadSessionSettings& settings,
                                                     std::shared_ptr<TGRpcConnectionsImpl> connections,
                                                     const TFederatedTopicClientSettings& clientSettings,
                                                     std::shared_ptr<TFederatedDbObserver> observer,
                                                     std::shared_ptr<std::unordered_map<NTopic::ECodec, std::unique_ptr<NTopic::ICodec>>> codecs)
    : Settings(settings)
    , Connections(std::move(connections))
    , SubClientSettings(FromFederated(clientSettings))
    , ProvidedCodecs(std::move(codecs))
    , Observer(std::move(observer))
    , AsyncInit(Observer->WaitForFirstState())
    , FederationState(nullptr)
    , EventFederator(std::make_shared<TEventFederator>())
    , Log(Connections->GetLog())
    , SessionId(CreateGuidAsString())
    , FederationDiscoveryInterval(clientSettings.FederationDiscoveryInterval_
                                      ? clientSettings.FederationDiscoveryInterval_
                                      : TDuration::Seconds(30))
    , OriginalTopics(settings.Topics_)
    , HandlersExecutor(Settings.EventHandlers_.HandlersExecutor_
                           ? Settings.EventHandlers_.HandlersExecutor_
                           : SubClientSettings.DefaultHandlersExecutor_
                                 ? SubClientSettings.DefaultHandlersExecutor_
                                 : NYdb::CreateThreadPoolExecutor(1))
{
}

TStringBuilder TFederatedReadSessionImpl::GetLogPrefix() const {
    return TStringBuilder() << GetDatabaseLogPrefix(SubClientSettings.Database_.value_or("")) << "[" << SessionId << "] ";
}

void TFederatedReadSessionImpl::Start() {
    AsyncInit.Subscribe([selfCtx = SelfContext](const auto& f){
        Y_UNUSED(f);
        if (auto self = selfCtx->LockShared()) {
            self->UpdateFederationState();
        }
    });
}

std::vector<TFederatedReadSessionImpl::TSubSession> TFederatedReadSessionImpl::OpenSubSessions(const std::vector<std::shared_ptr<TDbInfo>>& dbInfos) {
    if (dbInfos.empty()) {
        return {};
    }
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
    std::vector<TSubSession> subSessions;
    for (const auto& db : dbInfos) {
        NTopic::TTopicClientSettings settings = SubClientSettings;
        settings
            .Database(db->path())
            .DiscoveryEndpoint(db->endpoint());
        auto subclient = make_shared<NTopic::TTopicClient::TImpl>(Connections, settings);
        auto retired = std::make_shared<std::atomic<bool>>(false);
        auto subsession = subclient->CreateReadSession(FromFederated(Settings, db, EventFederator, retired));
        subSessions.emplace_back(subsession, db, retired);
    }
    return subSessions;
}

void TFederatedReadSessionImpl::UpdateFederationState() {
    std::vector<TSubSession> toClose;
    std::vector<std::shared_ptr<TDbInfo>> toOpen;
    bool failed = false;
    with_lock (Lock) {
        if (Closing) {
            return;
        }
        // The observer state is shared by all sessions of the client: copy it before
        // this session stores its own BAD_REQUEST there.
        FederationState = std::make_shared<TFederatedDbState>(*Observer->GetState());
        OnFederatedStateUpdateImpl(toClose, toOpen);
        failed = Closing;
        if (failed) {
            std::move(SubSessions.begin(), SubSessions.end(), std::back_inserter(toClose));
            SubSessions.clear();
        }
    }
    const bool changed = !toClose.empty() || !toOpen.empty();

    // Subsessions are closed and opened outside Lock: both can run user callbacks.
    // Databases that left are closed first, so two subsessions never read the same database.
    CloseSubSessions(std::move(toClose), TDuration::Zero(), failed ? ESubSessionClose::Discard : ESubSessionClose::Leave);
    auto opened = OpenSubSessions(toOpen);

    NThreading::TPromise<void> wakeup;
    with_lock (Lock) {
        if (!Closing) {
            std::move(opened.begin(), opened.end(), std::back_inserter(SubSessions));
            opened.clear();
            if (!ScheduleFederationStateUpdateImpl()) {
                CloseImpl();
                failed = true;
                std::move(SubSessions.begin(), SubSessions.end(), std::back_inserter(opened));
                SubSessions.clear();
            }
        }
        if (changed || Closing) {
            wakeup = std::exchange(Wakeup, {});
        }
    }
    if (wakeup.Initialized()) {
        wakeup.SetValue();
    }
    // What is left in `opened` belongs to a session that is closing.
    CloseSubSessions(std::move(opened), TDuration::Zero(), ESubSessionClose::Discard);
    if (failed) {
        NotifySessionClosed();
    }
}

bool TFederatedReadSessionImpl::ScheduleFederationStateUpdateImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());
    auto ctx = Connections->CreateContext();
    if (!ctx) {
        return false;
    }
    auto cb = [selfCtx = SelfContext](bool ok) {
        if (!ok) {
            return;
        }
        if (auto self = selfCtx->LockShared()) {
            self->UpdateFederationState();
        }
    };
    Connections->ScheduleCallback(FederationDiscoveryInterval, std::move(cb), std::move(ctx));
    return true;
}

void TFederatedReadSessionImpl::OnFederatedStateUpdateImpl(std::vector<TSubSession>& toClose, std::vector<std::shared_ptr<TDbInfo>>& toOpen) {
    if (!FederationState->Status.IsSuccess()) {
        LOG_LAZY(Log, TLOG_ERR, GetLogPrefix() << "Federated state update failed. FederationState: " << *FederationState);
        CloseImpl();
        return;
    }

    EventFederator->SetFederationState(FederationState);

    bool topicsChanged = false;
    if (Settings.IsReadMirroredEnabled()) {
        Y_ABORT_UNLESS(Settings.GetDatabasesToReadFrom().size() == 1);
        auto dbToReadFrom = *Settings.GetDatabasesToReadFrom().begin();

        auto topics = OriginalTopics;
        for (const auto& topic : OriginalTopics) {
            for (const auto& db : FederationState->DbInfos) {
                if (AsciiEqualsIgnoreCase(db->name(), dbToReadFrom) || !IsDatabaseReadable(db)) {
                    continue;
                }
                auto mirroredTopic = topic;
                mirroredTopic.PartitionIds_.clear();
                mirroredTopic.Path(topic.Path_ + "-mirrored-from-" + db->name());
                topics.push_back(mirroredTopic);
            }
        }
        auto paths = [](const std::vector<NTopic::TTopicReadSettings>& list) {
            std::set<std::string> result;
            for (const auto& topic : list) {
                result.insert(topic.Path_);
            }
            return result;
        };
        topicsChanged = paths(topics) != paths(Settings.Topics_);
        Settings.Topics_ = std::move(topics);
    }

    // A listed database keeps its subsession whatever its status: the subsession retries on its own,
    // and dropping it on a short UNAVAILABLE would reread uncommitted data.
    std::vector<std::shared_ptr<TDbInfo>> databases;
    bool anyReadable = false;

    for (const auto& db : FederationState->DbInfos) {
        if (IsDatabaseSelected(db)) {
            databases.push_back(db);
            anyReadable = anyReadable || IsDatabaseReadable(db);
        }
    }

    if (!anyReadable && !SawReadableDatabase) {
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
    SawReadableDatabase = true;

    auto same = [](const std::shared_ptr<TDbInfo>& lhs, const std::shared_ptr<TDbInfo>& rhs) {
        return AsciiEqualsIgnoreCase(lhs->name(), rhs->name()) && lhs->path() == rhs->path() && lhs->endpoint() == rhs->endpoint();
    };

    std::vector<TSubSession> kept;
    for (auto& sub : SubSessions) {
        if (!topicsChanged && std::any_of(databases.begin(), databases.end(), [&](const auto& db) { return same(db, sub.DbInfo); })) {
            kept.push_back(std::move(sub));
        } else {
            LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Stop reading database '" << sub.DbInfo->name() << "'");
            toClose.push_back(std::move(sub));
        }
    }
    SubSessions = std::move(kept);

    // A discovery response may repeat a database, and nothing validates that.
    for (const auto& db : databases) {
        if (IsDatabaseReadable(db) &&
            std::none_of(SubSessions.begin(), SubSessions.end(), [&](const auto& sub) { return same(db, sub.DbInfo); }) &&
            std::none_of(toOpen.begin(), toOpen.end(), [&](const auto& other) { return same(db, other); })) {
            toOpen.push_back(db);
        }
    }
}

bool TFederatedReadSessionImpl::IsDatabaseReadable(const std::shared_ptr<TDbInfo>& db) {
    return db->status() == TDbInfo::Status::DatabaseInfo_Status_AVAILABLE ||
        db->status() == TDbInfo::Status::DatabaseInfo_Status_READ_ONLY;
}

bool TFederatedReadSessionImpl::IsDatabaseSelected(const std::shared_ptr<TDbInfo>& db) {
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

bool TFederatedReadSessionImpl::CloseSubSessions(std::vector<TSubSession> subSessions, TDuration timeout, ESubSessionClose mode) {
    // Retire before Close: callbacks it queues must not recreate the cached partition sessions.
    std::vector<TEventFederator::TRetiredPartitionSession> lost;
    if (mode != ESubSessionClose::Graceful) {
        for (const auto& sub : subSessions) {
            auto retired = EventFederator->RetireReadSource(*sub.DbInfo, *sub.Retired);
            if (mode == ESubSessionClose::Leave) {
                std::move(retired.begin(), retired.end(), std::back_inserter(lost));
            }
        }
    }
    bool result = true;
    for (const auto& sub : subSessions) {
        // TODO substract from user timeout
        result = sub.Session->Close(timeout) && result;
    }
    ReportLostPartitions(std::move(lost));
    return result;
}

void TFederatedReadSessionImpl::ReportLostPartitions(std::vector<TEventFederator::TRetiredPartitionSession> lost) {
    const auto& handlers = Settings.FederatedEventHandlers_;
    // SimpleDataHandlers manage partition sessions on their own and drop these events.
    if (lost.empty() || (handlers.SimpleDataHandlers_.DataHandler && !handlers.PartitionSessionClosedHandler_)) {
        return;
    }

    std::vector<TReadSessionEvent::TPartitionSessionClosedEvent> events;
    for (auto& partition : lost) {
        LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Partition session " << partition.FederatedPartitionSession->GetPartitionSessionId()
            << " of database '" << partition.FederatedPartitionSession->GetReadSourceDatabaseName() << "' is lost: the database left the federation");
        events.emplace_back(
            NTopic::TReadSessionEvent::TPartitionSessionClosedEvent(
                std::move(partition.PartitionSession),
                NTopic::TReadSessionEvent::TPartitionSessionClosedEvent::EReason::ConnectionLost),
            std::move(partition.FederatedPartitionSession));
    }

    if (handlers.PartitionSessionClosedHandler_ || handlers.CommonHandler_) {
        Post([specific = handlers.PartitionSessionClosedHandler_, common = handlers.CommonHandler_, events = std::move(events)]() mutable {
            for (auto& event : events) {
                if (specific) {
                    specific(event);
                } else {
                    TReadSessionEvent::TEvent wrapped = std::move(event);
                    common(wrapped);
                }
            }
        });
        return;
    }

    NThreading::TPromise<void> wakeup;
    with_lock (Lock) {
        if (Closing) {
            return;
        }
        std::move(events.begin(), events.end(), std::back_inserter(PendingEvents));
        wakeup = std::exchange(Wakeup, {});
    }
    if (wakeup.Initialized()) {
        wakeup.SetValue();
    }
}

void TFederatedReadSessionImpl::Post(std::function<void()> callback) const {
    HandlersExecutor->Start();
    HandlersExecutor->Post(std::move(callback));
}

void TFederatedReadSessionImpl::NotifySessionClosed() const {
    const auto& handlers = Settings.FederatedEventHandlers_;
    if (!handlers.SessionClosedHandler_ && !handlers.CommonHandler_) {
        return;
    }
    // Runs on the discovery thread: user callbacks belong on the handlers executor.
    Post([specific = handlers.SessionClosedHandler_, common = handlers.CommonHandler_, event = MakeSessionClosedEvent()]() {
        if (specific) {
            specific(event);
        } else {
            TReadSessionEvent::TEvent wrapped = event;
            common(wrapped);
        }
    });
}

NThreading::TFuture<void> TFederatedReadSessionImpl::WaitEvent() {
    // TODO override with read session settings timeout
    return AsyncInit.Apply([selfCtx = SelfContext](const NThreading::TFuture<void>) {
        if (auto self = selfCtx->LockShared()) {
            std::lock_guard guard(self->Lock);
            if (self->Closing || !self->PendingEvents.empty()) {
                return NThreading::MakeFuture();
            }
            // Subscribing a new WaitAny to every call leaks: a caller that drops a timed-out
            // wait does not unsubscribe it, and it stays until some input completes.
            if (self->BlockedWait.Initialized() && !self->BlockedWait.IsReady()) {
                return self->BlockedWait;
            }
            std::vector<NThreading::TFuture<void>> waiters;
            self->Wakeup = NThreading::NewPromise<void>();
            waiters.push_back(self->Wakeup.GetFuture());
            for (const auto& sub : self->SubSessions) {
                waiters.emplace_back(sub.Session->WaitEvent());
            }
            self->BlockedWait = NThreading::WaitAny(std::move(waiters));
            return self->BlockedWait;
        }
        return NThreading::MakeFuture();
    });
}

NTopic::TSessionClosedEvent TFederatedReadSessionImpl::MakeSessionClosedEvent() const {
    if (!FederationState) {
        return NTopic::TSessionClosedEvent(EStatus::SUCCESS, {});
    }
    return NTopic::TSessionClosedEvent(FederationState->Status.GetStatus(), NYdb::NIssue::TIssues(FederationState->Status.GetIssues()));
}

std::vector<TReadSessionEvent::TEvent> TFederatedReadSessionImpl::GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) {
    std::vector<TReadSessionEvent::TEvent> result;
    while (true) {
        if (block) {
            WaitEvent().Wait();
        }
        with_lock (Lock) {
            if (Closing) {
                return {MakeSessionClosedEvent()};
            }
            if (!PendingEvents.empty()) {
                const size_t limit = maxEventsCount.value_or(std::numeric_limits<size_t>::max());
                while (!PendingEvents.empty() && result.size() < limit) {
                    result.push_back(std::move(PendingEvents.front()));
                    PendingEvents.pop_front();
                }
                return result;
            }
            // Blocking reads walk the subsessions once and, if none has events, wait again
            // instead of spinning: a membership change wakes WaitEvent without a data event.
            const size_t steps = block ? SubSessions.size() : std::min<size_t>(1, SubSessions.size());
            for (size_t step = 0; step < steps && result.empty(); ++step) {
                SubsessionIndex %= SubSessions.size();
                auto sub = SubSessions[SubsessionIndex];
                for (auto&& ev : sub.Session->GetEvents(false, maxEventsCount, maxByteSize)) {
                    if (auto federated = EventFederator->LocateFederate(std::move(ev), sub.DbInfo, *sub.Retired)) {
                        result.push_back(std::move(*federated));
                    }
                }
                SubsessionIndex = (SubsessionIndex + 1) % SubSessions.size();
            }
        }
        if (!result.empty() || !block) {
            return result;
        }
    }
}

void TFederatedReadSessionImpl::CloseImpl() {
    Closing = true;
}

bool TFederatedReadSessionImpl::Close(TDuration timeout) {
    std::vector<TSubSession> subSessions;
    NThreading::TPromise<void> wakeup;
    with_lock (Lock) {
        Closing = true;
        subSessions = std::move(SubSessions);
        SubSessions.clear();
        wakeup = std::exchange(Wakeup, {});
    }
    // Outside Lock: a subscriber of the wakeup may call back into the session.
    if (wakeup.Initialized()) {
        wakeup.SetValue();
    }
    // Subsessions are not retired: an explicit close still delivers their queued handlers and SessionClosedHandler.
    return CloseSubSessions(std::move(subSessions), timeout, ESubSessionClose::Graceful);
}

}  // namespace NYdb::NFederatedTopic
