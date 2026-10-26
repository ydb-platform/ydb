#include "federated_read_session.h"

#include <ydb/public/sdk/cpp/src/client/topic/common/log_lazy.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/topic_impl.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <library/cpp/threading/future/future.h>
#include <util/generic/guid.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <optional>
#include <utility>
#include <variant>

namespace NYdb::inline Dev::NFederatedTopic {

NTopic::TTopicClientSettings FromFederated(const TFederatedTopicClientSettings& settings);

template <typename TEvent, typename TFederatedEvent>
typename std::function<void(TEvent&)> WrapFederatedHandler(std::function<void(TFederatedEvent&)> outerHandler,
                                                           std::shared_ptr<TDbInfo> db,
                                                           std::shared_ptr<TEventFederator> federator,
                                                           ui64 readSourceGeneration) {
    if (outerHandler) {
        return [outerHandler, db = std::move(db), federator = std::move(federator), readSourceGeneration](TEvent& ev) {
            // A callback queued by an already retired subsession keeps the old generation.
            // LocateFederate then returns empty and does not recreate the cached partition session.
            auto fev = federator->LocateFederate(ev, db, readSourceGeneration);
            if (!fev) {
                return;
            }
            outerHandler(*fev);
        };
    }
    return {};
}

NTopic::TReadSessionSettings FromFederated(const TFederatedReadSessionSettings& settings,
                                           const std::shared_ptr<TDbInfo>& db,
                                           std::shared_ptr<TEventFederator> federator,
                                           std::shared_ptr<std::atomic<bool>> retired,
                                           ui64 readSourceGeneration,
                                           std::shared_ptr<std::atomic<bool>> closed,
                                           std::shared_ptr<std::atomic<bool>> closeEventDelivered) {
    NTopic::TReadSessionSettings SubsessionSettings = settings;
    SubsessionSettings.EventHandlers_.MaxMessagesBytes(settings.EventHandlers_.MaxMessagesBytes_);
    SubsessionSettings.EventHandlers_.HandlersExecutor(settings.EventHandlers_.HandlersExecutor_);

    if (settings.FederatedEventHandlers_.SimpleDataHandlers_.DataHandler) {
        SubsessionSettings.EventHandlers_.SimpleDataHandlers(
            WrapFederatedHandler<NTopic::TReadSessionEvent::TDataReceivedEvent, TReadSessionEvent::TDataReceivedEvent>(
                settings.FederatedEventHandlers_.SimpleDataHandlers_.DataHandler, db, federator, readSourceGeneration),
            settings.FederatedEventHandlers_.SimpleDataHandlers_.CommitDataAfterProcessing,
            settings.FederatedEventHandlers_.SimpleDataHandlers_.GracefulStopAfterCommit);
    }

#define MAYBE_CONVERT_HANDLER(type, name)                                                                                          \
    if (settings.FederatedEventHandlers_.name##_) {                                                                                \
        SubsessionSettings.EventHandlers_.name(                                                                                    \
            WrapFederatedHandler<NTopic::type, type>(settings.FederatedEventHandlers_.name##_, db, federator, readSourceGeneration)); \
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

    // A closed subsession must not look like the end of the federated session.
    // Intentional leaves set `retired` before Close(); unexpected closes still
    // reach the user handler and stay in the subsession queue for GetEvents.
    auto sessionClosedHandler = settings.FederatedEventHandlers_.SessionClosedHandler_;
    auto commonHandler = settings.FederatedEventHandlers_.CommonHandler_;
    SubsessionSettings.EventHandlers_.SessionClosedHandler(
        [retired = std::move(retired), closed = std::move(closed), closeEventDelivered = std::move(closeEventDelivered),
         sessionClosedHandler, commonHandler](const NTopic::TSessionClosedEvent& ev) {
            // The subsession queue never drops this event. Remember the closure so the next refresh
            // replaces the subsession, but count the event as delivered only when a user handler got it:
            // otherwise GetEvents must return it once.
            if (closed) {
                closed->store(true);
            }
            if (retired->load()) {
                return;
            }
            if (sessionClosedHandler) {
                closeEventDelivered->store(true);
                sessionClosedHandler(ev);
                return;
            }
            if (commonHandler) {
                closeEventDelivered->store(true);
                TReadSessionEvent::TEvent event = ev;
                commonHandler(event);
            }
        });

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
    AsyncInit.Subscribe([selfCtx = SelfContext](const auto& f) {
        Y_UNUSED(f);
        if (auto self = selfCtx->LockShared()) {
            self->UpdateFederationState();
        }
    });
}

void TFederatedReadSessionImpl::SignalWakeups(std::vector<TWakeup> wakeups) {
    for (auto& wakeup : wakeups) {
        if (wakeup.Promise.Initialized() && !wakeup.Promise.HasValue()) {
            wakeup.Promise.SetValue();
        }
    }
}

std::vector<TFederatedReadSessionImpl::TSubSession> TFederatedReadSessionImpl::OpenSubSessions(
    const std::vector<std::shared_ptr<TDbInfo>>& dbInfos)
{
    {
        TStringBuilder log(GetLogPrefix());
        log << "Open read subsessions to databases: ";
        bool first = true;
        for (const auto& db : dbInfos) {
            if (first) {
                first = false;
            } else {
                log << ", ";
            }
            log << "{ name: " << db->name()
                << ", endpoint: " << db->endpoint()
                << ", path: " << db->path() << " }";
        }
        LOG_LAZY(Log, TLOG_INFO, log);
    }
    std::vector<TSubSession> opened;
    opened.reserve(dbInfos.size());
    for (const auto& db : dbInfos) {
        NTopic::TTopicClientSettings settings = SubClientSettings;
        settings
            .Database(db->path())
            .DiscoveryEndpoint(db->endpoint());
        auto subclient = make_shared<NTopic::TTopicClient::TImpl>(Connections, settings);
        auto retired = std::make_shared<std::atomic<bool>>(false);
        auto closed = std::make_shared<std::atomic<bool>>(false);
        auto closeEventDelivered = std::make_shared<std::atomic<bool>>(false);
        // Capture the generation before the read session can queue callbacks.
        const ui64 generation = EventFederator->CurrentGeneration(db);
        auto subsession = subclient->CreateReadSession(
            FromFederated(Settings, db, EventFederator, retired, generation, closed, closeEventDelivered));
        opened.emplace_back(std::move(subsession), db, std::move(retired), generation, std::move(closed), std::move(closeEventDelivered));
    }
    return opened;
}

void TFederatedReadSessionImpl::RefreshMirroredTopics() {
    if (!Settings.IsReadMirroredEnabled()) {
        return;
    }
    Y_ABORT_UNLESS(Settings.GetDatabasesToReadFrom().size() == 1);
    const auto& dbToReadFrom = *Settings.GetDatabasesToReadFrom().begin();

    // Rebuild from the original topic list. Repeating the previous append on every
    // refresh would subscribe to the same mirrored topic more than once.
    Settings.Topics_ = OriginalTopics;
    for (const auto& topic : OriginalTopics) {
        for (const auto& db : FederationState->DbInfos) {
            if (AsciiEqualsIgnoreCase(db->name(), dbToReadFrom) || !IsDatabaseReadable(*db)) {
                continue;
            }
            auto mirroredTopic = topic;
            mirroredTopic.PartitionIds_.clear();
            mirroredTopic.Path(topic.Path_ + "-mirrored-from-" + db->name());
            Settings.AppendTopics(std::move(mirroredTopic));
        }
    }
}

std::string TFederatedReadSessionImpl::CurrentTopicSetKey() const {
    std::vector<std::string> paths;
    paths.reserve(Settings.Topics_.size());
    for (const auto& topic : Settings.Topics_) {
        paths.push_back(topic.Path_);
    }
    std::sort(paths.begin(), paths.end());
    std::string key;
    for (const auto& path : paths) {
        key.append(path);
        key.push_back('\n');
    }
    return key;
}

bool TFederatedReadSessionImpl::SameReadTarget(const TDbInfo& current, const TDbInfo& desired) {
    return AsciiEqualsIgnoreCase(current.name(), desired.name()) && current.path() == desired.path() && current.endpoint() == desired.endpoint();
}

void TFederatedReadSessionImpl::DetachSubsessions(std::vector<TClosingSubsession>& toClose, bool retire) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    toClose.reserve(toClose.size() + SubSessions.size());
    for (auto& sub : SubSessions) {
        if (retire && sub.Retired) {
            sub.Retired->store(true);
        }
        toClose.push_back(TClosingSubsession{std::move(sub.Session), sub.DbInfo});
    }
    SubSessions.clear();
    SubsessionIndex = 0;
}

void TFederatedReadSessionImpl::CloseSubsessions(std::vector<TClosingSubsession>& toClose, TDuration timeout, bool& lastCloseResult, ESubsessionClose mode) {
    // Unless the close is graceful, bump the generation before Close queues callbacks. Those
    // callbacks still hold the old generation and must not recreate the cached partition sessions.
    std::vector<TEventFederator::TRetiredPartitionSession> lost;
    if (mode != ESubsessionClose::Graceful) {
        for (const auto& sub : toClose) {
            auto retired = EventFederator->RetireReadSource(sub.DbInfo);
            if (mode == ESubsessionClose::Leave) {
                std::move(retired.begin(), retired.end(), std::back_inserter(lost));
            }
        }
    }
    for (auto& sub : toClose) {
        if (sub.Session) {
            // TODO substract from user timeout
            lastCloseResult = sub.Session->Close(timeout) && lastCloseResult;
        }
    }
    toClose.clear();
    ReportLostPartitions(std::move(lost));
}

void TFederatedReadSessionImpl::ReportLostPartitions(std::vector<TEventFederator::TRetiredPartitionSession> lost) {
    if (lost.empty()) {
        return;
    }
    const auto& handlers = Settings.FederatedEventHandlers_;
    // SimpleDataHandlers manage partition sessions on their own and drop these events.
    if (handlers.SimpleDataHandlers_.DataHandler && !handlers.PartitionSessionClosedHandler_) {
        return;
    }

    std::vector<TReadSessionEvent::TPartitionSessionClosedEvent> events;
    events.reserve(lost.size());
    for (auto& partition : lost) {
        LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Partition session " << partition.FederatedPartitionSession->GetPartitionSessionId()
            << " of database '" << partition.FederatedPartitionSession->GetReadSourceDatabaseName() << "' is lost: the database left the federation");
        events.emplace_back(
            NTopic::TReadSessionEvent::TPartitionSessionClosedEvent(
                std::move(partition.PartitionSession),
                NTopic::TReadSessionEvent::TPartitionSessionClosedEvent::EReason::ConnectionLost),
            std::move(partition.FederatedPartitionSession));
    }

    if (auto handler = handlers.PartitionSessionClosedHandler_) {
        Post([handler = std::move(handler), events = std::move(events)]() mutable {
            for (auto& event : events) {
                handler(event);
            }
        });
        return;
    }
    if (auto handler = handlers.CommonHandler_) {
        Post([handler = std::move(handler), events = std::move(events)]() mutable {
            for (auto& event : events) {
                TReadSessionEvent::TEvent wrapped = std::move(event);
                handler(wrapped);
            }
        });
        return;
    }

    std::vector<TWakeup> wakeups;
    with_lock (Lock) {
        if (Closing) {
            return;
        }
        for (auto& event : events) {
            PendingEvents.emplace_back(std::move(event));
        }
        wakeups = TakeWakeups();
    }
    SignalWakeups(std::move(wakeups));
}

void TFederatedReadSessionImpl::Post(std::function<void()> callback) const {
    HandlersExecutor->Start();
    HandlersExecutor->Post(std::move(callback));
}

void TFederatedReadSessionImpl::NotifySessionClosed() const {
    const auto event = MakeSessionClosedEvent();
    auto sessionHandler = Settings.FederatedEventHandlers_.SessionClosedHandler_;
    auto commonHandler = Settings.FederatedEventHandlers_.CommonHandler_;
    if (!sessionHandler && !commonHandler) {
        return;
    }

    // Discovery runs this method. User callbacks belong on the handlers executor.
    Post([sessionHandler = std::move(sessionHandler), commonHandler = std::move(commonHandler), event]() mutable {
        if (sessionHandler) {
            sessionHandler(event);
            return;
        }
        if (commonHandler) {
            TReadSessionEvent::TEvent wrapped = event;
            commonHandler(wrapped);
        }
    });
}

std::vector<TFederatedReadSessionImpl::TWakeup> TFederatedReadSessionImpl::TakeWakeups() {
    Y_ABORT_UNLESS(Lock.IsLocked());
    return std::move(Wakeups);
}

void TFederatedReadSessionImpl::ForgetWakeup(ui64 id) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    for (auto it = Wakeups.begin(); it != Wakeups.end(); ++it) {
        if (it->Id == id) {
            Wakeups.erase(it);
            return;
        }
    }
}

bool TFederatedReadSessionImpl::ScheduleFederationStateUpdateImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());
    if (Closing) {
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

    UpdateStateDelayContext = Connections->CreateContext();
    if (!UpdateStateDelayContext) {
        Closing = true;
        return false;
    }
    Connections->ScheduleCallback(FederationDiscoveryInterval, std::move(cb), UpdateStateDelayContext);
    return true;
}

void TFederatedReadSessionImpl::UpdateFederationState() {
    std::vector<TClosingSubsession> toClose;
    std::vector<std::shared_ptr<TDbInfo>> toOpen;
    bool terminalFailure = false;
    {
        std::lock_guard guard(Lock);
        if (Closing) {
            return;
        }
        FederationState = Observer->GetState();
        // The observer snapshot is shared by every session of this client. Keep a private copy
        // before this reader stores BAD_REQUEST on it, otherwise another healthy reader can observe
        // that status and shut down.
        if (FederationState) {
            FederationState = std::make_shared<TFederatedDbState>(*FederationState);
        }
        if (!FederationState || !FederationState->Status.IsSuccess()) {
            if (FederationState) {
                LOG_LAZY(Log, TLOG_ERR, GetLogPrefix() << "Federated state update failed. FederationState: " << *FederationState);
            } else {
                LOG_LAZY(Log, TLOG_ERR, GetLogPrefix() << "Federated state update failed. FederationState: <empty>");
            }
            CloseImpl();
            terminalFailure = true;
            DetachSubsessions(toClose, true);
        } else {
            EventFederator->SetFederationState(FederationState);
            RefreshMirroredTopics();
            const std::string topicKey = CurrentTopicSetKey();
            const bool topicsChanged = topicKey != AppliedTopicSetKey;
            AppliedTopicSetKey = topicKey;

            // A database stays subscribed while it is listed, whatever its status: the subsession
            // retries on its own, and dropping it on a short UNAVAILABLE would reread uncommitted data.
            // New subsessions are opened only for readable databases.
            std::vector<std::shared_ptr<TDbInfo>> selected;
            bool anyReadable = false;
            for (const auto& db : FederationState->DbInfos) {
                if (IsDatabaseSelected(db)) {
                    selected.push_back(db);
                    anyReadable = anyReadable || IsDatabaseReadable(*db);
                }
            }

            if (!anyReadable && !SawEligibleDatabase) {
                LOG_LAZY(Log, TLOG_ERR, GetLogPrefix() << "No available databases to read.");
                auto issues = FederationState->Status.GetIssues();
                TStringBuilder issue;
                issue << "Requested databases {";
                bool first = true;
                for (auto const& dbFromSettings : Settings.GetDatabasesToReadFrom()) {
                    if (first) {
                        first = false;
                    } else {
                        issue << ",";
                    }
                    issue << " " << dbFromSettings;
                }
                issue << " } not found. Available databases {";
                first = true;
                for (auto const& db : FederationState->DbInfos) {
                    if (db->status() == Ydb::FederationDiscovery::DatabaseInfo_Status_AVAILABLE) {
                        if (first) {
                            first = false;
                        } else {
                            issue << ",";
                        }
                        issue << " { name: " << db->name()
                              << ", endpoint: " << db->endpoint()
                              << ", path: " << db->path() << " }";
                    }
                }
                issue << " }";
                issues.AddIssue(issue);
                FederationState->Status = TStatus(EStatus::BAD_REQUEST, std::move(issues));
                CloseImpl();
                terminalFailure = true;
                DetachSubsessions(toClose, true);
            } else {
                if (anyReadable) {
                    SawEligibleDatabase = true;
                }

                // A discovery response may repeat a database, and nothing validates that.
                std::vector<std::shared_ptr<TDbInfo>> unique;
                unique.reserve(selected.size());
                for (const auto& db : selected) {
                    bool duplicate = false;
                    for (const auto& earlier : unique) {
                        if (SameReadTarget(*earlier, *db)) {
                            duplicate = true;
                            break;
                        }
                    }
                    if (!duplicate) {
                        unique.push_back(db);
                    }
                }
                selected = std::move(unique);

                std::vector<bool> used(SubSessions.size(), false);
                for (const auto& db : selected) {
                    size_t found = SubSessions.size();
                    if (!topicsChanged) {
                        for (size_t i = 0; i < SubSessions.size(); ++i) {
                            // A subsession that already closed can never deliver again. Leave it
                            // unmatched, so it is dropped below and this database is opened again.
                            if (!used[i] && SubSessions[i].DbInfo && !SubSessions[i].IsClosed()
                                && SameReadTarget(*SubSessions[i].DbInfo, *db)) {
                                found = i;
                                break;
                            }
                        }
                    }
                    if (found < SubSessions.size()) {
                        used[found] = true;
                        SubSessions[found].DbInfo = db;
                    } else if (IsDatabaseReadable(*db)) {
                        toOpen.push_back(db);
                    }
                }

                std::vector<TSubSession> kept;
                kept.reserve(SubSessions.size());
                for (size_t i = 0; i < SubSessions.size(); ++i) {
                    if (used[i]) {
                        kept.push_back(std::move(SubSessions[i]));
                    } else {
                        if (SubSessions[i].IsClosed() && SubSessions[i].DbInfo) {
                            LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Reopen closed subsession of database '" << SubSessions[i].DbInfo->name() << "'");
                        } else if (SubSessions[i].DbInfo) {
                            LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Stop reading database '" << SubSessions[i].DbInfo->name() << "'");
                        } else {
                            LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Stop reading database '<unknown>'");
                        }
                        if (SubSessions[i].Retired) {
                            SubSessions[i].Retired->store(true);
                        }
                        toClose.push_back(TClosingSubsession{std::move(SubSessions[i].Session), SubSessions[i].DbInfo});
                    }
                }
                SubSessions = std::move(kept);
                if (SubSessions.empty()) {
                    SubsessionIndex = 0;
                } else if (SubsessionIndex >= SubSessions.size()) {
                    SubsessionIndex %= SubSessions.size();
                }
            }
        }
    }

    const bool membershipChanged = !toClose.empty() || !toOpen.empty();

    // Drop databases that left before opening new ones, so two subsessions never
    // read the same database. Close outside Lock: it can run callbacks.
    bool ignoredCloseResult = true;
    CloseSubsessions(toClose, TDuration::Zero(), ignoredCloseResult,
                     terminalFailure ? ESubsessionClose::Discard : ESubsessionClose::Leave);

    // CreateReadSession can run user callbacks. Do it without Lock so a callback
    // that re-enters the federated session cannot deadlock on Lock.
    std::vector<TSubSession> opened;
    if (!toOpen.empty()) {
        opened = OpenSubSessions(toOpen);
    }

    std::vector<TWakeup> wakeups;
    {
        std::lock_guard guard(Lock);
        if (Closing) {
            for (auto& sub : opened) {
                if (sub.Retired) {
                    sub.Retired->store(true);
                }
                toClose.push_back(TClosingSubsession{std::move(sub.Session), sub.DbInfo});
            }
        } else {
            for (auto& sub : opened) {
                SubSessions.push_back(std::move(sub));
            }
            if (SubSessions.empty()) {
                SubsessionIndex = 0;
            } else if (SubsessionIndex >= SubSessions.size()) {
                SubsessionIndex %= SubSessions.size();
            }
        }
        if (Closing || membershipChanged) {
            wakeups = TakeWakeups();
        }
    }

    SignalWakeups(std::move(wakeups));
    CloseSubsessions(toClose, TDuration::Zero(), ignoredCloseResult, ESubsessionClose::Discard);

    // Schedule only after this update finishes. The callback is armed with a
    // delay, so the next update cannot overlap this one.
    {
        std::lock_guard guard(Lock);
        if (!Closing && !ScheduleFederationStateUpdateImpl()) {
            terminalFailure = true;
        }
        if (!Closing) {
            return;
        }
        // Membership removals already retired their subsessions. A terminal shutdown
        // keeps close callbacks enabled unless this failure path owns the close.
        DetachSubsessions(toClose, terminalFailure);
        wakeups = TakeWakeups();
    }
    SignalWakeups(std::move(wakeups));
    CloseSubsessions(toClose, TDuration::Zero(), ignoredCloseResult,
                     terminalFailure ? ESubsessionClose::Discard : ESubsessionClose::Graceful);
    if (terminalFailure) {
        NotifySessionClosed();
    }
}

bool TFederatedReadSessionImpl::IsDatabaseReadable(const TDbInfo& db) {
    return db.status() == TDbInfo::Status::DatabaseInfo_Status_AVAILABLE ||
        db.status() == TDbInfo::Status::DatabaseInfo_Status_READ_ONLY;
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

NThreading::TFuture<void> TFederatedReadSessionImpl::WaitEvent() {
    // TODO override with read session settings timeout
    return AsyncInit.Apply([selfCtx = SelfContext](const NThreading::TFuture<void>&) {
        std::optional<ui64> wakeupId;
        NThreading::TFuture<void> ready = NThreading::MakeFuture();
        if (auto self = selfCtx->LockShared()) {
            std::vector<NThreading::TFuture<void>> waiters;
            std::vector<TWakeup> evicted;
            {
                std::lock_guard guard(self->Lock);
                if (self->Closing || !self->PendingEvents.empty()) {
                    return ready;
                }
                wakeupId = self->NextWakeupId++;
                auto promise = NThreading::NewPromise<void>();
                waiters.push_back(promise.GetFuture());
                self->Wakeups.push_back(TWakeup{*wakeupId, std::move(promise)});
                // A caller that waits with a timeout and drops the future never completes it, so its
                // promise would stay here until the next membership change. Past the cap, complete the
                // oldest: abandoned waits are released and a genuinely blocked reader just waits again.
                while (self->Wakeups.size() > MaxPendingWakeups) {
                    evicted.push_back(std::move(self->Wakeups.front()));
                    self->Wakeups.erase(self->Wakeups.begin());
                }
                for (const auto& sub : self->SubSessions) {
                    // A closed subsession keeps its close event queued forever, so its WaitEvent is
                    // always ready and would turn a blocking read into a busy loop.
                    if (sub.Session && !sub.IsDrained()) {
                        waiters.emplace_back(sub.Session->WaitEvent());
                    }
                }
            }
            SignalWakeups(std::move(evicted));
            ready = NThreading::WaitAny(std::move(waiters));
        } else {
            return ready;
        }

        // Drop this wait's promise when the wait completes for any other reason.
        // Leaving it subscribed to a promise that membership changes never set
        // retains the WaitAny group for the rest of the session.
        const ui64 id = *wakeupId;
        ready.Subscribe([selfCtx, id](const NThreading::TFuture<void>&) {
            if (auto self = selfCtx->LockShared()) {
                std::lock_guard guard(self->Lock);
                self->ForgetWakeup(id);
            }
        });
        return ready;
    });
}

NTopic::TSessionClosedEvent TFederatedReadSessionImpl::MakeSessionClosedEvent() const {
    if (!FederationState) {
        return NTopic::TSessionClosedEvent(EStatus::SUCCESS, {});
    }
    return NTopic::TSessionClosedEvent(
        FederationState->Status.GetStatus(),
        NYdb::NIssue::TIssues(FederationState->Status.GetIssues()));
}

std::vector<TReadSessionEvent::TEvent> TFederatedReadSessionImpl::GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) {
    std::vector<TReadSessionEvent::TEvent> result;
    while (true) {
        if (block && result.empty()) {
            WaitEvent().Wait();
        }
        with_lock (Lock) {
            if (Closing) {
                if (result.empty()) {
                    result.push_back(MakeSessionClosedEvent());
                }
                return result;
            }
            if (!PendingEvents.empty()) {
                const size_t limit = maxEventsCount.value_or(std::numeric_limits<size_t>::max());
                while (!PendingEvents.empty() && result.size() < limit) {
                    result.push_back(std::move(PendingEvents.front()));
                    PendingEvents.pop_front();
                }
                return result;
            }
            if (!SubSessions.empty()) {
                if (SubsessionIndex >= SubSessions.size()) {
                    SubsessionIndex = 0;
                }
                // Non-blocking reads still visit a single subsession, preserving round-robin.
                // Blocking reads walk the current set once and, if it has no events, wait again
                // instead of spinning: a membership change wakes WaitEvent without a data event.
                const size_t steps = block ? SubSessions.size() : 1;
                for (size_t step = 0; step < steps && result.empty(); ++step) {
                    auto sub = SubSessions[SubsessionIndex];
                    if (sub.Session && !sub.IsDrained()) {
                        for (auto&& ev : sub.Session->GetEvents(false, maxEventsCount, maxByteSize)) {
                            if (std::holds_alternative<NTopic::TSessionClosedEvent>(ev)) {
                                // The queue returns this event on every call. Hand it over once and then
                                // leave the subsession out of the waits, so a blocking read blocks again
                                // instead of spinning on it. The next refresh opens a replacement.
                                if (sub.Closed) {
                                    sub.Closed->store(true);
                                }
                                if (sub.CloseEventDelivered) {
                                    sub.CloseEventDelivered->store(true);
                                }
                            }
                            if (auto federated = EventFederator->LocateFederate(std::move(ev), sub.DbInfo, sub.Generation)) {
                                result.push_back(std::move(*federated));
                            }
                        }
                    }
                    SubsessionIndex = (SubsessionIndex + 1) % SubSessions.size();
                }
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
    std::vector<TClosingSubsession> sessions;
    std::vector<TWakeup> wakeups;
    NYdbGrpc::IQueueClientContextPtr delayCtx;
    {
        std::lock_guard guard(Lock);
        Closing = true;
        delayCtx = std::exchange(UpdateStateDelayContext, nullptr);
        // Do not retire: an explicit close must still deliver SessionClosedHandler.
        DetachSubsessions(sessions, false);
        wakeups = TakeWakeups();
    }
    // Wake blocked readers before closing subsessions. SetValue is outside Lock:
    // its subscriber may call back into the session.
    SignalWakeups(std::move(wakeups));
    NTopic::Cancel(delayCtx);

    bool result = true;
    CloseSubsessions(sessions, timeout, result, ESubsessionClose::Graceful);
    return result;
}

} // namespace NYdb::inline Dev::NFederatedTopic
