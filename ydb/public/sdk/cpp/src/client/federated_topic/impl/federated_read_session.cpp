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
#include <utility>

namespace NYdb::inline Dev::NFederatedTopic {

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

NTopic::TReadSessionSettings FromFederated(const TFederatedReadSessionSettings& settings,
                                           const std::shared_ptr<TDbInfo>& db,
                                           std::shared_ptr<TEventFederator> federator,
                                           std::shared_ptr<std::atomic<bool>> retired) {
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
        WrapFederatedHandler<NTopic::type, type>(settings.FederatedEventHandlers_.name##_, db, federator)); \
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
        [retired = std::move(retired), sessionClosedHandler, commonHandler](const NTopic::TSessionClosedEvent& ev) {
            if (retired->load()) {
                return;
            }
            if (sessionClosedHandler) {
                sessionClosedHandler(ev);
                return;
            }
            if (commonHandler) {
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

void SignalWakeup(NThreading::TPromise<void> wake) {
    if (wake.Initialized() && !wake.HasValue()) {
        wake.SetValue();
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
        auto subsession = subclient->CreateReadSession(FromFederated(Settings, db, EventFederator, retired));
        opened.emplace_back(std::move(subsession), db, std::move(retired));
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
            if (AsciiEqualsIgnoreCase(db->name(), dbToReadFrom)) {
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

void TFederatedReadSessionImpl::DetachSubsessions(std::vector<std::shared_ptr<NTopic::IReadSession>>& toClose) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    toClose.reserve(toClose.size() + SubSessions.size());
    for (auto& sub : SubSessions) {
        if (sub.Retired) {
            sub.Retired->store(true);
        }
        if (sub.Session) {
            toClose.push_back(std::move(sub.Session));
        }
    }
    SubSessions.clear();
    SubsessionIndex = 0;
}

NThreading::TPromise<void> TFederatedReadSessionImpl::TakeWakeupPromise() {
    Y_ABORT_UNLESS(Lock.IsLocked());
    NThreading::TPromise<void> wake;
    if (SubsessionsChanged.Initialized() && !SubsessionsChanged.HasValue()) {
        SubsessionsChanged.Swap(wake);
    }
    SubsessionsChanged = NThreading::NewPromise<void>();
    return wake;
}

void TFederatedReadSessionImpl::ScheduleFederationStateUpdateImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());
    if (Closing) {
        return;
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
        return;
    }
    Connections->ScheduleCallback(FederationDiscoveryInterval, std::move(cb), UpdateStateDelayContext);
}

void TFederatedReadSessionImpl::UpdateFederationState() {
    std::vector<std::shared_ptr<NTopic::IReadSession>> toClose;
    std::vector<std::shared_ptr<TDbInfo>> toOpen;
    {
        std::lock_guard guard(Lock);
        if (Closing) {
            return;
        }
        FederationState = Observer->GetState();
        if (!FederationState || !FederationState->Status.IsSuccess()) {
            if (FederationState) {
                LOG_LAZY(Log, TLOG_ERR, GetLogPrefix() << "Federated state update failed. FederationState: " << *FederationState);
            } else {
                LOG_LAZY(Log, TLOG_ERR, GetLogPrefix() << "Federated state update failed. FederationState: <empty>");
            }
            CloseImpl();
            DetachSubsessions(toClose);
        } else {
            EventFederator->SetFederationState(FederationState);
            RefreshMirroredTopics();
            const std::string topicKey = CurrentTopicSetKey();
            const bool topicsChanged = topicKey != AppliedTopicSetKey;
            AppliedTopicSetKey = topicKey;

            std::vector<std::shared_ptr<TDbInfo>> eligible;
            for (const auto& db : FederationState->DbInfos) {
                if (IsDatabaseEligibleForRead(db)) {
                    eligible.push_back(db);
                }
            }

            if (eligible.empty() && !SawEligibleDatabase) {
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
                DetachSubsessions(toClose);
            } else {
                if (!eligible.empty()) {
                    SawEligibleDatabase = true;
                }

                std::vector<bool> used(SubSessions.size(), false);
                for (const auto& db : eligible) {
                    size_t found = SubSessions.size();
                    if (!topicsChanged) {
                        for (size_t i = 0; i < SubSessions.size(); ++i) {
                            if (!used[i] && SubSessions[i].DbInfo && SameReadTarget(*SubSessions[i].DbInfo, *db)) {
                                found = i;
                                break;
                            }
                        }
                    }
                    if (found < SubSessions.size()) {
                        used[found] = true;
                        SubSessions[found].DbInfo = db;
                    } else {
                        toOpen.push_back(db);
                    }
                }

                std::vector<TSubSession> kept;
                kept.reserve(SubSessions.size());
                for (size_t i = 0; i < SubSessions.size(); ++i) {
                    if (used[i]) {
                        kept.push_back(std::move(SubSessions[i]));
                    } else {
                        if (SubSessions[i].DbInfo) {
                            LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Stop reading database '" << SubSessions[i].DbInfo->name() << "'");
                        } else {
                            LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Stop reading database '<unknown>'");
                        }
                        if (SubSessions[i].Retired) {
                            SubSessions[i].Retired->store(true);
                        }
                        if (SubSessions[i].Session) {
                            toClose.push_back(std::move(SubSessions[i].Session));
                        }
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
    for (auto& session : toClose) {
        if (session) {
            auto closing = std::move(session);
            closing->Close(TDuration::Zero());
        }
    }
    toClose.clear();

    // CreateReadSession can run user callbacks. Do it without Lock so a callback
    // that re-enters the federated session cannot deadlock on Lock.
    std::vector<TSubSession> opened;
    if (!toOpen.empty()) {
        opened = OpenSubSessions(toOpen);
    }

    NThreading::TPromise<void> wake;
    {
        std::lock_guard guard(Lock);
        if (Closing) {
            for (auto& sub : opened) {
                if (sub.Retired) {
                    sub.Retired->store(true);
                }
                if (sub.Session) {
                    toClose.push_back(std::move(sub.Session));
                }
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
            wake = TakeWakeupPromise();
        }
    }

    SignalWakeup(std::move(wake));
    for (auto& session : toClose) {
        if (session) {
            auto closing = std::move(session);
            closing->Close(TDuration::Zero());
        }
    }

    // Schedule only after this update finishes. The callback is armed with a
    // delay, so the next update cannot overlap this one.
    {
        std::lock_guard guard(Lock);
        if (!Closing) {
            ScheduleFederationStateUpdateImpl();
        }
        if (!Closing) {
            return;
        }
        DetachSubsessions(toClose);
        wake = TakeWakeupPromise();
    }
    SignalWakeup(std::move(wake));
    for (auto& session : toClose) {
        if (session) {
            auto closing = std::move(session);
            closing->Close(TDuration::Zero());
        }
    }
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
            std::lock_guard guard(self->Lock);
            if (self->Closing) {
                return NThreading::MakeFuture();
            }
            if (!self->SubsessionsChanged.Initialized() || self->SubsessionsChanged.HasValue()) {
                self->SubsessionsChanged = NThreading::NewPromise<void>();
            }
            waiters.push_back(self->SubsessionsChanged.GetFuture());
            for (const auto& sub : self->SubSessions) {
                if (sub.Session) {
                    waiters.emplace_back(sub.Session->WaitEvent());
                }
            }
            return NThreading::WaitAny(std::move(waiters));
        }
        return NThreading::MakeFuture();
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
                    if (sub.Session) {
                        for (auto&& ev : sub.Session->GetEvents(false, maxEventsCount, maxByteSize)) {
                            result.push_back(EventFederator->LocateFederate(std::move(ev), sub.DbInfo));
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
    std::vector<std::shared_ptr<NTopic::IReadSession>> sessions;
    NThreading::TPromise<void> wake;
    NYdbGrpc::IQueueClientContextPtr delayCtx;
    {
        std::lock_guard guard(Lock);
        Closing = true;
        delayCtx = std::exchange(UpdateStateDelayContext, nullptr);
        DetachSubsessions(sessions);
        wake = TakeWakeupPromise();
    }
    // Wake blocked readers before closing subsessions. SetValue is outside Lock:
    // its subscriber may call back into the session.
    SignalWakeup(std::move(wake));
    NTopic::Cancel(delayCtx);

    bool result = true;
    for (const auto& session : sessions) {
        if (session) {
            // TODO substract from user timeout
            result = session->Close(timeout);
        }
    }
    return result;
}

} // namespace NYdb::inline Dev::NFederatedTopic
