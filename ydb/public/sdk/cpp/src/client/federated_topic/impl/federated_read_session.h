#pragma once

#include <ydb/public/sdk/cpp/src/client/federated_topic/impl/federated_topic_impl.h>

#include <ydb/public/sdk/cpp/src/client/topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/string_utils/helpers/helpers.h>

#include <atomic>
#include <deque>

namespace NYdb::inline Dev::NFederatedTopic {

class TEventFederator {
public:
    auto LocateTopicOrigin(const NTopic::TReadSessionEvent::TEvent& event) {
        std::shared_ptr<TDbInfo> topicOriginDbInfo;
        std::string topicOriginPath = "";

        auto topicPath = std::visit([](auto&& arg) -> std::string_view {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, NTopic::TSessionClosedEvent>) {
                return "";
            } else {
                return arg.GetPartitionSession()->GetTopicPath();
            }
        }, event);

        if (topicPath.find("-mirrored-from-") != std::string_view::npos) {
            std::string_view leftPart, rightPart;
            auto res = NUtils::TryRSplit(topicPath, leftPart, rightPart, "-mirrored-from-");
            Y_ABORT_UNLESS(res);

            // no additional validation required: TryGetDbInfo just returns nullptr for any bad input
            topicOriginDbInfo = FederationState->TryGetDbInfo(std::string(rightPart));
            if (topicOriginDbInfo) {
                topicOriginPath = leftPart;
            }
        }

        return std::make_tuple(topicOriginDbInfo, topicOriginPath);
    }

    // Returns nothing for an event of a retired subsession, so a callback queued before
    // RetireReadSource cannot put its partition session back into the cache.
    template <typename TEvent>
    auto LocateFederate(TEvent&& event, std::shared_ptr<TDbInfo> db, const std::atomic<bool>& retired) {
        using TResult = std::optional<decltype(Federate(std::declval<TEvent>(), TFederatedPartitionSession::TPtr{}))>;

        NTopic::TPartitionSession::TPtr psPtr;
        TFederatedPartitionSession::TPtr fps;

        using T = std::decay_t<TEvent>;
        if constexpr (std::is_same_v<T, NTopic::TSessionClosedEvent>) {
            return retired ? TResult{} : TResult{Federate(std::forward<TEvent>(event), std::move(fps))};
        } else if constexpr (std::is_same_v<T, NTopic::TReadSessionEvent::TEvent>) {
            psPtr = std::visit([](auto&& arg) -> NTopic::TPartitionSession::TPtr {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, NTopic::TSessionClosedEvent>) {
                    return nullptr;
                } else {
                    return arg.GetPartitionSession();
                }
            }, event);

            if (!psPtr) {  // TSessionClosedEvent
                return retired ? TResult{} : TResult{Federate(std::forward<TEvent>(event), std::move(fps))};
            }
        } else {
            psPtr = event.GetPartitionSession();
        }

        with_lock(Lock) {
            if (retired) {
                return TResult{};
            }
            if (!FederatedPartitionSessions.contains(psPtr.Get())) {
                auto [topicOriginDbInfo, topicOriginPath] = LocateTopicOrigin(event);
                FederatedPartitionSessions[psPtr.Get()] = MakeIntrusive<TFederatedPartitionSession>(psPtr, std::move(db), std::move(topicOriginDbInfo), std::move(topicOriginPath));
            }
            fps = FederatedPartitionSessions[psPtr.Get()];

            if constexpr (std::is_same_v<T, NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>) {
                FederatedPartitionSessions.erase(psPtr.Get());
            }
        }

        return TResult{Federate(std::forward<TEvent>(event), std::move(fps))};
    }

    template <typename TEvent>
    TReadSessionEvent::TFederated<TEvent> Federate(TEvent event, TFederatedPartitionSession::TPtr federatedPartitionSession) {
        return {std::move(event), std::move(federatedPartitionSession)};
    }

    TReadSessionEvent::TDataReceivedEvent Federate(NTopic::TReadSessionEvent::TDataReceivedEvent event,
                                                TFederatedPartitionSession::TPtr federatedPartitionSession) {
        return {std::move(event), std::move(federatedPartitionSession)};
    }

    TReadSessionEvent::TEvent Federate(NTopic::TReadSessionEvent::TEvent event,
                                    TFederatedPartitionSession::TPtr federatedPartitionSession) {
        return std::visit([fps = std::move(federatedPartitionSession)](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            std::optional<TReadSessionEvent::TEvent> ev;
            if constexpr (std::is_same_v<T, NTopic::TReadSessionEvent::TDataReceivedEvent>) {
                ev = TReadSessionEvent::TDataReceivedEvent(std::forward<decltype(arg)>(arg), std::move(fps));
            } else if constexpr (std::is_same_v<T, NTopic::TSessionClosedEvent>) {
                ev = std::forward<decltype(arg)>(arg);
            } else {
                ev = TReadSessionEvent::TFederated(std::forward<decltype(arg)>(arg), std::move(fps));
            }
            return *ev;
        },
        event);
    }

    void SetFederationState(std::shared_ptr<TFederatedDbState> state) {
        with_lock(Lock) {
            FederationState = std::move(state);
        }
    }

    struct TRetiredPartitionSession {
        NTopic::TPartitionSession::TPtr PartitionSession;
        TFederatedPartitionSession::TPtr FederatedPartitionSession;
    };

    // Marks a subsession retired and drops the cached partition sessions it read from `db`.
    // Returns the dropped sessions: the user has seen them and never got their close event.
    std::vector<TRetiredPartitionSession> RetireReadSource(const TDbInfo& db, std::atomic<bool>& retired) {
        std::vector<TRetiredPartitionSession> result;
        with_lock(Lock) {
            retired = true;
            for (auto it = FederatedPartitionSessions.begin(); it != FederatedPartitionSessions.end();) {
                const auto& fps = it->second;
                if (fps->GetReadSourceDatabaseId() == db.id() &&
                    fps->GetReadSourceDatabasePath() == db.path() &&
                    AsciiEqualsIgnoreCase(fps->GetReadSourceDatabaseName(), db.name())) {
                    // The cached federated session owns the partition session, so the raw key is alive.
                    result.push_back({NTopic::TPartitionSession::TPtr(it->first), fps});
                    it = FederatedPartitionSessions.erase(it);
                } else {
                    ++it;
                }
            }
        }
        return result;
    }

private:
    TAdaptiveLock Lock;
    std::unordered_map<NTopic::TPartitionSession*, TFederatedPartitionSession::TPtr> FederatedPartitionSessions;
    std::shared_ptr<TFederatedDbState> FederationState;
};

class TFederatedReadSessionImpl : public NTopic::TEnableSelfContext<TFederatedReadSessionImpl> {
    friend class TFederatedTopicClient::TImpl;
    friend class TFederatedReadSession;

private:
    struct TSubSession {
        TSubSession(std::shared_ptr<NTopic::IReadSession> session = {}, std::shared_ptr<TDbInfo> dbInfo = {}, std::shared_ptr<std::atomic<bool>> retired = {})
            : Session(std::move(session))
            , DbInfo(std::move(dbInfo))
            , Retired(std::move(retired))
            {}

        std::shared_ptr<NTopic::IReadSession> Session;
        std::shared_ptr<TDbInfo> DbInfo;
        // Set before the federated session closes the subsession on its own: the subsession's
        // events and callbacks are dropped, its close is not the end of the federated session.
        std::shared_ptr<std::atomic<bool>> Retired;
    };

public:
    TFederatedReadSessionImpl(const TFederatedReadSessionSettings& settings,
                              std::shared_ptr<TGRpcConnectionsImpl> connections,
                              const TFederatedTopicClientSettings& clientSetttings,
                              std::shared_ptr<TFederatedDbObserver> observer,
                              std::shared_ptr<std::unordered_map<NTopic::ECodec, std::unique_ptr<NTopic::ICodec>>> codecs);

    ~TFederatedReadSessionImpl() = default;

    NThreading::TFuture<void> WaitEvent();
    std::vector<TReadSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize);

    bool Close(TDuration timeout);

    inline std::string GetSessionId() const {
        return SessionId;
    }

    inline NTopic::TReaderCounters::TPtr GetCounters() const {
        return Settings.Counters_; // Always not nullptr.
    }

private:
    TStringBuilder GetLogPrefix() const;

    void Start();
    bool ValidateSettings();
    std::vector<TSubSession> OpenSubSessions(const std::vector<std::shared_ptr<TDbInfo>>& dbInfos);

    // Matches the database against DatabasesToReadFrom, ignoring its status.
    bool IsDatabaseSelected(const std::shared_ptr<TDbInfo>& db);
    static bool IsDatabaseReadable(const std::shared_ptr<TDbInfo>& db);

    // Re-reads federation membership, applies it and schedules the next update.
    void UpdateFederationState();
    // Requires Lock. Moves subsessions of databases that left to `toClose` and lists databases to open.
    // Sets Closing if the state is not usable.
    void OnFederatedStateUpdateImpl(std::vector<TSubSession>& toClose, std::vector<std::shared_ptr<TDbInfo>>& toOpen);
    // Requires Lock.
    bool ScheduleFederationStateUpdateImpl();

    enum class ESubSessionClose {
        // The database left the federation: drop its events and report its partitions as lost.
        Leave,
        // The whole session is going away on its own: drop its events.
        Discard,
        // Explicit Close(): handlers already queued on the executor still get their events.
        Graceful,
    };
    // Outside Lock.
    bool CloseSubSessions(std::vector<TSubSession> subSessions, TDuration timeout, ESubSessionClose mode);
    void ReportLostPartitions(std::vector<TEventFederator::TRetiredPartitionSession> lost);
    void Post(std::function<void()> callback) const;
    void NotifySessionClosed() const;

    NTopic::TSessionClosedEvent MakeSessionClosedEvent() const;

    void CloseImpl();

private:
    TFederatedReadSessionSettings Settings;

    // For subsessions creation
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    const NTopic::TTopicClientSettings SubClientSettings;
    std::shared_ptr<std::unordered_map<NTopic::ECodec, std::unique_ptr<NTopic::ICodec>>> ProvidedCodecs;

    std::shared_ptr<TFederatedDbObserver> Observer;
    NThreading::TFuture<void> AsyncInit;
    std::shared_ptr<TFederatedDbState> FederationState;
    std::shared_ptr<TEventFederator> EventFederator;

    TLog Log;

    const std::string SessionId;
    const TInstant StartSessionTime = TInstant::Now();
    const TDuration FederationDiscoveryInterval;
    const std::vector<NTopic::TTopicReadSettings> OriginalTopics;
    // The executor subsessions run handlers on. Session-level callbacks use it too,
    // so they are ordered with data handlers instead of running beside them.
    const IExecutor::TPtr HandlersExecutor;

    TAdaptiveLock Lock;

    std::vector<TSubSession> SubSessions;
    size_t SubsessionIndex = 0;
    // True after at least one readable database was observed. A later empty
    // membership drops subsessions but keeps the session, so a database that
    // comes back is read again. An empty first snapshot still fails the session.
    bool SawReadableDatabase = false;

    // Signals BlockedWait on a membership change or session close. SetValue happens
    // outside Lock: a subscriber may take Lock.
    NThreading::TPromise<void> Wakeup;
    // One WaitAny for every blocked reader, subscribed to Wakeup and the current
    // subsessions. Rebuilt only after it becomes ready: a timed-out wait drops the
    // caller's copy and does not subscribe again.
    NThreading::TFuture<void> BlockedWait;
    // Events produced by the federated session itself, returned by GetEvents
    // before subsession events. Only used when no handler takes them.
    std::deque<TReadSessionEvent::TEvent> PendingEvents;

    // Exiting.
    bool Closing = false;
};


class TFederatedReadSession : public IFederatedReadSession,
                              public NTopic::TContextOwner<TFederatedReadSessionImpl> {
    friend class TFederatedTopicClient::TImpl;

public:
    TFederatedReadSession(const TFederatedReadSessionSettings& settings,
                          std::shared_ptr<TGRpcConnectionsImpl> connections,
                          const TFederatedTopicClientSettings& clientSettings,
                          std::shared_ptr<TFederatedDbObserver> observer,
                          std::shared_ptr<std::unordered_map<NTopic::ECodec, std::unique_ptr<NTopic::ICodec>>> codecs)
        : TContextOwner(settings, std::move(connections), clientSettings, std::move(observer), std::move(codecs)) {
    }

    ~TFederatedReadSession() {
        TryGetImpl()->Close(TDuration::Zero());
    }

    NThreading::TFuture<void> WaitEvent() override  {
        return TryGetImpl()->WaitEvent();
    }

    std::vector<TReadSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) override {
        return TryGetImpl()->GetEvents(block, maxEventsCount, maxByteSize);
    }

    std::optional<TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) override {
    auto events = GetEvents(block, 1, maxByteSize);
        return events.empty() ? std::nullopt : std::optional<TReadSessionEvent::TEvent>{std::move(events.front())};
    }

    bool Close(TDuration timeout) override {
        return TryGetImpl()->Close(timeout);
    }

    inline std::string GetSessionId() const override {
        return TryGetImpl()->GetSessionId();
    }

    inline NTopic::TReaderCounters::TPtr GetCounters() const override {
        return TryGetImpl()->GetCounters();
    }

private:
    void Start() {
        return TryGetImpl()->Start();
    }
};

} // namespace NYdb::NFederatedTopic
