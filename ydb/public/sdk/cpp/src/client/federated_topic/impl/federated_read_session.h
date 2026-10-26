#pragma once

#include <ydb/public/sdk/cpp/src/client/federated_topic/impl/federated_topic_impl.h>

#include <ydb/public/sdk/cpp/src/client/topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/string_utils/helpers/helpers.h>

#include <atomic>
#include <deque>
#include <memory>
#include <optional>
#include <utility>

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

    // Generation is captured when the subsession is opened. RetireReadSource bumps it,
    // so a callback still queued for the closed subsession cannot insert that subsession back into the cache.
    template <typename TEvent>
    auto LocateFederate(TEvent&& event, std::shared_ptr<TDbInfo> db, ui64 readSourceGeneration) {
        using TResult = std::decay_t<decltype(Federate(std::declval<TEvent>(), TFederatedPartitionSession::TPtr{}))>;

        NTopic::TPartitionSession::TPtr psPtr;
        TFederatedPartitionSession::TPtr fps;

        using T = std::decay_t<TEvent>;
        if constexpr (std::is_same_v<T, NTopic::TSessionClosedEvent>) {
            if (!IsCurrentGeneration(db, readSourceGeneration)) {
                return std::optional<TResult>{};
            }
            return std::optional<TResult>{Federate(std::forward<TEvent>(event), std::move(fps))};
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
                if (!IsCurrentGeneration(db, readSourceGeneration)) {
                    return std::optional<TResult>{};
                }
                return std::optional<TResult>{Federate(std::forward<TEvent>(event), std::move(fps))};
            }
        } else {
            psPtr = event.GetPartitionSession();
        }

        with_lock(Lock) {
            if (!IsCurrentGenerationLocked(db, readSourceGeneration)) {
                return std::optional<TResult>{};
            }
            if (!FederatedPartitionSessions.contains(psPtr.Get())) {
                auto [topicOriginDbInfo, topicOriginPath] = LocateTopicOrigin(event);
                FederatedPartitionSessions[psPtr.Get()] = MakeIntrusive<TFederatedPartitionSession>(psPtr, db, std::move(topicOriginDbInfo), std::move(topicOriginPath));
            }
            fps = FederatedPartitionSessions[psPtr.Get()];

            if constexpr (std::is_same_v<T, NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>) {
                FederatedPartitionSessions.erase(psPtr.Get());
            }
        }

        return std::optional<TResult>{Federate(std::forward<TEvent>(event), std::move(fps))};
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

    ui64 CurrentGeneration(const std::shared_ptr<TDbInfo>& db) {
        if (!db) {
            return 0;
        }
        with_lock(Lock) {
            return GenerationOfLocked(*db);
        }
    }

    struct TRetiredPartitionSession {
        NTopic::TPartitionSession::TPtr PartitionSession;
        TFederatedPartitionSession::TPtr FederatedPartitionSession;
    };

    // Drops cached partition sessions for a read source that is no longer polled and
    // bumps its generation. A callback queued by the closed subsession keeps the old
    // generation, so LocateFederate will not put that subsession back into the cache.
    // Returns the dropped sessions: the user has seen them and never got their close event.
    std::vector<TRetiredPartitionSession> RetireReadSource(const std::shared_ptr<TDbInfo>& db) {
        std::vector<TRetiredPartitionSession> retired;
        if (!db) {
            return retired;
        }
        with_lock(Lock) {
            ++MutableGenerationLocked(*db);
            for (auto it = FederatedPartitionSessions.begin(); it != FederatedPartitionSessions.end();) {
                const auto& partitionSession = it->second;
                if (SameReadSource(partitionSession, *db)) {
                    // The cached federated session owns the partition session, so the raw key is alive.
                    retired.push_back(TRetiredPartitionSession{NTopic::TPartitionSession::TPtr(it->first), partitionSession});
                    it = FederatedPartitionSessions.erase(it);
                } else {
                    ++it;
                }
            }
        }
        return retired;
    }

private:
    struct TReadSourceGeneration {
        std::string Id;
        std::string Path;
        std::string Name;
        ui64 Generation = 0;
    };

    static bool SameReadSource(const TFederatedPartitionSession::TPtr& partitionSession, const TDbInfo& db) {
        return partitionSession &&
            partitionSession->GetReadSourceDatabaseId() == db.id() &&
            partitionSession->GetReadSourceDatabasePath() == db.path() &&
            AsciiEqualsIgnoreCase(partitionSession->GetReadSourceDatabaseName(), db.name());
    }

    ui64 GenerationOfLocked(const TDbInfo& db) const {
        for (const auto& source : ReadSourceGenerations) {
            if (source.Id == db.id() && source.Path == db.path() && AsciiEqualsIgnoreCase(source.Name, db.name())) {
                return source.Generation;
            }
        }
        return 0;
    }

    ui64& MutableGenerationLocked(const TDbInfo& db) {
        for (auto& source : ReadSourceGenerations) {
            if (source.Id == db.id() && source.Path == db.path() && AsciiEqualsIgnoreCase(source.Name, db.name())) {
                return source.Generation;
            }
        }
        ReadSourceGenerations.push_back(TReadSourceGeneration{std::string(db.id()), std::string(db.path()), std::string(db.name()), 0});
        return ReadSourceGenerations.back().Generation;
    }

    bool IsCurrentGeneration(const std::shared_ptr<TDbInfo>& db, ui64 readSourceGeneration) {
        if (!db) {
            return readSourceGeneration == 0;
        }
        with_lock(Lock) {
            return GenerationOfLocked(*db) == readSourceGeneration;
        }
    }

    bool IsCurrentGenerationLocked(const std::shared_ptr<TDbInfo>& db, ui64 readSourceGeneration) const {
        if (!db) {
            return readSourceGeneration == 0;
        }
        return GenerationOfLocked(*db) == readSourceGeneration;
    }

    TAdaptiveLock Lock;
    std::unordered_map<NTopic::TPartitionSession*, TFederatedPartitionSession::TPtr> FederatedPartitionSessions;
    std::vector<TReadSourceGeneration> ReadSourceGenerations;
    std::shared_ptr<TFederatedDbState> FederationState;
};

class TFederatedReadSessionImpl : public NTopic::TEnableSelfContext<TFederatedReadSessionImpl> {
    friend class TFederatedTopicClient::TImpl;
    friend class TFederatedReadSession;

private:
    struct TSubSession {
        TSubSession(std::shared_ptr<NTopic::IReadSession> session = {},
                    std::shared_ptr<TDbInfo> dbInfo = {},
                    std::shared_ptr<std::atomic<bool>> retired = {},
                    ui64 generation = 0)
            : Session(std::move(session))
            , DbInfo(std::move(dbInfo))
            , Generation(generation)
            , Retired(std::move(retired))
            {}

        std::shared_ptr<NTopic::IReadSession> Session;
        std::shared_ptr<TDbInfo> DbInfo;
        ui64 Generation = 0;
        // Set before an intentional close so a subsession SessionClosedEvent is not
        // reported as the end of the whole federated session.
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

    // Re-reads federation membership, opens subsessions for new readable databases
    // and closes subsessions for databases that left the federation.
    void UpdateFederationState();

    std::vector<TSubSession> OpenSubSessions(const std::vector<std::shared_ptr<TDbInfo>>& dbInfos);

    void RefreshMirroredTopics();
    std::string CurrentTopicSetKey() const;

    // Matches the database against DatabasesToReadFrom, ignoring its status.
    bool IsDatabaseSelected(const std::shared_ptr<TDbInfo>& db);
    static bool IsDatabaseReadable(const TDbInfo& db);
    static bool SameReadTarget(const TDbInfo& current, const TDbInfo& desired);

    struct TClosingSubsession {
        std::shared_ptr<NTopic::IReadSession> Session;
        std::shared_ptr<TDbInfo> DbInfo;
    };

    struct TWakeup {
        ui64 Id = 0;
        NThreading::TPromise<void> Promise;
    };

    // Requires Lock.
    bool ScheduleFederationStateUpdateImpl();
    std::vector<TWakeup> TakeWakeups();
    void ForgetWakeup(ui64 id);
    static void SignalWakeups(std::vector<TWakeup> wakeups);
    // retire=true swallows subsession close callbacks. Used when one database leaves.
    // Whole-session shutdown leaves retire=false so the existing handlers still run.
    void DetachSubsessions(std::vector<TClosingSubsession>& toClose, bool retire);

    enum class ESubsessionClose {
        // The database left the federation: drop its events and report its partitions as lost.
        Leave,
        // The whole session is going away and the subsession was retired: drop its events.
        Discard,
        // Explicit Close(): handlers already queued on the executor still get their events.
        Graceful,
    };
    // Outside Lock.
    void CloseSubsessions(std::vector<TClosingSubsession>& toClose, TDuration timeout, bool& lastCloseResult, ESubsessionClose mode);
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
    std::string AppliedTopicSetKey;
    // True after at least one readable database was observed. A later empty
    // membership drops subsessions but keeps the session, so a database that
    // comes back is read again. An empty first snapshot still fails the session.
    bool SawEligibleDatabase = false;

    NYdbGrpc::IQueueClientContextPtr UpdateStateDelayContext;
    // One promise per blocked WaitEvent. Completed waits drop their promise so a
    // stable federation does not keep a WaitAny subscription alive forever.
    // SetValue happens outside Lock: a subscriber may take Lock.
    std::vector<TWakeup> Wakeups;
    ui64 NextWakeupId = 1;
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
