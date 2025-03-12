#pragma once

#include <ydb/public/sdk/cpp/src/client/federated_topic/impl/federated_topic_impl.h>

#include <ydb/public/sdk/cpp/src/client/topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/string_utils/helpers/helpers.h>

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

    template <typename TEvent>
    auto LocateFederate(TEvent&& event, std::shared_ptr<TDbInfo> db) {
        NTopic::TPartitionSession::TPtr psPtr;
        TFederatedPartitionSession::TPtr fps;

        using T = std::decay_t<TEvent>;
        if constexpr (std::is_same_v<T, NTopic::TSessionClosedEvent>) {
            return Federate(std::move(event), std::move(fps));
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
                return Federate(std::move(event), std::move(fps));
            }
        } else {
            psPtr = event.GetPartitionSession();
        }

        with_lock(Lock) {
            if (!FederatedPartitionSessions.contains(psPtr.Get())) {
                auto [topicOriginDbInfo, topicOriginPath] = LocateTopicOrigin(event);
                FederatedPartitionSessions[psPtr.Get()] = MakeIntrusive<TFederatedPartitionSession>(psPtr, std::move(db), std::move(topicOriginDbInfo), std::move(topicOriginPath));
            }
            fps = FederatedPartitionSessions[psPtr.Get()];

            if constexpr (std::is_same_v<TEvent, NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>) {
                FederatedPartitionSessions.erase(psPtr.Get());
            }
        }

        return Federate(std::move(event), std::move(fps));
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
                ev = TReadSessionEvent::TDataReceivedEvent(std::move(arg), std::move(fps));
            } else if constexpr (std::is_same_v<T, NTopic::TSessionClosedEvent>) {
                ev = std::move(arg);
            } else {
                ev = TReadSessionEvent::TFederated(std::move(arg), std::move(fps));
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
        TSubSession(std::shared_ptr<NTopic::IReadSession> session = {}, std::shared_ptr<TDbInfo> dbInfo = {})
            : Session(std::move(session))
            , DbInfo(std::move(dbInfo))
            {}

        std::shared_ptr<NTopic::IReadSession> Session;
        std::shared_ptr<TDbInfo> DbInfo;
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
    void OpenSubSessionsImpl(const std::vector<std::shared_ptr<TDbInfo>>& dbInfos);

    std::vector<std::string> GetAllFederationDatabaseNames();

    bool IsDatabaseEligibleForRead(const std::shared_ptr<TDbInfo>& db);

    void OnFederatedStateUpdateImpl();

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

    TAdaptiveLock Lock;

    std::vector<TSubSession> SubSessions;
    size_t SubsessionIndex = 0;

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
