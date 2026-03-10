#include "mock_pq_gateway.h"

#include <ydb/library/testlib/common/test_utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/string/join.h>

namespace NTestUtils {

namespace {

class TMockSessionBase {
protected:
    NThreading::TFuture<void> GetFuture() const {
        return Promise.GetFuture();
    }

    void FillPromise() {
        if (!Promise.HasValue()) {
            Promise.SetValue();
        }
    }

    void ClearPromise() {
        if (Promise.HasValue()) {
            Promise = NThreading::NewPromise();
        }
    }

    TGuard<TMutex> Guard() const {
        return ::Guard(Mutex);
    }

    TInverseGuard<TMutex> Unguard() const {
        return ::Unguard(Mutex);
    }

private:
    TMutex Mutex;
    NThreading::TPromise<void> Promise = NThreading::NewPromise();
};

class TMockPqReadSession final : private TMockSessionBase, public IMockPqReadSession, public NYdb::NTopic::IReadSession {
    struct TMockPartitionSession final : public NYdb::NTopic::TPartitionSession {
        TMockPartitionSession(const TString& topicPath, ui64 partitionId) {
            PartitionSessionId = 0;
            TopicPath = topicPath;
            ReadSessionId = TStringBuilder() << "mock-session-to-" << topicPath << "-p" << partitionId;
            PartitionId = partitionId;
        }

        void RequestStatus() final {
            Y_ENSURE(false, "Not implemented");
        }
    };

public:
    TMockPqReadSession(const TString& topicPath, ui64 partitionId)
        : PartitionSession(MakeIntrusive<TMockPartitionSession>(topicPath, partitionId))
    {}

    ~TMockPqReadSession() {
        Close(TDuration::Max());
    }

    //// IReadSession interface implementation

    NThreading::TFuture<void> WaitEvent() final {
        return GetFuture();
    }

    std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) final {
        std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> result;
        while (!maxEventsCount || result.size() < *maxEventsCount) {
            if (auto event = GetEvent(block, maxByteSize)) {
                result.emplace_back(std::move(*event));
                block = false;
            } else {
                break;
            }

            if (const auto lock = Guard(); result.size() >= MaxEventsBatchSize) {
                break;
            }
        }

        return result;
    }

    std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(const NYdb::NTopic::TReadSessionGetEventSettings& settings) final {
        return GetEvents(settings.Block_, settings.MaxEventsCount_, settings.MaxByteSize_);
    }

    std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(bool block, size_t /*maxByteSize*/) final {
        const auto lock = Guard();

        if (Events.empty() && !EvGen) {
            if (!block) {
                return std::nullopt;
            }

            const auto unlock = Unguard();
            GetFuture().Wait();
        }

        if (EvGen) {
            const auto unlock = Unguard();
            return EvGen();
        }

        Y_ENSURE(!Events.empty());
        auto result = std::move(Events.front());
        Events.pop();
        if (Events.empty()) {
            ClearPromise();
        }

        return std::move(result);
    }

    std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(const NYdb::NTopic::TReadSessionGetEventSettings& settings) final {
        return GetEvent(settings.Block_, settings.MaxByteSize_);
    }

    bool Close(TDuration /*timeout*/) final {
        FillPromise();
        return true;
    }

    NYdb::NTopic::TReaderCounters::TPtr GetCounters() const final {
        auto result = MakeIntrusive<NYdb::NTopic::TReaderCounters>();
        NYdb::NTopic::MakeCountersNotNull(*result);
        return result;
    }

    std::string GetSessionId() const final {
        return PartitionSession->GetReadSessionId();
    }

    //// Mock API implementation

    NYdb::NTopic::TPartitionSession::TPtr GetPartitionSession() const final {
        return PartitionSession;
    }

    void SetEventProvider(TEvGen evGen) final {
        const auto lock = Guard();

        EvGen = evGen;
        MaxEventsBatchSize = EvGen ? 1 : std::numeric_limits<size_t>::max();
        FillPromise();
    }

    void AddEvent(NYdb::NTopic::TReadSessionEvent::TEvent&& ev) final {
        const auto lock = Guard();

        Events.emplace(std::move(ev));
        FillPromise();
    }

    void AddStartSessionEvent() final {
        AddEvent(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent(nullptr, 0, 0));
    }

    void AddDataReceivedEvent(ui64 offset, const TString& data) final {
        AddDataReceivedEvent({{.Offset = offset, .Data = data}});
    }

    void AddDataReceivedEvent(ui64 offset, const TString& data, TInstant messageTime) final {
        AddDataReceivedEvent({{.Offset = offset, .Data = data, .MessageTime = messageTime}});
    }

    void AddDataReceivedEvent(const std::vector<TMessage>& messages) final {
        const auto now = TInstant::Now();

        std::vector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> topicMessages;
        topicMessages.reserve(messages.size());
        for (const auto& message : messages) {
            const TInstant msgTime = message.MessageTime.value_or(now);
            topicMessages.push_back({
                message.Data,
                nullptr,
                NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageInformation(
                    message.Offset,
                    "",
                    0,
                    msgTime,
                    msgTime,
                    MakeIntrusive<NYdb::NTopic::TWriteSessionMeta>(),
                    MakeIntrusive<NYdb::NTopic::TMessageMeta>(),
                    message.Data.size(),
                    ""
                ),
                PartitionSession
            });
        }

        AddEvent(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent(std::move(topicMessages), {}, PartitionSession));
    }

    void AddCloseSessionEvent(NYdb::EStatus status, NYdb::NIssue::TIssues issues) final {
        AddEvent(NYdb::NTopic::TSessionClosedEvent(status, std::move(issues)));
    }

private:
    const NYdb::NTopic::TPartitionSession::TPtr PartitionSession;
    TEvGen EvGen;
    size_t MaxEventsBatchSize = std::numeric_limits<size_t>::max();
    std::queue<NYdb::NTopic::TReadSessionEvent::TEvent> Events;
};

class TMockPqWriteSession final : private TMockSessionBase, private NYdb::NTopic::TContinuationTokenIssuer, public IMockPqWriteSession, public NYdb::NTopic::IWriteSession {
public:
    explicit TMockPqWriteSession(TDuration operationTimeout)
        : OperationTimeout(operationTimeout)
    {
        Events.emplace(NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent(std::move(IssueContinuationToken())));
        FillPromise();
    }

    //// IReadSession interface implementation

    NThreading::TFuture<void> WaitEvent() final {
        return GetFuture();
    }

    std::optional<NYdb::NTopic::TWriteSessionEvent::TEvent> GetEvent(bool block) final {
        const auto lock = Guard();

        if (Events.empty() || Locked) {
            if (!block) {
                return std::nullopt;
            }

            const auto unlock = Unguard();
            GetFuture().Wait();
        }

        Y_ENSURE(!Events.empty() && !Locked);
        auto result = std::move(Events.front());
        Events.pop();
        if (Events.empty()) {
            ClearPromise();
        }

        return std::move(result);
    }

    std::vector<NYdb::NTopic::TWriteSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount) final {
        std::vector<NYdb::NTopic::TWriteSessionEvent::TEvent> result;
        while (!maxEventsCount || result.size() < *maxEventsCount) {
            if (auto event = GetEvent(block)) {
                result.emplace_back(std::move(*event));
            } else {
                break;
            }
        }

        return result;
    }

    NThreading::TFuture<uint64_t> GetInitSeqNo() final {
        return NThreading::MakeFuture<uint64_t>(0);
    }

    void Write(NYdb::NTopic::TContinuationToken&& continuationToken, NYdb::NTopic::TWriteMessage&& message, NYdb::TTransactionBase* /*tx*/) final {
        Write(std::move(continuationToken), message.Data, message.SeqNo_, message.CreateTimestamp_);
    }

    void Write(NYdb::NTopic::TContinuationToken&& continuationToken, std::string_view data, std::optional<uint64_t> seqNo, std::optional<TInstant> /*createTimestamp*/) final {
        const auto lock = Guard();

        Events.emplace(NYdb::NTopic::TWriteSessionEvent::TAcksEvent{.Acks = {NYdb::NTopic::TWriteSessionEvent::TWriteAck{
            .SeqNo = seqNo ? *seqNo : 0,
            .State = NYdb::NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN,
        }}});

        Events.emplace(NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent(std::move(continuationToken)));

        if (!Locked) {
            FillPromise();
        }

        Data.emplace_back(data);
    }

    void WriteEncoded(NYdb::NTopic::TContinuationToken&& continuationToken, NYdb::NTopic::TWriteMessage&& params, NYdb::TTransactionBase* tx) final {
        Write(std::move(continuationToken), std::move(params), tx);
    }

    void WriteEncoded(NYdb::NTopic::TContinuationToken&& continuationToken, std::string_view data, NYdb::NTopic::ECodec /*codec*/, uint32_t /*originalSize*/, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp) final {
        Write(std::move(continuationToken), data, seqNo, createTimestamp);
    }

    bool Close(TDuration /*closeTimeout*/) final {
        return true;
    }

    NYdb::NTopic::TWriterCounters::TPtr GetCounters() final {
        return MakeIntrusive<NYdb::NTopic::TWriterCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
    }

    //// Mock API implementation

    std::vector<TString> ExtractData() final {
        const auto lock = Guard();

        std::vector<TString> result = std::move(Data);
        Data.clear();
        return result;
    }

    void ExpectMessage(const TString& message) final {
        ExpectMessages({message}, /* sort */ false);
    }

    void ExpectMessages(std::vector<TString> messages, bool sort) final {
        std::vector<TString> receivedMessages;
        WaitFor(OperationTimeout, "read message from mock pq gateway", [&](TString& errorString) {
            auto data = ExtractData();
            receivedMessages.insert(
                receivedMessages.end(),
                std::make_move_iterator(data.begin()),
                std::make_move_iterator(data.end())
            );

            UNIT_ASSERT_C(messages.size() >= receivedMessages.size(), TStringBuilder()
                << "expected #" << messages.size() << " messages ("
                << JoinSeq(", ", messages) << "), got #" << receivedMessages.size()
                << " messages (" << JoinSeq(", ", receivedMessages) << ")");

            errorString = TStringBuilder() << "received " << receivedMessages.size() << " / " << messages.size() << " messages";
            return receivedMessages.size() >= messages.size();
        });

        if (sort) {
            Sort(receivedMessages);
            Sort(messages);
        }

        UNIT_ASSERT_VALUES_EQUAL(messages.size(), receivedMessages.size());
        for (size_t i = 0; i < messages.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(messages[i], receivedMessages[i], i);
        }
    }

    void Lock() final {
        const auto lock = Guard();

        Locked = true;
        ClearPromise();
    }

    void Unlock() final {
        const auto lock = Guard();

        Locked = false;
        if (!Events.empty()) {
            FillPromise();
        }
    }

private:
    const TDuration OperationTimeout;
    std::vector<TString> Data;
    bool Locked = false;
    std::queue<NYdb::NTopic::TWriteSessionEvent::TEvent> Events;
};

class TMockPqGateway final : public IMockPqGateway {
    class TMockTopicClient final : public NYql::ITopicClient {
    public:
        explicit TMockTopicClient(TMockPqGateway* self)
            : Self(self)
        {}

        NYdb::TAsyncStatus CreateTopic(const TString& /*path*/, const NYdb::NTopic::TCreateTopicSettings& /*settings*/) final {
            Y_ENSURE(false, "Not implemented");
        }

        NYdb::TAsyncStatus AlterTopic(const TString& /*path*/, const NYdb::NTopic::TAlterTopicSettings& /*settings*/) final {
            Y_ENSURE(false, "Not implemented");
        }

        NYdb::TAsyncStatus DropTopic(const TString& /*path*/, const NYdb::NTopic::TDropTopicSettings& /*settings*/) final {
            Y_ENSURE(false, "Not implemented");
        }

        NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic(const TString& /*path*/, const NYdb::NTopic::TDescribeTopicSettings& /*settings*/) final {
            Ydb::Topic::DescribeTopicResult describe;
            describe.add_partitions();
            return NThreading::MakeFuture(NYdb::NTopic::TDescribeTopicResult(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}), std::move(describe)));
        }

        NYdb::NTopic::TAsyncDescribeConsumerResult DescribeConsumer(const TString& /*path*/, const TString& /*consumer*/, const NYdb::NTopic::TDescribeConsumerSettings& /*settings*/) final {
            Y_ENSURE(false, "Not implemented");
        }

        NYdb::NTopic::TAsyncDescribePartitionResult DescribePartition(const TString& /*path*/, i64 /*partitionId*/, const NYdb::NTopic::TDescribePartitionSettings& /*settings*/) final {
            Y_ENSURE(false, "Not implemented");
        }

        std::shared_ptr<NYdb::NTopic::IReadSession> CreateReadSession(const NYdb::NTopic::TReadSessionSettings& settings) final {
            Y_ENSURE(settings.Topics_.size() == 1, "Expected only one topic to read, but got " << settings.Topics_.size());
            const auto& topic = settings.Topics_.front();
            Y_ENSURE(topic.PartitionIds_.size() == 1, "Expected only one partition to read, but got " << topic.PartitionIds_.size());
            return Self->CreateReadSession(topic.Path_, topic.PartitionIds_.front());
        }

        std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const NYdb::NTopic::TWriteSessionSettings& /*settings*/) final {
            Y_ENSURE(false, "Not implemented");
        }

        std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NTopic::TWriteSessionSettings& settings) final {
            return Self->CreateWriteSession(settings.Path_);
        }

        NYdb::TAsyncStatus CommitOffset(const TString& /*path*/, ui64 /*partitionId*/, const TString& /*consumerName*/, ui64 /*offset*/, const NYdb::NTopic::TCommitOffsetSettings& /*settings*/) final {
            Y_ENSURE(false, "Not implemented");
        }

    private:
        TMockPqGateway* Self;
    };

    class TMockFederatedTopicClient final : public NYql::IFederatedTopicClient {
    public:
        explicit TMockFederatedTopicClient(TMockPqGateway* self)
            : Self(self)
        {}

        NThreading::TFuture<std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo>> GetAllTopicClusters() final {
            std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo> dbInfo;

            with_lock (Self->Mutex) {
                dbInfo.reserve(Self->Topics.size());
                for (const auto& [topic, _] : Self->Topics) {
                    dbInfo.push_back({
                        .Name = topic,
                        .Endpoint = "",
                        .Path = topic,
                        .Status = NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo::EStatus::AVAILABLE
                    });
                }
            }

            return NThreading::MakeFuture(std::move(dbInfo));
        }

        std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NFederatedTopic::TFederatedWriteSessionSettings& settings) final {
            return Self->CreateWriteSession(settings.Path_);
        }

    private:
        TMockPqGateway* Self;
    };

    struct TTopicInfo {
        std::unordered_map<ui64, IMockPqReadSession::TPtr> ReadSessionsByPartition;
        ui64 LastCreatedPartitionId = 0;
        IMockPqWriteSession::TPtr WriteSession;
    };

public:
    explicit TMockPqGateway(const TMockPqGatewaySettings& settings)
        : Settings(settings)
    {}

    //// IPqGateway interface implementation

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& /*username*/) final {
        with_lock (Mutex) {
            Y_ENSURE(Sessions.emplace(sessionId).second, "Session " << sessionId << " is already opened in pq gateway");
        }
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<void> CloseSession(const TString& sessionId) final {
        with_lock (Mutex) {
            Y_ENSURE(Sessions.erase(sessionId), "Session " << sessionId << " is not opened in pq gateway");
        }
        return NThreading::MakeFuture();
    }

    NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(const TString& /*sessionId*/, const TString& /*cluster*/, const TString& /*database*/, const TString& path, const TString& /*token*/) final {
        NPq::NConfigurationManager::TTopicDescription result(path);
        result.PartitionsCount = 1;
        return NThreading::MakeFuture<NPq::NConfigurationManager::TDescribePathResult>(NPq::NConfigurationManager::TDescribePathResult::Make<NPq::NConfigurationManager::TTopicDescription>(result));
    }

    NThreading::TFuture<TListStreams> ListStreams(const TString& /*sessionId*/, const TString& /*cluster*/, const TString& /*database*/, const TString& /*token*/, ui32 /*limit*/, const TString& /*exclusiveStartStreamName*/) final {
        TListStreams streams;

        with_lock (Mutex) {
            streams.Names.reserve(Topics.size());
            for (const auto& [name, _] : Topics) {
                streams.Names.emplace_back(name);
            }
        }

        return NThreading::MakeFuture<TListStreams>(std::move(streams));
    }

    IPqGateway::TAsyncDescribeFederatedTopicResult DescribeFederatedTopic(const TString& /*sessionId*/, const TString& /*cluster*/, const TString& /*database*/, const TString& /*path*/, const TString& /*token*/) final {
        return NThreading::MakeFuture<TDescribeFederatedTopicResult>(IPqGateway::TDescribeFederatedTopicResult{{
            .PartitionsCount = 1,
        }});
    }

    void UpdateClusterConfigs(const TString& /*clusterName*/, const TString& /*endpoint*/, const TString& /*database*/, bool /*secure*/) final {
    }

    void UpdateClusterConfigs(const NYql::TPqGatewayConfigPtr& /*config*/) final {
    }

    NYql::ITopicClient::TPtr GetTopicClient(const NYdb::TDriver& /*driver*/, const NYdb::NTopic::TTopicClientSettings& /*settings*/) final {
        return MakeIntrusive<TMockTopicClient>(this);
    }

    NYql::IFederatedTopicClient::TPtr GetFederatedTopicClient(const NYdb::TDriver& /*driver*/, const NYdb::NFederatedTopic::TFederatedTopicClientSettings& /*settings*/) final {
        return MakeIntrusive<TMockFederatedTopicClient>(this);
    }

    NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() const final {
        return NYdb::NFederatedTopic::TFederatedTopicClientSettings();
    }

    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const final {
        return NYdb::NTopic::TTopicClientSettings();
    }

    void AddCluster(const NYql::TPqClusterConfig& /*cluster*/) final {
    }

    //// Mock API implementation

    IMockPqReadSession::TPtr ExtractReadSession(const TString& topic) final {
        IMockPqReadSession::TPtr session;
        with_lock (Mutex) {
            auto& info = Topics[topic];
            auto it = info.ReadSessionsByPartition.find(info.LastCreatedPartitionId);
            if (it != info.ReadSessionsByPartition.end()) {
                session = std::move(it->second);
                info.ReadSessionsByPartition.erase(it);
            }
        }
        return session;
    }

    IMockPqReadSession::TPtr GetReadSession(const TString& topic, ui64 partitionId) final {
        with_lock (Mutex) {
            auto& info = Topics[topic];
            auto it = info.ReadSessionsByPartition.find(partitionId);
            return it != info.ReadSessionsByPartition.end() ? it->second : nullptr;
        }
    }

    IMockPqReadSession::TPtr WaitReadSession(const TString& topic) final {
        return WaitForSession<IMockPqReadSession>(Settings.OperationTimeout, "read", [&]() {
            return ExtractReadSession(topic);
        });
    }

    IMockPqWriteSession::TPtr ExtractWriteSession(const TString& topic) final {
        auto& info = GetTopicInfo(topic);
        IMockPqWriteSession::TPtr session;

        with_lock (Mutex) {
            session = info.WriteSession;
            info.WriteSession = nullptr;
        }

        return session;
    }

    IMockPqWriteSession::TPtr WaitWriteSession(const TString& topic) final {
        return WaitForSession<IMockPqWriteSession>(Settings.OperationTimeout, "write", [&]() {
            return ExtractWriteSession(topic);
        });
    }

private:
    TTopicInfo& GetTopicInfo(const TString& topic) {
        with_lock (Mutex) {
            return Topics[topic];
        }
    }

    std::shared_ptr<NYdb::NTopic::IReadSession> CreateReadSession(const std::string& topic, ui64 partitionId) {
        if (Settings.Runtime && Settings.Notifier) {
            Settings.Runtime->Send(Settings.Notifier, NActors::TActorId(), new TEvMockPqEvents::TEvCreateSession());
        }

        const TString path(topic);
        auto& info = GetTopicInfo(path);
        auto session = std::make_shared<TMockPqReadSession>(path, partitionId);

        with_lock (Mutex) {
            info.ReadSessionsByPartition[partitionId] = session;
            info.LastCreatedPartitionId = partitionId;
        }

        return session;
    }

    std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const std::string& topic) {
        auto& info = GetTopicInfo(TString(topic));
        auto session = std::make_shared<TMockPqWriteSession>(Settings.OperationTimeout);

        if (Settings.LockWritingByDefault) {
            session->Lock();
        }

        with_lock (Mutex) {
            info.WriteSession = session;
        }

        return session;
    }

    template <typename TSession>
    static TSession::TPtr WaitForSession(TDuration timeout, const TString& info, std::function<typename TSession::TPtr()> sessionExtractor) {
        typename TSession::TPtr session;
        WaitFor(timeout, TStringBuilder() << info << " session from mock pq gateway", [&](TString& errorString) {
            if (session = sessionExtractor()) {
                return true;
            }

            errorString = "Session is not ready";
            return false;
        });

        return session;
    }

private:
    TMockPqGatewaySettings Settings;
    TMutex Mutex;
    std::unordered_set<TString> Sessions;
    std::unordered_map<TString, TTopicInfo> Topics;
};

}  // anonymous namespace

TIntrusivePtr<IMockPqGateway> CreateMockPqGateway(const TMockPqGatewaySettings& settings) {
    return MakeIntrusive<TMockPqGateway>(settings);
}

}  // namespace NTestUtils
