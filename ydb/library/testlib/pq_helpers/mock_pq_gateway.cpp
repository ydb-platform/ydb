#include "mock_pq_gateway.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/testlib/common/test_utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/string/join.h>
#include <util/system/mutex.h>

#include <queue>

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
    struct TMockPartitionSession final : public NYdb::NTopic::TPartitionSessionControl {
        TMockPartitionSession(TString topicPath, const ui64 partitionId) {
            PartitionSessionId = 0;
            TopicPath = std::move(topicPath);
            ReadSessionId = TStringBuilder() << "mock-session-to-" << TopicPath << "-p" << partitionId;
            PartitionId = partitionId;
        }

        void RequestStatus() final {
            Y_ENSURE(false, "Not implemented");
        }

        void Commit(uint64_t /*startOffset*/, uint64_t /*endOffset*/) override final {
        }

        void ConfirmCreate(std::optional<uint64_t> /*readOffset*/, std::optional<uint64_t> /*commitOffset*/, std::optional<uint64_t> /*maxOffset*/) override final {
        }

        void ConfirmDestroy() override final {
        }

        void ConfirmEnd(std::span<const uint32_t> /*childIds*/) override final {
        }
    };

public:
    TMockPqReadSession(TString topicPath, const ui64 partitionId, const TDuration operationTimeout)
        : PartitionSession(MakeIntrusive<TMockPartitionSession>(std::move(topicPath), partitionId))
        , OperationTimeout(operationTimeout)
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
        Closed = true;
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

    ui64 GetInflightEventsCount() const final {
        const auto lock = Guard();

        return Events.size();
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

    void AddStartSessionEvent(ui64 endOffset) final {
        AddEvent(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent(PartitionSession, 0, endOffset));
    }

    void AddDataReceivedEvent(ui64 offset, TString data) final {
        AddDataReceivedEvent({{.Offset = offset, .Data = std::move(data)}});
    }

    void AddDataReceivedEvent(ui64 offset, TString data, TInstant messageTime) final {
        AddDataReceivedEvent({{.Offset = offset, .Data = std::move(data), .MessageTime = messageTime}});
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

    void ExpectSessionClosed(std::optional<TDuration> timeout) final {
        WaitFor(timeout.value_or(OperationTimeout), "close read session", [this]() {
            return Closed.load();
        });
    }

private:
    const NYdb::NTopic::TPartitionSession::TPtr PartitionSession;
    const TDuration OperationTimeout;
    TEvGen EvGen;
    size_t MaxEventsBatchSize = std::numeric_limits<size_t>::max();
    std::queue<NYdb::NTopic::TReadSessionEvent::TEvent> Events;
    std::atomic<bool> Closed = false;
};

class TMockPqWriteSession final : private TMockSessionBase, private NYdb::NTopic::TContinuationTokenIssuer, public IMockPqWriteSession, public NYdb::NTopic::IWriteSession {
public:
    using TPtr = std::shared_ptr<TMockPqWriteSession>;

    TMockPqWriteSession(const bool lockFromStart, const TDuration operationTimeout)
        : OperationTimeout(operationTimeout)
    {
        if (lockFromStart) {
            Lock();
        }

        AddEvent(NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent(std::move(IssueContinuationToken())));
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
        AddAck(message.SeqNo_ ? *message.SeqNo_ : 0);
        AddEvent(NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent(std::move(continuationToken)));

        const auto lock = Guard();

        if (const auto& publication = message.DeferredPublication_) {
            UnpublishedData[publication->IntPublicationId].emplace_back(message.Data);
        } else {
            Data.emplace_back(message.Data);
        }
    }

    void Write(NYdb::NTopic::TContinuationToken&& continuationToken, std::string_view data, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp) final {
        NYdb::NTopic::TWriteMessage message(data);
        message.SeqNo(seqNo);
        message.CreateTimestamp(createTimestamp);
        Write(std::move(continuationToken), std::move(message), /* tx */ nullptr);
    }

    void WriteEncoded(NYdb::NTopic::TContinuationToken&& continuationToken, NYdb::NTopic::TWriteMessage&& params, NYdb::TTransactionBase* tx) final {
        Write(std::move(continuationToken), std::move(params), tx);
    }

    void WriteEncoded(NYdb::NTopic::TContinuationToken&& continuationToken, std::string_view data, NYdb::NTopic::ECodec codec, uint32_t originalSize, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp) final {
        auto message = NYdb::NTopic::TWriteMessage::CompressedMessage(data, codec, originalSize);
        message.SeqNo(seqNo);
        message.CreateTimestamp(createTimestamp);
        Write(std::move(continuationToken), std::move(message), /* tx */ nullptr);
    }

    bool Close(TDuration /*closeTimeout*/) final {
        Closed = true;
        return true;
    }

    NYdb::NTopic::TWriterCounters::TPtr GetCounters() final {
        return MakeIntrusive<NYdb::NTopic::TWriterCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>());
    }

    //// Mock API implementation

    void AddCloseSessionEvent(NYdb::EStatus status, NYdb::NIssue::TIssues issues) final {
        AddEvent(NYdb::NTopic::TSessionClosedEvent(status, std::move(issues)));
    }

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

    void ExpectSessionClosed() final {
        WaitFor(OperationTimeout, "close write session", [this]() {
            return Closed.load();
        });
    }

    void EnsureEmpty() final {
        const auto lock = Guard();
        UNIT_ASSERT_VALUES_EQUAL(Data.size(), 0);
    }

    void Lock() final {
        const auto lock = Guard();

        Locked = true;
        ClearPromise();
    }

    void LockAcks() final {
        const auto lock = Guard();

        if (!std::exchange(AcksLocked, true)) {
            std::queue<NYdb::NTopic::TWriteSessionEvent::TEvent> newEvents;

            while (!Events.empty()) {
                auto event = std::move(Events.front());
                Events.pop();

                if (std::holds_alternative<NYdb::NTopic::TWriteSessionEvent::TAcksEvent>(event)) {
                    DeferredAcks.emplace_back(std::move(std::get<NYdb::NTopic::TWriteSessionEvent::TAcksEvent>(event)));
                } else {
                    newEvents.push(std::move(event));
                }
            }

            Events = std::move(newEvents);
        }
    }

    void Unlock() final {
        const auto lock = Guard();

        Locked = false;
        if (!Events.empty()) {
            FillPromise();
        }
    }

    void UnlockAcks(const NYdb::NTopic::TWriteSessionEvent::TWriteAck::EEventState status) final {
        const auto lock = Guard();

        AcksLocked = false;

        while (!DeferredAcks.empty()) {
            auto event = std::move(DeferredAcks.front());
            DeferredAcks.pop_front();

            for (auto& ack : event.Acks) {
                ack.State = status;
            }

            AddEvent(std::move(event));
        }
    }

    void WaitAcks(const ui64 count) final {
        WaitFor(OperationTimeout, "wait acks", [this, count](TString& error) {
            const auto lock = Guard();
            UNIT_ASSERT_C(AcksLocked, "Acks not locked");

            ui64 acksSCount = 0;
            for (const auto& event : DeferredAcks) {
                acksSCount += event.Acks.size();
            }

            error = TStringBuilder() << acksSCount << " / " << count << " acks";
            return acksSCount >= count;
        });
    }

    //// Internal API

    void CommitDeferredPublication(const ui64 publicationIntId) {
        const auto lock = Guard();

        const auto it = UnpublishedData.find(publicationIntId);
        if (it == UnpublishedData.end()) {
            return;
        }

        Data.insert(Data.end(), it->second.begin(), it->second.end());
        UnpublishedData.erase(it);
    }

private:
    void AddAck(const ui64 seqNo) {
        auto ack = NYdb::NTopic::TWriteSessionEvent::TWriteAck{
            .SeqNo = seqNo,
            .State = NYdb::NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN,
        };

        const auto lock = Guard();

        if (AcksLocked) {
            if (DeferredAcks.empty()) {
                DeferredAcks.emplace_back();
            }

            DeferredAcks.back().Acks.emplace_back(std::move(ack));
        } else {
            AddEvent(NYdb::NTopic::TWriteSessionEvent::TAcksEvent{.Acks = {std::move(ack)}});
        }
    }

    void AddEvent(NYdb::NTopic::TWriteSessionEvent::TEvent&& ev) {
        const auto lock = Guard();

        Events.emplace(std::move(ev));

        if (!Locked) {
            FillPromise();
        }
    }

    const TDuration OperationTimeout;
    std::vector<TString> Data;
    std::unordered_map<ui64, std::vector<TString>> UnpublishedData;
    bool Locked = false;
    bool AcksLocked = false;
    std::queue<NYdb::NTopic::TWriteSessionEvent::TEvent> Events;
    std::deque<NYdb::NTopic::TWriteSessionEvent::TAcksEvent> DeferredAcks;
    std::atomic<bool> Closed = false;
};

class TMockPqGateway final : public IMockPqGateway {
    class TMockTopicClient final : public NYql::ITopicClient {
    public:
        explicit TMockTopicClient(TMockPqGateway* const self)
            : Self(self)
            , Topics(self->Settings.Topics)
        {}

        NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic(const TString& path, const NYdb::NTopic::TDescribeTopicSettings& /*settings*/) final {
            TMockPqGatewaySettings::TTopicInfo settings;
            if (const auto it = Topics.find(path); it != Topics.end()) {
                settings = it->second;
            }

            Ydb::Topic::DescribeTopicResult describe;
            for (ui64 i = 0; i < settings.PartitionCount; ++i) {
                auto* partition = describe.add_partitions();
                partition->set_partition_id(i);
            }

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
        TMockPqGateway* const Self = nullptr;
        const std::unordered_map<TString, TMockPqGatewaySettings::TTopicInfo> Topics;
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

    class TMockDeferredPublishClient final : public IMockPqDeferredPublishClient, public NYql::IDeferredPublishClient {
        struct TPublicationInfo {
            const std::string ExtPublicationId;
            const std::optional<std::string> WriterIdentity;
        };

    public:
        using TPtr = TIntrusivePtr<TMockDeferredPublishClient>;

        explicit TMockDeferredPublishClient(const TMockPqGateway* const self)
            : OperationTimeout(self->Settings.OperationTimeout)
        {}

        //// IDeferredPublishClient interface implementation

        NYdb::NTopic::TAsyncBeginPublicationResult BeginPublication(const TString& extPublicationId, const NYdb::NTopic::TBeginPublicationSettings& settings) final {
            ui64 intId = 0;
            with_lock (Mutex) {
                intId = ++PublicationIntId;
                Y_ENSURE(CreatedExtPublicationIds.emplace(extPublicationId).second, "Publication " << extPublicationId << " already created");
                Y_ENSURE(OpenedPublications.emplace(intId, TPublicationInfo{.ExtPublicationId = extPublicationId, .WriterIdentity = settings.WriterIdentity_}).second, "Publication #" << intId << " already opened");
            }

            return NThreading::MakeFuture<NYdb::NTopic::TBeginPublicationResult>(NYdb::NTopic::TBeginPublicationResult(
                NYdb::TStatus(NYdb::EStatus::SUCCESS, {}),
                NYdb::NTopic::TDeferredPublication(intId, extPublicationId)
            ));
        }

        NYdb::NTopic::TAsyncPublishResult Publish(const NYdb::NTopic::TDeferredPublication& publication, const NYdb::NTopic::TPublishSettings& settings) final {
            Y_UNUSED(settings);

            const ui64 intId = publication.IntPublicationId;

            with_lock (Mutex) {
                if (CommitsLocked) {
                    auto promise = NThreading::NewPromise<NYdb::NTopic::TPublishResult>();
                    DeferredCommits.emplace_back(promise, intId);
                    return promise.GetFuture();
                }

                return NThreading::MakeFuture<NYdb::NTopic::TPublishResult>(DoCommitDeferredPublication(intId));
            }
        }

        //// Mock API implementation

        void EnsureOpenedPublications(ui64 count, const TString& nameSubstring) final {
            with_lock (Mutex) {
                UNIT_ASSERT_VALUES_EQUAL(OpenedPublications.size(), count);
                for (const auto& [intId, info] : OpenedPublications) {
                    UNIT_ASSERT_STRING_CONTAINS_C(info.ExtPublicationId, nameSubstring, intId);
                    UNIT_ASSERT_C(info.WriterIdentity, intId);
                    UNIT_ASSERT_STRING_CONTAINS_C(*info.WriterIdentity, nameSubstring, intId);
                }
            }
        }

        void LockCommits() final {
            with_lock (Mutex) {
                CommitsLocked = true;
            }
        }

        void UnlockCommits() final {
            with_lock (Mutex) {
                CommitsLocked = false;

                for (auto& [promise, intId] : DeferredCommits) {
                    promise.SetValue(DoCommitDeferredPublication(intId));
                }
                DeferredCommits.clear();
            }
        }

        void WaitCommits(ui64 count) final {
            WaitFor(OperationTimeout, TStringBuilder() << "wait #" << count << " commits", [this, count](TString& errorString) {
                ui64 commitsCount = 0;
                with_lock (Mutex) {
                    UNIT_ASSERT_C(CommitsLocked, "Commits are not locked");
                    commitsCount = DeferredCommits.size();
                }

                UNIT_ASSERT_C(commitsCount <= count, TStringBuilder() << "expected " << count << " commits, got " << commitsCount);

                errorString = TStringBuilder() << "received " << commitsCount << " / " << count << " commits";
                return commitsCount >= count;
            });
        }

        void ClearCommits() final {
            with_lock (Mutex) {
                UNIT_ASSERT_C(CommitsLocked, "Commits are not locked");

                DeferredCommits.clear();
            }
        }

        void AcceptCommits(NYdb::EStatus status, NYdb::NIssue::TIssues issues) final {
            with_lock (Mutex) {
                UNIT_ASSERT_C(CommitsLocked, "Commits are not locked");

                const auto result = NYdb::NTopic::TPublishResult(NYdb::TStatus(status, std::move(issues)));
                for (auto& [promise, intId] : DeferredCommits) {
                    if (status == NYdb::EStatus::SUCCESS) {
                        DoCommitDeferredPublication(intId);
                    }

                    promise.SetValue(result);
                }

                DeferredCommits.clear();
            }
        }

        //// Internal API

        void RegisterWriteSession(TMockPqWriteSession::TPtr writeSession) {
            with_lock (Mutex) {
                WriteSessions.emplace_back(std::move(writeSession));
            }
        }

    private:
        NYdb::NTopic::TPublishResult DoCommitDeferredPublication(ui64 intId) {
            Y_ENSURE(OpenedPublications.erase(intId) == 1, "Publication #" << intId << " is not opened");

            for (const auto& writeSession : WriteSessions) {
                writeSession->CommitDeferredPublication(intId);
            }

            return NYdb::NTopic::TPublishResult(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}));
        }

        const TDuration OperationTimeout;
        TMutex Mutex;
        ui64 PublicationIntId = 0;
        std::unordered_set<TString> CreatedExtPublicationIds;
        std::unordered_map<ui64, TPublicationInfo> OpenedPublications;
        std::vector<TMockPqWriteSession::TPtr> WriteSessions;
        bool CommitsLocked = false;
        std::vector<std::pair<NThreading::TPromise<NYdb::NTopic::TPublishResult>, ui64>> DeferredCommits;
    };

    struct TTopicInfo {
        std::unordered_map<ui64, IMockPqReadSession::TPtr> ReadSessionsByPartition;
        std::queue<ui64> CreatedPartitionIds;
        TMockPqWriteSession::TPtr WriteSession;
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

    IPqGateway::TAsyncDescribeFederatedTopicResult DescribeFederatedTopic(const TString& /*sessionId*/, const TString& /*cluster*/, const TString& /*database*/, const TString& path, const TString& /*token*/) final {
        TMockPqGatewaySettings::TTopicInfo topicSettings;
        if (const auto it = Settings.Topics.find(path); it != Settings.Topics.end()) {
            topicSettings = it->second;
        }

        return NThreading::MakeFuture<TDescribeFederatedTopicResult>(IPqGateway::TDescribeFederatedTopicResult{{
            .PartitionsCount = topicSettings.PartitionCount,
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

    NYql::IDeferredPublishClient::TPtr GetDeferredPublishClient(const NYdb::TDriver& /*driver*/, const NYdb::TCommonClientSettings& /*settings*/) final {
        SetupDeferredPublishClient();
        return DeferredPublishClient;
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
            if (info.CreatedPartitionIds.empty()) {
                return session;
            }

            const auto it = info.ReadSessionsByPartition.find(info.CreatedPartitionIds.front());
            Y_ENSURE(it != info.ReadSessionsByPartition.end());
            info.CreatedPartitionIds.pop();
            session = it->second;
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

    IMockPqDeferredPublishClient& GetDeferredPublishClientController() final {
        SetupDeferredPublishClient();
        return *DeferredPublishClient;
    }

private:
    TTopicInfo& GetTopicInfo(const TString& topic) {
        with_lock (Mutex) {
            return Topics[topic];
        }
    }

    std::shared_ptr<NYdb::NTopic::IReadSession> CreateReadSession(const std::string& topic, ui64 partitionId) {
        const TString path(topic);
        auto session = std::make_shared<TMockPqReadSession>(path, partitionId, Settings.OperationTimeout);

        with_lock (Mutex) {
            auto& info = Topics[path];
            info.ReadSessionsByPartition[partitionId] = session;
            info.CreatedPartitionIds.emplace(partitionId);
        }

        if (Settings.Runtime && Settings.Notifier) {
            Settings.Runtime->Send(Settings.Notifier, NActors::TActorId(), new TEvMockPqEvents::TEvCreateSession());
        }

        return session;
    }

    std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const std::string& topic) {
        auto& info = GetTopicInfo(TString(topic));
        auto session = std::make_shared<TMockPqWriteSession>(Settings.LockWritingByDefault, Settings.OperationTimeout);

        with_lock (Mutex) {
            info.WriteSession = session;
        }

        SetupDeferredPublishClient();
        DeferredPublishClient->RegisterWriteSession(session);

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

    void SetupDeferredPublishClient() {
        with_lock (Mutex) {
            if (!DeferredPublishClient) {
                DeferredPublishClient = MakeIntrusive<TMockDeferredPublishClient>(this);
            }
        }
    }

private:
    TMockPqGatewaySettings Settings;
    TMutex Mutex;
    TMockDeferredPublishClient::TPtr DeferredPublishClient;
    std::unordered_set<TString> Sessions;
    std::unordered_map<TString, TTopicInfo> Topics;
};

}  // anonymous namespace

TIntrusivePtr<IMockPqGateway> CreateMockPqGateway(const TMockPqGatewaySettings& settings) {
    return MakeIntrusive<TMockPqGateway>(settings);
}

}  // namespace NTestUtils
