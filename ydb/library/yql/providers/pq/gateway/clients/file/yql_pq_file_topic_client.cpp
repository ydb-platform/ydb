#include "yql_pq_file_topic_client.h"
#include "yql_pq_blocking_queue.h"

#include <library/cpp/threading/future/async.h>

#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/stream/file.h>
#include <util/system/file.h>

#include <thread>

namespace NYql {

namespace {

using namespace NYdb;
using namespace NYdb::NTopic;

class TFileTopicReadSession final : public IReadSession {
    constexpr static TDuration FILE_POLL_PERIOD = TDuration::MilliSeconds(5);

    using TEQueue = TBlockingEQueue<TReadSessionEvent::TEvent>;
    using TMessageInformation = TReadSessionEvent::TDataReceivedEvent::TMessageInformation;
    using TMessage = TReadSessionEvent::TDataReceivedEvent::TMessage;

public:
    TFileTopicReadSession(TFile file, TPartitionSession::TPtr session, const TString& producerId, bool cancelOnFileFinish)
        : File(std::move(file))
        , Session(std::move(session))
        , ProducerId(producerId)
        , FilePoller([this]() { PollFileForChanges(); })
        , CancelOnFileFinish(cancelOnFileFinish)
    {
        Pool.Start(1);
    }

    ~TFileTopicReadSession() {
        try {
            Cleanup();
        } catch (...) {
            // ¯\_(ツ)_/¯
        }
    }

    NThreading::TFuture<void> WaitEvent() final {
        return NThreading::Async([this]() {
            EventsQ.BlockUntilEvent();
            return NThreading::MakeFuture();
        }, Pool);
    }

    std::vector<TReadSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) final {
        Y_UNUSED(maxByteSize);

        std::vector<TReadSessionEvent::TEvent> res;
        for (auto event = EventsQ.Pop(block); event.has_value() && res.size() < maxEventsCount.value_or(std::numeric_limits<size_t>::max()); event = EventsQ.Pop(/* block */ false)) {
            res.push_back(std::move(*event));
        }

        return res;
    }

    std::vector<TReadSessionEvent::TEvent> GetEvents(const TReadSessionGetEventSettings& settings) final {
        return GetEvents(settings.Block_, settings.MaxEventsCount_, settings.MaxByteSize_);
    }

    std::optional<TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) final {
        Y_UNUSED(maxByteSize);

        return EventsQ.Pop(block);
    }

    std::optional<TReadSessionEvent::TEvent> GetEvent(const TReadSessionGetEventSettings& settings) final {
        return GetEvent(settings.Block_, settings.MaxByteSize_);
    }

    bool Close(TDuration timeout) final {
        Y_UNUSED(timeout);

        Cleanup();
        return true;
    }

    TReaderCounters::TPtr GetCounters() const final {
        return nullptr;
    }

    std::string GetSessionId() const final {
        return ToString(Session->GetPartitionSessionId());
    }

private:
    TMessageInformation MakeNextMessageInformation(size_t offset, size_t uncompressedSize, const TString& messageGroupId) {
        const auto now = TInstant::Now();
        TMessageInformation msgInfo(
            offset,
            ProducerId,
            SeqNo,
            now,
            now,
            MakeIntrusive<TWriteSessionMeta>(),
            MakeIntrusive<TMessageMeta>(),
            uncompressedSize,
            messageGroupId
        );
        return msgInfo;
    }

    TMessage MakeNextMessage(const TString& msgBuff) {
        TMessage msg(msgBuff, nullptr, MakeNextMessageInformation(MsgOffset, msgBuff.size(), ""), Session);
        return msg;
    }

    void PollFileForChanges() {
        TFileInput fi(File);
        while (!EventsQ.IsStopped()) {
            TString rawMsg;
            TVector<TMessage> msgs;
            size_t size = 0;
            ui64 maxBatchRowSize = 100;

            while (fi.ReadLine(rawMsg)) {
                msgs.emplace_back(MakeNextMessage(rawMsg));
                MsgOffset++;
                if (!maxBatchRowSize--) {
                    break;
                }
                size += rawMsg.size();
            }

            if (!msgs.empty()) {
                EventsQ.Push(TReadSessionEvent::TDataReceivedEvent(msgs, {}, Session), size);
            } else if (CancelOnFileFinish) {
                EventsQ.Push(TSessionClosedEvent(EStatus::CANCELLED, {NIssue::TIssue("PQ file topic was finished")}), size);
            }

            Sleep(FILE_POLL_PERIOD);
        }
    }

    void Cleanup() {
        EventsQ.Stop();
        Pool.Stop();

        if (FilePoller.joinable()) {
            FilePoller.join();
        }
    }

    const TFile File;
    const TPartitionSession::TPtr Session;
    const TString ProducerId;
    std::thread FilePoller;
    const bool CancelOnFileFinish = false;
    TEQueue EventsQ = TEQueue(4_MB);
    TThreadPool Pool;
    size_t MsgOffset = 0;
    ui64 SeqNo = 0;
};

class TFileTopicWriteSession final : public IWriteSession, private TContinuationTokenIssuer {
    // We acquire ownership of messages immediately
    struct TOwningWriteMessage {
        explicit TOwningWriteMessage(TWriteMessage&& msg)
            : Content(msg.Data)
            , Msg(std::move(msg))
        {
            Msg.Data = Content;
        }

        TString Content;
        TWriteMessage Msg;
    };

    using TMsgQueue = TBlockingEQueue<TOwningWriteMessage>;
    using TEQueue = TBlockingEQueue<TWriteSessionEvent::TEvent>;

public:
    explicit TFileTopicWriteSession(TFile file)
        : File(std::move(file))
        , FileWriter([this]() { PushToFile(); })
    {
        Pool.Start(1);
        EventsQ.Push(TWriteSessionEvent::TReadyToAcceptEvent(IssueContinuationToken()));
    }

    ~TFileTopicWriteSession() final {
        try {
            Cleanup();
        } catch (...) {
            // ¯\_(ツ)_/¯
        }
    }

    NThreading::TFuture<void> WaitEvent() final {
        return NThreading::Async([this]() {
            EventsQ.BlockUntilEvent();
            return NThreading::MakeFuture();
        }, Pool);
    }

    std::optional<TWriteSessionEvent::TEvent> GetEvent(bool block) final {
        return EventsQ.Pop(block);
    }

    std::vector<TWriteSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount) final {
        std::vector<TWriteSessionEvent::TEvent> res;
        for (auto event = EventsQ.Pop(block); event.has_value() && res.size() < maxEventsCount.value_or(std::numeric_limits<size_t>::max()); event = EventsQ.Pop(/* block */ false)) {
            res.push_back(std::move(*event));
        }

        return res;
    }

    NThreading::TFuture<uint64_t> GetInitSeqNo() final {
        return NThreading::MakeFuture(SeqNo);
    }

    void Write(TContinuationToken&&, TWriteMessage&& message, TTransactionBase* tx) final {
        Y_UNUSED(tx);

        const auto size = message.Data.size();
        EventsMsgQ.Push(TOwningWriteMessage(std::move(message)), size);
    }

    void Write(TContinuationToken&& token, std::string_view data, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp) final {
        TWriteMessage message(data);
        if (seqNo.has_value()) {
            message.SeqNo(*seqNo);
        }
        if (createTimestamp.has_value()) {
            message.CreateTimestamp(*createTimestamp);
        }

        Write(std::move(token), std::move(message), nullptr);
    }

    // Ignores codec in message and always writes raw for debugging purposes
    void WriteEncoded(TContinuationToken&& token, TWriteMessage&& params, TTransactionBase* tx) final {
        Y_UNUSED(tx);

        TWriteMessage message(params.Data);

        if (params.CreateTimestamp_.has_value()) {
            message.CreateTimestamp(*params.CreateTimestamp_);
        }
        if (params.SeqNo_) {
            message.SeqNo(*params.SeqNo_);
        }
        message.MessageMeta(params.MessageMeta_);

        Write(std::move(token), std::move(message), nullptr);
    }

    // Ignores codec in message and always writes raw for debugging purposes
    void WriteEncoded(TContinuationToken&& token, std::string_view data, ECodec codec, uint32_t originalSize, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp) final {
        Y_UNUSED(codec, originalSize);

        TWriteMessage message(data);
        if (seqNo.has_value()) {
            message.SeqNo(*seqNo);
        }
        if (createTimestamp.has_value()) {
            message.CreateTimestamp(*createTimestamp);
        }

        Write(std::move(token), std::move(message), nullptr);
    }

    bool Close(TDuration timeout = TDuration::Max()) final {
        Y_UNUSED(timeout);

        Cleanup();
        return true;
    }

    TWriterCounters::TPtr GetCounters() final {
        return nullptr;
    }

private:
    void PushToFile() {
        TFileOutput fo(File);
        ui64 offset = 0;
        while (auto maybeMsg = EventsMsgQ.Pop(true)) {
            TWriteSessionEvent::TAcksEvent acks;

            do {
                auto& [content, msg] = *maybeMsg;
                TWriteSessionEvent::TWriteAck ack;
                if (msg.SeqNo_.has_value()) { // FIXME should be auto generated otherwise
                    ack.SeqNo = *msg.SeqNo_;
                }
                ack.State = TWriteSessionEvent::TWriteAck::EES_WRITTEN;
                ack.Details.emplace(offset, 0);
                acks.Acks.emplace_back(std::move(ack));
                offset += content.size() + 1;
                fo.Write(content);
                fo.Write('\n');
            } while ((maybeMsg = EventsMsgQ.Pop(false)));

            fo.Flush();
            EventsQ.Push(std::move(acks), 1 + acks.Acks.size());
            EventsQ.Push(TWriteSessionEvent::TReadyToAcceptEvent(IssueContinuationToken()), 1);

            if (EventsQ.IsStopped()) {
                break;
            }
        }
    }

    void Cleanup() {
        EventsQ.Stop();
        EventsMsgQ.Stop();
        Pool.Stop();

        if (FileWriter.joinable()) {
            FileWriter.join();
        }
    }

    const TFile File;
    std::thread FileWriter;
    TMsgQueue EventsMsgQ = TMsgQueue(4_MB);
    TEQueue EventsQ = TEQueue(128_KB);
    TThreadPool Pool;
    uint64_t SeqNo = 0;
};

struct TDummyPartitionSession final : public TPartitionSession {
    TDummyPartitionSession(ui64 sessionId, const TString& topicPath, ui64 partId) {
        PartitionSessionId = sessionId;
        TopicPath = topicPath;
        PartitionId = partId;
    }

    void RequestStatus() override {
    }
};

class TFileTopicClient final : public ITopicClient {
public:
    TFileTopicClient(const THashMap<TClusterNPath, TDummyTopic>& topics, const TFileTopicClientSettings& settings)
        : Database(settings.Database)
        , Topics(topics)
        , AllowSkipDatabasePrefix(settings.SkipDatabasePrefix)
    {}

    TAsyncStatus CreateTopic(const TString& path, const TCreateTopicSettings& settings) final {
        Y_UNUSED(path, settings);
        return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, {}));
    }

    TAsyncStatus AlterTopic(const TString& path, const TAlterTopicSettings& settings) final {
        Y_UNUSED(path, settings);
        return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, {}));
    }

    TAsyncStatus DropTopic(const TString& path, const TDropTopicSettings& settings) final {
        Y_UNUSED(path, settings);
        return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, {}));
    }

    TAsyncDescribeTopicResult DescribeTopic(const TString& path, const TDescribeTopicSettings& settings) final {
        Y_UNUSED(path, settings);
        return NThreading::MakeFuture(TDescribeTopicResult(TStatus(EStatus::SUCCESS, {}), {}));
    }

    TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString& consumer, const TDescribeConsumerSettings& settings) final {
        Y_UNUSED(path, consumer, settings);
        return NThreading::MakeFuture(TDescribeConsumerResult(TStatus(EStatus::SUCCESS, {}), {}));
    }

    TAsyncDescribePartitionResult DescribePartition(const TString& path, i64 partitionId, const TDescribePartitionSettings& settings) final {
        Y_UNUSED(path, partitionId, settings);
        return NThreading::MakeFuture(TDescribePartitionResult(TStatus(EStatus::SUCCESS, {}), {}));
    }

    std::shared_ptr<IReadSession> CreateReadSession(const TReadSessionSettings& settings) final {
        Y_ENSURE(!settings.Topics_.empty());
        const auto& topic = settings.Topics_.front();
        const TString topicPath(topic.Path_);

        Y_ENSURE(topic.PartitionIds_.size() >= 1);
        const ui64 partitionId = topic.PartitionIds_.front();

        const auto& key = std::make_pair("pq", SkipDatabasePrefix(topicPath));
        const auto topicsIt = Topics.find(key);
        Y_ENSURE(topicsIt != Topics.end(), "Cluster: " << key.first << ", topic: " << key.second << " not found");
        auto filePath = topicsIt->second.Path;
        Y_ENSURE(filePath);

        TFsPath fsPath(*filePath);
        if (fsPath.IsDirectory()) {
            filePath = TStringBuilder() << *filePath << "/" << ToString(partitionId);
        } else if (!fsPath.Exists()) {
            filePath = TStringBuilder() << *filePath << "_" << partitionId;
        }

        return std::make_shared<TFileTopicReadSession>(
            TFile(*filePath, EOpenMode::TEnum::RdOnly),
            MakeIntrusive<TDummyPartitionSession>(static_cast<ui64>(0), TString(topicPath), partitionId),
            "",
            topicsIt->second.CancelOnFileFinish
        );
    }

    std::shared_ptr<ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const TWriteSessionSettings& settings) final {
        Y_UNUSED(settings);
        return nullptr;
    }

    std::shared_ptr<IWriteSession> CreateWriteSession(const TWriteSessionSettings& settings) final {
        const auto& key = std::make_pair("pq", SkipDatabasePrefix(TString(settings.Path_)));
        const auto topicsIt = Topics.find(key);
        Y_ENSURE(topicsIt != Topics.end(), "Cluster: " << key.first << ", topic: " << key.second << " not found");
        const auto& filePath = topicsIt->second.Path;
        Y_ENSURE(filePath);

        return std::make_shared<TFileTopicWriteSession>(TFile(*filePath, EOpenMode::TEnum::RdWr));
    }

    TAsyncStatus CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset, const TCommitOffsetSettings& settings) final {
        Y_UNUSED(path, partitionId, consumerName, offset, settings);
        return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, {}));
    }

private:
    TString SkipDatabasePrefix(const TString& path) const {
        return AllowSkipDatabasePrefix ? NYql::SkipDatabasePrefix(path, Database) : path;
    }

    const TString Database;
    const THashMap<TClusterNPath, TDummyTopic> Topics;
    const bool AllowSkipDatabasePrefix = false;
};

} // anonymous namespace

ITopicClient::TPtr CreateFileTopicClient(const THashMap<TClusterNPath, TDummyTopic>& topics, const TFileTopicClientSettings& settings) {
    return MakeIntrusive<TFileTopicClient>(topics, settings);
}

} // namespace NYql
