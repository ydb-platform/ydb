#include "yql_pq_file_topic_client.h"
#include "yql_pq_blocking_queue.h"

#include <ydb/library/yql/providers/pq/gateway/clients/message_stream/yql_pq_message_stream_client.h>

#include <library/cpp/threading/future/async.h>

#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/system/fstat.h>

#include <thread>

namespace NYql {

namespace {

using namespace NYdb;
using namespace NYdb::NTopic;

struct TConfirmSessionInfo {
    std::optional<uint64_t> Offset;
};

class TFileTopicReadSession final : public IReadSession {
    constexpr static TDuration FILE_POLL_PERIOD = TDuration::MilliSeconds(5);

    using TEQueue = TBlockingEQueue<TReadSessionEvent::TEvent>;
    using TMessageInformation = TReadSessionEvent::TDataReceivedEvent::TMessageInformation;
    using TMessage = TReadSessionEvent::TDataReceivedEvent::TMessage;

public:
    TFileTopicReadSession(TFile file, TPartitionSession::TPtr session, const TString& producerId, bool cancelOnFileFinish, NThreading::TFuture<TConfirmSessionInfo> startFuture)
        : File(std::move(file))
        , Session(std::move(session))
        , ProducerId(producerId)
        , CancelOnFileFinish(cancelOnFileFinish)
        , StartFuture(startFuture)
        , FilePoller([this]() { PollFileForChanges(); })
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
        while (res.size() < maxEventsCount.value_or(std::numeric_limits<size_t>::max())) {
            auto event = EventsQ.Pop(block);
            block = false;
            if (!event) {
                break;
            }
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
        bool seekable = TFileStat(File).IsFile();
        EventsQ.Push(TReadSessionEvent::TStartPartitionSessionEvent(Session, /*committedOffset*/0, /*endOffset*/(seekable ? File.GetLength() : 0)));
        while (!StartFuture.IsReady()) {
            StartFuture.Wait(FILE_POLL_PERIOD);
            if (EventsQ.IsStopped()) {
                return;
            }
        }
        auto start = StartFuture.GetValueSync();
        if (start.Offset && seekable) {
            auto target = *start.Offset;
            while (MsgOffset < target) {
                auto skipped = fi.Skip(target - MsgOffset);
                if (skipped == 0) {
                    break;
                }
                MsgOffset += skipped;
            }
        }
        while (!EventsQ.IsStopped()) {
            TString rawMsg;
            TVector<TMessage> msgs;
            size_t size = 0;
            ui64 maxBatchRowSize = 100;

            size_t read;
            while ((read = fi.ReadLine(rawMsg))) {
                MsgOffset += read - 1;
                msgs.emplace_back(MakeNextMessage(rawMsg));
                MsgOffset ++;
                SeqNo ++;
                size += rawMsg.size();
                if (!maxBatchRowSize--) {
                    break;
                }
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
    const bool CancelOnFileFinish = false;
    TEQueue EventsQ = TEQueue(4_MB);
    NThreading::TFuture<TConfirmSessionInfo> StartFuture;
    std::thread FilePoller;
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
        while (res.size() < maxEventsCount.value_or(std::numeric_limits<size_t>::max())) {
            auto event = EventsQ.Pop(block);
            block = false;
            if (!event) {
                break;
            }
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
            auto acksSize = acks.Acks.size();
            EventsQ.Push(std::move(acks), 1 + acksSize);
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
    TMsgQueue EventsMsgQ = TMsgQueue(4_MB);
    TEQueue EventsQ = TEQueue(128_KB);
    TThreadPool Pool;
    uint64_t SeqNo = 0;
    std::thread FileWriter;
};

struct TDummyPartitionSession final : public TPartitionSessionControl {
    TDummyPartitionSession(ui64 sessionId, const TString& topicPath, ui64 partId, NThreading::TPromise<TConfirmSessionInfo> promise)
    : Promise(std::move(promise))
    {
        PartitionSessionId = sessionId;
        TopicPath = topicPath;
        PartitionId = partId;
    }

    ~TDummyPartitionSession() override {
        if (!Promise.IsReady()) {
            Promise.SetException("Session destroyed");
        }
    }

    void RequestStatus() override {
    }

    void Commit(uint64_t /*startOffset*/, uint64_t /*endOffset*/) override {
    }

    void ConfirmCreate(std::optional<uint64_t> readOffset, std::optional<uint64_t> /*commitOffset*/, std::optional<uint64_t> /*maxOffset*/) override {
        Promise.SetValue(TConfirmSessionInfo {
            .Offset = readOffset
        });
    }

    void ConfirmDestroy() override {
    }

    void ConfirmEnd(std::span<const uint32_t> /*childIds*/) override {
    }

private:
    NThreading::TPromise<TConfirmSessionInfo> Promise;
};

class TFileTopicClient final : public NFq::IMessageStreamClient {
public:
    TFileTopicClient(const TString& stream, const THashMap<TClusterNPath, TDummyTopic>& topics, const TFileTopicClientSettings& settings)
        : Stream(stream)
        , Database(settings.Database)
        , Topics(topics)
        , AllowSkipDatabasePrefix(settings.SkipDatabasePrefix)
    {
        if (Stream.empty()) {
            ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::InvalidArgument) << "Stream name must be nonempty";
        }
    }

    const TString& GetStream() const override {
        return Stream;
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamDescription>> DescribeStream() override {
        NFq::TMessageStreamResult<NFq::TMessageStreamDescription> result;
        const TClusterNPath key { "pq", SkipDatabasePrefix(Stream) };
        const auto topicsIt = Topics.find(key);
        if (topicsIt == Topics.end()) {
            result.Status = NFq::EMessageStreamStatus::NotFound;
            result.Issues.AddIssue(NYql::TIssue(TStringBuilder() << "Cluster: " << key.first << ", topic: " << key.second << " not found"));
            return NThreading::MakeFuture(std::move(result));
        }
        for (ui64 id = 0; id < topicsIt->second.PartitionsCount; ++id) {
            result.Value.Partitions.push_back({.PartitionId = {id}});
        }
        return NThreading::MakeFuture(NFq::TMessageStreamResult<NFq::TMessageStreamDescription>::Success(std::move(result.Value)));
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription>> DescribeConsumer(
        const TString&, const NFq::TMessageStreamDescribeConsumerSettings&) override
    {
        return NThreading::MakeFuture(NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription>::Failure(NFq::EMessageStreamStatus::Unsupported, {NYql::TIssue("File streams do not provide these metadata")}));
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription>> DescribePartition(NFq::TMessageStreamPartitionId) override {
        return NThreading::MakeFuture(NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription>::Failure(NFq::EMessageStreamStatus::Unsupported, {NYql::TIssue("File streams do not provide these metadata")}));
    }

    std::shared_ptr<NFq::IMessageStreamReadSession> CreateReadSession(const NFq::TMessageStreamReadSessionSettings& settings) override {
        const auto sdkSettings = ToSdkReadSettings(Stream, settings);
        Y_ENSURE(!sdkSettings.Topics_.empty());
        const auto& topic = sdkSettings.Topics_.front();
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
        auto promise = NThreading::NewPromise<TConfirmSessionInfo>();

        return WrapYdbReadSession(std::make_shared<TFileTopicReadSession>(
            TFile(*filePath, EOpenMode::TEnum::RdOnly),
            MakeIntrusive<TDummyPartitionSession>(static_cast<ui64>(0), TString(topicPath), partitionId, promise),
            "",
            topicsIt->second.CancelOnFileFinish,
            promise.GetFuture()
        ));
    }

    std::shared_ptr<IWriteSession> CreateSdkWriteSession(const TWriteSessionSettings& settings) {
        const auto& key = std::make_pair("pq", SkipDatabasePrefix(TString(settings.Path_)));
        const auto topicsIt = Topics.find(key);
        Y_ENSURE(topicsIt != Topics.end(), "Cluster: " << key.first << ", topic: " << key.second << " not found");
        const auto& filePath = topicsIt->second.Path;
        Y_ENSURE(filePath);

        return std::make_shared<TFileTopicWriteSession>(TFile(*filePath, EOpenMode::TEnum::WrOnly | EOpenMode::TEnum::ForAppend));
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerPosition>> CommitPosition(
        NFq::TMessageStreamPartitionId partitionId, const TString&, ui64 offset) override
    {
        return NThreading::MakeFuture(NFq::TMessageStreamResult<NFq::TMessageStreamConsumerPosition>{
            .Status = NFq::EMessageStreamStatus::Unsupported,
            .Issues = {NYql::TIssue("File streams do not persist consumer positions")},
            .Value = {.PartitionId = partitionId, .NextOffset = offset},
        });
    }

private:
    TString SkipDatabasePrefix(const TString& path) const {
        return AllowSkipDatabasePrefix ? NYql::SkipDatabasePrefix(path, Database) : path;
    }

    const TString Stream;
    const TString Database;
    const THashMap<TClusterNPath, TDummyTopic> Topics;
    const bool AllowSkipDatabasePrefix = false;
};

} // anonymous namespace

std::shared_ptr<NFq::IMessageStreamClient> CreateFileTopicClient(const TString& stream, const THashMap<TClusterNPath, TDummyTopic>& topics, const TFileTopicClientSettings& settings) {
    return std::make_shared<TFileTopicClient>(stream, topics, settings);
}

std::shared_ptr<NYdb::NTopic::IWriteSession> CreateFileTopicWriteSession(
    const THashMap<TClusterNPath, TDummyTopic>& topics,
    const TFileTopicClientSettings& settings,
    const NYdb::NTopic::TWriteSessionSettings& writeSettings)
{
    return TFileTopicClient(TString(writeSettings.Path_), topics, settings).CreateSdkWriteSession(writeSettings);
}

} // namespace NYql
