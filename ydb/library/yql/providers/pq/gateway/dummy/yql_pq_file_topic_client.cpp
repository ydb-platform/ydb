#include "yql_pq_file_topic_client.h"
#include "util/stream/file.h"

#include <thread>

#include <library/cpp/threading/future/async.h>

#include <util/folder/path.h>
#include <util/system/file.h>
#include "yql_pq_blocking_queue.h"

namespace NYql {

class TFileTopicReadSession : public NYdb::NTopic::IReadSession {

constexpr static auto FILE_POLL_PERIOD = TDuration::MilliSeconds(5);

public:
    TFileTopicReadSession(TFile file, NYdb::NTopic::TPartitionSession::TPtr session, const TString& producerId = "", bool cancelOnFileFinish = false)
        : File_(std::move(file))
        , Session_(std::move(session))
        , ProducerId_(producerId)
        , FilePoller_([this] () {
                PollFileForChanges();
            })
        , Counters_()
        , CancelOnFileFinish_(cancelOnFileFinish)
    {
        Pool_.Start(1);
    }

    NThreading::TFuture<void> WaitEvent() override {
        return NThreading::Async([this] () {
            EventsQ_.BlockUntilEvent();
            return NThreading::MakeFuture();
        }, Pool_);
    }

    std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) override {
        // TODO
        Y_UNUSED(maxByteSize);

        std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> res;
        for (auto event = EventsQ_.Pop(block); event.has_value() && res.size() < maxEventsCount.value_or(std::numeric_limits<size_t>::max()); event = EventsQ_.Pop(/*block=*/ false)) {
            res.push_back(std::move(*event));
        }
        return res;
    }

    std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(const NYdb::NTopic::TReadSessionGetEventSettings& settings) override {
        return GetEvents(settings.Block_, settings.MaxEventsCount_, settings.MaxByteSize_);
    }

    std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) override {
        // TODO
        Y_UNUSED(maxByteSize);

        return EventsQ_.Pop(block);
    }

    std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(const NYdb::NTopic::TReadSessionGetEventSettings& settings) override {
        return GetEvent(settings.Block_, settings.MaxByteSize_);
    }

    bool Close(TDuration timeout = TDuration::Max()) override {
        Y_UNUSED(timeout);
        // TODO send TSessionClosedEvent
        // XXX (... but if we stop queues, nobody will receive it, needs rethinking)
        EventsQ_.Stop();
        Pool_.Stop();

        if (FilePoller_.joinable()) {
            FilePoller_.join();
        }
        return true; // TODO incorrect if EventQ_ was non-empty
    }

    NYdb::NTopic::TReaderCounters::TPtr GetCounters() const override {
        return Counters_;
    }

    std::string GetSessionId() const override {
        return ToString(Session_->GetPartitionSessionId());
    }

    ~TFileTopicReadSession() {
        EventsQ_.Stop();
        Pool_.Stop();
        if (FilePoller_.joinable()) {
            FilePoller_.join();
        }
    }

private:
    using TMessageInformation = NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageInformation;
    using TMessage = NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage;

    TMessageInformation MakeNextMessageInformation(size_t offset, size_t uncompressedSize, const TString& messageGroupId = "") {
        auto now = TInstant::Now();
        TMessageInformation msgInfo(
            offset,
            ProducerId_,
            SeqNo_,
            now,
            now,
            MakeIntrusive<NYdb::NTopic::TWriteSessionMeta>(),
            MakeIntrusive<NYdb::NTopic::TMessageMeta>(),
            uncompressedSize,
            messageGroupId
        );
        return msgInfo;
    }

    TMessage MakeNextMessage(const TString& msgBuff) {
        TMessage msg(msgBuff, nullptr, MakeNextMessageInformation(MsgOffset_, msgBuff.size()), Session_);
        return msg;
    }

    void PollFileForChanges() {
        TFileInput fi(File_);
        while (!EventsQ_.IsStopped()) {
            TString rawMsg;
            TVector<TMessage> msgs;
            size_t size = 0;
            ui64 maxBatchRowSize = 100;

            while (fi.ReadLine(rawMsg)) {
                msgs.emplace_back(MakeNextMessage(rawMsg));
                MsgOffset_++;
                if (!maxBatchRowSize--) {
                    break;
                }
                size += rawMsg.size();
            }
            if (!msgs.empty()) {
                EventsQ_.Push(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent(msgs, {}, Session_), size);
            } else if (CancelOnFileFinish_) {
                EventsQ_.Push(NYdb::NTopic::TSessionClosedEvent(NYdb::EStatus::CANCELLED, {NYdb::NIssue::TIssue("PQ file topic was finished")}), size);
            }

            Sleep(FILE_POLL_PERIOD);
        }
    }

    TFile File_;
    TBlockingEQueue<NYdb::NTopic::TReadSessionEvent::TEvent> EventsQ_ {4_MB};
    NYdb::NTopic::TPartitionSession::TPtr Session_;
    TString ProducerId_;
    std::thread FilePoller_;
    NYdb::NTopic::TReaderCounters::TPtr Counters_;
    bool CancelOnFileFinish_ = false;

    TThreadPool Pool_;
    size_t MsgOffset_ = 0;
    ui64 SeqNo_ = 0;
};

class TFileTopicWriteSession : public NYdb::NTopic::IWriteSession, private NYdb::NTopic::TContinuationTokenIssuer {
public:
    explicit TFileTopicWriteSession(TFile file)
        : File_(std::move(file))
        , FileWriter_([this] () {
                PushToFile();
            })
        , Counters_()
    {
        Pool_.Start(1);
        EventsQ_.Push(NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()});
    }

    NThreading::TFuture<void> WaitEvent() override {
        return NThreading::Async([this] () {
            EventsQ_.BlockUntilEvent();
            return NThreading::MakeFuture();
        }, Pool_);
    }

    std::optional<NYdb::NTopic::TWriteSessionEvent::TEvent> GetEvent(bool block) override {
        return EventsQ_.Pop(block);
    }

    std::vector<NYdb::NTopic::TWriteSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount) override {
        std::vector<NYdb::NTopic::TWriteSessionEvent::TEvent> res;
        for (auto event = EventsQ_.Pop(block); event.has_value() && res.size() < maxEventsCount.value_or(std::numeric_limits<size_t>::max()); event = EventsQ_.Pop(/*block=*/ false)) {
            res.push_back(std::move(*event));
        }
        return res;
    }

    NThreading::TFuture<uint64_t> GetInitSeqNo() override {
        return NThreading::MakeFuture(SeqNo_);
    }

    void Write(NYdb::NTopic::TContinuationToken&&, NYdb::NTopic::TWriteMessage&& message,
               NYdb::TTransactionBase* tx) override {
        Y_UNUSED(tx);

        auto size = message.Data.size();
        EventsMsgQ_.Push(TOwningWriteMessage(std::move(message)), size);
    }

    void Write(NYdb::NTopic::TContinuationToken&& token, std::string_view data, std::optional<uint64_t> seqNo,
                       std::optional<TInstant> createTimestamp) override {
        NYdb::NTopic::TWriteMessage message(data);
        if (seqNo.has_value()) {
            message.SeqNo(*seqNo);
        }
        if (createTimestamp.has_value()) {
            message.CreateTimestamp(*createTimestamp);
        }

        Write(std::move(token), std::move(message), nullptr);
    }

    // Ignores codec in message and always writes raw for debugging purposes
    void WriteEncoded(NYdb::NTopic::TContinuationToken&& token, NYdb::NTopic::TWriteMessage&& params,
                      NYdb::TTransactionBase* tx) override {
        Y_UNUSED(tx);

        NYdb::NTopic::TWriteMessage message(params.Data);

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
    void WriteEncoded(NYdb::NTopic::TContinuationToken&& token, std::string_view data, NYdb::NTopic::ECodec codec, uint32_t originalSize,
                              std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp) override {
        Y_UNUSED(codec);
        Y_UNUSED(originalSize);

        NYdb::NTopic::TWriteMessage message(data);
        if (seqNo.has_value()) {
            message.SeqNo(*seqNo);
        }
        if (createTimestamp.has_value()) {
            message.CreateTimestamp(*createTimestamp);
        }

        Write(std::move(token), std::move(message), nullptr);
    }

    bool Close(TDuration timeout = TDuration::Max()) override {
        Y_UNUSED(timeout);
        // TODO send TSessionClosedEvent
        // XXX (... but if we stop queues, nobody will receive it, needs rethinking)
        EventsQ_.Stop();
        EventsMsgQ_.Stop();
        Pool_.Stop();

        if (FileWriter_.joinable()) {
            FileWriter_.join();
        }
        return true; // TODO incorrect if Event*Q_ was non-empty
    }

    NYdb::NTopic::TWriterCounters::TPtr GetCounters() override {
        return Counters_;
    }

    ~TFileTopicWriteSession() override {
        EventsQ_.Stop();
        EventsMsgQ_.Stop();
        Pool_.Stop();
        if (FileWriter_.joinable()) {
            FileWriter_.join();
        }
    }

private:
    void PushToFile() {
        TFileOutput fo(File_);
        ui64 offset = 0; // FIXME dummy
        ui64 partitionId = 0; // FIXME dummy
        while (auto maybeMsg = EventsMsgQ_.Pop(true)) {
            NYdb::NTopic::TWriteSessionEvent::TAcksEvent acks;
            do {
                auto& [content, msg] = *maybeMsg;
                NYdb::NTopic::TWriteSessionEvent::TWriteAck ack;
                if (msg.SeqNo_.has_value()) { // FIXME should be auto generated otherwise
                    ack.SeqNo = *msg.SeqNo_;
                }
                ack.State = NYdb::NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN;
                ack.Details.emplace(offset, partitionId);
                acks.Acks.emplace_back(std::move(ack));
                offset += content.size() + 1;
                fo.Write(content);
                fo.Write('\n');
            } while ((maybeMsg = EventsMsgQ_.Pop(false)));
            fo.Flush();
            EventsQ_.Push(std::move(acks), 1 + acks.Acks.size());
            EventsQ_.Push(NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()}, 1);
            if (EventsQ_.IsStopped()) {
                break;
            }
        }
    }

    TFile File_;

    // We acquire ownership of messages immediately
    // TODO: remove extra message copying to and from queue
    struct TOwningWriteMessage {
        TString content;
        NYdb::NTopic::TWriteMessage msg;

        explicit TOwningWriteMessage(NYdb::NTopic::TWriteMessage&& msg)
            : content(msg.Data)
            , msg(std::move(msg))
        {
            msg.Data = content;
        }
    };
    TBlockingEQueue<TOwningWriteMessage> EventsMsgQ_ {4_MB};

    TBlockingEQueue<NYdb::NTopic::TWriteSessionEvent::TEvent> EventsQ_ {128_KB};
    std::thread FileWriter_;

    TThreadPool Pool_;
    NYdb::NTopic::TWriterCounters::TPtr Counters_;
    uint64_t SeqNo_ = 0;
};

struct TDummyPartitionSession: public NYdb::NTopic::TPartitionSession {
    TDummyPartitionSession(ui64 sessionId, const TString& topicPath, ui64 partId) {
        PartitionSessionId = sessionId;
        TopicPath = topicPath;
        PartitionId = partId;
    }

    void RequestStatus() override {
        // TODO send TPartitionSessionStatusEvent
    }
};

TFileTopicClient::TFileTopicClient(THashMap<TDummyPqGateway::TClusterNPath, TDummyTopic> topics)
    : Topics_(std::move(topics))
{}

std::shared_ptr<NYdb::NTopic::IReadSession> TFileTopicClient::CreateReadSession(const NYdb::NTopic::TReadSessionSettings& settings) {
    Y_ENSURE(!settings.Topics_.empty());
    const auto& topic = settings.Topics_.front();
    auto topicPath = topic.Path_;
    Y_ENSURE(topic.PartitionIds_.size() >= 1);
    ui64 partitionId = topic.PartitionIds_.front();
    auto topicsIt = Topics_.find(make_pair("pq", topicPath));
    Y_ENSURE(topicsIt != Topics_.end());
    auto filePath = topicsIt->second.Path;
    Y_ENSURE(filePath);

    TFsPath fsPath(*filePath);
    if (fsPath.IsDirectory()) {
        filePath = TStringBuilder() << *filePath << "/" << ToString(partitionId);
    } else if (!fsPath.Exists()) {
        filePath = TStringBuilder() << *filePath << "_" << partitionId;
    }

    // TODO
    ui64 sessionId = 0;
    return std::make_shared<TFileTopicReadSession>(
        TFile(*filePath, EOpenMode::TEnum::RdOnly),
        MakeIntrusive<TDummyPartitionSession>(sessionId, TString{topicPath}, partitionId),
        "", topicsIt->second.CancelOnFileFinish
    );
}

NYdb::TAsyncStatus TFileTopicClient::CreateTopic(const TString& path, const NYdb::NTopic::TCreateTopicSettings& settings) {
    Y_UNUSED(path);
    Y_UNUSED(settings);
    return NThreading::MakeFuture(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}));
}

NYdb::TAsyncStatus TFileTopicClient::AlterTopic(const TString& path, const NYdb::NTopic::TAlterTopicSettings& settings) {
    Y_UNUSED(path);
    Y_UNUSED(settings);
    return NThreading::MakeFuture(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}));
}

NYdb::TAsyncStatus TFileTopicClient::DropTopic(const TString& path, const NYdb::NTopic::TDropTopicSettings& settings) {
    Y_UNUSED(path);
    Y_UNUSED(settings);
    return NThreading::MakeFuture(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}));
}

NYdb::NTopic::TAsyncDescribeTopicResult TFileTopicClient::DescribeTopic(const TString& path,
    const NYdb::NTopic::TDescribeTopicSettings& settings) {
    Y_UNUSED(path);
    Y_UNUSED(settings);

    NYdb::TStatus success(NYdb::EStatus::SUCCESS, {});
    return NThreading::MakeFuture(NYdb::NTopic::TDescribeTopicResult(std::move(success), {}));
}

NYdb::NTopic::TAsyncDescribeConsumerResult TFileTopicClient::DescribeConsumer(const TString& path, const TString& consumer,
    const NYdb::NTopic::TDescribeConsumerSettings& settings) {
    Y_UNUSED(path);
    Y_UNUSED(consumer);
    Y_UNUSED(settings);

    NYdb::TStatus success(NYdb::EStatus::SUCCESS, {});
    return NThreading::MakeFuture(NYdb::NTopic::TDescribeConsumerResult(std::move(success), {}));
}

NYdb::NTopic::TAsyncDescribePartitionResult TFileTopicClient::DescribePartition(const TString& path, i64 partitionId,
    const NYdb::NTopic::TDescribePartitionSettings& settings) {
    Y_UNUSED(path);
    Y_UNUSED(partitionId);
    Y_UNUSED(settings);

    NYdb::TStatus success(NYdb::EStatus::SUCCESS, {});
    return NThreading::MakeFuture(NYdb::NTopic::TDescribePartitionResult(std::move(success), {}));
}

std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession> TFileTopicClient::CreateSimpleBlockingWriteSession(
    const NYdb::NTopic::TWriteSessionSettings& settings) {
    Y_UNUSED(settings);
    return nullptr;
}

std::shared_ptr<NYdb::NTopic::IWriteSession> TFileTopicClient::CreateWriteSession(const NYdb::NTopic::TWriteSessionSettings& settings) {
    auto topicPath = TString{settings.Path_};
    auto topicsIt = Topics_.find(make_pair("pq", topicPath));
    Y_ENSURE(topicsIt != Topics_.end());
    auto filePath = topicsIt->second.Path;
    Y_ENSURE(filePath);

    return std::make_shared<TFileTopicWriteSession>(TFile(*filePath, EOpenMode::TEnum::RdWr));
}

NYdb::TAsyncStatus TFileTopicClient::CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset,
    const NYdb::NTopic::TCommitOffsetSettings& settings) {
    Y_UNUSED(path);
    Y_UNUSED(partitionId);
    Y_UNUSED(consumerName);
    Y_UNUSED(offset);
    Y_UNUSED(settings);
    return NThreading::MakeFuture(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}));
}

}
