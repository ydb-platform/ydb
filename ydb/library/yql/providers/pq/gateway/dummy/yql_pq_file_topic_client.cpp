#include "yql_pq_file_topic_client.h"
#include "util/stream/file.h"

#include <thread>

#include <library/cpp/threading/blocking_queue/blocking_queue.h>
#include <library/cpp/threading/future/async.h>

#include <util/system/file.h>
#include "yql_pq_blocking_queue.h"

namespace NYql {

class TFileTopicReadSession : public NYdb::NTopic::IReadSession {

constexpr static auto FILE_POLL_PERIOD = TDuration::MilliSeconds(5);    

public:
    TFileTopicReadSession(TFile file, NYdb::NTopic::TPartitionSession::TPtr session, const TString& producerId = ""): 
        File_(std::move(file)), Session_(std::move(session)), ProducerId_(producerId), 
        FilePoller_([this] () {
            PollFileForChanges();
        }), Counters_()
    {
        Pool_.Start(1);
    }

    NThreading::TFuture<void> WaitEvent() override {
        return NThreading::Async([this] () {
            EventsQ_.BlockUntilEvent();
            return NThreading::MakeFuture();
        }, Pool_);
    }

    TVector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(bool block, TMaybe<size_t> maxEventsCount, size_t maxByteSize) override {
        // TODO
        Y_UNUSED(maxByteSize);

        TVector<NYdb::NTopic::TReadSessionEvent::TEvent> res;
        for (auto event = EventsQ_.Pop(block); !event.Empty() &&  res.size() <= maxEventsCount.GetOrElse(std::numeric_limits<size_t>::max()); event = EventsQ_.Pop(/*block=*/ false)) {
            res.push_back(*event);
        }
        return res;
    }

    TVector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(const NYdb::NTopic::TReadSessionGetEventSettings& settings) override {
        return GetEvents(settings.Block_, settings.MaxEventsCount_, settings.MaxByteSize_);
    }

    TMaybe<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) override {
        // TODO
        Y_UNUSED(maxByteSize);

        return EventsQ_.Pop(block);
    }

    TMaybe<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(const NYdb::NTopic::TReadSessionGetEventSettings& settings) override {
        return GetEvent(settings.Block_, settings.MaxByteSize_);
    }

    bool Close(TDuration timeout = TDuration::Max()) override {
        Y_UNUSED(timeout);
        // TOOD send TSessionClosedEvent
        EventsQ_.Stop();
        Pool_.Stop();

        if (FilePoller_.joinable()) {
            FilePoller_.join();
        }
        return true;
    }

    NYdb::NTopic::TReaderCounters::TPtr GetCounters() const override {
        return Counters_;
    }

    TString GetSessionId() const override {
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

            while (size_t read = fi.ReadLine(rawMsg)) {
                msgs.emplace_back(MakeNextMessage(rawMsg));
                MsgOffset_++;
                if (!maxBatchRowSize--) {
                    break;
                }
                size += rawMsg.size();
            }
            if (!msgs.empty()) {
                EventsQ_.Push(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent(msgs, {}, Session_), size);
            }

            Sleep(FILE_POLL_PERIOD);
        }
    }
    
    TFile File_;
    TBlockingEQueue EventsQ_ {4_MB};
    NYdb::NTopic::TPartitionSession::TPtr Session_;
    TString ProducerId_;
    std::thread FilePoller_;
    NYdb::NTopic::TReaderCounters::TPtr Counters_;

    TThreadPool Pool_;
    size_t MsgOffset_ = 0;
    ui64 SeqNo_ = 0;
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

std::shared_ptr<NYdb::NTopic::IReadSession> TFileTopicClient::CreateReadSession(const NYdb::NTopic::TReadSessionSettings& settings) {
    Y_ENSURE(!settings.Topics_.empty());
    TString topicPath = settings.Topics_.front().Path_;

    auto topicsIt = Topics_.find(make_pair("pq", topicPath));
    Y_ENSURE(topicsIt != Topics_.end());
    auto filePath = topicsIt->second.FilePath;
    Y_ENSURE(filePath);
    
    // TODO
    ui64 sessionId = 0;
    ui64 partitionId = 0;

    return std::make_shared<TFileTopicReadSession>(
        TFile(*filePath, EOpenMode::TEnum::RdOnly),
        MakeIntrusive<TDummyPartitionSession>(sessionId, topicPath, partitionId)
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
    Y_UNUSED(settings);
    return nullptr;
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
