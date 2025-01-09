#pragma once

#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_rd_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <yql/essentials/minikql/mkql_alloc.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <queue>

#include <ydb/library/yql/providers/pq/gateway/dummy/yql_pq_blocking_queue.h>

namespace NYql::NDq {

namespace {

class TMockTopicReadSession : public NYdb::NTopic::IReadSession {
public:
    TMockTopicReadSession(NYdb::NTopic::TPartitionSession::TPtr session,  std::shared_ptr<NYql::TBlockingEQueue> queue)
        : Session(std::move(session))
        , Queue(queue) {
        Pool_.Start(1);
    }

    NThreading::TFuture<void> WaitEvent() override {
        return NThreading::Async([&] () {
            Queue->BlockUntilEvent();
            return NThreading::MakeFuture();
        }, Pool_);
    }

    TVector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(bool block, TMaybe<size_t> maxEventsCount, size_t maxByteSize) override {
        TVector<NYdb::NTopic::TReadSessionEvent::TEvent> res;
        for (auto event = Queue->Pop(block); !event.Empty() &&  res.size() <= maxEventsCount.GetOrElse(std::numeric_limits<size_t>::max()); event = Queue->Pop(/*block=*/ false)) {
            res.push_back(*event);
        }
        return res;
    }

    TVector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(const NYdb::NTopic::TReadSessionGetEventSettings& settings) override {
        return GetEvents(settings.Block_, settings.MaxEventsCount_, settings.MaxByteSize_);
    }

    TMaybe<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) override {
        return Queue->Pop(block);
    }

    TMaybe<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(const NYdb::NTopic::TReadSessionGetEventSettings& settings) override {
        return GetEvent(settings.Block_, settings.MaxByteSize_);
    }

    bool Close(TDuration timeout = TDuration::Max()) override {
        Y_UNUSED(timeout);
        Queue->Stop();
        Pool_.Stop();
        return true;
    }

    NYdb::NTopic::TReaderCounters::TPtr GetCounters() const override {return nullptr;}
    TString GetSessionId() const override {return "fake";}
private:
    TThreadPool Pool_;
    NYdb::NTopic::TPartitionSession::TPtr Session;
    std::shared_ptr<NYql::TBlockingEQueue> Queue;
};

struct TDummyPartitionSession: public NYdb::NTopic::TPartitionSession {
    TDummyPartitionSession() {}
    void RequestStatus() override {}
};

class TMockPqGateway : public NYql::IPqGateway {

    struct TMockTopicClient : public NYql::ITopicClient {

        TMockTopicClient(TMockPqGateway* self): Self(self) { }

        NYdb::TAsyncStatus CreateTopic(const TString& path, const NYdb::NTopic::TCreateTopicSettings& settings = {}) override {return NYdb::TAsyncStatus{};}
        NYdb::TAsyncStatus AlterTopic(const TString& path, const NYdb::NTopic::TAlterTopicSettings& settings = {}) override {return NYdb::TAsyncStatus{};}
        NYdb::TAsyncStatus DropTopic(const TString& path, const NYdb::NTopic::TDropTopicSettings& settings = {}) override {return NYdb::TAsyncStatus{};}
        NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic(const TString& path, 
            const NYdb::NTopic::TDescribeTopicSettings& settings = {}) override {return NYdb::NTopic::TAsyncDescribeTopicResult{};}

        NYdb::NTopic::TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString& consumer, 
            const NYdb::NTopic::TDescribeConsumerSettings& settings = {}) override {return NYdb::NTopic::TAsyncDescribeConsumerResult{};}

        NYdb::NTopic::TAsyncDescribePartitionResult DescribePartition(const TString& path, i64 partitionId, 
            const NYdb::NTopic::TDescribePartitionSettings& settings = {}) override {return NYdb::NTopic::TAsyncDescribePartitionResult{};}

        std::shared_ptr<NYdb::NTopic::IReadSession> CreateReadSession(const NYdb::NTopic::TReadSessionSettings& settings) override {
            Y_ENSURE(!settings.Topics_.empty());
            TString topic = settings.Topics_.front().Path_;
            return std::make_shared<TMockTopicReadSession>(MakeIntrusive<TDummyPartitionSession>(), Self->GetEventQueue(topic));
        }

        std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(
            const NYdb::NTopic::TWriteSessionSettings& settings) override {
                return nullptr;
        }
        std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NTopic::TWriteSessionSettings& settings) override {
            return nullptr;
        }

        NYdb::TAsyncStatus CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset,
            const NYdb::NTopic::TCommitOffsetSettings& settings = {}) override {return NYdb::TAsyncStatus{};}
        TMockPqGateway* Self;
    };
public:
    ~TMockPqGateway() {}

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) override {
        return NThreading::MakeFuture();
    }
    NThreading::TFuture<void> CloseSession(const TString& sessionId) override {
        return NThreading::MakeFuture();
    }

    ::NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(
        const TString& sessionId,
        const TString& cluster,
        const TString& database,
        const TString& path,
        const TString& token) override {
            return ::NPq::NConfigurationManager::TAsyncDescribePathResult{};
        }

    NThreading::TFuture<TListStreams> ListStreams(
        const TString& sessionId,
        const TString& cluster,
        const TString& database,
        const TString& token,
        ui32 limit,
        const TString& exclusiveStartStreamName = {}) override {
             return NThreading::TFuture<TListStreams>{};
        }

    void UpdateClusterConfigs(
        const TString& clusterName,
        const TString& endpoint,
        const TString& database,
        bool secure) override {}

    NYql::ITopicClient::TPtr GetTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings) override {
        return MakeIntrusive<TMockTopicClient>(this);
    }

    std::shared_ptr<NYql::TBlockingEQueue> GetEventQueue(const TString& topic) {
        if (!Queues.contains(topic)) {
            Queues[topic] = std::make_shared<NYql::TBlockingEQueue>(4_MB);
        }
        return Queues[topic];
    }

private:
    std::unordered_map<TString, std::shared_ptr<NYql::TBlockingEQueue>> Queues;
};

}

TIntrusivePtr<TDummyPartitionSession> CreatePartitionSession() {
    return MakeIntrusive<TDummyPartitionSession>();
}

TIntrusivePtr<TMockPqGateway> CreateMockPqGateway() {
    return MakeIntrusive<TMockPqGateway>();
}

}
