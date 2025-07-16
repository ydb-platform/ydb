#include "mock_pq_gateway.h"

#include <atomic>
#include <library/cpp/threading/future/async.h>

namespace NYql::NDq {

namespace {

using TQueue = NYql::TBlockingEQueue<NYdb::NTopic::TReadSessionEvent::TEvent>;

class TMockTopicReadSession : public NYdb::NTopic::IReadSession {
public:
    TMockTopicReadSession(NYdb::NTopic::TPartitionSession::TPtr session,  std::shared_ptr<TQueue> queue)
        : Session(std::move(session))
        , Queue(queue) {
        if (Queue->IsStopped()) {
            Queue->~TBlockingEQueue();
            new (Queue.get()) TQueue(4_MB);
        }
        ThreadPool.Start(1);
    }

    NThreading::TFuture<void> WaitEvent() override {
        return NThreading::Async([&] () {
            Queue->BlockUntilEvent();
            return NThreading::MakeFuture();
        }, ThreadPool);
    }

    std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t /*maxByteSize*/) override {
        std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> res;
        for (auto event = Queue->Pop(block); event.has_value() && res.size() <= maxEventsCount.value_or(std::numeric_limits<size_t>::max()); event = Queue->Pop(/*block=*/ false)) {
            res.push_back(*event);
        }
        return res;
    }

    std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(const NYdb::NTopic::TReadSessionGetEventSettings& settings) override {
        return GetEvents(settings.Block_, settings.MaxEventsCount_, settings.MaxByteSize_);
    }

    std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(bool block, size_t /*maxByteSize*/) override {
        return Queue->Pop(block);
    }

    std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(const NYdb::NTopic::TReadSessionGetEventSettings& settings) override {
        return GetEvent(settings.Block_, settings.MaxByteSize_);
    }

    bool Close(TDuration timeout = TDuration::Max()) override {
        Y_UNUSED(timeout);
        Queue->Stop();
        ThreadPool.Stop();
        return true;
    }

    NYdb::NTopic::TReaderCounters::TPtr GetCounters() const override {return nullptr;}
    std::string GetSessionId() const override {return "fake";}
private:
    TThreadPool ThreadPool;
    NYdb::NTopic::TPartitionSession::TPtr Session;
    std::shared_ptr<TQueue> Queue;
};

struct TDummyPartitionSession: public NYdb::NTopic::TPartitionSession {
    TDummyPartitionSession() {}
    void RequestStatus() override {}
};

class TMockPqGateway : public IMockPqGateway {

    struct TMockTopicClient : public NYql::ITopicClient {

        TMockTopicClient(TMockPqGateway* self): Self(self) { }

        NYdb::TAsyncStatus CreateTopic(const TString& /*path*/, const NYdb::NTopic::TCreateTopicSettings& /*settings*/ = {}) override {return NYdb::TAsyncStatus{};}
        NYdb::TAsyncStatus AlterTopic(const TString& /*path*/, const NYdb::NTopic::TAlterTopicSettings& /*settings*/ = {}) override {return NYdb::TAsyncStatus{};}
        NYdb::TAsyncStatus DropTopic(const TString& /*path*/, const NYdb::NTopic::TDropTopicSettings& /*settings*/ = {}) override {return NYdb::TAsyncStatus{};}
        NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic(const TString& /*path*/, 
            const NYdb::NTopic::TDescribeTopicSettings& /*settings*/ = {}) override {
            NYdb::TStatus success(NYdb::EStatus::SUCCESS, {});
            Ydb::Topic::DescribeTopicResult describe;
            describe.Addpartitions();
            return NThreading::MakeFuture(NYdb::NTopic::TDescribeTopicResult(std::move(success), std::move(describe)));
        }

        NYdb::NTopic::TAsyncDescribeConsumerResult DescribeConsumer(const TString& /*path*/, const TString& /*consumer*/, 
            const NYdb::NTopic::TDescribeConsumerSettings& /*settings*/ = {}) override {return NYdb::NTopic::TAsyncDescribeConsumerResult{};}

        NYdb::NTopic::TAsyncDescribePartitionResult DescribePartition(const TString& /*path*/, i64 /*partitionId*/, 
            const NYdb::NTopic::TDescribePartitionSettings& /*settings*/ = {}) override {return NYdb::NTopic::TAsyncDescribePartitionResult{};}

        std::shared_ptr<NYdb::NTopic::IReadSession> CreateReadSession(const NYdb::NTopic::TReadSessionSettings& settings) override {
            Y_ENSURE(!settings.Topics_.empty());
            auto topic = TString{settings.Topics_.front().Path_};
            Self->Runtime.Send(new NActors::IEventHandle(Self->Notifier, NActors::TActorId(), new NYql::NDq::TEvMockPqEvents::TEvCreateSession()));
            return std::make_shared<TMockTopicReadSession>(MakeIntrusive<TDummyPartitionSession>(), Self->GetEventQueue(topic));
        }

        std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(
            const NYdb::NTopic::TWriteSessionSettings& /*settings*/) override {
                return nullptr;
        }
        std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NTopic::TWriteSessionSettings& /*settings*/) override {
            return nullptr;
        }

        NYdb::TAsyncStatus CommitOffset(const TString& /*path*/, ui64 /*partitionId*/, const TString& /*consumerName*/, ui64 /*offset*/,
            const NYdb::NTopic::TCommitOffsetSettings& /*settings*/ = {}) override {return NYdb::TAsyncStatus{};}

        TMockPqGateway* Self;
    };

    struct TMockFederatedTopicClient : public IFederatedTopicClient {
        NThreading::TFuture<std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo>> GetAllTopicClusters() override {
            std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo> dbInfo;
            dbInfo.emplace_back(
                    "", "dummy", "/Root",
                    NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo::EStatus::AVAILABLE);
            return NThreading::MakeFuture(std::move(dbInfo));
        }
        std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NFederatedTopic::TFederatedWriteSessionSettings& /*settings*/) override {
            return nullptr;
        }
    };

public:

    TMockPqGateway(
        NActors::TTestActorRuntime& runtime,
        NActors::TActorId notifier)
        : Runtime(runtime)
        , Notifier(notifier) {}

    ~TMockPqGateway() {}

    NThreading::TFuture<void> OpenSession(const TString& /*sessionId*/, const TString& /*username*/) override {
        return NThreading::MakeFuture();
    }
    NThreading::TFuture<void> CloseSession(const TString& /*sessionId*/) override {
        return NThreading::MakeFuture();
    }

    ::NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(
        const TString& /*sessionId*/,
        const TString& /*cluster*/,
        const TString& /*database*/,
        const TString& /*path*/,
        const TString& /*token*/) override {
            return ::NPq::NConfigurationManager::TAsyncDescribePathResult{};
        }

    NThreading::TFuture<TListStreams> ListStreams(
        const TString& /*sessionId*/,
        const TString& /*cluster*/,
        const TString& /*database*/,
        const TString& /*token*/,
        ui32 /*limit*/,
        const TString& /*exclusiveStartStreamName*/ = {}) override {
             return NThreading::TFuture<TListStreams>{};
        }

    IPqGateway::TAsyncDescribeFederatedTopicResult DescribeFederatedTopic(
        const TString& /*sessionId*/,
        const TString& /*cluster*/,
        const TString& /*database*/,
        const TString& /*path*/,
        const TString& /*token*/) override {
            TDescribeFederatedTopicResult result;
            auto& cluster = result.emplace_back();
            cluster.PartitionsCount = 1;
            return NThreading::MakeFuture(result);
        }

    void UpdateClusterConfigs(
        const TString& /*clusterName*/,
        const TString& /*endpoint*/,
        const TString& /*database*/,
        bool /*secure*/) override {}

    void UpdateClusterConfigs(const TPqGatewayConfigPtr& /*config*/) override {};

    NYql::ITopicClient::TPtr GetTopicClient(const NYdb::TDriver& /*driver*/, const NYdb::NTopic::TTopicClientSettings& /*settings*/) override {
        return MakeIntrusive<TMockTopicClient>(this);
    }

    IFederatedTopicClient::TPtr GetFederatedTopicClient(const NYdb::TDriver& /*driver*/, const NYdb::NFederatedTopic::TFederatedTopicClientSettings& /*settings*/) override {
        return MakeIntrusive<TMockFederatedTopicClient>();
    }

    NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() const override {
        return {};
    }

    std::shared_ptr<TQueue> GetEventQueue(const TString& topic) {
        if (!Queues.contains(topic)) {
            Queues[topic] = std::make_shared<TQueue>(4_MB);
        }
        return Queues[topic];
    }

     void AddEvent(const TString& topic, NYdb::NTopic::TReadSessionEvent::TEvent&& e, size_t size) override {
        GetEventQueue(topic)->Push(std::move(e), size);
     }

     NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const override {
        return NYdb::NTopic::TTopicClientSettings();
     }

    void AddCluster(const NYql::TPqClusterConfig& /*cluster*/) override {}


private:
    std::unordered_map<TString, std::shared_ptr<TQueue>> Queues;
    NActors::TTestActorRuntime& Runtime;
    NActors::TActorId Notifier;
};

}

NYdb::NTopic::TPartitionSession::TPtr CreatePartitionSession() {
    return MakeIntrusive<TDummyPartitionSession>();
}

TIntrusivePtr<IMockPqGateway> CreateMockPqGateway(
    NActors::TTestActorRuntime& runtime,
    NActors::TActorId notifier) {
    return MakeIntrusive<TMockPqGateway>(runtime, notifier);
}

}
