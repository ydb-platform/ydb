#include "mock_pq_gateway.h"

#include <library/cpp/threading/future/async.h>

#include <ydb/library/yql/providers/pq/gateway/dummy/yql_pq_blocking_queue.h>

namespace NTestUtils {

namespace {

using TQueue = NYql::TBlockingEQueue<NYdb::NTopic::TReadSessionEvent::TEvent>;

class TMockTopicReadSession : public NYdb::NTopic::IReadSession {
public:
    struct TSettings {
        IMockPqGateway::TEvGen EvGen;
        NYdb::NTopic::TPartitionSession::TPtr Session;
    };

    TMockTopicReadSession(std::shared_ptr<TQueue> queue, const TSettings& settings)
        : Settings(settings)
        , MaxBatchSize(Settings.EvGen ? 1 : std::numeric_limits<size_t>::max())
        , Queue(queue)
    {
        if (Queue->IsStopped()) {
            Queue->~TBlockingEQueue();
            new (Queue.get()) TQueue(4_MB);
        }
        ThreadPool.Start(1);
    }

    NThreading::TFuture<void> WaitEvent() override {
        return Settings.EvGen
            ? NThreading::MakeFuture()
            : NThreading::Async([&] () {
                Queue->BlockUntilEvent();
                return NThreading::MakeFuture();
            }, ThreadPool);
    }

    std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) override {
        const auto maxEvents = std::min(MaxBatchSize, maxEventsCount.value_or(std::numeric_limits<size_t>::max()));

        std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> res;
        while (res.size() < maxEvents) {
            if (auto event = GetEvent(block, maxByteSize)) {
                res.push_back(std::move(*event));
                block = false;
            } else {
                break;
            }
        }

        return res;
    }

    std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(const NYdb::NTopic::TReadSessionGetEventSettings& settings) override {
        return GetEvents(settings.Block_, settings.MaxEventsCount_, settings.MaxByteSize_);
    }

    std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(bool block, size_t /*maxByteSize*/) override {
        if (auto event = Queue->Pop(block)) {
            return std::move(*event);
        } else if (Settings.EvGen) {
            return Settings.EvGen({
                .Session = Settings.Session
            });
        }
        return std::nullopt;
    }

    std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvent(const NYdb::NTopic::TReadSessionGetEventSettings& settings) override {
        return GetEvent(settings.Block_, settings.MaxByteSize_);
    }

    bool Close(TDuration /*timeout*/) override {
        Queue->Stop();
        ThreadPool.Stop();
        return true;
    }

    NYdb::NTopic::TReaderCounters::TPtr GetCounters() const override {
        const auto result = MakeIntrusive<NYdb::NTopic::TReaderCounters>();
        NYdb::NTopic::MakeCountersNotNull(*result);
        return result;
    }

    std::string GetSessionId() const override {
        return "0";
    }

private:
    TThreadPool ThreadPool;
    const TSettings Settings;
    const ui64 MaxBatchSize = std::numeric_limits<size_t>::max();
    std::shared_ptr<TQueue> Queue;
};

struct TMockPartitionSession : public NYdb::NTopic::TPartitionSession {
    explicit TMockPartitionSession(const TString& topicPath) {
        PartitionSessionId = 0;
        TopicPath = topicPath;
        PartitionId = 0;
    }

    void RequestStatus() override {
        Y_ENSURE(false, "Not implemented");
    }
};

class TMockPqGateway : public IMockPqGateway {
    struct TTopicInfo {
        TTopicInfo()
            : Queue(std::make_shared<TQueue>(4_MB))
        {}

        std::shared_ptr<TQueue> Queue;
        TEvGen EvGen;
    };

    struct TMockTopicClient : public NYql::ITopicClient {
        explicit TMockTopicClient(TMockPqGateway* self)
            : Self(self)
        {}

        NYdb::TAsyncStatus CreateTopic(const TString& /*path*/, const NYdb::NTopic::TCreateTopicSettings& /*settings*/ = {}) override {
            Y_ENSURE(false, "Not implemented");
        }

        NYdb::TAsyncStatus AlterTopic(const TString& /*path*/, const NYdb::NTopic::TAlterTopicSettings& /*settings*/ = {}) override {
            Y_ENSURE(false, "Not implemented");
        }

        NYdb::TAsyncStatus DropTopic(const TString& /*path*/, const NYdb::NTopic::TDropTopicSettings& /*settings*/ = {}) override {
            Y_ENSURE(false, "Not implemented");
        }

        NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic(const TString& /*path*/, const NYdb::NTopic::TDescribeTopicSettings& /*settings*/ = {}) override {
            Ydb::Topic::DescribeTopicResult describe;
            describe.add_partitions();
            return NThreading::MakeFuture(NYdb::NTopic::TDescribeTopicResult(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}), std::move(describe)));
        }

        NYdb::NTopic::TAsyncDescribeConsumerResult DescribeConsumer(const TString& /*path*/, const TString& /*consumer*/, const NYdb::NTopic::TDescribeConsumerSettings& /*settings*/ = {}) override {
            Y_ENSURE(false, "Not implemented");
        }

        NYdb::NTopic::TAsyncDescribePartitionResult DescribePartition(const TString& /*path*/, i64 /*partitionId*/, const NYdb::NTopic::TDescribePartitionSettings& /*settings*/ = {}) override {
            Y_ENSURE(false, "Not implemented");
        }

        std::shared_ptr<NYdb::NTopic::IReadSession> CreateReadSession(const NYdb::NTopic::TReadSessionSettings& settings) override {
            Y_ENSURE(settings.Topics_.size() == 1, "Expected only one topic to read, but got " << settings.Topics_.size());

            const auto& topic = settings.Topics_.front();
            Y_ENSURE(topic.PartitionIds_.size() == 1, "Expected only one partition to read, but got " << topic.PartitionIds_.size());

            if (Self->Runtime) {
                Self->Runtime->Send(new NActors::IEventHandle(Self->Notifier, NActors::TActorId(), new TEvMockPqEvents::TEvCreateSession()));
            }

            const auto& path = TString(topic.Path_);
            const auto& topicInfo = Self->GetTopicInfo(path);
            return std::make_shared<TMockTopicReadSession>(topicInfo.Queue, TMockTopicReadSession::TSettings{
                .EvGen = topicInfo.EvGen,
                .Session = MakeIntrusive<TMockPartitionSession>(path),
            });
        }

        std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const NYdb::NTopic::TWriteSessionSettings& /*settings*/) override {
            Y_ENSURE(false, "Not implemented");
        }

        std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NTopic::TWriteSessionSettings& /*settings*/) override {
            Y_ENSURE(false, "Not implemented");
        }

        NYdb::TAsyncStatus CommitOffset(const TString& /*path*/, ui64 /*partitionId*/, const TString& /*consumerName*/, ui64 /*offset*/, const NYdb::NTopic::TCommitOffsetSettings& /*settings*/ = {}) override {
            Y_ENSURE(false, "Not implemented");
        }

    private:
        TMockPqGateway* Self;
    };

    struct TMockFederatedTopicClient : public NYql::IFederatedTopicClient {
        explicit TMockFederatedTopicClient(TMockPqGateway* self)
            : Self(self)
        {}

        NThreading::TFuture<std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo>> GetAllTopicClusters() override {
            std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo> dbInfo;
            dbInfo.reserve(Self->Topics.size());
            for (const auto& [topic, _] : Self->Topics) {
                dbInfo.push_back({
                    .Name = topic,
                    .Endpoint = "",
                    .Path = topic,
                    .Status = NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo::EStatus::AVAILABLE
                });
            }

            return NThreading::MakeFuture(std::move(dbInfo));
        }

        std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NFederatedTopic::TFederatedWriteSessionSettings& /*settings*/) override {
            Y_ENSURE(false, "Not implemented");
        }

    private:
        TMockPqGateway* Self;
    };

public:
    explicit TMockPqGateway(const TMockPqGatewaySettings& settings)
        : Runtime(settings.Runtime)
        , Notifier(settings.Notifier)
    {}

    ~TMockPqGateway() {
    }

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& /*username*/) override {
        with_lock (Mutex) {
            Y_ENSURE(Sessions.emplace(sessionId).second, "Session " << sessionId << " is already opened in pq gateway");
        }
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<void> CloseSession(const TString& sessionId) override {
        with_lock (Mutex) {
            Y_ENSURE(Sessions.erase(sessionId), "Session " << sessionId << " is not opened in pq gateway");
        }
        return NThreading::MakeFuture();
    }

    NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(const TString& /*sessionId*/, const TString& /*cluster*/, const TString& /*database*/, const TString& path, const TString& /*token*/) override {
        CheckTopicPath(path);

        NPq::NConfigurationManager::TTopicDescription result(path);
        result.PartitionsCount = 1;
        return NThreading::MakeFuture<NPq::NConfigurationManager::TDescribePathResult>(NPq::NConfigurationManager::TDescribePathResult::Make<NPq::NConfigurationManager::TTopicDescription>(result));
    }

    NThreading::TFuture<TListStreams> ListStreams(const TString& /*sessionId*/, const TString& /*cluster*/, const TString& /*database*/, const TString& /*token*/, ui32 /*limit*/, const TString& /*exclusiveStartStreamName*/ = {}) override {
        Y_ENSURE(false, "Not implemented");
    }

    IPqGateway::TAsyncDescribeFederatedTopicResult DescribeFederatedTopic(const TString& /*sessionId*/, const TString& /*cluster*/, const TString& /*database*/, const TString& path, const TString& /*token*/) override {
        CheckTopicPath(path);

        return NThreading::MakeFuture<TDescribeFederatedTopicResult>(IPqGateway::TDescribeFederatedTopicResult{{
            .PartitionsCount = 1,
        }});
    }

    void UpdateClusterConfigs(const TString& /*clusterName*/, const TString& /*endpoint*/, const TString& /*database*/, bool /*secure*/) override {
    }

    void UpdateClusterConfigs(const NYql::TPqGatewayConfigPtr& /*config*/) override {
    }

    NYql::ITopicClient::TPtr GetTopicClient(const NYdb::TDriver& /*driver*/, const NYdb::NTopic::TTopicClientSettings& /*settings*/) override {
        return MakeIntrusive<TMockTopicClient>(this);
    }

    NYql::IFederatedTopicClient::TPtr GetFederatedTopicClient(const NYdb::TDriver& /*driver*/, const NYdb::NFederatedTopic::TFederatedTopicClientSettings& /*settings*/) override {
        return MakeIntrusive<TMockFederatedTopicClient>(this);
    }

    NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() const override {
        return NYdb::NFederatedTopic::TFederatedTopicClientSettings();
    }

    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const override {
        return NYdb::NTopic::TTopicClientSettings();
    }

    void AddCluster(const NYql::TPqClusterConfig& /*cluster*/) override {
    }

    void AddEvent(const TString& topic, NYdb::NTopic::TReadSessionEvent::TEvent&& e, size_t size) override {
        GetTopicInfo(topic).Queue->Push(std::move(e), size);
    }

    void AddEventProvider(const TString& topic, TEvGen evGen) override {
        GetTopicInfo(topic).EvGen = evGen;
    }

private:
    TTopicInfo& GetTopicInfo(const TString& topic) {
        with_lock (Mutex) {
            return Topics[topic];
        }
    }

    void CheckTopicPath(const TString& path) const {
        with_lock (Mutex) {
            Y_ENSURE(Topics.contains(path), "Topic " << path << " is not registered in pq gateway");
        }
    }

private:
    TMutex Mutex;
    std::unordered_set<TString> Sessions;
    std::unordered_map<TString, TTopicInfo> Topics;
    NActors::TTestActorRuntime* Runtime;
    const NActors::TActorId Notifier;
};

}  // anonymous namespace

NYdb::NTopic::TPartitionSession::TPtr CreatePartitionSession(const TString& path) {
    return MakeIntrusive<TMockPartitionSession>(path);
}

TIntrusivePtr<IMockPqGateway> CreateMockPqGateway(const TMockPqGatewaySettings& settings) {
    return MakeIntrusive<TMockPqGateway>(settings);
}

NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent MakePqMessage(ui64 offset, const TString& data, const TMockPqSession& meta) {
    const auto now = TInstant::Now();

    return NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent({
        NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage(
            data,
            nullptr,
            NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageInformation(
                offset,
                "",
                0,
                now,
                now,
                MakeIntrusive<NYdb::NTopic::TWriteSessionMeta>(),
                MakeIntrusive<NYdb::NTopic::TMessageMeta>(),
                data.size(),
                ""
            ),
            meta.Session
        )
    }, {}, meta.Session);
}

}  // namespace NTestUtils
