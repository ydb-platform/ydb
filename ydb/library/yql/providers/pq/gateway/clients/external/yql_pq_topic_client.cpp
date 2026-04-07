#include "yql_pq_topic_client.h"

namespace NYql {

namespace {

using namespace NYdb;
using namespace NYdb::NTopic;

class TNativeTopicClient final : public ITopicClient {
public:
    TNativeTopicClient(const TDriver& driver, const TTopicClientSettings& settings)
        : Client(driver, settings)
    {}

    TAsyncStatus CreateTopic(const TString& path, const TCreateTopicSettings& settings) final {
        return Client.CreateTopic(path, settings);
    }

    TAsyncStatus AlterTopic(const TString& path, const TAlterTopicSettings& settings) final {
        return Client.AlterTopic(path, settings);
    }

    TAsyncStatus DropTopic(const TString& path, const TDropTopicSettings& settings) final {
        return Client.DropTopic(path, settings);
    }

    TAsyncDescribeTopicResult DescribeTopic(const TString& path, const TDescribeTopicSettings& settings) final {
        return Client.DescribeTopic(path, settings);
    }

    TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString& consumer, const TDescribeConsumerSettings& settings) final {
        return Client.DescribeConsumer(path, consumer, settings);
    }

    TAsyncDescribePartitionResult DescribePartition(const TString& path, i64 partitionId, const TDescribePartitionSettings& settings) final {
        return Client.DescribePartition(path, partitionId, settings);
    }

    std::shared_ptr<IReadSession> CreateReadSession(const TReadSessionSettings& settings) final {
        return Client.CreateReadSession(settings);
    }

    std::shared_ptr<ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const TWriteSessionSettings& settings) final {
        return Client.CreateSimpleBlockingWriteSession(settings);
    }

    std::shared_ptr<IWriteSession> CreateWriteSession(const TWriteSessionSettings& settings) final {
        return Client.CreateWriteSession(settings);
    }

    TAsyncStatus CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset, const TCommitOffsetSettings& settings) final {
        return Client.CommitOffset(path, partitionId, consumerName, offset, settings);
    }

private:
    TTopicClient Client;
};

} // anonymous namespace

ITopicClient::TPtr CreateExternalTopicClient(const TDriver& driver, const TTopicClientSettings& settings){
    return MakeIntrusive<TNativeTopicClient>(driver, settings);
}

} // namespace NYql
