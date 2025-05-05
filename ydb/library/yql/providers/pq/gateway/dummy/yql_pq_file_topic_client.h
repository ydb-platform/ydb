#include "yql_pq_dummy_gateway.h"

#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

namespace NYql {
struct TFileTopicClient : public ITopicClient {
    explicit TFileTopicClient(THashMap<TDummyPqGateway::TClusterNPath, TDummyTopic> topics);

    NYdb::TAsyncStatus CreateTopic(const TString& path, const NYdb::NTopic::TCreateTopicSettings& settings = {}) override;

    NYdb::TAsyncStatus AlterTopic(const TString& path, const NYdb::NTopic::TAlterTopicSettings& settings = {}) override;

    NYdb::TAsyncStatus DropTopic(const TString& path, const NYdb::NTopic::TDropTopicSettings& settings = {}) override;

    NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic(const TString& path, 
        const NYdb::NTopic::TDescribeTopicSettings& settings = {}) override;

    NYdb::NTopic::TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString& consumer, 
        const NYdb::NTopic::TDescribeConsumerSettings& settings = {}) override;

    NYdb::NTopic::TAsyncDescribePartitionResult DescribePartition(const TString& path, i64 partitionId, 
        const NYdb::NTopic::TDescribePartitionSettings& settings = {}) override;

    std::shared_ptr<NYdb::NTopic::IReadSession> CreateReadSession(const NYdb::NTopic::TReadSessionSettings& settings) override;

    std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(
        const NYdb::NTopic::TWriteSessionSettings& settings) override;
    std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NTopic::TWriteSessionSettings& settings) override;

    NYdb::TAsyncStatus CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset,
        const NYdb::NTopic::TCommitOffsetSettings& settings = {}) override;

private:
    THashMap<TDummyPqGateway::TClusterNPath, TDummyTopic> Topics_;
    bool CancelOnFileFinish_ = false;
};

}
