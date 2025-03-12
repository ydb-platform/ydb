#pragma once
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

namespace NYql {
class ITopicClient : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ITopicClient>;

    virtual NYdb::TAsyncStatus CreateTopic(const TString& path, const NYdb::NTopic::TCreateTopicSettings& settings = {}) = 0;

    virtual NYdb::TAsyncStatus AlterTopic(const TString& path, const NYdb::NTopic::TAlterTopicSettings& settings = {}) = 0;

    virtual NYdb::TAsyncStatus DropTopic(const TString& path, const NYdb::NTopic::TDropTopicSettings& settings = {}) = 0;

    virtual NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic(const TString& path, 
        const NYdb::NTopic::TDescribeTopicSettings& settings = {}) = 0;

    virtual NYdb::NTopic::TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString& consumer, 
        const NYdb::NTopic::TDescribeConsumerSettings& settings = {}) = 0;

    virtual NYdb::NTopic::TAsyncDescribePartitionResult DescribePartition(const TString& path, i64 partitionId, 
        const NYdb::NTopic::TDescribePartitionSettings& settings = {}) = 0;

    virtual std::shared_ptr<NYdb::NTopic::IReadSession> CreateReadSession(const NYdb::NTopic::TReadSessionSettings& settings) = 0;

    virtual std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(
        const NYdb::NTopic::TWriteSessionSettings& settings) = 0;
    virtual std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NTopic::TWriteSessionSettings& settings) = 0;

    virtual NYdb::TAsyncStatus CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset,
        const NYdb::NTopic::TCommitOffsetSettings& settings = {}) = 0;
};

class TNativeTopicClient : public ITopicClient {
public:
    TNativeTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings = {}):
        Driver_(driver), Client_(Driver_, settings) {}

    NYdb::TAsyncStatus CreateTopic(const TString& path, const NYdb::NTopic::TCreateTopicSettings& settings = {}) override {
        return Client_.CreateTopic(path, settings);
    }

    NYdb::TAsyncStatus AlterTopic(const TString& path, const NYdb::NTopic::TAlterTopicSettings& settings = {}) override {
        return Client_.AlterTopic(path, settings);
    }

    NYdb::TAsyncStatus DropTopic(const TString& path, const NYdb::NTopic::TDropTopicSettings& settings = {}) override {
        return Client_.DropTopic(path, settings);
    }

    NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic(const TString& path, 
        const NYdb::NTopic::TDescribeTopicSettings& settings = {}) override {
        return Client_.DescribeTopic(path, settings);
    }

    NYdb::NTopic::TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString& consumer, 
        const NYdb::NTopic::TDescribeConsumerSettings& settings = {}) override {
        return Client_.DescribeConsumer(path, consumer, settings);
    }

    NYdb::NTopic::TAsyncDescribePartitionResult DescribePartition(const TString& path, i64 partitionId, 
        const NYdb::NTopic::TDescribePartitionSettings& settings = {}) override {
        return Client_.DescribePartition(path, partitionId, settings);
    }

    std::shared_ptr<NYdb::NTopic::IReadSession> CreateReadSession(const NYdb::NTopic::TReadSessionSettings& settings) override {
        return Client_.CreateReadSession(settings);
    }

    std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(
        const NYdb::NTopic::TWriteSessionSettings& settings) override {
        return Client_.CreateSimpleBlockingWriteSession(settings);
    }

    std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NTopic::TWriteSessionSettings& settings) override {
        return Client_.CreateWriteSession(settings);
    }

    NYdb::TAsyncStatus CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset,
        const NYdb::NTopic::TCommitOffsetSettings& settings = {}) override {
        return Client_.CommitOffset(path, partitionId, consumerName, offset, settings);
    }

    ~TNativeTopicClient() {}
private:
    NYdb::TDriver Driver_;
    NYdb::NTopic::TTopicClient Client_;
};
}