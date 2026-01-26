#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_session.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <util/generic/ptr.h>

namespace NYql {

class ITopicClient : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ITopicClient>;

    virtual NYdb::TAsyncStatus CreateTopic(const TString& path, const NYdb::NTopic::TCreateTopicSettings& settings = {}) = 0;

    virtual NYdb::TAsyncStatus AlterTopic(const TString& path, const NYdb::NTopic::TAlterTopicSettings& settings = {}) = 0;

    virtual NYdb::TAsyncStatus DropTopic(const TString& path, const NYdb::NTopic::TDropTopicSettings& settings = {}) = 0;

    virtual NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic(const TString& path, const NYdb::NTopic::TDescribeTopicSettings& settings = {}) = 0;

    virtual NYdb::NTopic::TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString& consumer, const NYdb::NTopic::TDescribeConsumerSettings& settings = {}) = 0;

    virtual NYdb::NTopic::TAsyncDescribePartitionResult DescribePartition(const TString& path, i64 partitionId, const NYdb::NTopic::TDescribePartitionSettings& settings = {}) = 0;

    virtual std::shared_ptr<NYdb::NTopic::IReadSession> CreateReadSession(const NYdb::NTopic::TReadSessionSettings& settings) = 0;

    virtual std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const NYdb::NTopic::TWriteSessionSettings& settings) = 0;

    virtual std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NTopic::TWriteSessionSettings& settings) = 0;

    virtual NYdb::TAsyncStatus CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset, const NYdb::NTopic::TCommitOffsetSettings& settings = {}) = 0;
};

} // namespace NYql 
