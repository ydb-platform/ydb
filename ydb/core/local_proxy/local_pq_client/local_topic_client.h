#pragma once

#include "local_topic_client_helpers.h"

#include <ydb/library/yql/providers/abstract/message_stream/message_stream_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/ptr.h>

#include <memory>

namespace NKikimr::NKqp {

// In-process topic client. Unit tests call the SDK methods directly.
// Query code receives CreateLocalMessageStreamClient().
class TLocalTopicClient : public TLocalTopicClientBase, public TThrRefBase {
public:
    using TLocalTopicClientBase::TLocalTopicClientBase;

    NYdb::NTopic::TAsyncDescribeTopicResult DescribeTopic(const TString& path, const NYdb::NTopic::TDescribeTopicSettings& settings = {});
    NYdb::NTopic::TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString& consumer, const NYdb::NTopic::TDescribeConsumerSettings& settings = {});
    NYdb::NTopic::TAsyncDescribePartitionResult DescribePartition(const TString& path, i64 partitionId, const NYdb::NTopic::TDescribePartitionSettings& settings = {});

    std::shared_ptr<NYdb::NTopic::IReadSession> CreateReadSession(const NYdb::NTopic::TReadSessionSettings& settings);
    std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const NYdb::NTopic::TWriteSessionSettings& settings);
    std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NTopic::TWriteSessionSettings& settings);
    NYdb::TAsyncStatus CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset, const NYdb::NTopic::TCommitOffsetSettings& settings = {});
};

TIntrusivePtr<TLocalTopicClient> CreateLocalTopicClient(const TLocalTopicClientSettings& localSettings, const NYdb::NTopic::TTopicClientSettings& clientSettings);

std::shared_ptr<NFq::IMessageStreamClient> CreateLocalMessageStreamClient(const TString& stream, const TLocalTopicClientSettings& localSettings, const NYdb::NTopic::TTopicClientSettings& clientSettings);

} // namespace NKikimr::NKqp
