#pragma once

#include <ydb/library/yql/providers/abstract/message_stream/message_stream_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <memory>

namespace NYql {

std::shared_ptr<NFq::IMessageStreamClient> CreateMessageStreamClient(NYdb::NTopic::TTopicClient client);

std::shared_ptr<NFq::IMessageStreamReadSession> WrapYdbReadSession(std::shared_ptr<NYdb::NTopic::IReadSession> session);

NYdb::NTopic::TReadSessionSettings ToSdkReadSettings(const NFq::TMessageStreamReadSettings& settings);

NFq::TMessageStreamResult<NFq::TMessageStreamTopicDescription> ToMessageStream(const NYdb::NTopic::TDescribeTopicResult& result);
NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription> ToMessageStream(const NYdb::NTopic::TDescribeConsumerResult& result);
NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription> ToMessageStream(const NYdb::NTopic::TDescribePartitionResult& result);
NFq::TMessageStreamResult<NFq::TMessageStreamOffset> ToMessageStreamOffset(const NYdb::TStatus& status, ui64 partitionId, ui64 offset);

NYdb::TStatus ToSdkStatus(NFq::EMessageStreamStatus status, const NYql::TIssues& issues);

} // namespace NYql
