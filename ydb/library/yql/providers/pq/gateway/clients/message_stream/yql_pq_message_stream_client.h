#pragma once

#include <ydb/library/yql/providers/abstract/message_stream/message_stream_client.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <memory>
#include <optional>

namespace NYql {

std::shared_ptr<NFq::IMessageStreamClient> CreateMessageStreamClient(const TString& stream, NYdb::NTopic::TTopicClient client);

std::shared_ptr<NFq::IMessageStreamReadSession> WrapYdbReadSession(std::shared_ptr<NYdb::NTopic::IReadSession> session);

NYdb::NTopic::TReadSessionSettings ToSdkReadSettings(const TString& stream, const NFq::TMessageStreamReadSessionSettings& settings);

NFq::TMessageStreamResult<NFq::TMessageStreamDescription> ToMessageStream(const NYdb::NTopic::TDescribeTopicResult& result);
NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription> ToMessageStream(const NYdb::NTopic::TDescribeConsumerResult& result);
NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription> ToMessageStream(const NYdb::NTopic::TDescribePartitionResult& result);
NFq::TMessageStreamResult<NFq::TMessageStreamConsumerPosition> ToMessageStreamConsumerPosition(const NYdb::TStatus& status, NFq::TMessageStreamPartitionId partitionId, ui64 offset);

// Unknown has no server status code; callers choose their fallback.
std::optional<Ydb::StatusIds::StatusCode> ToYdbStatus(NFq::EMessageStreamStatus status);

NYdb::TStatus ToSdkStatus(NFq::EMessageStreamStatus status, const NYql::TIssues& issues);

} // namespace NYql
