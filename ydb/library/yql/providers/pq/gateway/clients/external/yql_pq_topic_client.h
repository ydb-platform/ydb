#pragma once

#include <ydb/library/yql/providers/abstract/message_stream/message_stream_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <memory>

namespace NYql {

std::shared_ptr<NFq::IMessageStreamClient> CreateExternalTopicClient(const TString& stream, const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings);

} // namespace NYql
