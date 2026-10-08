#pragma once

#include "yql_pq_file_topic_defs.h"

#include <ydb/library/yql/providers/abstract/message_stream/message_stream_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h>

#include <memory>

namespace NYql {

std::shared_ptr<NFq::IMessageStreamClient> CreateFileTopicClient(const TString& stream, const THashMap<TClusterNPath, TDummyTopic>& topics, const TFileTopicClientSettings& settings);

std::shared_ptr<NYdb::NTopic::IWriteSession> CreateFileTopicWriteSession(
    const THashMap<TClusterNPath, TDummyTopic>& topics,
    const TFileTopicClientSettings& settings,
    const NYdb::NTopic::TWriteSessionSettings& writeSettings);

} // namespace NYql
