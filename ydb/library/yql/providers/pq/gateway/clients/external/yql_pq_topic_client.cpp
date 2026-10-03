#include "yql_pq_topic_client.h"

#include <ydb/library/yql/providers/pq/gateway/clients/message_stream/yql_pq_message_stream_client.h>

namespace NYql {

std::shared_ptr<NFq::IMessageStreamClient> CreateExternalTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings) {
    return CreateMessageStreamClient(NYdb::NTopic::TTopicClient(driver, settings));
}

} // namespace NYql
