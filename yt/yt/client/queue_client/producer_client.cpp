#include "producer_client.h"

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NQueueClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const TTableSchemaPtr YTProducerTableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
    TColumnSchema("queue_cluster", EValueType::String, ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("queue_path", EValueType::String, ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("session_id", EValueType::String, ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("sequence_number", EValueType::Uint64, ESortOrder::Ascending).SetRequired(true),
    TColumnSchema("epoch", EValueType::Uint64).SetRequired(true),
    TColumnSchema("user_meta", EValueType::Any).SetRequired(false),
    TColumnSchema("system_meta", EValueType::Any).SetRequired(false),
}, /*strict*/ true, /*uniqueKeys*/ true);

////////////////////////////////////////////////////////////////////////////////

const NTableClient::TTableSchemaPtr& GetProducerSchema()
{
    return YTProducerTableSchema;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
