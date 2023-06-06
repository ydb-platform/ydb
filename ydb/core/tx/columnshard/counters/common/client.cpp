#include "client.h"
#include "agent.h"

namespace NKikimr::NColumnShard {

TValueAggregationClient::TValueAggregationClient(std::shared_ptr<TValueAggregationAgent> owner)
    : Owner(owner)
    , ValuePtr(Owner->RegisterValue(0))
{

}

void TValueAggregationClient::Set(const i64 value) const {
    *ValuePtr = value;
}

}
