#include "client.h"
#include "agent.h"

namespace NKikimr::NColumnShard {

TValueAggregationClient::TValueAggregationClient(std::shared_ptr<TValueAggregationAgent> owner, std::list<TValueAggregationClient*>::iterator it)
    : Owner(owner)
    , PositionIterator(it)
{

}

TValueAggregationClient::~TValueAggregationClient() {
    Owner->UnregisterClient(PositionIterator);
}

}
