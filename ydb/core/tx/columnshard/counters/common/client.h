#pragma once
#include <ydb/library/accessor/accessor.h>
#include <util/system/types.h>
#include <util/generic/noncopyable.h>
#include <list>
#include <memory>

namespace NKikimr::NColumnShard {
class TValueAggregationAgent;

class TValueAggregationClient: TNonCopyable {
private:
    std::shared_ptr<TValueAggregationAgent> Owner;
    std::list<TValueAggregationClient*>::iterator PositionIterator;
    YDB_ACCESSOR(i64, Value, 0);
public:
    TValueAggregationClient(std::shared_ptr<TValueAggregationAgent> owner, std::list<TValueAggregationClient*>::iterator it);
    ~TValueAggregationClient();
};

}
