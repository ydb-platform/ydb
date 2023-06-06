#pragma once
#include <memory>
#include <util/system/types.h>

namespace NKikimr::NColumnShard {
class TValueAggregationAgent;

class TValueAggregationClient {
private:
    std::shared_ptr<TValueAggregationAgent> Owner;
    i64* ValuePtr = nullptr;
public:
    TValueAggregationClient(std::shared_ptr<TValueAggregationAgent> owner);

    void Set(const i64 value) const;
};

}
