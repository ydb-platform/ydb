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
    YDB_ACCESSOR(i64, Value, 0);
public:
    void Add(const i64 v) {
        Value += v;
    }
    void Remove(const i64 v) {
        Value -= v;
    }
};

}
