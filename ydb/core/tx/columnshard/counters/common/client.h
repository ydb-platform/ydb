#pragma once
#include <ydb/library/accessor/accessor.h>
#include <util/system/types.h>
#include <util/generic/noncopyable.h>
#include <list>
#include <memory>
#include <atomic>

namespace NKikimr::NColumnShard {
class TValueAggregationAgent;

class TValueAggregationClient: TNonCopyable {
private:
    std::atomic<i64> Value;
public:
    TValueAggregationClient() {
        Value = 0;
    }

    i64 GetValue() const {
        return Value;
    }

    void SetValue(const i64 value) {
        Value = value;
    }

    void Add(const i64 v) {
        Value += v;
    }
    void Remove(const i64 v) {
        Value -= v;
    }
};

}
