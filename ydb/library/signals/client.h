#pragma once
#include <ydb/library/accessor/accessor.h>
#include <util/datetime/base.h>
#include <util/system/types.h>
#include <util/generic/noncopyable.h>
#include <list>
#include <memory>
#include <atomic>
#include <optional>

namespace NKikimr::NColumnShard {
class TValueAggregationAgent;

class TValueAggregationClient: TNonCopyable {
private:
    std::atomic<i64> Value;
    std::atomic<bool> Written = false;
    std::optional<TInstant> DeadlineActuality;
public:
    TValueAggregationClient() {
        Value = 0;
    }

    bool IsWritten() const {
        return Written.load(std::memory_order_relaxed);
    }

    std::optional<i64> GetValue(const TInstant reqInstant) const {
        if (!DeadlineActuality || *DeadlineActuality > reqInstant) {
            return Value;
        } else {
            return {};
        }
    }

    i64 GetValueSimple() const {
        return Value;
    }

    void SetValue(const i64 value, const std::optional<TInstant> d = {}) {
        Value = value;
        Written.store(true, std::memory_order_relaxed);
        if (d) {
            DeadlineActuality = d;
        }
    }

    void Add(const i64 v) {
        Value += v;
        Written.store(true, std::memory_order_relaxed);
    }
    void Remove(const i64 v) {
        Value -= v;
        Written.store(true, std::memory_order_relaxed);
    }
};

}
