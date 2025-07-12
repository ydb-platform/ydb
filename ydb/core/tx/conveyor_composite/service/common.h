#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/monotonic.h>
#include <ydb/library/signals/owner.h>

#include <util/datetime/base.h>

namespace NKikimr::NConveyorComposite {

class TTaskCPUUsage {
private:
    YDB_READONLY_DEF(TMonotonic, Start);
    YDB_READONLY_DEF(TMonotonic, Finish);
    YDB_READONLY_DEF(TDuration, Duration);

public:
    [[nodiscard]] bool Cut(const TMonotonic start) {
        if (Finish <= start) {
            return false;
        }
        if (Start <= start) {
            Start = start;
        }
        return true;
    }

    TTaskCPUUsage(const TMonotonic start, const TMonotonic finish)
        : Start(start)
        , Finish(finish)
        , Duration(finish - start) {
    }
};

template <class T>
class TAverageCalcer {
private:
    const ui32 Count = 100;
    std::deque<T> Values;
    T Sum = T();

public:
    TAverageCalcer(const ui32 count = 100)
        : Count(count) {
    }

    void Add(const T value) {
        Values.emplace_back(value);
        Sum += value;
        if (Values.size() > Count) {
            Sum -= Values.front();
            Values.pop_front();
        }
    }

    T GetValue() const {
        return Values.size() ? (Sum / Values.size()) : T();
    }
};

class TCPUUsage {
private:
    YDB_READONLY_DEF(TDuration, Duration);
    YDB_READONLY_DEF(TDuration, PredictedDuration);
    std::shared_ptr<TCPUUsage> Parent;
    TMonotonic StartInstant = TMonotonic::Zero();

public:
    TCPUUsage(const std::shared_ptr<TCPUUsage>& parent)
        : Parent(parent) {
    }

    void Clear() {
        Duration = TDuration::Zero();
        PredictedDuration = TDuration::Zero();
        StartInstant = TMonotonic::Zero();
    }

    TDuration CalcWeight(const double w) const {
        if (w <= 0) {
            return TDuration::Max();
        } else {
            return (Duration + PredictedDuration) * w;
        }
    }

    void AddPredicted(const TDuration d) {
        PredictedDuration += d;
        if (Parent) {
            Parent->AddPredicted(d);
        }
    }

    void AddUsage(const TTaskCPUUsage& usage) {
        Duration += usage.GetDuration();
        if (Parent) {
            Parent->AddUsage(usage);
        }
    }

    void Exchange(const TDuration predicted, const TMonotonic start, const TMonotonic finish);
};

class TCPUGroup {
    YDB_ACCESSOR_DEF(double, CPUThreadsLimit);
    YDB_ACCESSOR(double, Weight, 1);

public:
    using TPtr = std::shared_ptr<TCPUGroup>;

    TCPUGroup(const double cpuThreadsLimit, const double weight = 1)
        : CPUThreadsLimit(cpuThreadsLimit)
        , Weight(weight)
    {
    }

};

}   // namespace NKikimr::NConveyorComposite
