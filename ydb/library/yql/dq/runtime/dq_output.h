#pragma once

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>

#include "dq_async_stats.h"

namespace NYql {
namespace NDqProto {

class TCheckpoint;
class TTaskInput;
} // namespace NDqProto

namespace NUdf {
class TUnboxedValue;
} // namespace NUdf

namespace NDq {

struct TDqOutputStats : public TDqAsyncStats {
    // profile stats
    ui64 MaxMemoryUsage = 0;
    ui64 MaxRowsInMemory = 0;
};

enum EDqFillLevel {
    NoLimit,
    SoftLimit,
    HardLimit
};

const constexpr ui32 FILL_COUNTERS_SIZE = 4u;

struct TDqFillAggregator {

    alignas(64) std::array<std::atomic<ui64>, FILL_COUNTERS_SIZE> Counts;

    ui64 GetCount(EDqFillLevel level) {
        ui32 index = static_cast<ui32>(level);
        YQL_ENSURE(index < FILL_COUNTERS_SIZE);
        return Counts[index].load();
    }

    void AddCount(EDqFillLevel level) {
        ui32 index = static_cast<ui32>(level);
        YQL_ENSURE(index < FILL_COUNTERS_SIZE);
        Counts[index]++;
    }

    void UpdateCount(EDqFillLevel prevLevel, EDqFillLevel level) {
        ui32 index1 = static_cast<ui32>(prevLevel);
        ui32 index2 = static_cast<ui32>(level);
        YQL_ENSURE(index1 < FILL_COUNTERS_SIZE && index2 < FILL_COUNTERS_SIZE);
        if (index1 != index2) {
            Counts[index2]++;
            Counts[index1]--;
        }
    }

    EDqFillLevel GetFillLevel() const {
        if (Counts[static_cast<ui32>(HardLimit)].load()) {
            return HardLimit;
        }
        if (Counts[static_cast<ui32>(NoLimit)].load()) {
            return NoLimit;
        }
        return Counts[static_cast<ui32>(SoftLimit)].load() ? SoftLimit : NoLimit;
    }

    TString DebugString() {
        return TStringBuilder() << "TDqFillAggregator { N=" << Counts[static_cast<ui32>(NoLimit)].load()
            << " S=" << Counts[static_cast<ui32>(SoftLimit)].load()
            << " H=" << Counts[static_cast<ui32>(HardLimit)].load()
            << " }";
    }
};

class IDqOutput : public TSimpleRefCount<IDqOutput> {
public:
    using TPtr = TIntrusivePtr<IDqOutput>;

    virtual ~IDqOutput() = default;

    virtual const TDqOutputStats& GetPushStats() const = 0;

    // <| producer methods
    virtual EDqFillLevel GetFillLevel() const = 0;
    virtual EDqFillLevel UpdateFillLevel() = 0;
    virtual void SetFillAggregator(std::shared_ptr<TDqFillAggregator> aggregator) = 0;
    // can throw TDqChannelStorageException
    virtual void Push(NUdf::TUnboxedValue&& value) = 0;
    virtual void WidePush(NUdf::TUnboxedValue* values, ui32 count) = 0;
    virtual void Push(NDqProto::TWatermark&& watermark) = 0;
    // Push checkpoint. Checkpoints may be pushed to channel even after it is finished.
    virtual void Push(NDqProto::TCheckpoint&& checkpoint) = 0;
    virtual void Finish() = 0;

    // <| consumer methods
    [[nodiscard]]
    virtual bool HasData() const = 0;
    virtual bool IsFinished() const = 0;

    virtual NKikimr::NMiniKQL::TType* GetOutputType() const = 0;
};

} // namespace NDq
} // namespace NYql
