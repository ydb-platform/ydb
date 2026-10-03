#pragma once

#include <ydb/core/protos/counters_detailed_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {

namespace NDetailedMetricsTests {

constexpr ui32 ROW_COUNT = NDataShard::COUNTER_DATASHARD_ROW_COUNT;
constexpr ui32 SIZE_BYTES = NDataShard::COUNTER_DATASHARD_SIZE_BYTES;
constexpr ui32 WRITE_ROWS = NDataShard::COUNTER_DATASHARD_WRITE_ROWS;
constexpr ui32 CONSUMED_CPU_MICROSECONDS = NDataShard::COUNTER_DATASHARD_CONSUMED_CPU_MICROSECONDS;
constexpr ui32 USED_CORE_PERCENTS = NDataShard::COUNTER_DATASHARD_USED_CORE_PERCENTS;

/**
 * Normalizes the given JSON to be well formatted with all keys sorted.
 *
 * @warning This function sorts only maps by the key value. It does not sort
 *          items in arrays at all. Luckily, all counters and groups in TDynamicCounters
 *          are stored in SORTED maps, which means that the array of sensors
 *          is always inherently sorted in a stable order. This makes it safe
 *          to compare sensor arrays directly without sorting them.
 *
 * @param[in] jsonString The JSON to normalize (as a string)
 *
 * @return The corresponding normalized JSON
 */
TString NormalizeJson(const TString& jsonString);

/**
 * The counter names of a TTestCounters bank, nullptr for an unnamed slot.
 */
struct TTestNames {
    TVector<const char*> Simple;
    TVector<const char*> Cumulative;
    TVector<const char*> Percentile;
};

/**
 * A synthetic bank of the Executor or the application counters.
 */
class TTestCounters {
public:
    explicit TTestCounters(TTestNames names = {})
        : Names(std::move(names))
        , Counters(MakeHolder<TTabletCountersBase>(
            Names.Simple.size(),
            Names.Cumulative.size(),
            Names.Percentile.size(),
            Names.Simple.data(),
            Names.Cumulative.data(),
            Names.Percentile.data()))
    {
    }

    template <ui32 RangeCount>
    TTestCounters& InitPercentile(
        ui32 slot,
        const TTabletPercentileCounter::TRangeDef (&ranges)[RangeCount],
        bool integral)
    {
        Counters->Percentile()[slot].Initialize(ranges, integral);
        return *this;
    }

    const TTabletCountersBase& Get() const {
        return *Counters;
    }

    TTabletCountersBase& Get() {
        return *Counters;
    }

private:
    // The counters point into the name vectors, which survive a move
    TTestNames Names;
    THolder<TTabletCountersBase> Counters;
};

} // namespace NDetailedMetricsTests

} // namespace NKikimr
