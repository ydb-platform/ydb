#pragma once
#include <unordered_map>
#include <ydb/core/base/defs.h>
#include <ydb/core/util/tuples.h>
#include <ydb/core/util/metrics.h>
#include <ydb/core/protos/tablet.pb.h>

namespace NKikimr {
namespace NMetrics {

enum EResource { // DO NOT REORDER, DO NOT REMOVE ITEMS
    CPU, // the same as COUNTER_TT_EXECUTE_CPUTIME + COUNTER_TT_BOOKKEEPING_CPUTIME
    Memory, // the same as COUNTER_RECORD_BYTES for KV tablet
    Network, // Cumulative network bandwidth, used by a tablet
    Counter, // ALWAYS 1
};

class TDecayingAverageWithSum
    : public TDecayingAverageValue<ui64, DurationPerMinute, DurationPerSecond>
{
public:
    void Increment(std::make_signed_t<ui64> value, TInstant now = TInstant::Now());
    ui64 GetRawValue() const;

private:
    ui64 RawValue = 0;
};

using TChannel = ui8;
using TGroupId = ui32;
using TGroupThroughputValue = TDecayingAverageWithSum;
using TTabletThroughputValue = std::unordered_map<std::pair<TChannel, TGroupId>, TGroupThroughputValue>;
using TTabletThroughputRawValue = std::unordered_map<std::pair<TChannel, TGroupId>, ui64>; // TStackYHash ?
using TGroupIopsValue = TDecayingAverageWithSum;
using TTabletIopsValue = std::unordered_map<std::pair<TChannel, TGroupId>, TGroupIopsValue>;
using TTabletIopsRawValue = std::unordered_map<std::pair<TChannel, TGroupId>, ui64>;

constexpr TTimeBase<TInstant>::TValue DurationPer15Seconds = DurationPerSecond * 15;

class TResourceMetricsValues {
public:
    TDecayingAverageValue<ui64, DurationPer15Seconds, DurationPerSecond> CPU;
    TGaugeValue<ui64> Memory;
    TDecayingAverageValue<ui64, DurationPer15Seconds, DurationPerSecond> Network;
    TGaugeValue<ui64> StorageSystem;
    TGaugeValue<ui64> StorageUser;
    TTabletThroughputValue ReadThroughput;
    TTabletThroughputValue WriteThroughput;
    TTabletIopsValue ReadIops;
    TTabletIopsValue WriteIops;

    void Fill(NKikimrTabletBase::TMetrics& metrics) const;
};

class TResourceMetricsSendState {
public:
    TResourceMetricsSendState(ui64 tabletId, ui32 followerId, const TActorId& launcher);
    bool FillChanged(TResourceMetricsValues& src, NKikimrTabletBase::TMetrics& metrics, TInstant now = TInstant::Now(), bool forceAll = false);
    bool TryUpdate(TResourceMetricsValues& src, const TActorContext& ctx);

protected:
    static constexpr ui64 SignificantChangeCPU = 100000/* 100msec */;
    static constexpr ui64 SignificantChangeMemory = 1 << 20/* 1Mb */;
    static constexpr ui64 SignificantChangeNetwork = 1 << 20/* 1Mb */;
    static constexpr ui64 SignificantChangeThroughput = 1 << 20/* 1Mb */;
    static constexpr ui64 SignificantChangeStorage = 100 << 20/* 100Mb */;
    static constexpr ui64 SignificantChangeIops = 10 /* 10 iops? */;

    const ui64 TabletId;
    const ui32 FollowerId;
    const TActorId Launcher;
    std::optional<ui32> LevelCPU;
    std::optional<ui32> LevelMemory;
    std::optional<ui32> LevelNetwork;
    std::optional<ui32> LevelStorage;
    std::optional<ui32> LevelIops;
    THashMap<std::pair<TChannel, TGroupId>, ui32> LevelReadThroughput;
    THashMap<std::pair<TChannel, TGroupId>, ui32> LevelWriteThroughput;
    THashMap<std::pair<TChannel, TGroupId>, ui32> LevelReadIops;
    THashMap<std::pair<TChannel, TGroupId>, ui32> LevelWriteIops;
    TInstant LastUpdate;
};

class TResourceMetrics : public TResourceMetricsValues, public TResourceMetricsSendState {
public:
    TResourceMetrics(ui64 tabletId, ui32 followerId, const TActorId& launcher)
        : TResourceMetricsSendState(tabletId, followerId, launcher) {}

    bool FillChanged(NKikimrTabletBase::TMetrics& metrics, TInstant now = TInstant::Now(), bool forceAll = false) {
        return TResourceMetricsSendState::FillChanged(*this, metrics, now, forceAll);
    }

    bool TryUpdate(const TActorContext& ctx) {
        return TResourceMetricsSendState::TryUpdate(*this, ctx);
    }
};

template <typename>
struct TRendererHtml {};

template <>
struct TRendererHtml<TResourceMetrics> {
    const TResourceMetrics& Metrics;
    TRendererHtml(const TResourceMetrics& metrics)
        : Metrics(metrics)
    {}
    friend IOutputStream& operator <<(IOutputStream& str, const TRendererHtml<TResourceMetrics>& metrics);
};

template <typename Type>
TRendererHtml<Type> AsHTML(const Type& object) {
    return TRendererHtml<Type>(object);
}

} // NMetrics
} // NKikimr
