#include <ydb/core/mind/local.h>
#include "tablet_metrics.h"

namespace NKikimr {
namespace NMetrics {

void TDecayingAverageWithSum::Increment(std::make_signed_t<ui64> value, TInstant now)
{
    RawValue += value;
    return TDecayingAverageValue::Increment(value, now);
}

ui64 TDecayingAverageWithSum::GetRawValue() const
{
    return RawValue;
}

void TResourceMetricsValues::Fill(NKikimrTabletBase::TMetrics& metrics) const {
    if (CPU.IsValueReady()) {
        metrics.SetCPU(CPU.GetValue());
    }
    if (Memory.IsValueReady()) {
        metrics.SetMemory(Memory.GetValue());
    }
    if (Network.IsValueReady()) {
        metrics.SetNetwork(Network.GetValue());
    }
    if (StorageSystem.IsValueReady() || StorageUser.IsValueReady()) {
        metrics.SetStorage(StorageSystem.GetValue() + StorageUser.GetValue());
    }
    {
        metrics.ClearGroupReadThroughput();
        for (auto it = ReadThroughput.begin(); it != ReadThroughput.end(); ++it) {
            auto groupId(it->first);
            const auto& value(it->second);
            if (value.IsValueReady()) {
                auto val = value.GetValue();
                auto& throughput= *metrics.AddGroupReadThroughput();
                throughput.SetChannel(groupId.first);
                throughput.SetGroupID(groupId.second);
                throughput.SetThroughput(val);
            }
        }
    }
    {
        metrics.ClearGroupWriteThroughput();
        for (auto it = WriteThroughput.begin(); it != WriteThroughput.end(); ++it) {
            auto groupId(it->first);
            const auto& value(it->second);
            if (value.IsValueReady()) {
                auto val = value.GetValue();
                auto& throughput= *metrics.AddGroupWriteThroughput();
                throughput.SetChannel(groupId.first);
                throughput.SetGroupID(groupId.second);
                throughput.SetThroughput(val);
            }
        }
    }
    {
        metrics.ClearGroupReadIops();
        for (const auto& kv : ReadIops) {
            auto groupId = kv.first;
            if (kv.second.IsValueReady()) {
                auto value = kv.second.GetValue();
                auto& iops = *metrics.AddGroupReadIops();
                iops.SetChannel(groupId.first);
                iops.SetGroupID(groupId.second);
                iops.SetIops(value);
            }
        }
    }
    {
        metrics.ClearGroupWriteIops();
        for (const auto& kv : WriteIops) {
            auto groupId = kv.first;
            if (kv.second.IsValueReady()) {
                auto value = kv.second.GetValue();
                auto& iops = *metrics.AddGroupWriteIops();
                iops.SetChannel(groupId.first);
                iops.SetGroupID(groupId.second);
                iops.SetIops(value);
            }
        }
    }
}

TResourceMetricsSendState::TResourceMetricsSendState(ui64 tabletId, ui32 followerId, const TActorId& launcher)
    : TabletId(tabletId)
    , FollowerId(followerId)
    , Launcher(launcher)
{}

namespace {
    template<class TGroupValues, class TGroupLevels, class TCallback>
    bool ProcessChangedGroupMetrics(
            TGroupValues& values,
            TGroupLevels& levels,
            ui64 significantChange,
            TInstant now,
            bool force,
            const TCallback& callback)
    {
        bool haveChanges = false;
        for (auto it = values.begin(); it != values.end(); ++it) {
            auto groupId = it->first;
            const auto& value = it->second;
            if (value.IsValueReady()) {
                auto val = !value.IsValueObsolete(now) ? value.GetValue() : 0;
                ui32 levelVal = val / significantChange;
                auto [lit, inserted] = levels.insert({groupId, 0});
                if (inserted || lit->second != levelVal || force) {
                    lit->second = levelVal;
                    haveChanges = true;
                    // N.B. keep going so all levels are properly updated
                }
            }
        }
        if (!haveChanges) {
            return false;
        }
        for (auto it = values.begin(); it != values.end();) {
            auto groupId(it->first);
            const auto& value(it->second);
            if (value.IsValueReady()) {
                auto val = !value.IsValueObsolete(now) ? value.GetValue() : 0;
                callback(groupId.first, groupId.second, val);
                if (val == 0) {
                    it = values.erase(it);
                    continue;
                }
            }
            ++it;
        }
        return true;
    }
}

bool TResourceMetricsSendState::FillChanged(TResourceMetricsValues& src, NKikimrTabletBase::TMetrics& metrics, TInstant now, bool force) {
    bool have = false;

    if (src.CPU.IsValueReady()) {
        auto cpu = !src.CPU.IsValueObsolete(now) ? src.CPU.GetValue() : 0;
        ui32 levelCPU = cpu / SignificantChangeCPU;
        if (levelCPU != LevelCPU || force) {
            metrics.SetCPU(cpu);
            LevelCPU = levelCPU;
            have = true;
        }
    } else if (force && src.CPU.IsValueObsolete(now)) {
        src.CPU.Set(0, now);
        metrics.SetCPU(0);
        have = true;
    }

    if (src.Memory.IsValueReady()) {
        auto memory = !src.Memory.IsValueObsolete(now) ? src.Memory.GetValue() : 0;
        ui32 levelMemory = memory / SignificantChangeMemory;
        if (levelMemory != LevelMemory || force) {
            metrics.SetMemory(memory);
            LevelMemory = levelMemory;
            have = true;
        }
    } else if (force && src.Memory.IsValueObsolete(now)) {
        src.Memory.Set(0);
        metrics.SetMemory(0);
        have = true;
    }

    if (src.Network.IsValueReady()) {
        auto network = !src.Network.IsValueObsolete(now) ? src.Network.GetValue() : 0;
        ui32 levelNetwork = network / SignificantChangeNetwork;
        if (levelNetwork != LevelNetwork || force) {
            metrics.SetNetwork(network);
            LevelNetwork = levelNetwork;
            have = true;
        }
    } else if (force && src.Network.IsValueObsolete(now)) {
        src.Network.Set(0, now);
        metrics.SetNetwork(0);
        have = true;
    }

    if (src.StorageSystem.IsValueReady() || src.StorageUser.IsValueReady()) {
        auto storageSystem = (src.StorageSystem.IsValueReady() && !src.StorageSystem.IsValueObsolete(now)) ? src.StorageSystem.GetValue() : 0;
        auto storageUser = (src.StorageUser.IsValueReady() && !src.StorageUser.IsValueObsolete(now)) ? src.StorageUser.GetValue() : 0;
        auto storage = storageSystem + storageUser;
        ui32 levelStorage = storage / SignificantChangeStorage;
        if (levelStorage != LevelStorage || force) {
            metrics.SetStorage(storage);
            LevelStorage = levelStorage;
            have = true;
        }
    } else if (force) {
        if (src.StorageSystem.IsValueObsolete(now)) {
            src.StorageSystem.Set(0);
        }
        if (src.StorageUser.IsValueObsolete(now)) {
            src.StorageUser.Set(0);
        }
        auto storageSystem = (src.StorageSystem.IsValueReady() && !src.StorageSystem.IsValueObsolete(now)) ? src.StorageSystem.GetValue() : 0;
        auto storageUser = (src.StorageUser.IsValueReady() && !src.StorageUser.IsValueObsolete(now)) ? src.StorageUser.GetValue() : 0;
        auto storage = storageSystem + storageUser;
        ui32 levelStorage = storage / SignificantChangeStorage;
        metrics.SetStorage(storage);
        LevelStorage = levelStorage;
        have = true;
    }

    have |= ProcessChangedGroupMetrics(
        src.ReadThroughput,
        LevelReadThroughput,
        SignificantChangeThroughput,
        now,
        force,
        [&metrics](TChannel channel, TGroupId groupId, ui64 value) {
            auto& throughput= *metrics.AddGroupReadThroughput();
            throughput.SetChannel(channel);
            throughput.SetGroupID(groupId);
            throughput.SetThroughput(value);
        });

    have |= ProcessChangedGroupMetrics(
        src.WriteThroughput,
        LevelWriteThroughput,
        SignificantChangeThroughput,
        now,
        force,
        [&metrics](TChannel channel, TGroupId groupId, ui64 value) {
            auto& throughput= *metrics.AddGroupWriteThroughput();
            throughput.SetChannel(channel);
            throughput.SetGroupID(groupId);
            throughput.SetThroughput(value);
        });

    have |= ProcessChangedGroupMetrics(
        src.ReadIops,
        LevelReadIops,
        SignificantChangeIops,
        now,
        force,
        [&metrics](TChannel channel, TGroupId groupId, ui64 value) {
            auto& iops = *metrics.AddGroupReadIops();
            iops.SetChannel(channel);
            iops.SetGroupID(groupId);
            iops.SetIops(value);
        });

    have |= ProcessChangedGroupMetrics(
        src.WriteIops,
        LevelWriteIops,
        SignificantChangeIops,
        now,
        force,
        [&metrics](TChannel channel, TGroupId groupId, ui64 value) {
            auto& iops = *metrics.AddGroupWriteIops();
            iops.SetChannel(channel);
            iops.SetGroupID(groupId);
            iops.SetIops(value);
        });

    return have;
}

bool TResourceMetricsSendState::TryUpdate(TResourceMetricsValues& src, const TActorContext& ctx) {
    TInstant now = ctx.Now();
    TDuration past = now - LastUpdate;
    if (past < TDuration::Seconds(1)) {
        return false; // too soon
    }
    NKikimrTabletBase::TMetrics values;
    bool updated = FillChanged(src, values, now, past > TDuration::Seconds(60));
    if (updated) {
        ctx.Send(Launcher, new TEvLocal::TEvTabletMetrics(TabletId, FollowerId, values));
        LastUpdate = now;
    }
    return updated;
}

template <typename ValueType>
static TString FormatValue(ValueType val);

template <>
TString FormatValue(ui64 val) {
    TString result = Sprintf("%lu", val);
    auto cnt = 0;
    for (auto i = result.size() - 1; i != 0; --i) {
        if (++cnt == 3) {
            cnt = 0;
            result.insert(i, " ");
        }
    }
    return result;
}

template <typename MetricType>
static void RenderMetric(IOutputStream& str, TStringBuf name, const MetricType& metric) {
    str << "<tr><td>" << name << "</td><td>" << FormatValue(metric.GetValue()) << "</td><td>";
    if (!metric.IsValueReady()) {
        str << "(not ready)";
    }
    if (metric.IsValueObsolete()) {
        str << "(obsolete)";
    }
    str << "</td></tr>";
}

IOutputStream& operator <<(IOutputStream& str, const TRendererHtml<TResourceMetrics>& met) {
    str << "<h3>Executor Metrics</h3>";
    str << "<table class='metrics'>";
    RenderMetric(str, "CPU", met.Metrics.CPU);
    RenderMetric(str, "Memory", met.Metrics.Memory);
    RenderMetric(str, "Network", met.Metrics.Network);
    RenderMetric(str, "System Storage", met.Metrics.StorageSystem);
    RenderMetric(str, "User Storage", met.Metrics.StorageUser);
    str << "</table>";
    return str;
}

}
}
