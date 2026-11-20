#include "kqp_read_iterator_common.h"

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <ydb/core/protos/kqp_stats.pb.h>

namespace NKikimr {
namespace NKqp {

struct TBackoffStorage {
    THotSwap<NKikimr::NKqp::TIteratorReadBackoffSettings> SettingsPtr;

    TBackoffStorage() {
        SettingsPtr.AtomicStore(new NKikimr::NKqp::TIteratorReadBackoffSettings());
    }
};

struct TEvReadDefaultSettings {
    THotSwap<TEvReadSettings> Settings;

    TEvReadDefaultSettings() {
        Settings.AtomicStore(MakeIntrusive<TEvReadSettings>());
    }

};

void SetDefaultIteratorQuotaSettings(ui32 rows, ui32 bytes) {
    TEvReadSettings settings;

    settings.Read.SetMaxRows(rows);
    settings.Ack.SetMaxRows(rows);

    settings.Read.SetMaxBytes(bytes);
    settings.Ack.SetMaxBytes(bytes);

    SetDefaultReadSettings(settings.Read);
    SetDefaultReadAckSettings(settings.Ack);
}

THolder<NKikimr::TEvDataShard::TEvRead> GetDefaultReadSettings() {
    auto result = MakeHolder<NKikimr::TEvDataShard::TEvRead>();
    auto ptr = Singleton<TEvReadDefaultSettings>()->Settings.AtomicLoad();
    result->Record.MergeFrom(ptr->Read);
    return result;
}

void SetDefaultReadSettings(const NKikimrTxDataShard::TEvRead& read) {
    auto ptr = Singleton<TEvReadDefaultSettings>()->Settings.AtomicLoad();
    TEvReadSettings settings = *ptr;
    settings.Read.MergeFrom(read);
    Singleton<TEvReadDefaultSettings>()->Settings.AtomicStore(MakeIntrusive<TEvReadSettings>(settings));
}

THolder<NKikimr::TEvDataShard::TEvReadAck> GetDefaultReadAckSettings() {
    auto result = MakeHolder<NKikimr::TEvDataShard::TEvReadAck>();
    auto ptr = Singleton<TEvReadDefaultSettings>()->Settings.AtomicLoad();
    result->Record.MergeFrom(ptr->Ack);
    return result;
}

void SetDefaultReadAckSettings(const NKikimrTxDataShard::TEvReadAck& ack) {
    auto ptr = Singleton<TEvReadDefaultSettings>()->Settings.AtomicLoad();
    TEvReadSettings settings = *ptr;
    settings.Ack.MergeFrom(ack);
    Singleton<TEvReadDefaultSettings>()->Settings.AtomicStore(MakeIntrusive<TEvReadSettings>(settings));
}

TDuration TIteratorReadBackoffSettings::CalcShardDelay(size_t attempt, bool allowInstantRetry) {
    if (allowInstantRetry && attempt == 1) {
        return TDuration::Zero();
    }

    auto delay = StartRetryDelay;
    for (size_t i = 0; i < attempt; ++i) {
        delay *= Multiplier;
        delay = Min(delay, MaxRetryDelay);
    }

    delay *= (1 - UncertaintyRatio * RandomNumber<double>());

    return delay;
}

void SetReadIteratorBackoffSettings(TIntrusivePtr<TIteratorReadBackoffSettings> ptr) {
    Singleton<TBackoffStorage>()->SettingsPtr.AtomicStore(ptr);
}

TDuration CalcDelay(size_t attempt, bool allowInstantRetry) {
    return Singleton<TBackoffStorage>()->SettingsPtr.AtomicLoad()->CalcShardDelay(attempt, allowInstantRetry);
}

size_t MaxShardResolves() {
    return Singleton<TBackoffStorage>()->SettingsPtr.AtomicLoad()->MaxShardResolves;
}

size_t MaxShardRetries() {
    return Singleton<TBackoffStorage>()->SettingsPtr.AtomicLoad()->MaxShardAttempts;
}

TMaybe<size_t> MaxTotalRetries() {
    return Singleton<TBackoffStorage>()->SettingsPtr.AtomicLoad()->MaxTotalRetries;
}

TMaybe<TDuration> ShardTimeout() {
    return Singleton<TBackoffStorage>()->SettingsPtr.AtomicLoad()->ReadResponseTimeout;
}

size_t MaxRowsProcessingStreamLookup() {
    return Singleton<TBackoffStorage>()->SettingsPtr.AtomicLoad()->MaxRowsProcessingStreamLookup;
}

ui64 MaxTotalBytesQuotaStreamLookup() {
    return Singleton<TBackoffStorage>()->SettingsPtr.AtomicLoad()->MaxTotalBytesQuotaStreamLookup;
}

ui64 MaxInFlightReadsStreamLookup() {
    return Singleton<TBackoffStorage>()->SettingsPtr.AtomicLoad()->MaxInFlightReadsStreamLookup;
}

ui64 MaxBytesPerFetchStreamLookup() {
    return Singleton<TBackoffStorage>()->SettingsPtr.AtomicLoad()->MaxBytesPerFetchStreamLookup;
}

ui64 MaxInFlightLocksStreamLookup() {
    return Singleton<TBackoffStorage>()->SettingsPtr.AtomicLoad()->MaxInFlightLocksStreamLookup;
}

void TReadLockInfo::Add(const NKikimrTxDataShard::TEvReadResult& record) {
    for (auto& lock : record.GetTxLocks()) {
        Locks.push_back(lock);
    }

    for (auto& lock : record.GetBrokenTxLocks()) {
        BrokenLocks.push_back(lock);
    }

    // Collect deferred breaker info for TLI logging
    {
        const auto& traceIds = record.GetDeferredBreakerQuerySpanIds();
        const auto& nodeIds = record.GetDeferredBreakerNodeIds();
        for (int i = 0; i < traceIds.size(); ++i) {
            DeferredBreakers.push_back({traceIds[i], i < nodeIds.size() ? nodeIds[i] : 0u});
        }
    }

    if (record.HasDeferredVictimQuerySpanId() && DeferredVictimQuerySpanId == 0) {
        DeferredVictimQuerySpanId = record.GetDeferredVictimQuerySpanId();
    }
}

NKikimrTxDataShard::TEvKqpInputActorResultInfo TReadLockInfo::GetExtraData() {
    NKikimrTxDataShard::TEvKqpInputActorResultInfo resultInfo;
    for (auto& lock : Locks) {
        resultInfo.AddLocks()->CopyFrom(lock);
    }
    for (auto& lock : BrokenLocks) {
        resultInfo.AddLocks()->CopyFrom(lock);
    }
    // Add deferred breaker info for TLI logging
    for (const auto& breaker : DeferredBreakers) {
        resultInfo.AddDeferredBreakerQuerySpanIds(breaker.QuerySpanId);
        resultInfo.AddDeferredBreakerNodeIds(breaker.NodeId);
    }
    if (DeferredVictimQuerySpanId) {
        resultInfo.SetDeferredVictimQuerySpanId(DeferredVictimQuerySpanId);
    }
    return resultInfo;
}

void TReadLockInfo::FillExtraStats(NYql::NDqProto::TDqTaskStats* stats) {
    // Add lock stats for broken locks from read operations
    if (!BrokenLocks.empty()) {
        NKqpProto::TKqpTaskExtraStats extraStats;
        if (stats->HasExtra()) {
            stats->GetExtra().UnpackTo(&extraStats);
        }
        extraStats.MutableLockStats()->SetBrokenAsVictim(
            extraStats.GetLockStats().GetBrokenAsVictim() + BrokenLocks.size());
        stats->MutableExtra()->PackFrom(extraStats);
    }
}

} // namespace NKqp
} // namespace NKikimr
