#include "kqp_read_iterator_common.h"

#include <library/cpp/threading/hot_swap/hot_swap.h>

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

    delay *= (1 - UnsertaintyRatio * RandomNumber<double>());

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

} // namespace NKqp
} // namespace NKikimr
