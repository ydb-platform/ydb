#pragma once

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr {
namespace NKqp {

struct TIteratorReadBackoffSettings : TAtomicRefCount<TIteratorReadBackoffSettings> {
    TDuration CalcShardDelay(size_t attempt, bool allowInstantRetry);

    TDuration StartRetryDelay = TDuration::MilliSeconds(5);
    size_t MaxShardAttempts = 10;
    size_t MaxShardResolves = 3;
    double UnsertaintyRatio = 0.5;
    double Multiplier = 2.0;
    TDuration MaxRetryDelay = TDuration::Seconds(1);

    TMaybe<size_t> MaxTotalRetries;
    TMaybe<TDuration> ReadResponseTimeout;
};

struct TEvReadSettings : public TAtomicRefCount<TEvReadSettings> {
    TEvReadSettings() {
        Read.SetMaxRows(32767);
        Read.SetMaxBytes(5_MB);

        Ack.SetMaxRows(32767);
        Ack.SetMaxBytes(5_MB);
    }

    NKikimrTxDataShard::TEvRead Read;
    NKikimrTxDataShard::TEvReadAck Ack;
};

void SetReadIteratorBackoffSettings(TIntrusivePtr<TIteratorReadBackoffSettings>);
TDuration CalcDelay(size_t attempt, bool allowInstantRetry);
size_t MaxShardResolves();
size_t MaxShardRetries();
TMaybe<size_t> MaxTotalRetries();
TMaybe<TDuration> ShardTimeout();

void SetDefaultIteratorQuotaSettings(ui32 rows, ui32 bytes);
THolder<NKikimr::TEvDataShard::TEvRead> GetDefaultReadSettings();
void SetDefaultReadSettings(const NKikimrTxDataShard::TEvRead&);
THolder<NKikimr::TEvDataShard::TEvReadAck> GetDefaultReadAckSettings();
void SetDefaultReadAckSettings(const NKikimrTxDataShard::TEvReadAck&);

} // namespace NKqp
} // namespace NKikimr