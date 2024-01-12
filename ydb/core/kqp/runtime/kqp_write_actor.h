#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimrDataEvents {
class TEvWrite;
class TEvWriteResult;
}

namespace NKikimr {
namespace NKqp {

/*struct TWriteBackoffSettings : TAtomicRefCount<TWriteBackoffSettings> {
    TDuration StartRetryDelay = TDuration::MilliSeconds(5);
    size_t MaxShardAttempts = 10;
    size_t MaxShardResolves = 3;
    double UnsertaintyRatio = 0.5;
    double Multiplier = 2.0;
    TDuration MaxRetryDelay = TDuration::Seconds(1);

    TMaybe<size_t> MaxTotalRetries;
    TMaybe<TDuration> ReadResponseTimeout;

    TDuration CalcShardDelay(size_t attempt, bool allowInstantRetry) {
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
};*/

void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory&, TIntrusivePtr<TKqpCounters>);

}
}