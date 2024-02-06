#pragma once

#include <ydb/core/protos/counters_datashard.pb.h>
#include <util/datetime/base.h>
#include <util/system/hp_timer.h>

#include <functional>

namespace NKikimr {
namespace NDataShard {

    class TMicrosecTimerCounter {
    public:
        template<class TDataShardClass>
        TMicrosecTimerCounter(TDataShardClass& dataShard, NDataShard::ECumulativeCounters counter) noexcept
            : Callback(
                [&dataShard, counter](ui64 us) {
                    dataShard.IncCounter(counter, us);
                })
        { }

        ~TMicrosecTimerCounter() noexcept {
            Callback(ui64(1000000.0 * Timer.Passed()));
        }

        TMicrosecTimerCounter(const TMicrosecTimerCounter&) = delete;
        TMicrosecTimerCounter& operator=(const TMicrosecTimerCounter&) = delete;

    private:
        const std::function<void(ui64)> Callback;
        THPTimer Timer;
    };

} // namespace NDataShard
} // namespace NKikimr
