#include <ydb/core/util/backoff.h>
#include <ydb/core/util/counted_leaky_bucket.h>
#include <ydb/core/util/token_bucket.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/system/yassert.h>

#include <cmath>
#include <limits>

namespace {

using namespace NKikimr;

constexpr size_t MaxOperations = 512;
constexpr double Eps = 1e-7;

TString SmallName(FuzzedDataProvider& fdp, TStringBuf prefix) {
    return TString(prefix) + ToString(fdp.ConsumeIntegralInRange<ui32>(0, 7));
}

double SmallDouble(FuzzedDataProvider& fdp, ui32 min = 0, ui32 max = 10000) {
    return static_cast<double>(fdp.ConsumeIntegralInRange<ui32>(min, max)) / 4.0;
}

void CheckFiniteNonNegative(double value) {
    Y_ABORT_UNLESS(std::isfinite(value));
    Y_ABORT_UNLESS(value >= -Eps);
}

void RunRateQuotaCountersFuzz(FuzzedDataProvider& fdp) {
    TInstant wallNow = TInstant::MicroSeconds(1);
    TTokenBucket tokenBucket;
    bool tokenUnlimited = true;

    TCountedLeakyBucket leaky(
        SmallDouble(fdp, 1, 4096),
        TDuration::MicroSeconds(fdp.ConsumeIntegralInRange<ui64>(1, 10'000'000)),
        wallNow);

    const ui64 initialBackoff = fdp.ConsumeIntegralInRange<ui64>(1, 500);
    const ui64 maxBackoff = fdp.ConsumeIntegralInRange<ui64>(initialBackoff, 20'000);
    TBackoffTimer backoffTimer(initialBackoff, maxBackoff);
    TBackoff backoff(
        fdp.ConsumeIntegralInRange<size_t>(0, 32),
        TDuration::MilliSeconds(initialBackoff),
        TDuration::MilliSeconds(maxBackoff));

    auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TVector<NMonitoring::TDynamicCounters::TCounterPtr> counters;
    TVector<NMonitoring::TDynamicCounterPtr> groups{root};

    for (size_t step = 0; step < MaxOperations && fdp.remaining_bytes(); ++step) {
        wallNow += TDuration::MicroSeconds(fdp.ConsumeIntegralInRange<ui64>(0, 250'000));

        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 14)) {
            case 0: {
                const double rate = SmallDouble(fdp, 1, 40000);
                const double capacity = SmallDouble(fdp, 1, 40000);
                tokenBucket.SetCapacity(capacity);
                tokenBucket.SetRate(rate);
                tokenUnlimited = false;
                Y_ABORT_UNLESS(tokenBucket.GetRate() == rate);
                Y_ABORT_UNLESS(tokenBucket.GetCapacity() == capacity);
                break;
            }

            case 1:
                tokenBucket.SetUnlimited();
                tokenUnlimited = true;
                Y_ABORT_UNLESS(tokenBucket.IsUnlimited());
                break;

            case 2:
                tokenBucket.Fill(wallNow);
                break;

            case 3: {
                const double amount = SmallDouble(fdp, 0, 40000);
                const double taken = tokenBucket.FillAndTryTake(wallNow, amount);
                Y_ABORT_UNLESS(taken <= amount + Eps);
                if (!tokenUnlimited) {
                    CheckFiniteNonNegative(taken);
                }
                break;
            }

            case 4: {
                const double amount = SmallDouble(fdp, 0, 40000);
                tokenBucket.Take(amount);
                if (tokenUnlimited) {
                    Y_ABORT_UNLESS(tokenBucket.Available() == std::numeric_limits<double>::infinity());
                }
                break;
            }

            case 5:
                tokenBucket.FillAndNextAvailableDelay(wallNow);
                if (!tokenUnlimited && tokenBucket.Available() < 0) {
                    Y_ABORT_UNLESS(tokenBucket.GetRate() > 0);
                    Y_ABORT_UNLESS(tokenBucket.NextAvailableDelay() >= TDuration::Zero());
                }
                break;

            case 6:
                if (fdp.ConsumeBool()) {
                    leaky.Update(wallNow);
                } else {
                    wallNow -= TDuration::MicroSeconds(fdp.ConsumeIntegralInRange<ui64>(0, 1000));
                    leaky.Update(wallNow);
                }
                break;

            case 7: {
                const double value = SmallDouble(fdp, 0, 4096);
                const bool canPush = leaky.CanPush(value);
                const bool pushed = leaky.TryPush(wallNow, value);
                Y_ABORT_UNLESS(canPush == pushed);
                break;
            }

            case 8:
                leaky.Push(wallNow, SmallDouble(fdp, 0, 4096));
                break;

            case 9: {
                const ui64 delayMs = backoffTimer.NextBackoffMs();
                Y_ABORT_UNLESS(delayMs >= initialBackoff);
                Y_ABORT_UNLESS(delayMs <= maxBackoff);
                break;
            }

            case 10:
                if (backoff.HasMore()) {
                    const size_t before = backoff.GetIteration();
                    const TDuration delay = backoff.Next();
                    Y_ABORT_UNLESS(backoff.GetIteration() == before + 1);
                    Y_ABORT_UNLESS(delay >= TDuration::MilliSeconds(initialBackoff));
                    Y_ABORT_UNLESS(delay <= TDuration::MilliSeconds(maxBackoff + maxBackoff / 4 + 1));
                }
                break;

            case 11:
                backoffTimer.Reset();
                backoff.Reset();
                Y_ABORT_UNLESS(backoff.GetIteration() == 0);
                break;

            case 12: {
                auto group = groups[fdp.ConsumeIntegralInRange<size_t>(0, groups.size() - 1)]
                    ->GetSubgroup(SmallName(fdp, "label"), SmallName(fdp, "value"));
                groups.push_back(group);
                if (groups.size() > 16) {
                    groups.erase(groups.begin() + 1);
                }
                break;
            }

            case 13: {
                auto group = groups[fdp.ConsumeIntegralInRange<size_t>(0, groups.size() - 1)];
                auto counter = fdp.ConsumeBool()
                    ? group->GetCounter(SmallName(fdp, "counter"), fdp.ConsumeBool())
                    : group->GetExpiringCounter(SmallName(fdp, "expiring"), fdp.ConsumeBool());
                if (fdp.ConsumeBool()) {
                    counter->Add(fdp.ConsumeIntegralInRange<i64>(-1000, 1000));
                } else {
                    counter->Set(fdp.ConsumeIntegralInRange<i64>(0, 1'000'000));
                }
                counters.push_back(counter);
                if (counters.size() > 32) {
                    counters.erase(counters.begin());
                }
                break;
            }

            case 14: {
                auto group = groups[fdp.ConsumeIntegralInRange<size_t>(0, groups.size() - 1)];
                group->RemoveCounter(SmallName(fdp, "counter"));
                group->RemoveNamedCounter("sensor", SmallName(fdp, "counter"));
                group->RemoveSubgroup(SmallName(fdp, "label"), SmallName(fdp, "value"));
                group->ResetCounters(fdp.ConsumeBool());
                TStringStream out;
                group->OutputPlainText(out);
                group->ReadSnapshot();
                break;
            }
        }

        if (!tokenUnlimited) {
            Y_ABORT_UNLESS(tokenBucket.Available() <= tokenBucket.GetCapacity() + Eps);
            Y_ABORT_UNLESS(std::isfinite(tokenBucket.Available()));
        }

        CheckFiniteNonNegative(leaky.BucketSize);
        Y_ABORT_UNLESS(leaky.BucketDuration > TDuration::Zero());
        Y_ABORT_UNLESS(leaky.Available <= leaky.BucketSize + Eps);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    RunRateQuotaCountersFuzz(fdp);
    return 0;
}
