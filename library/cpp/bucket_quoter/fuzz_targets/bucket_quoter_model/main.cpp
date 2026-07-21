#include <library/cpp/bucket_quoter/bucket_quoter.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/algorithm.h>
#include <util/system/yassert.h>

namespace {

struct TDeterministicTimer {
    using TTime = ui64;
    static constexpr ui64 Resolution = 1000;

    static TTime Now() {
        return NowValue;
    }

    static ui64 Duration(TTime from, TTime to) {
        return to > from ? to - from : 0;
    }

    static ui64 NowValue;
};

ui64 TDeterministicTimer::NowValue = 0;

struct TBucketModel {
    i64 Bucket = 0;
    ui64 LastAdd = 0;
    ui64 Inflow = 1;
    ui64 Capacity = 1;
    ui64 MsgPassed = 0;
    ui64 BucketUnderflows = 0;
    ui64 TokensUsed = 0;
    ui64 AggregateInflow = 0;
    ui64 NextSeqno = 1;

    void Fill() {
        const ui64 now = TDeterministicTimer::Now();
        const ui64 elapsed = TDeterministicTimer::Duration(LastAdd, now);
        const ui64 delta = Inflow * elapsed;
        if (delta >= TDeterministicTimer::Resolution) {
            const ui64 inflow = delta / TDeterministicTimer::Resolution;
            AggregateInflow += inflow;
            Bucket += static_cast<i64>(inflow);
            if (Bucket > static_cast<i64>(Capacity)) {
                Bucket = static_cast<i64>(Capacity);
            }
            LastAdd = now;
        }
    }

    void Use(ui64 tokens) {
        Bucket -= static_cast<i64>(tokens);
        TokensUsed += tokens;
        ++MsgPassed;
    }

    void Add(ui64 tokens) {
        Bucket += static_cast<i64>(tokens);
        if (Bucket > static_cast<i64>(Capacity)) {
            Bucket = static_cast<i64>(Capacity);
        }
    }

    ui64 GetAvail() const {
        return Max<i64>(0, Bucket);
    }

    ui32 GetWaitTime() const {
        if (Bucket >= 0) {
            return 0;
        }
        return static_cast<ui32>((static_cast<ui64>(-Bucket) * 1000000) / Inflow);
    }
};

using TQuoter = TBucketQuoter<ui64, TMutex, TDeterministicTimer>;

void CheckCounters(ui64 msgPassed, ui64 bucketUnderflows, ui64 tokensUsed, ui64 aggregateInflow, const TBucketModel& model) {
    Y_ABORT_UNLESS(msgPassed == model.MsgPassed);
    Y_ABORT_UNLESS(bucketUnderflows == model.BucketUnderflows);
    Y_ABORT_UNLESS(tokensUsed == model.TokensUsed);
    Y_ABORT_UNLESS(aggregateInflow == model.AggregateInflow);
}

void CheckResult(const TBucketModel& model, const TQuoter::TResult& result, i64 before, i64 after) {
    Y_ABORT_UNLESS(result.Before == before);
    Y_ABORT_UNLESS(result.After == after);
    Y_ABORT_UNLESS(result.Seqno == model.NextSeqno);
}

template <typename TMakeQuoter>
void RunScenario(FuzzedDataProvider& provider, TBucketModel model, TMakeQuoter makeQuoter) {
    TDeterministicTimer::NowValue = 0;

    ui64 msgPassed = 0;
    ui64 bucketUnderflows = 0;
    ui64 tokensUsed = 0;
    ui64 aggregateInflow = 0;
    TQuoter quoter = makeQuoter(&msgPassed, &bucketUnderflows, &tokensUsed, &aggregateInflow);

    const ui8 operations = provider.ConsumeIntegralInRange<ui8>(1, 128);
    for (ui8 i = 0; i < operations; ++i) {
        const ui8 command = provider.ConsumeIntegralInRange<ui8>(0, 8);
        const ui64 tokens = provider.ConsumeIntegralInRange<ui64>(0, 4096);

        switch (command) {
            case 0: {
                const ui64 jump = provider.ConsumeIntegralInRange<ui64>(0, TDeterministicTimer::Resolution * 8);
                TDeterministicTimer::NowValue += jump;
                break;
            }
            case 1: {
                quoter.Use(tokens);
                model.Use(tokens);
                break;
            }
            case 2: {
                TQuoter::TResult result;
                const i64 before = model.Bucket;
                quoter.Use(tokens, result);
                model.Use(tokens);
                CheckResult(model, result, before, model.Bucket);
                ++model.NextSeqno;
                break;
            }
            case 3: {
                quoter.Add(tokens);
                model.Add(tokens);
                break;
            }
            case 4: {
                TQuoter::TResult result;
                const i64 before = model.Bucket;
                quoter.Add(tokens, result);
                model.Add(tokens);
                CheckResult(model, result, before, model.Bucket);
                ++model.NextSeqno;
                break;
            }
            case 5: {
                TQuoter::TResult result;
                const i64 before = model.Bucket;
                const ui64 actual = quoter.GetAvail(result);
                model.Fill();
                const ui64 expected = model.GetAvail();
                Y_ABORT_UNLESS(actual == expected);
                CheckResult(model, result, before, model.Bucket);
                ++model.NextSeqno;
                break;
            }
            case 6: {
                TQuoter::TResult result;
                const i64 before = model.Bucket;
                const bool actual = quoter.IsAvail(result);
                model.Fill();
                if (model.Bucket < 0) {
                    ++model.BucketUnderflows;
                }
                const bool expected = model.Bucket >= 0;
                Y_ABORT_UNLESS(actual == expected);
                CheckResult(model, result, before, model.Bucket);
                ++model.NextSeqno;
                break;
            }
            case 7: {
                TQuoter::TResult result;
                const i64 before = model.Bucket;
                const ui32 actual = quoter.GetWaitTime(result);
                model.Fill();
                const ui32 expected = model.GetWaitTime();
                Y_ABORT_UNLESS(actual == expected);
                CheckResult(model, result, before, model.Bucket);
                ++model.NextSeqno;
                break;
            }
            case 8: {
                const i64 actual = quoter.UseAndFill(tokens);
                model.Fill();
                model.Use(tokens);
                Y_ABORT_UNLESS(actual == model.Bucket);
                break;
            }
        }

        Y_ABORT_UNLESS(quoter.GetAvail() == [&model]() {
            model.Fill();
            return model.GetAvail();
        }());
        CheckCounters(msgPassed, bucketUnderflows, tokensUsed, aggregateInflow, model);
    }
}

void FuzzBucketQuoter(FuzzedDataProvider& provider) {
    TBucketModel model;
    model.Inflow = provider.ConsumeIntegralInRange<ui64>(1, 1000000);
    model.Capacity = provider.ConsumeIntegralInRange<ui64>(1, 1000000);
    const bool fill = provider.ConsumeBool();
    model.Bucket = fill ? static_cast<i64>(model.Capacity) : 0;

    if (provider.ConsumeBool()) {
        RunScenario(provider, model, [inflow = model.Inflow, capacity = model.Capacity, fill](ui64* msgPassed, ui64* bucketUnderflows, ui64* tokensUsed, ui64* aggregateInflow) {
            return TQuoter(inflow, capacity, msgPassed, bucketUnderflows, tokensUsed, nullptr, fill, aggregateInflow);
        });
    } else {
        TAtomic inflow = model.Inflow;
        TAtomic capacity = model.Capacity;
        RunScenario(provider, model, [&inflow, &capacity, fill](ui64* msgPassed, ui64* bucketUnderflows, ui64* tokensUsed, ui64* aggregateInflow) {
            return TQuoter(&inflow, &capacity, msgPassed, bucketUnderflows, tokensUsed, nullptr, fill, aggregateInflow);
        });
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    FuzzBucketQuoter(provider);
    return 0;
}
