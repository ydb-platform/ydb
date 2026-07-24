#include <ydb/core/tx/columnshard/transactions/locks/interaction.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>

namespace {

using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NTxInteractions;

constexpr ui64 KeySpace = 96;
constexpr size_t MaxSteps = 320;

struct TInterval {
    ui64 LockId = 0;
    TInternalPathId PathId;
    ui64 From = 0;
    ui64 To = 0;
};

std::shared_ptr<arrow::Schema> GetSchema() {
    static auto schema = arrow::schema({ arrow::field("pk", arrow::uint64()) });
    return schema;
}

std::shared_ptr<arrow::RecordBatch> MakeBatch(const TVector<ui64>& keys) {
    arrow::UInt64Builder builder;
    Y_ABORT_UNLESS(builder.AppendValues(keys).ok());
    auto array = builder.Finish().ValueOrDie();
    return arrow::RecordBatch::Make(GetSchema(), keys.size(), { array });
}

TIntervalPoint MakePoint(ui64 key) {
    auto batch = MakeBatch(TVector<ui64>{ key });
    NKikimr::NArrow::TSimpleRow row(batch, 0);
    return TIntervalPoint::Equal(row);
}

TInternalPathId PickPath(FuzzedDataProvider& provider) {
    return TInternalPathId::FromRawValue(provider.ConsumeIntegralInRange<ui64>(1, 5));
}

ui64 PickKey(FuzzedDataProvider& provider) {
    return provider.ConsumeIntegralInRange<ui64>(0, KeySpace - 1);
}

void CheckSetEquals(const THashSet<ui64>& actual, const THashSet<ui64>& expected) {
    Y_ABORT_UNLESS(actual.size() == expected.size());
    for (ui64 value : expected) {
        Y_ABORT_UNLESS(actual.contains(value));
    }
}

THashSet<ui64> ExpectedAffected(
    const TVector<TInterval>& intervals,
    const TInternalPathId pathId,
    const TVector<ui64>& keys)
{
    THashSet<ui64> expected;
    for (const TInterval& interval : intervals) {
        if (interval.PathId != pathId) {
            continue;
        }
        for (ui64 key : keys) {
            if (interval.From <= key && key <= interval.To) {
                expected.emplace(interval.LockId);
                break;
            }
        }
    }
    return expected;
}

bool HasPath(const TVector<TInterval>& intervals, const TInternalPathId pathId) {
    return std::any_of(intervals.begin(), intervals.end(), [pathId](const TInterval& interval) {
        return interval.PathId == pathId;
    });
}

void CheckHasReadIntervals(const TInteractionsContext& context, const TVector<TInterval>& intervals) {
    for (ui64 path = 1; path <= 5; ++path) {
        const TInternalPathId pathId = TInternalPathId::FromRawValue(path);
        Y_ABORT_UNLESS(context.HasReadIntervals(pathId) == HasPath(intervals, pathId));
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    TInteractionsContext context;
    TVector<TInterval> intervals;

    const size_t steps = provider.ConsumeIntegralInRange<size_t>(0, MaxSteps);
    for (size_t step = 0; step < steps && provider.remaining_bytes(); ++step) {
        switch (provider.ConsumeIntegralInRange<ui8>(0, 2)) {
            case 0: {
                TInterval interval;
                interval.LockId = provider.ConsumeIntegralInRange<ui64>(1, 40);
                interval.PathId = PickPath(provider);
                interval.From = PickKey(provider);
                interval.To = PickKey(provider);
                if (interval.To < interval.From) {
                    std::swap(interval.From, interval.To);
                }
                context.AddInterval(
                    interval.LockId,
                    interval.PathId,
                    MakePoint(interval.From),
                    MakePoint(interval.To));
                intervals.emplace_back(interval);
                break;
            }
            case 1: {
                if (intervals.empty()) {
                    break;
                }
                const size_t idx = provider.ConsumeIntegralInRange<size_t>(0, intervals.size() - 1);
                const TInterval interval = intervals[idx];
                context.RemoveInterval(
                    interval.LockId,
                    interval.PathId,
                    MakePoint(interval.From),
                    MakePoint(interval.To));
                intervals.erase(intervals.begin() + idx);
                break;
            }
            case 2: {
                TVector<ui64> keys;
                const size_t count = provider.ConsumeIntegralInRange<size_t>(0, 16);
                keys.reserve(count);
                for (size_t i = 0; i < count; ++i) {
                    keys.emplace_back(PickKey(provider));
                }
                std::sort(keys.begin(), keys.end());
                const TInternalPathId pathId = PickPath(provider);
                CheckSetEquals(
                    context.GetAffectedLockIds(pathId, MakeBatch(keys)),
                    ExpectedAffected(intervals, pathId, keys));
                break;
            }
        }
        CheckHasReadIntervals(context, intervals);
    }

    return 0;
}
