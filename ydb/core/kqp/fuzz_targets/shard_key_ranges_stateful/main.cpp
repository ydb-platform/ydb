#include <ydb/core/kqp/executer_actor/shard_key_ranges.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;

constexpr size_t MaxSteps = 220;

TVector<NScheme::TTypeInfo> KeyTypes() {
    return {NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)};
}

TSerializedCellVec Point(ui64 value) {
    TCell cell = TCell::Make(value);
    return TSerializedCellVec(TConstArrayRef<TCell>(&cell, 1));
}

TSerializedTableRange Range(ui64 from, ui64 to, bool fromInclusive, bool toInclusive) {
    if (to < from) {
        std::swap(from, to);
    }
    TCell fromCell = TCell::Make(from);
    TCell toCell = TCell::Make(to);
    return TSerializedTableRange(
        TConstArrayRef<TCell>(&fromCell, 1),
        fromInclusive,
        TConstArrayRef<TCell>(&toCell, 1),
        toInclusive);
}

ui64 ValueOf(const TSerializedCellVec& vec) {
    Y_ABORT_UNLESS(vec.GetCells().size() == 1);
    return vec.GetCells()[0].AsValue<ui64>();
}

TVector<ui64> SortedUnique(TVector<ui64> values) {
    Sort(values);
    values.erase(std::unique(values.begin(), values.end()), values.end());
    return values;
}

void CheckPointMerge(TVector<ui64> left, TVector<ui64> right) {
    auto keyTypes = KeyTypes();
    TShardKeyRanges ranges;
    for (ui64 value : SortedUnique(left)) {
        ranges.AddPoint(Point(value));
    }

    TShardKeyRanges other;
    for (ui64 value : SortedUnique(right)) {
        other.AddPoint(Point(value));
    }

    left.insert(left.end(), right.begin(), right.end());
    const auto expected = SortedUnique(left);
    ranges.MergeWritePoints(std::move(other), keyTypes);
    Y_ABORT_UNLESS(!ranges.IsFullRange());
    Y_ABORT_UNLESS(ranges.Ranges.size() == expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        Y_ABORT_UNLESS(std::holds_alternative<TSerializedCellVec>(ranges.Ranges[i]));
        Y_ABORT_UNLESS(ValueOf(std::get<TSerializedCellVec>(ranges.Ranges[i])) == expected[i]);
    }
}

void CheckRoundTrip(const TShardKeyRanges& ranges) {
    NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::TReadOpMeta proto;
    ranges.SerializeTo(&proto);

    TShardKeyRanges parsed;
    parsed.ParseFrom(proto);
    if (ranges.IsFullRange()) {
        Y_ABORT_UNLESS(parsed.Ranges.size() == 1);
    } else {
        Y_ABORT_UNLESS(parsed.Ranges.size() == ranges.Ranges.size());
    }

    NKikimrTxDataShard::TKqpReadRangesSourceSettings source;
    ranges.SerializeTo(&source, true);
    NKikimrTxDataShard::TKqpReadRangesSourceSettings sourceNoPoints;
    ranges.SerializeTo(&sourceNoPoints, false);
    Y_ABORT_UNLESS(source.ByteSizeLong() >= 0);
    Y_ABORT_UNLESS(sourceNoPoints.ByteSizeLong() >= 0);
}

void CheckRightBorder(const TShardKeyRanges& ranges) {
    if (!ranges.IsFullRange() && ranges.Ranges.empty()) {
        return;
    }
    const auto [border, inclusive] = ranges.GetRightBorder();
    Y_ABORT_UNLESS(border);
    Y_ABORT_UNLESS(border->GetCells().size() <= 1);
    if (ranges.IsFullRange()) {
        Y_ABORT_UNLESS(inclusive || !ranges.FullRange->Point);
    } else if (std::holds_alternative<TSerializedCellVec>(ranges.Ranges.back())) {
        Y_ABORT_UNLESS(inclusive);
        Y_ABORT_UNLESS(border == &std::get<TSerializedCellVec>(ranges.Ranges.back()));
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    TShardKeyRanges ranges;
    TVector<ui64> leftPoints;
    TVector<ui64> rightPoints;

    for (size_t step = 0; step < MaxSteps && provider.remaining_bytes(); ++step) {
        switch (provider.ConsumeIntegralInRange<ui8>(0, 6)) {
            case 0: {
                const ui64 value = provider.ConsumeIntegralInRange<ui64>(0, 128);
                ranges.AddPoint(Point(value));
                leftPoints.push_back(value);
                break;
            }
            case 1: {
                ranges.AddRange(Range(
                    provider.ConsumeIntegralInRange<ui64>(0, 128),
                    provider.ConsumeIntegralInRange<ui64>(0, 128),
                    provider.ConsumeBool(),
                    provider.ConsumeBool()));
                break;
            }
            case 2: {
                ranges.MakeFullRange(Range(
                    provider.ConsumeIntegralInRange<ui64>(0, 128),
                    provider.ConsumeIntegralInRange<ui64>(0, 128),
                    provider.ConsumeBool(),
                    provider.ConsumeBool()));
                break;
            }
            case 3: {
                ranges.MakeFullPoint(Point(provider.ConsumeIntegralInRange<ui64>(0, 128)));
                break;
            }
            case 4: {
                rightPoints.push_back(provider.ConsumeIntegralInRange<ui64>(0, 128));
                break;
            }
            case 5: {
                CheckRoundTrip(ranges);
                CheckRightBorder(ranges);
                break;
            }
            case 6: {
                CheckPointMerge(leftPoints, rightPoints);
                break;
            }
        }
        if (ranges.IsFullRange()) {
            leftPoints.clear();
        }
        Y_ABORT_UNLESS(ranges.IsFullRange() || ranges.GetRanges().size() <= step + 1);
    }

    CheckRoundTrip(ranges);
    CheckRightBorder(ranges);
    CheckPointMerge(leftPoints, rightPoints);
    return 0;
}
