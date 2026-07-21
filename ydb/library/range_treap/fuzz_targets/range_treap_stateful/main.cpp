#include <ydb/library/range_treap/range_treap.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/system/yassert.h>

#include <algorithm>
#include <map>
#include <utility>
#include <vector>

namespace {

constexpr size_t MaxOps = 192;
constexpr size_t MaxRanges = 80;
constexpr ui32 RangeDomain = 63;


struct TRangeRecord {
    ui32 Left = 0;
    ui32 Right = 0;
    ui32 Value = 0;
};

using TRangeModel = std::map<std::pair<ui32, ui32>, ui32>;

std::vector<TRangeRecord> ModelRanges(const TRangeModel& model) {
    std::vector<TRangeRecord> result;
    result.reserve(model.size());
    for (const auto& [key, right] : model) {
        result.push_back({key.first, right, key.second});
    }
    return result;
}

bool Intersects(const TRangeRecord& range, ui32 left, ui32 right) {
    return range.Left <= right && left <= range.Right;
}

void CheckTreap(const NKikimr::NRangeTreap::TRangeTreap<ui32, ui32>& treap, const TRangeModel& model) {
    treap.Validate();
    const auto ranges = ModelRanges(model);
    Y_ABORT_UNLESS(treap.Size() == ranges.size());

    size_t pos = 0;
    treap.EachRange([&](const auto& range, ui32 value) {
        Y_ABORT_UNLESS(pos < ranges.size());
        Y_ABORT_UNLESS(range.LeftInclusive);
        Y_ABORT_UNLESS(range.RightInclusive);
        Y_ABORT_UNLESS(range.LeftKey == ranges[pos].Left);
        Y_ABORT_UNLESS(range.RightKey == ranges[pos].Right);
        Y_ABORT_UNLESS(value == ranges[pos].Value);
        ++pos;
        return true;
    });
    Y_ABORT_UNLESS(pos == ranges.size());

    for (ui32 point : {0u, 1u, 7u, 31u, 63u}) {
        std::vector<TRangeRecord> expected;
        for (const auto& range : ranges) {
            if (Intersects(range, point, point)) {
                expected.push_back(range);
            }
        }

        pos = 0;
        treap.EachIntersection(point, [&](const auto& range, ui32 value) {
            Y_ABORT_UNLESS(pos < expected.size());
            Y_ABORT_UNLESS(range.LeftKey == expected[pos].Left);
            Y_ABORT_UNLESS(range.RightKey == expected[pos].Right);
            Y_ABORT_UNLESS(value == expected[pos].Value);
            ++pos;
            return true;
        });
        Y_ABORT_UNLESS(pos == expected.size());
    }

    const ui32 ql = 3;
    const ui32 qr = 39;
    std::vector<TRangeRecord> expected;
    for (const auto& range : ranges) {
        if (Intersects(range, ql, qr)) {
            expected.push_back(range);
        }
    }
    pos = 0;
    treap.EachIntersection(NKikimr::NRangeTreap::TRange<ui32>(ql, true, qr, true), [&](const auto& range, ui32 value) {
        Y_ABORT_UNLESS(pos < expected.size());
        Y_ABORT_UNLESS(range.LeftKey == expected[pos].Left);
        Y_ABORT_UNLESS(range.RightKey == expected[pos].Right);
        Y_ABORT_UNLESS(value == expected[pos].Value);
        ++pos;
        return true;
    });
    Y_ABORT_UNLESS(pos == expected.size());

    size_t limited = 0;
    const bool completed = treap.EachRange([&](const auto&, ui32) {
        ++limited;
        return limited < 3;
    });
    Y_ABORT_UNLESS(completed == (ranges.size() < 3));
    Y_ABORT_UNLESS(limited == std::min<size_t>(ranges.size(), 3));
}

void ExerciseRangeTreap(FuzzedDataProvider& fdp) {
    using TRange = NKikimr::NRangeTreap::TRange<ui32>;
    NKikimr::NRangeTreap::TRangeTreap<ui32, ui32> treap;
    TRangeModel model;

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        ui32 left = fdp.ConsumeIntegralInRange<ui32>(0, RangeDomain);
        ui32 right = fdp.ConsumeIntegralInRange<ui32>(0, RangeDomain);
        if (left > right) {
            std::swap(left, right);
        }
        const ui32 value = fdp.ConsumeIntegralInRange<ui32>(0, 15);

        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 4)) {
            case 0:
            case 1:
                if (model.size() < MaxRanges) {
                    treap.AddRange(TRange(left, true, right, true), value);
                    auto& storedRight = model[{left, value}];
                    storedRight = std::max(storedRight, right);
                }
                break;
            case 2:
                treap.RemoveRanges(value);
                for (auto it = model.begin(); it != model.end();) {
                    if (it->first.second == value) {
                        it = model.erase(it);
                    } else {
                        ++it;
                    }
                }
                break;
            case 3:
                treap.Clear();
                model.clear();
                break;
            default:
                break;
        }

        CheckTreap(treap, model);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    ExerciseRangeTreap(fdp);

    return 0;
}
