#include <ydb/core/util/btree.h>
#include <ydb/core/util/btree_cow.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/memory/pool.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <iterator>
#include <map>
#include <optional>
#include <utility>
#include <vector>

namespace {

constexpr size_t MaxOps = 192;
constexpr size_t MaxTreeItems = 96;
constexpr ui32 KeyDomain = 63;

using TMultiModel = std::vector<std::pair<ui32, ui32>>;

ui32 SmallKey(FuzzedDataProvider& fdp) {
    return fdp.ConsumeIntegralInRange<ui32>(0, KeyDomain);
}

ui32 SmallValue(FuzzedDataProvider& fdp) {
    return fdp.ConsumeIntegralInRange<ui32>(0, 255);
}

auto UpperByKey(TMultiModel& model, ui32 key) {
    return std::upper_bound(
        model.begin(),
        model.end(),
        key,
        [](ui32 lhs, const auto& rhs) {
            return lhs < rhs.first;
        });
}

auto UpperByKey(const TMultiModel& model, ui32 key) {
    return std::upper_bound(
        model.begin(),
        model.end(),
        key,
        [](ui32 lhs, const auto& rhs) {
            return lhs < rhs.first;
        });
}

auto LowerByKey(const TMultiModel& model, ui32 key) {
    return std::lower_bound(
        model.begin(),
        model.end(),
        key,
        [](const auto& lhs, ui32 rhs) {
            return lhs.first < rhs;
        });
}

std::optional<ui32> LastValueForKey(const TMultiModel& model, ui32 key) {
    auto it = UpperByKey(model, key);
    if (it == model.begin()) {
        return std::nullopt;
    }
    --it;
    if (it->first != key) {
        return std::nullopt;
    }
    return it->second;
}

void InsertAfterDuplicates(TMultiModel& model, ui32 key, ui32 value) {
    model.insert(UpperByKey(model, key), {key, value});
}

template <class TIterator>
void CheckIteratorAt(TIterator& it, const TMultiModel& model, size_t pos) {
    if (pos < model.size()) {
        Y_ABORT_UNLESS(it.IsValid());
        Y_ABORT_UNLESS(it.GetKey() == model[pos].first);
        Y_ABORT_UNLESS(it.GetValue() == model[pos].second);
    } else {
        Y_ABORT_UNLESS(!it.IsValid());
    }
}

template <class TIteratorFactory>
void CheckForwardBackward(TIteratorFactory makeIterator, const TMultiModel& model) {
    auto it = makeIterator();
    Y_ABORT_UNLESS(it.SeekFirst() == !model.empty());
    for (const auto& [key, value] : model) {
        Y_ABORT_UNLESS(it.IsValid());
        Y_ABORT_UNLESS(it.GetKey() == key);
        Y_ABORT_UNLESS(it.GetValue() == value);
        it.Next();
    }
    Y_ABORT_UNLESS(!it.IsValid());

    it = makeIterator();
    Y_ABORT_UNLESS(it.SeekLast() == !model.empty());
    for (auto rit = model.rbegin(); rit != model.rend(); ++rit) {
        Y_ABORT_UNLESS(it.IsValid());
        Y_ABORT_UNLESS(it.GetKey() == rit->first);
        Y_ABORT_UNLESS(it.GetValue() == rit->second);
        it.Prev();
    }
    Y_ABORT_UNLESS(!it.IsValid());
}

template <class TIteratorFactory>
void CheckTreeSeeks(TIteratorFactory makeIterator, const TMultiModel& model) {
    CheckForwardBackward(makeIterator, model);

    for (ui32 key : {0u, 1u, 2u, 7u, 31u, 63u, 64u}) {
        auto it = makeIterator();
        auto lower = LowerByKey(model, key);
        Y_ABORT_UNLESS(it.SeekLowerBound(key) == (lower != model.end()));
        CheckIteratorAt(it, model, lower - model.begin());

        it = makeIterator();
        auto upper = UpperByKey(model, key);
        Y_ABORT_UNLESS(it.SeekUpperBound(key) == (upper != model.end()));
        CheckIteratorAt(it, model, upper - model.begin());

        it = makeIterator();
        auto exactFirst = lower;
        const bool hasExact = exactFirst != model.end() && exactFirst->first == key;
        Y_ABORT_UNLESS(it.SeekExactFirst(key) == hasExact);
        CheckIteratorAt(it, model, hasExact ? size_t(exactFirst - model.begin()) : model.size());

        it = makeIterator();
        auto exactLast = UpperByKey(model, key);
        const bool hasExactLast = exactLast != model.begin() && std::prev(exactLast)->first == key;
        Y_ABORT_UNLESS(it.SeekExact(key) == hasExactLast);
        CheckIteratorAt(it, model, hasExactLast ? size_t(std::prev(exactLast) - model.begin()) : model.size());

        it = makeIterator();
        Y_ABORT_UNLESS(it.SeekLowerBound(key, true) == (lower != model.begin()));
        CheckIteratorAt(it, model, lower == model.begin() ? model.size() : size_t(std::prev(lower) - model.begin()));

        it = makeIterator();
        Y_ABORT_UNLESS(it.SeekUpperBound(key, true) == (upper != model.begin()));
        CheckIteratorAt(it, model, upper == model.begin() ? model.size() : size_t(std::prev(upper) - model.begin()));
    }
}

void ExerciseBTree(FuzzedDataProvider& fdp) {
    using TTree = NKikimr::TBTree<ui32, ui32, TLess<ui32>, TMemoryPool, 128>;

    TMemoryPool pool(4096);
    TTree tree(pool);
    TMultiModel model;

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        const ui32 key = SmallKey(fdp);
        const ui32 value = SmallValue(fdp);
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 7)) {
            case 0: {
                const bool expected = !LastValueForKey(model, key).has_value();
                const bool inserted = tree.Emplace(key, value);
                Y_ABORT_UNLESS(inserted == expected);
                if (inserted) {
                    InsertAfterDuplicates(model, key, value);
                }
                break;
            }
            case 1:
            case 2:
                if (model.size() < MaxTreeItems) {
                    tree.FindPtr(key);
                    tree.EmplaceUnsafe(key, value);
                    InsertAfterDuplicates(model, key, value);
                }
                break;
            case 3: {
                const auto expected = LastValueForKey(model, key);
                const ui32* found = tree.FindPtr(key);
                Y_ABORT_UNLESS(bool(found) == expected.has_value());
                if (found) {
                    Y_ABORT_UNLESS(*found == *expected);
                }
                break;
            }
            case 4:
                tree.Clear();
                model.clear();
                break;
            default:
                break;
        }

        Y_ABORT_UNLESS(tree.Size() == model.size());
        CheckTreeSeeks([&] { return tree.SafeAccess().Iterator(); }, model);
    }
}

template <class TSnapshot>
void CheckCowSnapshot(const TSnapshot& snapshot, const std::map<ui32, ui32>& model) {
    Y_ABORT_UNLESS(snapshot.Size() == model.size());
    auto it = snapshot.Iterator();

    Y_ABORT_UNLESS(it.SeekFirst() == !model.empty());
    for (const auto& [key, value] : model) {
        Y_ABORT_UNLESS(it.IsValid());
        Y_ABORT_UNLESS(it.GetKey() == key);
        Y_ABORT_UNLESS(it.GetValue() == value);
        const ui32* found = it.Find(key);
        Y_ABORT_UNLESS(found && *found == value);
        it.Next();
    }
    Y_ABORT_UNLESS(!it.IsValid());

    it = snapshot.Iterator();
    Y_ABORT_UNLESS(it.SeekLast() == !model.empty());
    for (auto rit = model.rbegin(); rit != model.rend(); ++rit) {
        Y_ABORT_UNLESS(it.IsValid());
        Y_ABORT_UNLESS(it.GetKey() == rit->first);
        Y_ABORT_UNLESS(it.GetValue() == rit->second);
        it.Prev();
    }
    Y_ABORT_UNLESS(!it.IsValid());
}

void ExerciseCowBTree(FuzzedDataProvider& fdp) {
    using TTree = NKikimr::TCowBTree<ui32, ui32, TLess<ui32>, std::allocator<ui32>, 128>;

    struct TSavedSnapshot {
        TTree::TSnapshot Snapshot;
        std::map<ui32, ui32> Model;
    };

    TTree tree;
    std::map<ui32, ui32> model;
    std::vector<TSavedSnapshot> snapshots;

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        const ui32 key = SmallKey(fdp);
        const ui32 value = SmallValue(fdp);
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 9)) {
            case 0: {
                const bool expected = !model.contains(key);
                const bool inserted = tree.Emplace(key, value);
                Y_ABORT_UNLESS(inserted == expected);
                if (inserted) {
                    model[key] = value;
                }
                break;
            }
            case 1: {
                ui32& ref = tree.InsertOrUpdate(key);
                ref = value;
                model[key] = value;
                break;
            }
            case 2: {
                ui32& ref = tree[key];
                ref = value;
                model[key] = value;
                break;
            }
            case 3: {
                ui32* found = tree.FindForUpdate(key);
                Y_ABORT_UNLESS(bool(found) == model.contains(key));
                if (found) {
                    *found = value;
                    model[key] = value;
                }
                break;
            }
            case 4: {
                const ui32* found = tree.Find(key);
                Y_ABORT_UNLESS(bool(found) == model.contains(key));
                if (found) {
                    Y_ABORT_UNLESS(*found == model.at(key));
                }
                break;
            }
            case 5:
                if (snapshots.size() < 8) {
                    snapshots.push_back({tree.Snapshot(), model});
                }
                break;
            case 6:
                if (!snapshots.empty()) {
                    tree.RollbackTo(snapshots.back().Snapshot);
                    model = snapshots.back().Model;
                    snapshots.pop_back();
                }
                break;
            case 7:
                tree.CollectGarbage();
                break;
            case 8:
                tree.Clear();
                model.clear();
                snapshots.clear();
                break;
            default:
                break;
        }

        Y_ABORT_UNLESS(tree.Size() == model.size());
        Y_ABORT_UNLESS(tree.Empty() == model.empty());
        CheckCowSnapshot(tree.UnsafeSnapshot(), model);
        if (!snapshots.empty()) {
            const size_t pos = fdp.ConsumeIntegralInRange<size_t>(0, snapshots.size() - 1);
            CheckCowSnapshot(snapshots[pos].Snapshot, snapshots[pos].Model);
        }
    }
}


} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    if (fdp.ConsumeBool()) {
        ExerciseBTree(fdp);
    } else {
        ExerciseCowBTree(fdp);
    }

    return 0;
}
