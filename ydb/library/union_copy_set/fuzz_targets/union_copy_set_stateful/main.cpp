#include <ydb/library/union_copy_set/union_copy_set.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/system/yassert.h>

#include <array>
#include <map>
#include <memory>
#include <utility>

namespace {

constexpr size_t MaxOps = 192;
constexpr size_t MaxUnionSets = 8;
constexpr size_t MaxUnionItems = 24;

struct TUnionValue : public NKikimr::TUnionCopySet<TUnionValue>::TItem {
    ui32 Id = 0;
    explicit TUnionValue(ui32 id)
        : Id(id)
    {}
};

using TUnionSet = NKikimr::TUnionCopySet<TUnionValue>;
using TUnionModel = std::map<ui32, size_t>;

void AddModel(TUnionModel& dst, const TUnionModel& src) {
    for (const auto& [id, count] : src) {
        dst[id] += count;
    }
}

void RemoveItemFromModels(std::array<TUnionModel, MaxUnionSets>& models, ui32 id) {
    for (auto& model : models) {
        model.erase(id);
    }
}

TUnionModel EnumerateUnionSet(const TUnionSet& set) {
    TUnionModel result;
    set.ForEachValue([&](const TUnionValue* value) {
        ++result[value->Id];
        return true;
    });
    return result;
}

void CheckUnionSets(const std::array<TUnionSet, MaxUnionSets>& sets, const std::array<TUnionModel, MaxUnionSets>& models) {
    for (size_t i = 0; i < MaxUnionSets; ++i) {
        Y_ABORT_UNLESS(sets[i].Empty() == models[i].empty());
        Y_ABORT_UNLESS(static_cast<bool>(sets[i]) == !models[i].empty());
        Y_ABORT_UNLESS(EnumerateUnionSet(sets[i]) == models[i]);
    }
}

void ExerciseUnionCopySet(FuzzedDataProvider& fdp) {
    std::array<TUnionSet, MaxUnionSets> sets;
    std::array<TUnionModel, MaxUnionSets> models;
    std::array<std::unique_ptr<TUnionValue>, MaxUnionItems> items;

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        const size_t set = fdp.ConsumeIntegralInRange<size_t>(0, MaxUnionSets - 1);
        const size_t other = fdp.ConsumeIntegralInRange<size_t>(0, MaxUnionSets - 1);
        const size_t item = fdp.ConsumeIntegralInRange<size_t>(0, MaxUnionItems - 1);
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 7)) {
            case 0:
                if (!items[item]) {
                    items[item] = std::make_unique<TUnionValue>(static_cast<ui32>(item));
                }
                sets[set].Add(items[item].get());
                ++models[set][static_cast<ui32>(item)];
                break;
            case 1:
                sets[set].Add(sets[other]);
                if (set != other) {
                    AddModel(models[set], models[other]);
                }
                break;
            case 2:
                sets[set].Add(std::move(sets[other]));
                if (set != other) {
                    AddModel(models[set], models[other]);
                    models[other].clear();
                }
                break;
            case 3:
                sets[set].Clear();
                models[set].clear();
                break;
            case 4:
                if (set != other) {
                    sets[set] = sets[other];
                    models[set] = models[other];
                }
                break;
            case 5:
                if (set != other) {
                    sets[set] = std::move(sets[other]);
                    models[set] = std::move(models[other]);
                    models[other].clear();
                }
                break;
            case 6:
                if (items[item]) {
                    items[item].reset();
                    RemoveItemFromModels(models, static_cast<ui32>(item));
                }
                break;
            default:
                CheckUnionSets(sets, models);
                break;
        }
        CheckUnionSets(sets, models);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);
    ExerciseUnionCopySet(fdp);

    return 0;
}
