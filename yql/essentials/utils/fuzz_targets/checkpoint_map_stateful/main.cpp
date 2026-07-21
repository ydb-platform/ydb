#include <yql/essentials/utils/checkpoint_map.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <map>
#include <string>
#include <vector>

namespace {

constexpr size_t MaxOps = 192;
constexpr size_t MaxString = 24;

TString ToTString(std::string value) {
    return TString(value.data(), value.size());
}

TString ConsumeString(FuzzedDataProvider& fdp) {
    const size_t limit = std::min(MaxString, fdp.remaining_bytes());
    const size_t size = fdp.ConsumeIntegralInRange<size_t>(0, limit);
    return ToTString(fdp.ConsumeBytesAsString(size));
}

template <class TMap>
void CheckMapEquals(const NYql::TCheckpointHashMap<ui32, TString>& actual, const TMap& model) {
    Y_ABORT_UNLESS(actual.size() == model.size());
    Y_ABORT_UNLESS(actual.empty() == model.empty());
    for (const auto& [key, value] : model) {
        auto it = actual.find(key);
        Y_ABORT_UNLESS(it != actual.end());
        Y_ABORT_UNLESS(it->second == value);
        Y_ABORT_UNLESS(actual.contains(key));
        Y_ABORT_UNLESS(actual.count(key) == 1);
        Y_ABORT_UNLESS(actual.Get(key) == value);
    }
    size_t seen = 0;
    for (auto it = actual.begin(); it != actual.end(); ++it) {
        Y_ABORT_UNLESS(model.contains(it->first));
        Y_ABORT_UNLESS(model.at(it->first) == it->second);
        ++seen;
    }
    Y_ABORT_UNLESS(seen == model.size());
    Y_ABORT_UNLESS(actual.GetUnderlyingMap().size() == model.size());
}

void ExerciseCheckpointMap(FuzzedDataProvider& fdp) {
    NYql::TCheckpointHashMap<ui32, TString> map;
    std::map<ui32, TString> model;
    std::vector<std::map<ui32, TString>> snapshots;

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 7)) {
            case 0: {
                const ui32 key = fdp.ConsumeIntegralInRange<ui32>(0, 31);
                const TString value = ConsumeString(fdp);
                map.Set(key, value);
                model[key] = value;
                break;
            }
            case 1: {
                const ui32 key = fdp.ConsumeIntegralInRange<ui32>(0, 31);
                const auto removed = map.erase(key);
                const auto modelRemoved = model.erase(key);
                Y_ABORT_UNLESS(removed == modelRemoved);
                break;
            }
            case 2:
                if (snapshots.size() < 16) {
                    map.Checkpoint();
                    snapshots.push_back(model);
                }
                break;
            case 3:
                if (!snapshots.empty()) {
                    map.Rollback();
                    model = snapshots.back();
                    snapshots.pop_back();
                }
                break;
            case 4: {
                const ui32 key = fdp.ConsumeIntegralInRange<ui32>(0, 31);
                const auto it = map.find(key);
                Y_ABORT_UNLESS((it != map.end()) == model.contains(key));
                if (it != map.end()) {
                    Y_ABORT_UNLESS(it->second == model.at(key));
                }
                break;
            }
            case 5:
                if (snapshots.size() < 16) {
                    const auto before = model;
                    {
                        NYql::TCheckpointGuard<ui32, TString> guard(map);
                        const ui32 key = fdp.ConsumeIntegralInRange<ui32>(0, 31);
                        map.Set(key, ConsumeString(fdp));
                    }
                    model = before;
                }
                break;
            case 6: {
                NYql::TCheckpointHashMap<ui32, TString> other;
                for (const auto& [key, value] : model) {
                    other.Set(key, value);
                }
                Y_ABORT_UNLESS(map == other);
                Y_ABORT_UNLESS(!(map != other));
                break;
            }
            default:
                CheckMapEquals(map, model);
                break;
        }
        CheckMapEquals(map, model);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);
    ExerciseCheckpointMap(fdp);

    return 0;
}
