#include <ydb/library/time_series_vec/time_series_vec.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/system/yassert.h>

#include <algorithm>
#include <map>
#include <optional>
#include <vector>

namespace {

constexpr size_t MaxOps = 192;
constexpr ui64 TimeIntervalUs = 10000;

class TTimeModel {
public:
    explicit TTimeModel(size_t size)
        : Values(size, 0)
        , Id(size - 1)
    {}

    size_t Size() const {
        return Values.size();
    }

    ui64 BeginId() const {
        return Id + 1 - Values.size();
    }

    ui64 EndId() const {
        return Id + 1;
    }

    i64 Get(ui64 id) const {
        return Values[id % Values.size()];
    }

    bool Add(ui64 id, i64 value, std::optional<ui64> beginLimit = std::nullopt) {
        if (beginLimit) {
            const ui64 limitId = *beginLimit + Values.size() - 1;
            if (id > limitId) {
                Propagate(limitId);
                return false;
            }
        }
        Propagate(id);
        if (id < BeginId()) {
            return true;
        }
        Values[id % Values.size()] += value;
        return true;
    }

    size_t AddArray(ui64 start, ui64 step, const std::vector<i64>& values, std::optional<ui64> beginLimit = std::nullopt) {
        if (step == 1) {
            ui64 id1 = start;
            ui64 id2 = id1 + values.size();
            if (beginLimit) {
                id2 = std::min(id2, *beginLimit + Values.size() - 1);
            }
            Propagate(id2 - 1);

            ui64 skip = 0;
            if (BeginId() > id1) {
                skip = BeginId() - id1;
            }
            if (skip >= values.size() || id1 + skip >= id2) {
                return 0;
            }

            size_t added = 0;
            for (ui64 id = id1 + skip; id < id2; ++id, ++added) {
                Values[id % Values.size()] += values[skip + added];
            }
            return added;
        }

        size_t added = 0;
        for (size_t i = 0; i < values.size(); ++i) {
            if (!Add(start + i * step, values[i], beginLimit)) {
                break;
            }
            ++added;
        }
        return added;
    }

    void Clear() {
        std::fill(Values.begin(), Values.end(), 0);
    }

private:
    void Propagate(ui64 id) {
        if (id <= Id) {
            return;
        }
        if (id - Id >= Values.size()) {
            Clear();
            Id = id;
        } else {
            while (Id < id) {
                Values[++Id % Values.size()] = 0;
            }
        }
    }

private:
    std::vector<i64> Values;
    ui64 Id = 0;
};

TInstant InstantFromId(ui64 id) {
    return TInstant::MicroSeconds(id * TimeIntervalUs);
}

bool HasNonZeroValues(const std::vector<i64>& values) {
    return std::any_of(values.begin(), values.end(), [](i64 value) {
        return value != 0;
    });
}

void CheckTimeSeries(const NKikimr::TTimeSeriesVec<i64, TimeIntervalUs>& series, const TTimeModel& model) {
    Y_ABORT_UNLESS(series.Size() == model.Size());
    Y_ABORT_UNLESS(series.Begin().MicroSeconds() == model.BeginId() * TimeIntervalUs);
    Y_ABORT_UNLESS(series.End().MicroSeconds() == model.EndId() * TimeIntervalUs);
    for (ui64 id = model.BeginId(); id < model.EndId(); ++id) {
        Y_ABORT_UNLESS(series.Get(InstantFromId(id)) == model.Get(id));
        Y_ABORT_UNLESS(series.Align(InstantFromId(id) + TDuration::MicroSeconds(TimeIntervalUs / 2)) == InstantFromId(id));
    }
}

void ExerciseTimeSeries(FuzzedDataProvider& fdp) {
    const size_t size = fdp.ConsumeIntegralInRange<size_t>(1, 8);
    NKikimr::TTimeSeriesVec<i64, TimeIntervalUs> series(size);
    TTimeModel model(size);

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        const ui64 id = fdp.ConsumeIntegralInRange<ui64>(0, 96);
        const i64 value = fdp.ConsumeIntegralInRange<i64>(-128, 128);
        const bool useLimit = fdp.ConsumeBool();
        const ui64 limitId = fdp.ConsumeIntegralInRange<ui64>(0, 96);
        const auto limit = useLimit ? InstantFromId(limitId) : TInstant::Max();
        const std::optional<ui64> modelLimit = useLimit ? std::optional<ui64>(limitId) : std::nullopt;

        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 5)) {
            case 0: {
                const bool actual = series.Add(InstantFromId(id), value, limit);
                const bool expected = model.Add(id, value, modelLimit);
                Y_ABORT_UNLESS(actual == expected);
                break;
            }
            case 1: {
                const size_t count = fdp.ConsumeIntegralInRange<size_t>(0, 8);
                std::vector<i64> values;
                values.reserve(count);
                for (size_t j = 0; j < count; ++j) {
                    values.push_back(fdp.ConsumeIntegralInRange<i64>(-64, 64));
                }
                if (values.empty() || !HasNonZeroValues(values)) {
                    break;
                }
                const ui64 step = fdp.ConsumeBool() ? 1 : fdp.ConsumeIntegralInRange<ui64>(1, 4);
                const size_t actual = series.Add(TDuration::MicroSeconds(TimeIntervalUs * step), InstantFromId(id), values.data(), values.size(), limit);
                const size_t expected = model.AddArray(id, step, values, modelLimit);
                Y_ABORT_UNLESS(actual == expected);
                break;
            }
            case 2: {
                NKikimr::TTimeSeriesMap<i64, TimeIntervalUs> sparse;
                std::map<ui64, i64> sparseModel;
                const size_t count = fdp.ConsumeIntegralInRange<size_t>(0, 8);
                for (size_t j = 0; j < count; ++j) {
                    const ui64 sparseId = fdp.ConsumeIntegralInRange<ui64>(0, 96);
                    const i64 sparseValue = fdp.ConsumeIntegralInRange<i64>(-64, 64);
                    sparse.Add(InstantFromId(sparseId), sparseValue);
                    sparseModel[sparseId] += sparseValue;
                }
                size_t expected = 0;
                for (const auto& [sparseId, sparseValue] : sparseModel) {
                    if (model.Add(sparseId, sparseValue, modelLimit)) {
                        ++expected;
                    }
                }
                const size_t actual = series.Add(sparse, limit);
                Y_ABORT_UNLESS(actual == expected);
                break;
            }
            case 3:
                series.Clear();
                model.Clear();
                break;
            case 4: {
                const ui64 begin = std::min(id, fdp.ConsumeIntegralInRange<ui64>(0, 96));
                const ui64 end = fdp.ConsumeIntegralInRange<ui64>(begin, 96);
                NKikimr::TTimeSeriesVec<i64, TimeIntervalUs> sub(series, InstantFromId(begin), InstantFromId(end));
                Y_ABORT_UNLESS(sub.Size() >= 1);
                break;
            }
            default:
                CheckTimeSeries(series, model);
                break;
        }
        CheckTimeSeries(series, model);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);
    ExerciseTimeSeries(fdp);

    return 0;
}
