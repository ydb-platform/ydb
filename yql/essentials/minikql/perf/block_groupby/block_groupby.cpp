#include <benchmark/benchmark.h>

#include <cmath>
#include <util/system/unaligned_mem.h>

#include <yql/essentials/minikql/comp_nodes/mkql_rh_hash.h>

#include <yql/essentials/minikql/arrow/arrow_defs.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/datum.h>

#include <util/digest/numeric.h>
#include <util/digest/fnv.h>
#include <util/digest/murmur.h>
#include <util/digest/city.h>

namespace {

enum class EDistribution {
    Const,
    Linear,
    Random,
    Few,
    RandomFew
};

enum class EShape {
    Default,
    Sqrt,
    Log
};

arrow::Datum MakeIntColumn(ui32 len, EDistribution dist, EShape shape, ui32 buckets) {
    arrow::Int32Builder builder;
    ARROW_OK(builder.Reserve(len));
    for (ui32 i = 0; i < len; ++i) {
        ui32 val;
        switch (shape) {
            case EShape::Default:
                val = i;
                break;
            case EShape::Sqrt:
                val = (ui32)sqrt(i);
                break;
            case EShape::Log:
                val = (ui32)log(1 + i);
                break;
        }

        switch (dist) {
            case EDistribution::Const:
                builder.UnsafeAppend(0);
                break;
            case EDistribution::Few:
                builder.UnsafeAppend(val % buckets);
                break;
            case EDistribution::Linear:
                builder.UnsafeAppend(val);
                break;
            case EDistribution::Random:
                builder.UnsafeAppend(IntHash(val));
                break;
            case EDistribution::RandomFew:
                builder.UnsafeAppend(IntHash(val) % buckets);
                break;
        }
    }

    std::shared_ptr<arrow::ArrayData> result;
    ARROW_OK(builder.FinishInternal(&result));
    return arrow::Datum(result);
}

class IAggregator {
public:
    virtual ~IAggregator() = default;
    virtual void Init(i64* state, i32 payload) = 0;
    virtual void Update(i64* state, i32 payload) = 0;
};

class TSumAggregator: public IAggregator {
public:
    void Init(i64* state, i32 payload) final {
        *state = payload;
    }

    void Update(i64* state, i32 payload) final {
        *state += payload;
    }
};

template <typename T>
struct TCityHasher {
public:
    ui64 operator()(const T& x) const {
        return CityHash64(TStringBuf((char*)&x, sizeof(x)));
    }
};

// sum(payloads) group by keys
template <bool CalculateHashStats, bool UseRH>
class TAggregate {
private:
    struct TOneCell {
        i32 Key = 0;
        bool IsEmpty = true;
        i64 State = 0;
    };

    struct TCell {
        i32 Key = 0;
        i32 PSL = -1;
        i64 State = 0;
    };

public:
    explicit TAggregate(const std::vector<IAggregator*>& aggs)
        : Aggs_(aggs)
        , Rh_(sizeof(i64))
    {
        Cells_.resize(1U << 8);
    }

    void AddBatch(arrow::Datum keys, arrow::Datum payloads) {
        const auto& arrKeys = keys.array();
        const auto& arrPayloads = payloads.array();
        auto len = arrKeys->length;
        const i32* ptrKeys = arrKeys->GetValues<i32>(1);
        const i32* ptrPayloads = arrPayloads->GetValues<i32>(1);
        for (int64_t i = 0; i < len; ++i) {
            auto key = ptrKeys[i];
            auto payload = ptrPayloads[i];
            if (!MoreThanOne_) {
                if (One_.IsEmpty) {
                    One_.IsEmpty = false;
                    One_.Key = key;
                    for (const auto& a : Aggs_) {
                        a->Init(&One_.State, payload);
                    }

                    Size_ = 1;
                    continue;
                } else {
                    if (key == One_.Key) {
                        for (const auto& a : Aggs_) {
                            a->Update(&One_.State, payload);
                        }

                        continue;
                    } else {
                        MoreThanOne_ = true;
                        if constexpr (UseRH) {
                            bool isNew;
                            auto iter = Rh_.Insert(One_.Key, isNew);
                            Y_ASSERT(isNew);
                            WriteUnaligned<i64>(Rh_.GetMutablePayloadPtr(iter), One_.State);
                        } else {
                            bool isNew;
                            ui64 bucket = AddBucketFromKeyImpl(One_.Key, Cells_, isNew);
                            auto& c = Cells_[bucket];
                            c.PSL = 0;
                            c.Key = One_.Key;
                            c.State = One_.State;
                        }
                    }
                }
            }

            if constexpr (UseRH) {
                bool isNew = false;
                auto iter = Rh_.Insert(key, isNew);
                if (isNew) {
                    for (const auto& a : Aggs_) {
                        a->Init((i64*)Rh_.GetPayloadPtr(iter), payload);
                    }

                    Rh_.CheckGrow();
                } else {
                    for (const auto& a : Aggs_) {
                        a->Update((i64*)Rh_.GetPayloadPtr(iter), payload);
                    }
                }
            } else {
                bool isNew = false;
                ui64 bucket = AddBucketFromKey(key, isNew);
                auto& c = Cells_[bucket];
                if (isNew) {
                    Size_ += 1;
                    for (const auto& a : Aggs_) {
                        a->Init(&c.State, payload);
                    }

                    if (Size_ * 2 >= Cells_.size()) {
                        Grow();
                    }
                } else {
                    for (const auto& a : Aggs_) {
                        a->Update(&c.State, payload);
                    }
                }
            }
        }
    }

    static ui64 MakeHash(i32 key) {
        // auto hash = FnvHash<ui64>(&key, sizeof(key));
        // auto hash = MurmurHash<ui64>(&key, sizeof(key));
        auto hash = CityHash64(TStringBuf((char*)&key, sizeof(key)));
        // auto hash = key;
        return hash;
    }

    Y_FORCE_INLINE ui64 AddBucketFromKey(i32 key, bool& isNew) {
        return AddBucketFromKeyImpl(key, Cells_, isNew);
    }

    Y_FORCE_INLINE ui64 AddBucketFromKeyImpl(i32 key, std::vector<TCell>& cells, bool& isNew) {
        isNew = false;
        ui32 chainLen = 0;
        if constexpr (CalculateHashStats) {
            HashSearches_++;
        }

        ui64 bucket = MakeHash(key) & (cells.size() - 1);
        i32 distance = 0;
        ui64 returnBucket;
        i64 oldState;
        for (;;) {
            if constexpr (CalculateHashStats) {
                HashProbes_++;
                chainLen++;
            }

            if (cells[bucket].PSL < 0) {
                isNew = true;
                cells[bucket].Key = key;
                cells[bucket].PSL = distance;

                if constexpr (CalculateHashStats) {
                    MaxHashChainLen_ = Max(MaxHashChainLen_, chainLen);
                }

                return bucket;
            }

            if (cells[bucket].Key == key) {
                if constexpr (CalculateHashStats) {
                    MaxHashChainLen_ = Max(MaxHashChainLen_, chainLen);
                }

                return bucket;
            }

            if (distance > cells[bucket].PSL) {
                // swap keys & state
                returnBucket = bucket;
                oldState = cells[bucket].State;
                std::swap(key, cells[bucket].Key);
                std::swap(distance, cells[bucket].PSL);
                isNew = true;

                ++distance;
                bucket = (bucket + 1) & (cells.size() - 1);
                break;
            }

            ++distance;
            bucket = (bucket + 1) & (cells.size() - 1);
        }

        for (;;) {
            if constexpr (CalculateHashStats) {
                HashProbes_++;
                chainLen++;
            }

            if (cells[bucket].PSL < 0) {
                if constexpr (CalculateHashStats) {
                    MaxHashChainLen_ = Max(MaxHashChainLen_, chainLen);
                }

                cells[bucket].Key = key;
                cells[bucket].State = oldState;
                cells[bucket].PSL = distance;
                return returnBucket; // for original key
            }

            Y_ENSURE(cells[bucket].Key != key);
            if (distance > cells[bucket].PSL) {
                // swap keys & state
                std::swap(key, cells[bucket].Key);
                std::swap(oldState, cells[bucket].State);
                std::swap(distance, cells[bucket].PSL);
            }

            ++distance;
            bucket = (bucket + 1) & (cells.size() - 1);
        }
    }

    void Grow() {
        std::vector<TCell> newCells;
        newCells.resize(Cells_.size() * 2); // must be power of 2
        for (const auto& c : Cells_) {
            if (c.PSL < 0) {
                continue;
            }

            bool isNew;
            auto newBucket = AddBucketFromKeyImpl(c.Key, newCells, isNew);
            auto& nc = newCells[newBucket];
            nc.State = c.State;
        }

        Cells_.swap(newCells);
    }

    double GetAverageHashChainLen() {
        return 1.0 * HashProbes_ / HashSearches_;
    }

    ui32 GetMaxHashChainLen() {
        return MaxHashChainLen_;
    }

    void GetResult(arrow::Datum& keys, arrow::Datum& sums) {
        arrow::Int32Builder keysBuilder;
        arrow::Int64Builder sumsBuilder;
        if (!MoreThanOne_) {
            if (!One_.IsEmpty) {
                ARROW_OK(keysBuilder.Reserve(1));
                ARROW_OK(sumsBuilder.Reserve(1));
                keysBuilder.UnsafeAppend(One_.Key);
                sumsBuilder.UnsafeAppend(One_.State);
            }
        } else {
            ui64 size;
            if constexpr (UseRH) {
                size = Rh_.GetSize();
            } else {
                size = Size_;
            }

            ARROW_OK(keysBuilder.Reserve(size));
            ARROW_OK(sumsBuilder.Reserve(size));
            i32 maxPSL = 0;
            i64 sumPSL = 0;
            if constexpr (UseRH) {
                for (auto iter = Rh_.Begin(); iter != Rh_.End(); Rh_.Advance(iter)) {
                    auto psl = ReadUnaligned<typename decltype(Rh_)::TPSLStorage>(Rh_.GetPslPtr(iter));
                    if (psl.Distance < 0) {
                        continue;
                    }

                    keysBuilder.UnsafeAppend(Rh_.GetKeyValue(iter));
                    sumsBuilder.UnsafeAppend(ReadUnaligned<i64>(Rh_.GetPayloadPtr(iter)));
                    maxPSL = Max(psl.Distance, maxPSL);
                    sumPSL += psl.Distance;
                }
            } else {
                for (const auto& c : Cells_) {
                    if (c.PSL < 0) {
                        continue;
                    }

                    keysBuilder.UnsafeAppend(c.Key);
                    sumsBuilder.UnsafeAppend(c.State);
                    maxPSL = Max(c.PSL, maxPSL);
                    sumPSL += c.PSL;
                }
            }

            if constexpr (CalculateHashStats) {
                Cerr << "maxPSL = " << maxPSL << "\n";
                Cerr << "avgPSL = " << 1.0 * sumPSL / size << "\n";
            }
        }

        std::shared_ptr<arrow::ArrayData> keysData;
        ARROW_OK(keysBuilder.FinishInternal(&keysData));
        keys = keysData;

        std::shared_ptr<arrow::ArrayData> sumsData;
        ARROW_OK(sumsBuilder.FinishInternal(&sumsData));
        sums = sumsData;
    }

private:
    bool MoreThanOne_ = false;
    TOneCell One_;
    std::vector<TCell> Cells_;
    ui64 Size_ = 0;

    const std::vector<IAggregator*> Aggs_;
    ui64 HashProbes_ = 0;
    ui64 HashSearches_ = 0;
    ui32 MaxHashChainLen_ = 0;

    NKikimr::NMiniKQL::TRobinHoodHashMap<i32> Rh_;
    NKikimr::NMiniKQL::TRobinHoodHashSet<i32> Rhs_;
};

void VerifyAggregation(const arrow::Datum& payloads, const arrow::Datum& sums) {
    ui64 totalPayloads = 0;
    for (ui32 i = 0; i < payloads.length(); ++i) {
        totalPayloads += payloads.array()->GetValues<i32>(1)[i];
    }

    ui64 totalSums = 0;
    for (ui32 i = 0; i < sums.length(); ++i) {
        totalSums += sums.array()->GetValues<i64>(1)[i];
    }
    Y_ENSURE(totalPayloads == totalSums);
}

void GroupBy(benchmark::State& state, EDistribution distribution, EShape shape) {
    const ui32 rows = state.range(0);
    const ui32 buckets = state.range(1);
    auto keysColumn = MakeIntColumn(rows, distribution, shape, buckets);
    auto payloads = MakeIntColumn(rows, EDistribution::Linear, EShape::Default, buckets);
    TSumAggregator sum;
    std::vector<IAggregator*> aggregators = {&sum};
    TAggregate<true, true> warmup(aggregators);
    warmup.AddBatch(keysColumn, payloads);
    arrow::Datum keys;
    arrow::Datum sums;
    warmup.GetResult(keys, sums);
    VerifyAggregation(payloads, sums);

    for (auto _ : state) {
        TAggregate<false, true> aggregate(aggregators);
        aggregate.AddBatch(keysColumn, payloads);
        arrow::Datum resultKeys;
        arrow::Datum resultSums;
        aggregate.GetResult(resultKeys, resultSums);
        benchmark::DoNotOptimize(resultKeys);
        benchmark::DoNotOptimize(resultSums);
    }
}

BENCHMARK_CAPTURE(GroupBy, Const_Default, EDistribution::Const, EShape::Default)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, Const_Sqrt, EDistribution::Const, EShape::Sqrt)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, Const_Log, EDistribution::Const, EShape::Log)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, Linear_Default, EDistribution::Linear, EShape::Default)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, Linear_Sqrt, EDistribution::Linear, EShape::Sqrt)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, Linear_Log, EDistribution::Linear, EShape::Log)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, Random_Default, EDistribution::Random, EShape::Default)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, Random_Sqrt, EDistribution::Random, EShape::Sqrt)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, Random_Log, EDistribution::Random, EShape::Log)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, Few_Default, EDistribution::Few, EShape::Default)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, Few_Sqrt, EDistribution::Few, EShape::Sqrt)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, Few_Log, EDistribution::Few, EShape::Log)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, RandomFew_Default, EDistribution::RandomFew, EShape::Default)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, RandomFew_Sqrt, EDistribution::RandomFew, EShape::Sqrt)->Args({1000000, 16});
BENCHMARK_CAPTURE(GroupBy, RandomFew_Log, EDistribution::RandomFew, EShape::Log)->Args({1000000, 16});

} // namespace
