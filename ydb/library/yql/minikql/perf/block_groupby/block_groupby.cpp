#include <util/datetime/cputimer.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_rh_hash.h>

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/datum.h>

#include <library/cpp/getopt/last_getopt.h>
#include <util/digest/fnv.h>
#include <util/digest/murmur.h>
#include <util/digest/city.h>

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

class TSumAggregator : public IAggregator {
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
    TAggregate(const std::vector<IAggregator*>& aggs)
        : Aggs(aggs)
        , RH(sizeof(i64))
    {
        Cells.resize(1u << 8);
    }

    void AddBatch(arrow::Datum keys, arrow::Datum payloads) {
        auto arrKeys = keys.array();
        auto arrPayloads = payloads.array();
        auto len = arrKeys->length;
        const i32* ptrKeys = arrKeys->GetValues<i32>(1);
        const i32* ptrPayloads = arrPayloads->GetValues<i32>(1);
        for (int64_t i = 0; i < len; ++i) {
            auto key = ptrKeys[i];
            auto payload = ptrPayloads[i];
            if (!MoreThanOne) {
                if (One.IsEmpty) {
                    One.IsEmpty = false;
                    One.Key = key;
                    for (const auto& a : Aggs) {
                        a->Init(&One.State, payload);
                    }

                    Size = 1;
                    continue;
                } else {
                    if (key == One.Key) {
                        for (const auto& a : Aggs) {
                            a->Update(&One.State, payload);
                        }

                        continue;
                    } else {
                        MoreThanOne = true;
                        if constexpr (UseRH) {
                            bool isNew;
                            auto iter = RH.Insert(One.Key, isNew);
                            Y_ASSERT(isNew);
                            *(i64*)RH.GetPayload(iter) = One.State;
                        } else {
                            bool isNew;
                            ui64 bucket = AddBucketFromKeyImpl(One.Key, Cells, isNew);
                            auto& c = Cells[bucket];
                            c.PSL = 0;
                            c.Key = One.Key;
                            c.State = One.State;
                        }
                    }
                }
            }

            if constexpr (UseRH) {
                bool isNew = false;
                auto iter = RH.Insert(key, isNew);
                if (isNew) {
                    for (const auto& a : Aggs) {
                        a->Init((i64*)RH.GetPayload(iter), payload);
                    }

                    RH.CheckGrow();
                } else {
                    for (const auto& a : Aggs) {
                        a->Update((i64*)RH.GetPayload(iter), payload);
                    }
                }
            } else {
                bool isNew = false;
                ui64 bucket = AddBucketFromKey(key, isNew);
                auto& c = Cells[bucket];
                if (isNew) {
                    Size += 1;
                    for (const auto& a : Aggs) {
                        a->Init(&c.State, payload);
                    }

                    if (Size * 2 >= Cells.size()) {
                        Grow();
                    }
                } else {
                    for (const auto& a : Aggs) {
                        a->Update(&c.State, payload);
                    }
                }
            }
        }
    }

    static ui64 MakeHash(i32 key) {
        //auto hash = FnvHash<ui64>(&key, sizeof(key));
        //auto hash = MurmurHash<ui64>(&key, sizeof(key));
        auto hash = CityHash64(TStringBuf((char*)&key, sizeof(key)));
        //auto hash = key;
        return hash;
    }

    Y_FORCE_INLINE ui64 AddBucketFromKey(i32 key, bool& isNew) {
        return AddBucketFromKeyImpl(key, Cells, isNew);
    }

    Y_FORCE_INLINE ui64 AddBucketFromKeyImpl(i32 key, std::vector<TCell>& cells, bool& isNew) {
        isNew = false;
        ui32 chainLen = 0;
        if constexpr (CalculateHashStats) {
            HashSearches++;
        }

        ui64 bucket = MakeHash(key) & (cells.size() - 1);
        i32 distance = 0;
        ui64 returnBucket;
        i64 oldState;
        for (;;) {
            if constexpr (CalculateHashStats) {
                HashProbes++;
                chainLen++;
            }

            if (cells[bucket].PSL < 0) {
                isNew = true;
                cells[bucket].Key = key;
                cells[bucket].PSL = distance;

                if constexpr (CalculateHashStats) {
                    MaxHashChainLen = Max(MaxHashChainLen, chainLen);
                }

                return bucket;
            }

            if (cells[bucket].Key == key) {
                if constexpr (CalculateHashStats) {
                    MaxHashChainLen = Max(MaxHashChainLen, chainLen);
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
                HashProbes++;
                chainLen++;
            }

            if (cells[bucket].PSL < 0) {
                if constexpr (CalculateHashStats) {
                    MaxHashChainLen = Max(MaxHashChainLen, chainLen);
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
        newCells.resize(Cells.size() * 2); // must be power of 2
        for (const auto& c : Cells) {
            if (c.PSL < 0) {
                continue;
            }

            bool isNew;
            auto newBucket = AddBucketFromKeyImpl(c.Key, newCells, isNew);
            auto& nc = newCells[newBucket];
            nc.State = c.State;
        }

        Cells.swap(newCells);
    }

    double GetAverageHashChainLen() {
        return 1.0*HashProbes/HashSearches;
    }

    ui32 GetMaxHashChainLen() {
        return MaxHashChainLen;
    }

    void GetResult(arrow::Datum& keys, arrow::Datum& sums) {
        arrow::Int32Builder keysBuilder;
        arrow::Int64Builder sumsBuilder;
        if (!MoreThanOne) {
            if (!One.IsEmpty) {
                ARROW_OK(keysBuilder.Reserve(1));
                ARROW_OK(sumsBuilder.Reserve(1));
                keysBuilder.UnsafeAppend(One.Key);
                sumsBuilder.UnsafeAppend(One.State);
            }
        } else {
            ui64 size;
            if constexpr (UseRH) {
                size = RH.GetSize();
            } else {
                size = Size;
            }

            ARROW_OK(keysBuilder.Reserve(size));
            ARROW_OK(sumsBuilder.Reserve(size));
            i32 maxPSL = 0;
            i64 sumPSL = 0;
            if constexpr (UseRH) {
                for (auto iter = RH.Begin(); iter != RH.End(); RH.Advance(iter)) {
                    auto& psl = RH.GetPSL(iter);
                    if (psl.Distance < 0) {
                        continue;
                    }

                    keysBuilder.UnsafeAppend(RH.GetKey(iter));
                    sumsBuilder.UnsafeAppend(*(i64*)RH.GetPayload(iter));
                    maxPSL = Max(psl.Distance, maxPSL);
                    sumPSL += psl.Distance;
                }
            } else {
                for (const auto& c : Cells) {
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
                Cerr << "avgPSL = " << 1.0*sumPSL/size << "\n";
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
    bool MoreThanOne = false;
    TOneCell One;
    std::vector<TCell> Cells;
    ui64 Size = 0;

    const std::vector<IAggregator*> Aggs;
    ui64 HashProbes = 0;
    ui64 HashSearches = 0;
    ui32 MaxHashChainLen = 0;

    NKikimr::NMiniKQL::TRobinHoodHashMap<i32> RH;
    NKikimr::NMiniKQL::TRobinHoodHashSet<i32> RHS;
};

int main(int argc, char** argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    TString keysDistributionStr;
    TString shapeStr="default";
    ui32 nIters = 100;
    ui32 nRows = 1000000;
    ui32 nBuckets = 16;
    ui32 nRepeats = 10;
    opts.AddLongOption('k', "keys", "distribution of keys (const, linear, random, few, randomfew)").StoreResult(&keysDistributionStr).Required();
    opts.AddLongOption('s', "shape", "shape of counter (default, sqrt, log)").StoreResult(&shapeStr);
    opts.AddLongOption('i', "iter", "# of iterations").StoreResult(&nIters);
    opts.AddLongOption('r', "rows", "# of rows").StoreResult(&nRows);
    opts.AddLongOption('b', "buckets", "modulo for few/randomfew").StoreResult(&nBuckets);
    opts.AddLongOption('t', "repeats", "# of repeats").StoreResult(&nRepeats);
    opts.SetFreeArgsMax(0);
    NLastGetopt::TOptsParseResult res(&opts, argc, argv);
    EDistribution keysDist;
    EShape shape = EShape::Default;
    if (keysDistributionStr == "const") {
        keysDist = EDistribution::Const;
    } else if (keysDistributionStr == "linear") {
        keysDist = EDistribution::Linear;
    } else if (keysDistributionStr == "random") {
        keysDist = EDistribution::Random;
    } else if (keysDistributionStr == "few") {
        keysDist = EDistribution::Few;
    } else if (keysDistributionStr == "randomfew") {
        keysDist = EDistribution::RandomFew;
    } else {
        ythrow yexception() << "Unsupported distribution: " << keysDistributionStr;
    }

    if (shapeStr == "default") {
        shape = EShape::Default;
    } else if (shapeStr == "sqrt") {
        shape = EShape::Sqrt;
    } else if (shapeStr == "log") {
        shape = EShape::Log;
    } else {
        ythrow yexception() << "Unsupported shape: " << shapeStr;
    }

    auto col1 = MakeIntColumn(nRows, keysDist, shape, nBuckets);
    auto col2 = MakeIntColumn(nRows, EDistribution::Linear, EShape::Default, nBuckets);
    Cerr << "col1.length: " << col1.length() << "\n";
    Cerr << "col2.length: " << col2.length() << "\n";

    TSumAggregator sum;
    std::vector<IAggregator*> aggs;
    aggs.push_back(&sum);
    TAggregate<true, true> agg(aggs);
    agg.AddBatch(col1, col2);
    arrow::Datum keys, sums;
    agg.GetResult(keys, sums);
    ui64 total1 = 0;
    for (ui32 i = 0; i < col2.length(); ++i) {
        total1 += col2.array()->GetValues<i32>(1)[i];
    }

    Cerr << "total1: " << total1 << "\n";
    ui64 total2 = 0;

    Cerr << "keys.length: " << keys.length() << "\n";
    Cerr << "sums.length: " << sums.length() << "\n";
    for (ui32 i = 0; i < sums.length(); ++i) {
        total2 += sums.array()->GetValues<i64>(1)[i];
    }

    Cerr << "total2: " << total2 << "\n";
    Y_ENSURE(total1 == total2);
    Cerr << "AverageHashChainLen: " << agg.GetAverageHashChainLen() << "\n";
    Cerr << "MaxHashChainLen: " << agg.GetMaxHashChainLen() << "\n";

    std::vector<double> durations;
    for (ui32 j = 0; j < nRepeats; ++j) {
        TSimpleTimer timer;
        for (ui32 i = 0; i < nIters; ++i) {
            TAggregate<false, true> agg(aggs);
            agg.AddBatch(col1, col2);
            arrow::Datum keys, sums;
            agg.GetResult(keys, sums);
        }

        auto duration = timer.Get();
        durations.push_back(1e-6*duration.MicroSeconds());
    }

    double sumDurations = 0.0, sumDurationsQ = 0.0;
    for (auto d : durations) {
        sumDurations += d;
        sumDurationsQ += d * d;
    }

    double avgDuration = sumDurations / nRepeats;
    double dispDuration = sqrt(sumDurationsQ / nRepeats - avgDuration * avgDuration);
    Cerr << "Elapsed: " << avgDuration << ", noise: " << 100*dispDuration/avgDuration << "%\n";
    Cerr << "Speed: " << 1e-6 * (ui64(nIters) * nRows / avgDuration) << " M rows/sec\n";
    Cerr << "Speed: " << 1e-6 * (2 * sizeof(i32) * ui64(nIters) * nRows / avgDuration) << " M bytes/sec\n";
    return 0;
}
