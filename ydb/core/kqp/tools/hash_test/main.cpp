#include <ydb/library/yql/dq/comp_nodes/dq_rh_hash.h>
#include <ydb/library/yql/dq/comp_nodes/type_utils.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/output.h>

struct TIntSequenceGen
{
    TIntSequenceGen(ui64 max)
        : Max(max)
    {
    }

    NYql::NUdf::TUnboxedValue Next() {
        if (Curr >= Max) {
            return NYql::NUdf::TUnboxedValuePod{};
        }
        return NYql::NUdf::TUnboxedValuePod(Curr++);
    }

    NKikimr::NMiniKQL::TKeyTypes Types() {
        return {
            {NYql::NUdf::EDataSlot::Uint64, false}
        };
    }

    ui64 Max;
    ui64 Curr = 0;
};

constexpr static const ui32 BucketBits = 7u;

struct TMaxMin
{
    bool Empty = true;
    ui64 Min = 0;
    ui64 Max = 0;

    void Update(ui64 val) {
        if (Empty) {
            Min = val;
            Max = val;
            Empty = false;
        } else {
            Min = std::min<ui64>(Min, val);
            Max = std::max<ui64>(Max, val);
        }
    }
};

template<>
void Out<TMaxMin>(IOutputStream& stream, const TMaxMin& val) {
    stream << "[" << val.Min << ", " << val.Max << "]";
}

template<typename TGenerator>
void HasherTest(TGenerator& gen, size_t capacityShift)
{
    std::unordered_set<ui64> sampleBucketValues;
    size_t sampleBucketSize = 0;
    size_t totalCount = 0;

    constexpr const size_t NumBuckets = (1ull << BucketBits);
    constexpr const ui64 FibonacciConstant = 11400714819323198485llu;

    char seedbuf[sizeof(char*)];
    memcpy(seedbuf, &gen, sizeof(char*));
    ui64 seed = CityHash64(seedbuf, sizeof(char*));

    TMaxMin bucketDistr;
    TMaxMin bucketItemUpperBounds;
    TMaxMin bucketItemLowerBounds;
    TMaxMin rawHashDistr;
    std::vector<TMaxMin> bucketItemDistr;
    bucketItemDistr.resize(NumBuckets);

    NKikimr::NMiniKQL::TKeyTypes types = gen.Types();
    NKikimr::NMiniKQL::TWideUnboxedHasher hashShuffleHasher(types, 0);
    NKikimr::NMiniKQL::TWideUnboxedHasher hasher(types, seed);

    for (NYql::NUdf::TUnboxedValue val = gen.Next(); val.HasValue(); val = gen.Next()) {
        // Simulate the hash shuffle dispatch to one of the 128 tasks
        const ui64 shuffleHash = hashShuffleHasher(&val) * FibonacciConstant;
        if (shuffleHash % 128 != 100) {
            continue;
        }

        ++totalCount;

        // Simulate the dq_hash_combine bucket selection
        const ui64 hash = hasher(&val);
        const ui64 bucketId = ((hash * FibonacciConstant) >> 32) & (NumBuckets - 1ull);
        bucketDistr.Update(bucketId);
        rawHashDistr.Update(hash);

        // Simulate the dq_rh_hash map
        const ui32 shortHash = static_cast<ui32>(hash);
        const ui32 mapEntry = static_cast<ui32>((static_cast<ui64>(shortHash) * FibonacciConstant)) >> capacityShift;
        bucketItemDistr[bucketId].Update(mapEntry);
        if (bucketId == 0) {
            ++sampleBucketSize;
            sampleBucketValues.insert(mapEntry);
        }
    }

    UNIT_ASSERT(bucketDistr.Min == 0);
    UNIT_ASSERT(bucketDistr.Max == NumBuckets - 1);

    for (size_t i = 0; i < bucketItemDistr.size(); ++i) {
        bucketItemLowerBounds.Update(bucketItemDistr[i].Min);
        bucketItemUpperBounds.Update(bucketItemDistr[i].Max);
    }

    Cout << "Total records after 'hash shuffle': " << totalCount << Endl;
    Cout << "Raw hash bounds: " << rawHashDistr << Endl;
    Cout << "Per-bucket item hashes lower bounds: " << bucketItemLowerBounds << Endl;
    Cout << "Per-bucket item hashes upper bounds: " << bucketItemUpperBounds << Endl;
    Cout << "Per-bucket item hash upper bound expected: " << (1ull << (32 - capacityShift)) - 1 << Endl;
    Cout << "Sample bucket item count: " << sampleBucketSize << ", average should be " << (totalCount / NumBuckets) << Endl;
    Cout << "Sample bucket distinct values: " << sampleBucketValues.size() << Endl;
}

int main([[maybe_unused]] int argc, [[maybe_unused]] const char* argv[])
{
    TIntSequenceGen gen(10ULL * 1000 * 1000 * 1000);
    HasherTest(gen, 8); // Assume capacity ~ 200 mil entries
}
