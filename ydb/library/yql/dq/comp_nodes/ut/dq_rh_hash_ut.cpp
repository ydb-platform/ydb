#include <ydb/library/yql/dq/comp_nodes/dq_rh_hash.h>
#include <ydb/library/yql/dq/comp_nodes/type_utils.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>

#include <limits>

using NYql::NUdf::TUnboxedValue;
using NYql::NUdf::TUnboxedValuePod;
using NYql::NUdf::EDataSlot;
using NKikimr::NMiniKQL::TDqRobinHoodHashSet;
using NKikimr::NMiniKQL::TWideUnboxedHasherFib;
using NKikimr::NMiniKQL::TWideUnboxedEqual;
using NKikimr::NMiniKQL::TKeyTypes;
using NKikimr::NMiniKQL::TScopedAlloc;

// Generate a bunch of distinct integral values for key in [0..2^16)
template<typename T>
TUnboxedValue EncodeUint(ui64 key, bool largeValues) {
    if (largeValues && sizeof(T) > 2) {
        key <<= (sizeof(T) * 8 / 2);
    }
    if (key > std::numeric_limits<T>::max()) {
        key = key % std::numeric_limits<T>::max();
    }
    return TUnboxedValuePod(static_cast<T>(key));
}

template<typename T>
TUnboxedValue EncodeInt(ui64 key, bool largeValues) {
    if (largeValues && sizeof(T) > 2) {
        key <<= (sizeof(T) * 8 / 2);
    }
    if constexpr (sizeof(T) < sizeof(ui64)) {
        if (key / 2 > static_cast<ui64>(std::numeric_limits<T>::max())) {
            key = key % (static_cast<ui64>(std::numeric_limits<T>::max()) * 2u + 2u);
        }
    }

    T signedKey;
    if (key == 0) {
        signedKey = 0;
    } else {
        signedKey = (key % 2 == 0) ? static_cast<T>((key) >> 1) : static_cast<T>(-((key + 1) >> 1));
    }

    return TUnboxedValuePod(signedKey);
}

void GenerateSamples(TKeyTypes& types, ui64 key, TArrayRef<TUnboxedValue> result, bool largeValues)
{
    TUnboxedValue* it = result.begin();

    for (auto type : types) {
        UNIT_ASSERT(it < result.end());
        TUnboxedValue& next = *it;

        key = (key + 1) % (1 << 16);

        switch (type.first) {
        case EDataSlot::Bool:
            next = TUnboxedValuePod(static_cast<bool>((key % 2) == 0));
            break;
        case EDataSlot::Uint16:
            next = EncodeUint<ui16>(key, largeValues);
            break;
        case EDataSlot::Uint32:
            next = EncodeUint<ui32>(key, largeValues);
            break;
        case EDataSlot::Uint64:
            next = EncodeUint<ui64>(key, largeValues);
            break;
        case EDataSlot::Int16:
            next = EncodeInt<i16>(key, largeValues);
            break;
        case EDataSlot::Int32:
            next = EncodeInt<i32>(key, largeValues);
            break;
        case EDataSlot::Int64:
            next = EncodeInt<i64>(key, largeValues);
            break;
        case EDataSlot::Float:
            next = TUnboxedValuePod(static_cast<float>(key));
            break;
        case EDataSlot::Double:
            next = TUnboxedValuePod(static_cast<double>(key));
            break;
        case EDataSlot::String:
            next = TUnboxedValuePod(NYql::NUdf::TStringValue(Sprintf("%08" PRIu64 ".%08" PRIu64, key, key)));
            break;
        default:
            UNIT_FAIL(TStringBuilder() << "can't generate values of type " << type.first);
        }

        ++it;
    }
}

using TRHMap = TDqRobinHoodHashSet<TUnboxedValue*, NKikimr::NMiniKQL::TWideUnboxedEqual, std::allocator<char>>;

// Number of buckets used to estimate how evenly the entries are spread across the table.
static constexpr ui32 NumBuckets = 256;

// Runs the generate/insert/measure loop for a single set of input types.
void TestDistributionScenario(TKeyTypes types, ui64 samples, bool largeValues) {
    size_t nullableCount = 0;
    for (const auto& type : types) {
        nullableCount += type.second ? 1 : 0;
    }

    // Also add all the combinations with null values (use key == 0 for non-null values).
    // The mask == 0 combination (all zeroes, no nulls) is skipped because it duplicates
    // the key == 0 sample, so we add (2^nullableCount - 1) extra entries, all containing at least one null.
    size_t nullCombos = nullableCount ? ((size_t(1) << nullableCount) - 1) : 0;

    size_t samplesWithNulls = samples + nullCombos;

    std::vector<TUnboxedValue> store;
    store.resize(types.size() * samplesWithNulls);

    TWideUnboxedHasherFib<true> hasher(types);

    TRHMap map(TWideUnboxedEqual(types), samples * 2);

    for (ui64 i = 0; i < samples; ++i) {
        TUnboxedValue* valuesPtr = store.data() + types.size() * i;
        GenerateSamples(types, i, TArrayRef<TUnboxedValue>(valuesPtr, types.size()), largeValues);
    }

    for (size_t mask = 1; mask <= nullCombos; ++mask) {
        size_t i = mask - 1 + samples;

        TUnboxedValue* valuesPtr = store.data() + (types.size() * i);
        GenerateSamples(types, 0, TArrayRef<TUnboxedValue>(valuesPtr, types.size()), largeValues);

        size_t nullableBit = 0;
        for (ui32 j = 0; j < types.size(); ++j) {
            if (types[j].second) {
                if (mask & (size_t(1) << nullableBit)) {
                    valuesPtr[j] = TUnboxedValuePod();
                }
                ++nullableBit;
            }
        }
    }

    for (size_t i = 0; i < store.size(); i += types.size()) {
        bool isNew = false;
        TUnboxedValue* valuesPtr = store.data() + i;
        // Hasher params & the bit shift follow the real-world usage pattern of this map
        // (not extracted into a function yet)
        map.Insert(valuesPtr, hasher(valuesPtr) >> 32, isNew);
        UNIT_ASSERT(isNew);
    }

    UNIT_ASSERT(map.GetSize() == samplesWithNulls);

    // Collect hash distribution stats into a set of buckets
    // Also collect the max PSL over the entire map
    std::vector<size_t> bucketSizes(NumBuckets, 0);
    char* iter = map.Begin();
    char* end = map.End();
    size_t capacity = map.GetCapacity();

    i32 maxPsl = 0;
    size_t idx = 0;
    while (iter != end) {
        if (map.IsValid(iter)) {
            size_t bucket = idx * NumBuckets / capacity;
            ++bucketSizes[bucket];

            TRHMap::TPSLStorage psl = ReadUnaligned<TRHMap::TPSLStorage>(iter);
            // collect the max of psl.Distance
            maxPsl = std::max(maxPsl, psl.Distance);
        }

        map.Advance(iter);
        idx++;
    }

    size_t maxBucket = 0;
    size_t minBucket = std::numeric_limits<size_t>::max();
    for (size_t b : bucketSizes) {
        maxBucket = std::max(maxBucket, b);
        minBucket = std::min(minBucket, b);
    }

    /*
    for (const auto& type : types) {
        Cerr << type.first << ", " << type.second << Endl;
    }

    Cerr << "bucket spread (max - min): " << (maxBucket - minBucket)
         << " (max=" << maxBucket << ", min=" << minBucket << ", buckets=" << NumBuckets << ")" << Endl;
    Cerr << maxPsl << Endl;
    */

    UNIT_ASSERT_C(maxBucket - minBucket < 150, "Hash distribution is unbalanced");
    UNIT_ASSERT_C(maxPsl < 15, "Hash table max PSL is too large");
}

Y_UNIT_TEST_SUITE(TDqRhHashTest) {
    Y_UNIT_TEST_TWIN(TestDistribution, LargeValues) {
        // The test should run the initialization/verification loop for every single-value case
        // (nullable and non-nullable types - 9 types each currently excluding bool which has too few values to compute any distributions)
        // and for every two-value case (with types.size() == 2) with either both types being non-nullable
        // and both types being nullable (99 type combinations for non-nullable types, 99 type combinations with nullable types - except <bool, bool>)
        // Should be fast enough for 64ki samples

        // String values are allocated via the MiniKQL allocator, so a scope must be bound.
        // It must outlive the per-scenario stores holding the TUnboxedValues.
        TScopedAlloc alloc(__LOCATION__);

        ui64 samples = 1 << 16;

        // Single-value types
        static const EDataSlot singleValueTypes[] = {
            EDataSlot::Uint16,
            EDataSlot::Uint32,
            EDataSlot::Uint64,
            EDataSlot::Int16,
            EDataSlot::Int32,
            EDataSlot::Int64,
            EDataSlot::Float,
            EDataSlot::Double,
            EDataSlot::String,
        };

        // Two-value types: bool is included
        static const EDataSlot tupleValueTypes[] = {
            EDataSlot::Bool,
            EDataSlot::Uint16,
            EDataSlot::Uint32,
            EDataSlot::Uint64,
            EDataSlot::Int16,
            EDataSlot::Int32,
            EDataSlot::Int64,
            EDataSlot::Float,
            EDataSlot::Double,
            EDataSlot::String,
        };

        // Skip {Bool} and {Bool, Bool} cases because we need at least 2^16 distinct values

        // Single-value cases: non-nullable and nullable variants of every type
        for (bool nullable : {false, true}) {
            for (EDataSlot slot : singleValueTypes) {
                TestDistributionScenario(TKeyTypes{{slot, nullable}}, samples, LargeValues);
            }
        }

        // Two-value cases: every ordered pair of types, both non-nullable or both nullable
        for (bool nullable : {false, true}) {
            for (EDataSlot first : tupleValueTypes) {
                for (EDataSlot second : tupleValueTypes) {
                    if (first == EDataSlot::Bool && second == EDataSlot::Bool) {
                        continue;
                    }
                    TestDistributionScenario(TKeyTypes{{first, nullable}, {second, nullable}}, samples, LargeValues);
                }
            }
        }
    }
}
