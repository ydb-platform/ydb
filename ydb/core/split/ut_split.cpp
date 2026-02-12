
#include "split.h"

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <ydb/core/tablet_flat/flat_stat_table.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>

using namespace NKikimr;
using namespace NKikimr::NSplitMerge;

namespace {

const NScheme::TTypeRegistry TypeRegistry;

using TTestKey = std::vector<std::string>;

// Helper function to create cells from test keys
TSerializedCellVec MakeCells(const TTestKey& tuple, const TVector<NScheme::TTypeInfo>& keyColumnTypes) {
    Y_ABORT_UNLESS(tuple.size() <= keyColumnTypes.size());

    TSmallVec<TCell> cells;
    for (size_t i = 0; i < tuple.size(); ++i) {
        if (tuple[i] == "NULL") {
            cells.push_back(TCell());
        } else {
            switch (keyColumnTypes[i].GetTypeId()) {
            case NScheme::NTypeIds::Bool:
                cells.emplace_back(TCell::Make(FromString<bool>(tuple[i])));
                break;
            case NScheme::NTypeIds::Uint8:
                cells.emplace_back(TCell::Make(FromString<ui8>(tuple[i])));
                break;
            case NScheme::NTypeIds::Int8:
                cells.emplace_back(TCell::Make(FromString<i8>(tuple[i])));
                break;
            case NScheme::NTypeIds::Uint16:
                cells.emplace_back(TCell::Make(FromString<ui16>(tuple[i])));
                break;
            case NScheme::NTypeIds::Int16:
                cells.emplace_back(TCell::Make(FromString<i16>(tuple[i])));
                break;
            case NScheme::NTypeIds::Uint32:
                cells.emplace_back(TCell::Make(FromString<ui32>(tuple[i])));
                break;
            case NScheme::NTypeIds::Int32:
                cells.emplace_back(TCell::Make(FromString<i32>(tuple[i])));
                break;
            case NScheme::NTypeIds::Uint64:
                cells.emplace_back(TCell::Make(FromString<ui64>(tuple[i])));
                break;
            case NScheme::NTypeIds::Int64:
                cells.emplace_back(TCell::Make(FromString<i64>(tuple[i])));
                break;
            case NScheme::NTypeIds::Double:
                cells.emplace_back(TCell::Make(FromString<double>(tuple[i])));
                break;
            case NScheme::NTypeIds::Float:
                cells.emplace_back(TCell::Make(FromString<float>(tuple[i])));
                break;
            case NScheme::NTypeIds::Date:
                cells.emplace_back(TCell::Make(FromString<ui16>(tuple[i])));
                break;
            case NScheme::NTypeIds::Datetime:
                cells.emplace_back(TCell::Make(FromString<ui32>(tuple[i])));
                break;
            case NScheme::NTypeIds::Timestamp:
                cells.emplace_back(TCell::Make(FromString<ui64>(tuple[i])));
                break;
            case NScheme::NTypeIds::Interval:
                cells.emplace_back(TCell::Make(FromString<i64>(tuple[i])));
                break;
            case NScheme::NTypeIds::Date32:
            case NScheme::NTypeIds::Datetime64:
            case NScheme::NTypeIds::Timestamp64:
            case NScheme::NTypeIds::Interval64:
                cells.emplace_back(TCell::Make(FromString<i64>(tuple[i])));
                break;
            case NScheme::NTypeIds::String:
            case NScheme::NTypeIds::Utf8:
                cells.push_back(TCell(tuple[i].data(), tuple[i].size()));
                break;
            default:
                Y_ABORT("Unexpected type");
            }
        }
    }

    return TSerializedCellVec(cells);
}

[[maybe_unused]] TString PrintKey(const TSerializedCellVec& key, const TVector<NScheme::TTypeInfo>& keyColumnTypes) {
    const auto& cells = key.GetCells();
    return DbgPrintTuple(TDbTupleRef(keyColumnTypes.data(), cells.data(), cells.size()), TypeRegistry);
}

bool IsEmpty(const TSerializedCellVec& key) {
    return key.GetCells().empty();
}

// Helper to create data size histogram from test data
NTable::THistogram MakeHistogram(const TVector<std::pair<TTestKey, ui64>>& data,
                                  const TVector<NScheme::TTypeInfo>& keyColumnTypes) {
    NTable::THistogram hist;
    for (const auto& [key, value] : data) {
        hist.push_back({MakeCells(key, keyColumnTypes).GetBuffer(), value});
    }
    return hist;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TSplitBuildKeyAccessHistogram) {
    Y_UNIT_TEST(EmptyHistogram) {
        TKeyAccessHistogram hist;
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);
        UNIT_ASSERT_VALUES_EQUAL(hist.size(), 0);
    }

    Y_UNIT_TEST(SingleEntry) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"100"}, keyTypes), 10}
        };

        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);
        UNIT_ASSERT_VALUES_EQUAL(hist.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[0].first, keyTypes), "(Uint32 : 100)");
        UNIT_ASSERT_VALUES_EQUAL(hist[0].second, 10);
    }

    Y_UNIT_TEST(SortingAndAccumulation) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"300"}, keyTypes), 5},
            {MakeCells({"100"}, keyTypes), 10},
            {MakeCells({"200"}, keyTypes), 15}
        };

        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        UNIT_ASSERT_VALUES_EQUAL(hist.size(), 3);
        // Should be sorted and accumulated
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[0].first, keyTypes), "(Uint32 : 100)");
        UNIT_ASSERT_VALUES_EQUAL(hist[0].second, 10);  // 100: 10
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[1].first, keyTypes), "(Uint32 : 200)");
        UNIT_ASSERT_VALUES_EQUAL(hist[1].second, 25);  // 200: 10+15
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[2].first, keyTypes), "(Uint32 : 300)");
        UNIT_ASSERT_VALUES_EQUAL(hist[2].second, 30);  // 300: 10+15+5
    }

    Y_UNIT_TEST(MergeDuplicateKeys) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"100"}, keyTypes), 10},
            {MakeCells({"100"}, keyTypes), 5},
            {MakeCells({"200"}, keyTypes), 15}
        };

        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        UNIT_ASSERT_VALUES_EQUAL(hist.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[0].first, keyTypes), "(Uint32 : 100)");
        UNIT_ASSERT_VALUES_EQUAL(hist[0].second, 15);  // 100: merged 10+5
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[1].first, keyTypes), "(Uint32 : 200)");
        UNIT_ASSERT_VALUES_EQUAL(hist[1].second, 30);  // 200: 15+15
    }

    Y_UNIT_TEST(MergeDuplicateUnsortedKeys) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"200"}, keyTypes), 1},
            {MakeCells({"100"}, keyTypes), 1},
            {MakeCells({"300"}, keyTypes), 1},
            {MakeCells({"300"}, keyTypes), 1},
            {MakeCells({"100"}, keyTypes), 1},
        };

        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        UNIT_ASSERT_VALUES_EQUAL(hist.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[0].first, keyTypes), "(Uint32 : 100)");
        UNIT_ASSERT_VALUES_EQUAL(hist[0].second, 2);  // 100: merged 1+1
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[1].first, keyTypes), "(Uint32 : 200)");
        UNIT_ASSERT_VALUES_EQUAL(hist[1].second, 3);  // 200: 2+1
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[2].first, keyTypes), "(Uint32 : 300)");
        UNIT_ASSERT_VALUES_EQUAL(hist[2].second, 5);  // 300: 3+2
    }

    Y_UNIT_TEST(CompositeKeys) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"200", "b"}, keyTypes), 5},
            {MakeCells({"100", "a"}, keyTypes), 10},
            {MakeCells({"100", "b"}, keyTypes), 7},
            {MakeCells({"200", "a"}, keyTypes), 3}
        };

        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        UNIT_ASSERT_VALUES_EQUAL(hist.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[0].first, keyTypes), "(Uint32 : 100, Utf8 : a)");
        UNIT_ASSERT_VALUES_EQUAL(hist[0].second, 10);  // (100, a)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[1].first, keyTypes), "(Uint32 : 100, Utf8 : b)");
        UNIT_ASSERT_VALUES_EQUAL(hist[1].second, 17);  // (100, b)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[2].first, keyTypes), "(Uint32 : 200, Utf8 : a)");
        UNIT_ASSERT_VALUES_EQUAL(hist[2].second, 20);  // (200, a)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[3].first, keyTypes), "(Uint32 : 200, Utf8 : b)");
        UNIT_ASSERT_VALUES_EQUAL(hist[3].second, 25);  // (200, b)
    }

    Y_UNIT_TEST(KeysWithDifferentLengths) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"100"}, keyTypes), 10},
            {MakeCells({"100", "a"}, keyTypes), 5},
            {MakeCells({"200"}, keyTypes), 15}
        };

        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        // Shorter keys are treated as having +inf for missing columns
        UNIT_ASSERT_VALUES_EQUAL(hist.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[0].first, keyTypes), "(Uint32 : 100, Utf8 : a)");
        UNIT_ASSERT_VALUES_EQUAL(hist[0].second, 5);   // (100, a)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[1].first, keyTypes), "(Uint32 : 100)");
        UNIT_ASSERT_VALUES_EQUAL(hist[1].second, 15);  // (100, +inf)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[2].first, keyTypes), "(Uint32 : 200)");
        UNIT_ASSERT_VALUES_EQUAL(hist[2].second, 30);  // (200, +inf)
    }

    Y_UNIT_TEST(NullValues) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"100", "NULL"}, keyTypes), 10},
            {MakeCells({"100", "a"}, keyTypes), 5},
            {MakeCells({"NULL", "a"}, keyTypes), 3}
        };

        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        UNIT_ASSERT_VALUES_EQUAL(hist.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[0].first, keyTypes), "(Uint32 : NULL, Utf8 : a)");
        UNIT_ASSERT_VALUES_EQUAL(hist[0].second, 3);
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[1].first, keyTypes), "(Uint32 : 100, Utf8 : NULL)");
        UNIT_ASSERT_VALUES_EQUAL(hist[1].second, 13);
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(hist[2].first, keyTypes), "(Uint32 : 100, Utf8 : a)");
        UNIT_ASSERT_VALUES_EQUAL(hist[2].second, 18);
    }
}

Y_UNIT_TEST_SUITE(TSplitSelectShortestMedianKeyPrefixByLoad) {
    Y_UNIT_TEST(EmptyHistogram) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist;
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(IsEmpty(result));
    }

    Y_UNIT_TEST(SingleEntry) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"100"}, keyTypes), 10}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(IsEmpty(result));
    }

    Y_UNIT_TEST(MedianAtBoundaryLow) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"100"}, keyTypes), 50},
            {MakeCells({"200"}, keyTypes), 1},
            {MakeCells({"300"}, keyTypes), 1}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(IsEmpty(result));
    }

    Y_UNIT_TEST(MedianAtBoundaryHigh) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"100"}, keyTypes), 1},
            {MakeCells({"200"}, keyTypes), 1},
            {MakeCells({"300"}, keyTypes), 50}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(IsEmpty(result));
    }

    Y_UNIT_TEST(ValidSplitSingleColumn) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"100"}, keyTypes), 10},
            {MakeCells({"200"}, keyTypes), 10},
            {MakeCells({"300"}, keyTypes), 10}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Should select key around median (200)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 200)");
    }

    Y_UNIT_TEST(ValidSplitCompositeKey) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"100", "a"}, keyTypes), 10},
            {MakeCells({"200", "b"}, keyTypes), 10},
            {MakeCells({"300", "c"}, keyTypes), 10}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Should select median key with shortest prefix (200, NULL)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 200, Utf8 : NULL)");
    }

    Y_UNIT_TEST(ShortestPrefixSelection) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        // Keys differ in first column - should use 1-column prefix
        TKeyAccessHistogram hist = {
            {MakeCells({"100", "1", "1"}, keyTypes), 10},
            {MakeCells({"200", "2", "2"}, keyTypes), 10},
            {MakeCells({"300", "3", "3"}, keyTypes), 10}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Should use shortest prefix (1 column) padded with NULLs
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 200, Uint32 : NULL, Uint32 : NULL)");
    }

    Y_UNIT_TEST(RequiresLongerPrefix) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        // Keys have same first column - need 2-column prefix
        TKeyAccessHistogram hist = {
            {MakeCells({"100", "1"}, keyTypes), 10},
            {MakeCells({"100", "2"}, keyTypes), 10},
            {MakeCells({"100", "3"}, keyTypes), 10}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Need full 2-column prefix since first column is same
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 100, Uint32 : 2)");
    }

    Y_UNIT_TEST(MedianWithEvenEntries) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"100"}, keyTypes), 10},
            {MakeCells({"200"}, keyTypes), 10},
            {MakeCells({"300"}, keyTypes), 10},
            {MakeCells({"400"}, keyTypes), 10}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));
        // Total 40, median at 20, upper_bound finds first > 20 which is 300
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 300)");
    }

    Y_UNIT_TEST(MedianTieInCumulativeWeights) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"100"}, keyTypes), 10},
            {MakeCells({"200"}, keyTypes), 10},  // Cumulative: 20 (exactly 50%)
            {MakeCells({"300"}, keyTypes), 10},
            {MakeCells({"400"}, keyTypes), 10}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));
        // upper_bound(20) finds first > 20, which is 300
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 300)");
    }

    Y_UNIT_TEST(AllNullKeysNoSplit) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"NULL", "NULL"}, keyTypes), 10},
            {MakeCells({"NULL", "NULL"}, keyTypes), 10},
            {MakeCells({"NULL", "NULL"}, keyTypes), 10}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        // All same key - no valid split
        UNIT_ASSERT(IsEmpty(result));
    }

    Y_UNIT_TEST(NullInFirstColumnSplit) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"NULL", "a"}, keyTypes), 10},
            {MakeCells({"100", "b"}, keyTypes), 10},
            {MakeCells({"200", "c"}, keyTypes), 10}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));
        // NULL sorts first, median at 100
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 100, Utf8 : NULL)");
    }

    Y_UNIT_TEST(UnbalancedDistribution) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"100"}, keyTypes), 5},
            {MakeCells({"200"}, keyTypes), 20},
            {MakeCells({"300"}, keyTypes), 5}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median is at 200 (cumulative: 5, 25, 30)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 200)");
    }

    Y_UNIT_TEST(ManyKeys) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist;
        for (ui32 i = 0; i < 100; ++i) {
            hist.push_back({MakeCells({ToString(i * 10)}, keyTypes), 1});
        }
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median at index 50 (upper_bound finds first > 50)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 500)");
    }

    Y_UNIT_TEST(StringKeys) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"apple"}, keyTypes), 10},
            {MakeCells({"banana"}, keyTypes), 10},
            {MakeCells({"cherry"}, keyTypes), 10}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median is "banana" (strings print without quotes)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Utf8 : banana)");
    }

    Y_UNIT_TEST(MixedTypes) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
            NScheme::TTypeInfo(NScheme::NTypeIds::Int64)
        };

        TKeyAccessHistogram hist = {
            {MakeCells({"100", "a", "1000"}, keyTypes), 10},
            {MakeCells({"200", "b", "2000"}, keyTypes), 10},
            {MakeCells({"300", "c", "3000"}, keyTypes), 10}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // First column differs, so shortest prefix with NULLs
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 200, Utf8 : NULL, Int64 : NULL)");
    }

    Y_UNIT_TEST(VeryLargeHistogram) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)
        };

        TKeyAccessHistogram hist;
        for (ui64 i = 0; i < 10000; ++i) {
            hist.push_back({MakeCells({ToString(i)}, keyTypes), 1});
        }
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median at index 5000 (upper_bound finds first > 5000)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint64 : 5000)");
    }

    Y_UNIT_TEST(ManyColumnsKey) {
        TVector<NScheme::TTypeInfo> keyTypes;
        for (size_t i = 0; i < 10; ++i) {
            keyTypes.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32));
        }

        TKeyAccessHistogram hist;
        for (ui32 i = 0; i < 100; ++i) {
            TTestKey key;
            for (size_t col = 0; col < 10; ++col) {
                key.push_back(ToString(i + col * 100));
            }
            hist.push_back({MakeCells(key, keyTypes), 1});
        }
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // First column differs, shortest prefix with NULLs
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 50, Uint32 : NULL, Uint32 : NULL, Uint32 : NULL, Uint32 : NULL, Uint32 : NULL, Uint32 : NULL, Uint32 : NULL, Uint32 : NULL, Uint32 : NULL)");
    }

    Y_UNIT_TEST(LongStringKeys) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        TKeyAccessHistogram hist;
        for (ui32 i = 0; i < 100; ++i) {
            TString longKey(1000, 'a' + (i % 26));
            longKey += ToString(i);
            hist.push_back({MakeCells({longKey}, keyTypes), 1});
        }
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // After sorting: 'a' keys (4), 'b' keys (4), ..., 'l' keys (4) = 48 total
        // 'm' keys at cumulative 49,50,51,52. upper_bound(50) finds cumulative 51 (3rd 'm' key = i=64)
        TString expectedKey(1000, 'm');
        expectedKey += "64";
        auto expected = MakeCells({expectedKey}, keyTypes);
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), PrintKey(expected, keyTypes));
    }

    Y_UNIT_TEST(MixedNullAndNonNull) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        TKeyAccessHistogram hist;
        for (ui32 i = 0; i < 50; ++i) {
            hist.push_back({MakeCells({ToString(i), "NULL"}, keyTypes), 1});
            hist.push_back({MakeCells({ToString(i), "value"}, keyTypes), 1});
        }
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median around (25, NULL) - shortest prefix
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 25, Utf8 : NULL)");
    }

    Y_UNIT_TEST(AllTypesComposite) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Bool),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint8),
            NScheme::TTypeInfo(NScheme::NTypeIds::Int16),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Int64),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
            NScheme::TTypeInfo(NScheme::NTypeIds::Double)
        };

        TKeyAccessHistogram hist;
        for (ui32 i = 0; i < 100; ++i) {
            hist.push_back({
                MakeCells({
                    ToString(i % 2),
                    ToString(i % 256),
                    ToString(i),
                    ToString(i * 100),
                    ToString(i * 1000),
                    TString("key") + ToString(i),
                    ToString(i * 1.5)
                }, keyTypes),
                1
            });
        }
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // After sorting: Bool=0 keys (50 entries), then Bool=1 keys (50 entries)
        // Median at index 50 (first Bool=1 key, which is i=1): Bool=1, Uint8=1
        // Since med != lo (Bool differs), we use the median key's first column
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Bool : true, Uint8 : 1, Int16 : NULL, Uint32 : NULL, Int64 : NULL, Utf8 : NULL, Double : NULL)");
    }

    Y_UNIT_TEST(PrefixBinarySearchConvergence) {
        // P1 IMPORTANT: Test binary search convergence in prefix selection
        // Tests key_access.cpp:118-126 - while (minPrefix + 1 < maxPrefix)
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist;
        hist.push_back({MakeCells({"100", "100", "100", "1"}, keyTypes), 10});
        hist.push_back({MakeCells({"100", "100", "100", "2"}, keyTypes), 20});
        hist.push_back({MakeCells({"100", "100", "100", "3"}, keyTypes), 30});
        hist.push_back({MakeCells({"200", "200", "200", "1"}, keyTypes), 40});
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));
        // Binary search finds that full 4-column prefix is needed
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 100, Uint32 : 100, Uint32 : 100, Uint32 : 3)");
    }

    Y_UNIT_TEST(LoadBased10And90PercentExact) {
        // P2 MODERATE: Test exact 10% and 90% boundaries in load-based splitting
        // Tests key_access.cpp:67-69 - upper_bound at total*0.1 and total*0.9
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist;
        hist.push_back({MakeCells({"100"}, keyTypes), 10});  // Exactly 10%
        hist.push_back({MakeCells({"200"}, keyTypes), 50});
        hist.push_back({MakeCells({"300"}, keyTypes), 90});  // Exactly 90%
        hist.push_back({MakeCells({"400"}, keyTypes), 100});
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));
        // upper_bound finds the element after 50%, which is at cumulative 90
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 300)");
    }

    Y_UNIT_TEST(RealWorldScenarioUserTable) {
        // Simulate a user table with (user_id, timestamp) key
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),  // user_id
            NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp) // timestamp
        };

        TKeyAccessHistogram hist;
        for (ui64 userId = 1; userId <= 100; ++userId) {
            for (ui64 ts = 1000; ts <= 1010; ts += 5) {
                hist.push_back({
                    MakeCells({ToString(userId), ToString(ts)}, keyTypes),
                    1
                });
            }
        }
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median at index 51 (upper_bound finds first > 50%)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint64 : 51, Timestamp : NULL)");
    }

    Y_UNIT_TEST(HighCardinalityFirstColumn) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist;
        for (ui64 i = 0; i < 1000; ++i) {
            hist.push_back({
                MakeCells({ToString(i * 1000), "1"}, keyTypes),
                1
            });
        }
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median at index 500 (upper_bound finds first > 50%)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint64 : 500000, Uint32 : NULL)");
    }

    Y_UNIT_TEST(LowCardinalityFirstColumn) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)
        };

        TKeyAccessHistogram hist;
        // Only 3 distinct values in first column
        for (ui32 first = 1; first <= 3; ++first) {
            for (ui64 second = 0; second < 100; ++second) {
                hist.push_back({
                    MakeCells({ToString(first), ToString(second * 1000)}, keyTypes),
                    1
                });
            }
        }
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Uses shortest prefix - first column differs, so only first column
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 2, Uint64 : NULL)");
    }

    Y_UNIT_TEST(SkewedDistribution) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        // 90% of load on one key
        TKeyAccessHistogram hist = {
            {MakeCells({"100"}, keyTypes), 1},
            {MakeCells({"200"}, keyTypes), 90},
            {MakeCells({"300"}, keyTypes), 1}
        };
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        // Median at boundary, no valid split
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "()");
    }

    Y_UNIT_TEST(UniformDistribution) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        TKeyAccessHistogram hist;
        for (ui32 i = 0; i < 100; ++i) {
            hist.push_back({MakeCells({ToString(i)}, keyTypes), 1});
        }
        MakeKeyAccessHistogram(hist, keyTypes);
        ConvertToCumulativeHistogram(hist);

        auto result = SelectShortestMedianKeyPrefix(hist, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median at index 50 (upper_bound finds first > 50)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 50)");
    }
}

Y_UNIT_TEST_SUITE(TSplitSelectShortestMedianKeyPrefixBySize) {
    Y_UNIT_TEST(EmptyHistogram) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        NTable::THistogram hist;
        auto result = SelectShortestMedianKeyPrefix(hist, 0, keyTypes);
        UNIT_ASSERT(IsEmpty(result));
    }

    Y_UNIT_TEST(SingleBucket) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"100"}, 100}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 100, keyTypes);
        UNIT_ASSERT(IsEmpty(result));
    }

    Y_UNIT_TEST(ValidSplitBalanced) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"100"}, 50},
            {{"200"}, 100},
            {{"300"}, 150}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 150, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median at index 0 (key 100), med==lo so increment: 100+1=101
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 101)");
    }

    Y_UNIT_TEST(MedianSelection) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"100"}, 30},
            {{"200"}, 60},
            {{"300"}, 90},
            {{"400"}, 120}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 120, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median at 200 or 300 (60 is closest to 60)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 200)");
    }

    Y_UNIT_TEST(CompositeKeysSameFirstColumn) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"100", "1"}, 30},
            {{"100", "2"}, 60},
            {{"100", "3"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // First column same, so use second column (median at 2)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 100, Uint32 : 2)");
    }

    Y_UNIT_TEST(CompositeKeysDifferentFirstColumn) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"100", "1"}, 30},
            {{"200", "2"}, 60},
            {{"300", "3"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median at index 0 (key 100), med==lo so increment: 100+1=101
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 101, Uint32 : NULL)");
    }

    Y_UNIT_TEST(IntegerTypeIncrement) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        // med == lo, should increment integer value
        auto hist = MakeHistogram({
            {{"100", "1"}, 30},
            {{"100", "1"}, 60},
            {{"100", "2"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // med==lo for second column (both 1), but doesn't increment - stays at med value
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 100, Uint32 : 1)");
    }

    Y_UNIT_TEST(StringTypesBinarySearch) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        auto hist = MakeHistogram({
            {{"a", "x"}, 30},
            {{"a", "x"}, 60},
            {{"a", "y"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // med==lo for second column (both "x"), stays at med value
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Utf8 : a, Utf8 : x)");
    }

    Y_UNIT_TEST(CannotSplitScenarios) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        // Test: split would create parts < 25%
        {
            auto hist = MakeHistogram({
                {{"100"}, 10},
                {{"200"}, 90}
            }, keyTypes);

            auto result = SelectShortestMedianKeyPrefix(hist, 100, keyTypes);
            UNIT_ASSERT(IsEmpty(result));
        }

        // Test: unbalanced sizes (10/110 < 25%)
        {
            auto hist = MakeHistogram({
                {{"100"}, 10},
                {{"200"}, 100},
                {{"300"}, 110}
            }, keyTypes);

            auto result = SelectShortestMedianKeyPrefix(hist, 110, keyTypes);
            // 10/110 < 25%, no valid split
            UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "()");
        }
    }



    Y_UNIT_TEST(ThreeColumnKey) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
            NScheme::TTypeInfo(NScheme::NTypeIds::Int64)
        };

        auto hist = MakeHistogram({
            {{"100", "a", "1000"}, 30},
            {{"200", "b", "2000"}, 60},
            {{"300", "c", "3000"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median at index 0 (key 100), med==lo so increment: 100+1=101
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 101, Utf8 : NULL, Int64 : NULL)");
    }

    Y_UNIT_TEST(NullValuesInKeys) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        auto hist = MakeHistogram({
            {{"100", "NULL"}, 30},
            {{"200", "a"}, 60},
            {{"300", "b"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median at index 0 (key 100), med==lo so increment: 100+1=101
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 101, Utf8 : NULL)");
    }

    Y_UNIT_TEST(SignedIntegerEdgeCases) {
        // Test Int32 with negative values
        {
            TVector<NScheme::TTypeInfo> keyTypes = {
                NScheme::TTypeInfo(NScheme::NTypeIds::Int32)
            };

            auto hist = MakeHistogram({
                {{"-100"}, 30},
                {{"-100"}, 60},
                {{"0"}, 90}
            }, keyTypes);

            auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
            UNIT_ASSERT(!IsEmpty(result));
            // Median at index 0, uses median value directly (not incremented)
            UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Int32 : -100)");
        }

        // Test Int8 with min value (-128)
        {
            TVector<NScheme::TTypeInfo> keyTypes = {
                NScheme::TTypeInfo(NScheme::NTypeIds::Int8)
            };

            auto hist = MakeHistogram({
                {{"-128"}, 30},
                {{"-128"}, 60},
                {{"-127"}, 90}
            }, keyTypes);

            auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
            UNIT_ASSERT(!IsEmpty(result));
            // Median at index 0, uses median value directly
            UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Int8 : -128)");
        }
    }

    Y_UNIT_TEST(BoundaryConditions25Percent) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        // Test exactly at 25% boundary (left side)
        {
            auto hist = MakeHistogram({
                {{"100"}, 25},
                {{"200"}, 50},
                {{"300"}, 75},
                {{"400"}, 100}
            }, keyTypes);

            auto result = SelectShortestMedianKeyPrefix(hist, 100, keyTypes);
            UNIT_ASSERT(!IsEmpty(result));
            // Exactly 25% on left is valid, median at 200
            UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 200)");
        }

        // Test just above 25% (26%)
        {
            auto hist = MakeHistogram({
                {{"100"}, 26},
                {{"200"}, 50},
                {{"300"}, 74},
                {{"400"}, 100}
            }, keyTypes);

            auto result = SelectShortestMedianKeyPrefix(hist, 100, keyTypes);
            UNIT_ASSERT(!IsEmpty(result));
            // 26/100 > 25%, valid split
            UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 200)");
        }

        // Test exactly at 25% boundary (right side)
        {
            auto hist = MakeHistogram({
                {{"100"}, 75},
                {{"200"}, 100}
            }, keyTypes);

            auto result = SelectShortestMedianKeyPrefix(hist, 100, keyTypes);
            UNIT_ASSERT(!IsEmpty(result));
            // Right side exactly 25% is valid, uses median value directly
            UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 100)");
        }
    }

    Y_UNIT_TEST(BinarySearchStringTypes) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        auto hist = MakeHistogram({
            {{"a", "x"}, 30},
            {{"a", "x"}, 60},
            {{"a", "y"}, 70},
            {{"a", "z"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));
        // First column same, second column uses median value directly
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Utf8 : a, Utf8 : x)");
    }

    Y_UNIT_TEST(BinarySearchFloatTypes) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Double),
            NScheme::TTypeInfo(NScheme::NTypeIds::Double)
        };

        auto hist = MakeHistogram({
            {{"1.0", "1.0"}, 30},
            {{"1.0", "1.0"}, 60},
            {{"1.0", "1.5"}, 70},
            {{"1.0", "2.0"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));
        // First column same, second column uses median value directly
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Double : 1, Double : 1)");
    }

    Y_UNIT_TEST(NullInSizeBasedSplitting) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        auto hist = MakeHistogram({
            {{"100", "NULL"}, 30},
            {{"100", "NULL"}, 60},
            {{"100", "a"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));
        // First column same, second column uses median value (NULL) directly
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 100, Utf8 : NULL)");
    }

    Y_UNIT_TEST(DateTimeTypesHandling) {
        // Test Datetime type
        {
            TVector<NScheme::TTypeInfo> keyTypes = {
                NScheme::TTypeInfo(NScheme::NTypeIds::Datetime)
            };

            auto hist = MakeHistogram({
                {{"1000000"}, 30},  // Seconds since epoch
                {{"1000000"}, 60},
                {{"2000000"}, 90}
            }, keyTypes);

            auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
            UNIT_ASSERT(!IsEmpty(result));
            // Median at index 0, uses median value directly
            auto expected = MakeCells({"1000000"}, keyTypes);
            UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), PrintKey(expected, keyTypes));
        }

        // Test Timestamp type
        {
            TVector<NScheme::TTypeInfo> keyTypes = {
                NScheme::TTypeInfo(NScheme::NTypeIds::Timestamp)
            };

            auto hist = MakeHistogram({
                {{"1000000000"}, 30},  // Microseconds since epoch
                {{"1000000000"}, 60},
                {{"2000000000"}, 90}
            }, keyTypes);

            auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
            UNIT_ASSERT(!IsEmpty(result));
            // Median at index 0, uses median value directly
            auto expected = MakeCells({"1000000000"}, keyTypes);
            UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), PrintKey(expected, keyTypes));
        }

        // Test Interval with negative values
        {
            TVector<NScheme::TTypeInfo> keyTypes = {
                NScheme::TTypeInfo(NScheme::NTypeIds::Interval)
            };

            auto hist = MakeHistogram({
                {{"-1000000"}, 30},
                {{"-1000000"}, 60},
                {{"0"}, 90}
            }, keyTypes);

            auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
            UNIT_ASSERT(!IsEmpty(result));
            // Median at index 0, uses median value directly
            auto expected = MakeCells({"-1000000"}, keyTypes);
            UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), PrintKey(expected, keyTypes));
        }

        // Test Date type
        {
            TVector<NScheme::TTypeInfo> keyTypes = {
                NScheme::TTypeInfo(NScheme::NTypeIds::Date)
            };

            auto hist = MakeHistogram({
                {{"18000"}, 30},  // Days since epoch
                {{"18001"}, 60},
                {{"18002"}, 90}
            }, keyTypes);

            auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
            // Date values are printed as raw bytes by DbgPrintValue
            // 18001 (0x4651) appears as "QF" (0x51='Q', 0x46='F' in little-endian)
            UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Date : QF)");
        }
    }

    Y_UNIT_TEST(IntegerIncrementAtExactMax) {
        // CRITICAL: Test what happens when trying to increment MAX value
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"4294967295"}, 30},  // Uint32 MAX
            {{"4294967295"}, 60},  // Same MAX value
            {{"4294967295"}, 90}   // Same MAX value
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        // All same key, should return that key (or empty if cannot split)
        UNIT_ASSERT(!IsEmpty(result));
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 4294967295)");
    }

    Y_UNIT_TEST(BinarySearchNoKeyGreaterThanMedian) {
        // IMPORTANT: Test binary search when no key exists > median in the range
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        auto hist = MakeHistogram({
            {{"100", "a"}, 30},
            {{"100", "a"}, 60},  // med==lo, same string value
            {{"100", "a"}, 90},  // hi also same
            {{"200", "b"}, 120}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 120, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));
        // Should handle case where binary search finds no key > "a" before reaching different first column
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 100, Utf8 : a)");
    }

    Y_UNIT_TEST(AllColumnsEqualLoHi) {
        // MODERATE: Test when all columns have lo[i] == hi[i]
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"100", "200", "300"}, 30},
            {{"100", "200", "400"}, 60},
            {{"100", "200", "500"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));
        // First two columns same (lo==hi), third column: med=lo=300, so increments to 301
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 100, Uint32 : 200, Uint32 : 301)");
    }

    Y_UNIT_TEST(SingleColumnAllSameValues) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"100"}, 30},
            {{"100"}, 60},
            {{"100"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 100)");
    }

    Y_UNIT_TEST(VeryLargeValues) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)
        };

        auto hist = MakeHistogram({
            {{"18446744073709551600"}, 1000000000},
            {{"18446744073709551610"}, 2000000000},
            {{"18446744073709551614"}, 3000000000}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 3000000000, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median at index 0, med==lo so increment: 18446744073709551600+1=18446744073709551601
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint64 : 18446744073709551601)");
    }

    Y_UNIT_TEST(FloatAndDoubleTypes) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Double)
        };

        auto hist = MakeHistogram({
            {{"1.5"}, 30},
            {{"2.5"}, 60},
            {{"3.5"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // Median is 2.5
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Double : 2.5)");
    }

    Y_UNIT_TEST(BoolType) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Bool),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"0", "100"}, 30},
            {{"0", "200"}, 60},
            {{"1", "100"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));

        // First column same (0), second column med==lo (both 100), increment to 101
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Bool : false, Uint32 : 101)");
    }

    Y_UNIT_TEST(IntegerIncrementOverflow) {
        // P0 CRITICAL: Test integer increment at MAX value
        // Tests data_size.cpp:106 - what happens when val++ at UINT32_MAX?
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"4294967295"}, 50},  // UINT32_MAX
            {{"4294967295"}, 100}  // Same value, cannot split
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 100, keyTypes);
        // Implementation returns MAX value unchanged (no wrap-around check)
        UNIT_ASSERT(!IsEmpty(result));
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 4294967295)");
    }

    Y_UNIT_TEST(BinarySearchEmptyRange) {
        // P0 CRITICAL: Test binary search when idxMed == idxHi
        // Tests data_size.cpp:118-122 - UpperBound on empty range
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        auto hist = MakeHistogram({
            {{"100", "a"}, 25},
            {{"100", "a"}, 75},  // med and hi are same (idxMed == idxHi)
            {{"200", "b"}, 100}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 100, keyTypes);
        // Should handle empty search range gracefully
        UNIT_ASSERT(!IsEmpty(result));
    }

    Y_UNIT_TEST(TwentyFivePercentExact) {
        // P1 IMPORTANT: Test exact 25% boundary conditions
        // Tests data_size.cpp:62-67 - leftSize * 4 >= totalSize
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"100"}, 25},  // Exactly 25%
            {{"200"}, 50},
            {{"300"}, 75},  // Exactly 75% (rightSize = 25%)
            {{"400"}, 100}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 100, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));
        // Should find valid split point
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 200)");
    }

    Y_UNIT_TEST(IntegerIncrementWithNull) {
        // P1 IMPORTANT: Test NULL handling in integer increment path
        // Tests data_size.cpp:100 - if (IsIntegerType && !IsNull())
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"NULL"}, 30},
            {{"NULL"}, 60},  // med == lo == NULL
            {{"100"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        // When med==lo==NULL, implementation returns NULL (cannot increment NULL)
        UNIT_ASSERT(!IsEmpty(result));
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : NULL)");
    }

    Y_UNIT_TEST(AllColumnsSameLoHiCritical) {
        // P2 MODERATE: Test when all columns have lo[i] == hi[i]
        // Tests data_size.cpp:85-92 - loop completes without breaking
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"100", "200"}, 30},
            {{"100", "200"}, 60},  // All columns same
            {{"100", "200"}, 90}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 90, keyTypes);
        // Implementation returns the key even when all columns are same
        UNIT_ASSERT(!IsEmpty(result));
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 100, Uint32 : 200)");
    }

    Y_UNIT_TEST(MedianSelectionTie) {
        // P2 MODERATE: Test tie in median selection
        // Tests data_size.cpp:56-60 - multiple points with same sizesDiff
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
        };

        auto hist = MakeHistogram({
            {{"100"}, 40},  // left=40, right=60, diff=20
            {{"200"}, 60},  // left=60, right=40, diff=20 (tie!)
            {{"300"}, 100}
        }, keyTypes);

        auto result = SelectShortestMedianKeyPrefix(hist, 100, keyTypes);
        UNIT_ASSERT(!IsEmpty(result));
        // Should pick first occurrence of minimum diff (idx=0)
        UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 101)");
    }
}
