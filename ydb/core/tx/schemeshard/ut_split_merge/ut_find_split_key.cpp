#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NSchemeShard {

// defined in ydb/core/tx/schemeshard/schemeshard__table_stats_histogram.cpp
TSerializedCellVec ChooseSplitKeyByKeySample(const NKikimrTableStats::THistogram& keySample, const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes);

}

using namespace NKikimr;
using namespace NSchemeShard;


namespace {

const NScheme::TTypeRegistry TypeRegistry;

using TTestKey = std::vector<std::string>;
using TTestKeySampleEntry = std::pair<TTestKey, ui64>;
using TTestKeySample = std::vector<TTestKeySampleEntry>;

NKikimr::TSerializedCellVec MakeCells(const TTestKey& tuple, const TVector<NScheme::TTypeInfo>& keyColumnTypes) {
    UNIT_ASSERT(tuple.size() <= keyColumnTypes.size());

    TSmallVec<NKikimr::TCell> cells;
    for (size_t i = 0; i < tuple.size(); ++i) {
        if (tuple[i] == "NULL") {
            cells.push_back(NKikimr::TCell());

        } else {

#define MAKE_CELL_FROM_STRING(ydbType, cppType) \
            case NKikimr::NScheme::NTypeIds::ydbType: { \
                cells.emplace_back(TCell::Make(FromString<cppType>(tuple[i]))); \
                break; \
            }

            switch (keyColumnTypes[i].GetTypeId()) {

            MAKE_CELL_FROM_STRING(Bool, bool);

            MAKE_CELL_FROM_STRING(Uint8, ui8);
            MAKE_CELL_FROM_STRING(Int8, i8);
            MAKE_CELL_FROM_STRING(Uint16, ui16);
            MAKE_CELL_FROM_STRING(Int16, i16);
            MAKE_CELL_FROM_STRING(Uint32, ui32);
            MAKE_CELL_FROM_STRING(Int32, i32);
            MAKE_CELL_FROM_STRING(Uint64, ui64);
            MAKE_CELL_FROM_STRING(Int64, i64);

            MAKE_CELL_FROM_STRING(Double, double);
            MAKE_CELL_FROM_STRING(Float, float);

            case NKikimr::NScheme::NTypeIds::String:
            case NKikimr::NScheme::NTypeIds::Utf8: {
                cells.push_back(NKikimr::TCell(tuple[i].data(), tuple[i].size()));
                break;
            }
#undef MAKE_CELL_FROM_STRING

            default:
                UNIT_ASSERT_C(false, "Unexpected type");
            }
        }
    }

    return NKikimr::TSerializedCellVec(cells);
}

bool IsEmpty(const TSerializedCellVec& key) {
    return key.GetCells().empty();
}

TString PrintKey(const TSerializedCellVec& key, const TVector<NScheme::TTypeInfo>& keyColumnTypes) {
    const auto& cells = key.GetCells();
    return DbgPrintTuple(TDbTupleRef(keyColumnTypes.data(), cells.data(), cells.size()), TypeRegistry);
}

TString SerializeKey(const TTestKey& tuple, const TVector<NScheme::TTypeInfo>& keyColumnTypes) {
    return MakeCells(tuple, keyColumnTypes).GetBuffer();
}

NKikimrTableStats::THistogram MakeKeyHist(const TTestKeySample& sample, const TVector<NScheme::TTypeInfo>& keyColumnTypes) {
    NKikimrTableStats::THistogram hist;
    for (auto& [key, value] : sample) {
        auto bucket = hist.AddBuckets();
        bucket->SetKey(SerializeKey(key, keyColumnTypes));
        bucket->SetValue(value);
    }
    return hist;
}

struct TKeyHelper {
    const TVector<NKikimr::NScheme::TTypeInfo> KeyColumnTypes;

    TKeyHelper(const TVector<NScheme::TTypeInfo>& keyColumnTypes)
        : KeyColumnTypes(keyColumnTypes)
    {}

    TSerializedCellVec ChooseSplitKey(const TTestKeySample& sample) {
        auto hist = MakeKeyHist(sample, KeyColumnTypes);
        return ChooseSplitKeyByKeySample(hist, KeyColumnTypes);
    }
    TString PrintKey(const TSerializedCellVec& key) {
        return ::PrintKey(key, KeyColumnTypes);
    }
};

}  // anonymous namespace


Y_UNIT_TEST_SUITE(TSchemeShardSplitBySample) {

    Y_UNIT_TEST(NoResultOnEmptySample) {
        auto result = ChooseSplitKeyByKeySample(NKikimrTableStats::THistogram(), {});
        UNIT_ASSERT(IsEmpty(result));  // e.g. equal to TSerializedCellVec()
    }

    // Sample with size <= 2 considered empty
    Y_UNIT_TEST(NoResultOnSampleTooSmall) {
        TKeyHelper helper({
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
        });

        {
            auto result = helper.ChooseSplitKey({
                {{"0", "a"}, 1},
            });
            UNIT_ASSERT(IsEmpty(result));  // is equal to TSerializedCellVec()
        }
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "a"}, 1},
                {{"11", "a"}, 1},
            });
            UNIT_ASSERT(IsEmpty(result));  // is equal to TSerializedCellVec()
        }
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "a"}, 1},
                {{"11", "a"}, 1},
                {{"100", "a"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
        }
     }

    Y_UNIT_TEST(FlatlistNoResultWhenMedianKeyIsAtBoundary) {
        TKeyHelper helper({
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
        });


        {
            auto result = helper.ChooseSplitKey({
                {{"0", "a"}, 1},
                {{"0", "a"}, 1},
                {{"0", "a"}, 1},
                {{"0", "a"}, 1},
                {{"0", "a"}, 1},
                {{"11", "a"}, 1},
                {{"100", "a"}, 1},
            });
            UNIT_ASSERT(IsEmpty(result));
        }
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "a"}, 1},
                {{"11", "a"}, 1},
                {{"100", "a"}, 1},
                {{"100", "a"}, 1},
                {{"100", "a"}, 1},
                {{"100", "a"}, 1},
                {{"100", "a"}, 1},
            });
            UNIT_ASSERT(IsEmpty(result));
        }
    }

    Y_UNIT_TEST(HistogramNoResultWhenMedianKeyIsAtBoundary) {
        TKeyHelper helper({
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
        });

        {
            auto result = helper.ChooseSplitKey({
                {{"0", "a"}, 5},
                {{"11", "a"}, 1},
                {{"100", "a"}, 1},
            });
            UNIT_ASSERT(IsEmpty(result));
        }
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "a"}, 1},
                {{"11", "a"}, 1},
                {{"100", "a"}, 5},
            });
            UNIT_ASSERT(IsEmpty(result));
        }
    }

    // Despite being NKikimrTableStats::THistogram, key sample is in fact just a flat list
    // with repeated keys and unit weights.
    // ChooseSplitKeyByKeySample() transforms this flat list sample into a proper histogram
    // by merging entries with the same keys and accumulating their weights.
    Y_UNIT_TEST(FlatList) {
        TKeyHelper helper({
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
        });

        {
            auto result = helper.ChooseSplitKey({
                {{"0", "a"}, 1},
                {{"11", "a"}, 1},
                {{"11", "a"}, 1},
                {{"11", "a"}, 1},
                {{"11", "a"}, 1},
                {{"11", "a"}, 1},
                {{"100", "a"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Utf8 : NULL)");
        }
    }

    // ChooseSplitKeyByKeySample() also works for key samples being real histograms
    // with non repeating keys and non-unit weights.
    Y_UNIT_TEST(Histogram) {
        TKeyHelper helper({
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
        });

        {
            auto result = helper.ChooseSplitKey({
                {{"0", "a"}, 1},
                {{"11", "a"}, 5},
                {{"100", "a"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Utf8 : NULL)");
        }
    }

    // ChooseSplitKeyByKeySample() also works for the mixed key samples --
    // -- one that have both repeating keys and non-unit weights.
    Y_UNIT_TEST(Mixed) {
        TKeyHelper helper({
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
        });

        // Weights are carefully crafted to ensure that weights of {"11", "a"} and {"11", "b"} are
        // the only factor determining the result.

        {
            auto result = helper.ChooseSplitKey({
                {{"0", "a"}, 2},

                {{"11", "a"}, 6},  // a -- 6
                {{"11", "b"}, 1},  // b -- 4
                {{"11", "b"}, 1},
                {{"11", "b"}, 1},
                {{"11", "b"}, 1},

                {{"11", "c"}, 1},
                {{"100", "a"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Utf8 : a)");
        }
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "a"}, 2},

                {{"11", "a"}, 5},  // a -- 5
                {{"11", "b"}, 1},  // b -- 5
                {{"11", "b"}, 1},
                {{"11", "b"}, 1},
                {{"11", "b"}, 1},
                {{"11", "b"}, 1},

                {{"11", "c"}, 1},
                {{"100", "a"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Utf8 : b)");
        }
    }

    // ChooseSplitKeyByKeySample() sorts keys internally
    Y_UNIT_TEST(EntryOrderDoesNotCount) {
        TKeyHelper helper({
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
        });

        // sorted order
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "a"}, 1},
                {{"10", "NULL"}, 1},
                {{"10", "a"}, 1},
                {{"10", "b"}, 1},
                {{"11", "NULL"}, 1},
                {{"11", "a"}, 1},
                {{"11", "b"}, 1},
                {{"12", "NULL"}, 1},
                {{"12", "a"}, 1},
                {{"12", "b"}, 1},
                {{"100", "a"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Utf8 : NULL)");
        }
        // reverse sorted order
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "a"}, 1},
                {{"12", "b"}, 1},
                {{"12", "a"}, 1},
                {{"12", "NULL"}, 1},
                {{"11", "b"}, 1},
                {{"11", "a"}, 1},
                {{"11", "NULL"}, 1},
                {{"10", "b"}, 1},
                {{"10", "a"}, 1},
                {{"10", "NULL"}, 1},
                {{"100", "a"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Utf8 : NULL)");
        }
    }

    Y_UNIT_TEST(DifferentSizeKeysWorks) {
        TKeyHelper helper({
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
        });

        auto result = helper.ChooseSplitKey({
            {{"0", "a"}, 1},
            {{"11"}, 1},
            {{"100", "a"}, 1},
        });
        UNIT_ASSERT(!IsEmpty(result));
        UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Utf8 : NULL)");
    }

    Y_UNIT_TEST(EdgeSelectionWithSameSizeKeys) {
        TKeyHelper helper({
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint8),
        });

        // Weights are carefully crafted here to make weights of 2 keys of interest
        // be the only factor determining the result.

        // max value to next NULL
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "NULL"}, 1},
                {{"10", "0"}, 1},

                {{"10", "255"}, 6},  // <-- winner by weights
                {{"11", "NULL"}, 4},

                {{"11", "255"}, 1},
                {{"100", "NULL"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 10, Uint8 : 255)");
        }
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "NULL"}, 1},
                {{"10", "0"}, 1},

                {{"10", "255"}, 5},
                {{"11", "NULL"}, 5},  // <-- winner by weights

                {{"11", "255"}, 1},
                {{"100", "NULL"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Uint8 : NULL)");
        }

        // NULL to min value
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "NULL"}, 1},
                {{"10", "0"}, 1},

                {{"11", "NULL"}, 6},  // <-- winner by weights
                {{"11", "0"}, 4},

                {{"11", "255"}, 1},
                {{"100", "NULL"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Uint8 : NULL)");
        }
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "NULL"}, 1},
                {{"10", "0"}, 1},

                {{"11", "NULL"}, 5},
                {{"11", "0"}, 5},  // <-- winner by weights

                {{"11", "255"}, 1},
                {{"100", "NULL"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Uint8 : 0)");
        }

        // max value to next min value
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "NULL"}, 1},
                {{"10", "0"}, 1},

                {{"10", "255"}, 6},  // <-- winner by weights
                {{"11", "0"}, 4},

                {{"11", "255"}, 1},
                {{"100", "NULL"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 10, Uint8 : 255)");
        }
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "NULL"}, 1},
                {{"10", "0"}, 1},

                {{"10", "255"}, 5},
                {{"11", "0"}, 5},  // <-- winner by weights

                {{"11", "255"}, 1},
                {{"100", "NULL"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Uint8 : 0)");
        }
    }

    Y_UNIT_TEST(EdgeSelectionWithDifferentSizeKeys) {
        TKeyHelper helper({
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint8),
        });

        // Weights are carefully crafted here to make weights of 2 keys of interest
        // be the only factor determining the result.

        // +inf to next NULL
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "NULL"}, 1},
                {{"10", "0"}, 1},

                {{"10" /*+inf*/}, 6},  // <-- winner by weights
                {{"11", "NULL"}, 4},

                {{"11", "255"}, 1},
                {{"100", "NULL"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 10, Uint8 : NULL)");
        }
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "NULL"}, 1},
                {{"10", "0"}, 1},

                {{"10" /*+inf*/}, 5},
                {{"11", "NULL"}, 5},  // <-- winner by weights

                {{"11", "255"}, 1},
                {{"100", "NULL"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Uint8 : NULL)");
        }

        // max value to +inf
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "NULL"}, 1},
                {{"11", "0"}, 1},

                {{"11", "255"}, 6},  // <-- winner by weights
                {{"11" /*+inf*/}, 4},

                {{"12", "NULL"}, 1},
                {{"100", "NULL"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Uint8 : 255)");
        }
        {
            auto result = helper.ChooseSplitKey({
                {{"0", "NULL"}, 1},
                {{"11", "0"}, 1},

                {{"11", "255"}, 5},
                {{"11" /*+inf*/}, 5},  // <-- winner by weights

                {{"12", "NULL"}, 1},
                {{"100", "NULL"}, 1},
            });
            UNIT_ASSERT(!IsEmpty(result));
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint32 : 11, Uint8 : NULL)");
        }
    }

}
