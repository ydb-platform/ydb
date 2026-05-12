#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers_flags_n.h>  // for Y_UNIT_TEST_FLAGS_N

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NSchemeShard {

// defined in ydb/core/tx/schemeshard/schemeshard__table_stats_histogram.cpp

TSerializedCellVec GetSplitBoundaryByLoad(const NKikimrTableStats::TTableStats& inputStats, const TConstArrayRef<NScheme::TTypeInfo> &keyColumnTypes);
TSerializedCellVec ChooseSplitKeyByKeySample(const NKikimrTableStats::THistogram& keySample, const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes, bool sortHistogram);

TSerializedCellVec GetSplitBoundaryBySize(const NKikimrTableStats::TTableStats& inputStats, const TConstArrayRef<NScheme::TTypeInfo> &keyColumnTypes);
TSerializedCellVec ChooseSplitKeyByHistogram(const NKikimrTableStats::THistogram& histogram, const TConstArrayRef<NScheme::TTypeInfo> &keyColumnTypes, ui64 totalSize);

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

}  // anonymous namespace


Y_UNIT_TEST_SUITE(TSchemeShardSplitModeLoad) {

    enum ESelectTestResult {
        EMPTY,
        SUGGESTED,
        SELECTED
    };
    struct TSelectTestParam {
        ui32 ProtocolVersion = 0;           // Protocol version in stats
        bool HasSuggestedKey = false;       // Whether stats contain a suggested split key
        bool HasKeyAccessSample = false;    // Whether stats contain a key access sample
        ESelectTestResult Result;           // Expected result: what key should be a result
    };

    static void SelectTest(const TSelectTestParam& param) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        NKikimrTableStats::TTableStats stats;
        if (param.ProtocolVersion > 0) {
            stats.SetSplitProtocolVersion(param.ProtocolVersion);
        }
        if (param.HasSuggestedKey) {
            stats.SetSplitByLoadSuggestedKey(SerializeKey({"1000", "suggested-key"}, keyTypes));
        }
        if (param.HasKeyAccessSample) {
            stats.MutableKeyAccessSample()->CopyFrom(MakeKeyHist(
                {
                    {{"2", "a"}, 1},
                    {{"4", "selected-key"}, 1},
                    {{"6", "a"}, 1}
                },
                keyTypes
            ));
        }

        // test body
        auto result = GetSplitBoundaryByLoad(stats, keyTypes);

        // result check
        switch(param.Result) {
            case EMPTY:
                UNIT_ASSERT_VALUES_EQUAL(result.GetCells().empty(), true);
                break;
            case SUGGESTED:
                UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 1000, Utf8 : suggested-key)");
                break;
            case SELECTED:
                UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 4, Utf8 : NULL)");
                break;
        }
    }

    // Protocol 0: No histogram and no suggested key support.
    // Schemeshard must build proper histogram and select key itself.
    Y_UNIT_TEST_FLAGS_N(Select_Protocol0, bool HasSuggestedKey, bool HasKeyAccessSample) {
        SelectTest({
            .ProtocolVersion = 0,
            .HasSuggestedKey = HasSuggestedKey,
            .HasKeyAccessSample = HasKeyAccessSample,
            .Result = (HasKeyAccessSample ? SELECTED : EMPTY),
        });
    }

    static const std::vector<TSelectTestParam> SelectTests = {
        // Protocol 0: Split boundary is selected from histogram, if present.
        { .ProtocolVersion = 0, .HasSuggestedKey = false, .HasKeyAccessSample = false, .Result = EMPTY },
        { .ProtocolVersion = 0, .HasSuggestedKey = false, .HasKeyAccessSample = true,  .Result = SELECTED },
        { .ProtocolVersion = 0, .HasSuggestedKey = true,  .HasKeyAccessSample = false, .Result = EMPTY },
        { .ProtocolVersion = 0, .HasSuggestedKey = true,  .HasKeyAccessSample = true,  .Result = SELECTED },
        // Protocol 1: Split boundary is selected from histogram, if present.
        { .ProtocolVersion = 1, .HasSuggestedKey = false, .HasKeyAccessSample = false, .Result = EMPTY },
        { .ProtocolVersion = 1, .HasSuggestedKey = false, .HasKeyAccessSample = true,  .Result = SELECTED },
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasKeyAccessSample = false, .Result = EMPTY },
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasKeyAccessSample = true,  .Result = SELECTED },
        // Protocol 2: Split boundary is taken from suggested key, if present. Histogram is ignored.
        { .ProtocolVersion = 2, .HasSuggestedKey = false, .HasKeyAccessSample = false, .Result = EMPTY },
        { .ProtocolVersion = 2, .HasSuggestedKey = false, .HasKeyAccessSample = true,  .Result = EMPTY },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasKeyAccessSample = false, .Result = SUGGESTED },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasKeyAccessSample = true,  .Result = SUGGESTED },
        // Protocol 3: Same as version 2.
        { .ProtocolVersion = 3, .HasSuggestedKey = false, .HasKeyAccessSample = false, .Result = EMPTY },
        { .ProtocolVersion = 3, .HasSuggestedKey = false, .HasKeyAccessSample = true,  .Result = EMPTY },
        { .ProtocolVersion = 3, .HasSuggestedKey = true,  .HasKeyAccessSample = false, .Result = SUGGESTED },
        { .ProtocolVersion = 3, .HasSuggestedKey = true,  .HasKeyAccessSample = true,  .Result = SUGGESTED },
        // Protocol 4: Unknown version, can't use anything
        { .ProtocolVersion = 4, .HasSuggestedKey = false, .HasKeyAccessSample = false, .Result = EMPTY },
        { .ProtocolVersion = 4, .HasSuggestedKey = false, .HasKeyAccessSample = true,  .Result = EMPTY },
        { .ProtocolVersion = 4, .HasSuggestedKey = true,  .HasKeyAccessSample = false, .Result = EMPTY },
        { .ProtocolVersion = 4, .HasSuggestedKey = true,  .HasKeyAccessSample = true,  .Result = EMPTY },
    };
    struct TTestRegistrationSelectTest {
        TTestRegistrationSelectTest() {
            static std::vector<TString> TestNames;

            for (const auto& param : SelectTests) {
                TestNames.emplace_back(TStringBuilder()
                    << "Select-Protocol" << param.ProtocolVersion
                    << (param.HasSuggestedKey ? "-HasSuggestedKey": "")
                    << (param.HasKeyAccessSample ? "-HasKeyAccessSample": "")
                );

                TCurrentTest::AddTest(
                    TestNames.back().c_str(),
                    std::bind(std::bind(SelectTest, param), std::placeholders::_1),
                    /*forceFork*/ false
                );
            }
        }
    };
    static TTestRegistrationSelectTest testRegistrationSelectTest;
}

Y_UNIT_TEST_SUITE(TSchemeShardSplitModeSize) {

    enum ESelectTestResult {
        EMPTY,
        SUGGESTED,
        SELECTED
    };
    struct TSelectTestParam {
        ui32 ProtocolVersion = 0;           // Protocol version in stats
        bool HasSuggestedKey = false;       // Whether stats contain a suggested split key
        bool HasDataSizeHistogram = false;  // Whether stats contain a data size histogram
        ESelectTestResult Result;           // Expected result: what key should be a result
    };

    static void SelectTest(const TSelectTestParam& param) {
        TVector<NScheme::TTypeInfo> keyTypes = {
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
        };

        NKikimrTableStats::TTableStats stats;
        if (param.ProtocolVersion > 0) {
            stats.SetSplitProtocolVersion(param.ProtocolVersion);
        }
        if (param.HasSuggestedKey) {
            stats.SetSplitBySizeSuggestedKey(SerializeKey({"1000", "suggested-key"}, keyTypes));
        }
        if (param.HasDataSizeHistogram) {
            stats.MutableDataSizeHistogram()->CopyFrom(MakeKeyHist(
                {
                    {{"2", "a"}, 1},
                    {{"4", "selected-key"}, 2},
                    {{"5", "a"}, 3},
                    {{"6", "a"}, 4}
                },
                keyTypes
            ));
        }
        stats.SetDataSize(4);

        // test body
        auto result = GetSplitBoundaryBySize(stats, keyTypes);

        // result check
        switch(param.Result) {
            case EMPTY:
                UNIT_ASSERT_VALUES_EQUAL(result.GetCells().empty(), true);
                break;
            case SUGGESTED:
                UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 1000, Utf8 : suggested-key)");
                break;
            case SELECTED:
                UNIT_ASSERT_VALUES_EQUAL(PrintKey(result, keyTypes), "(Uint32 : 4, Utf8 : NULL)");
                break;
        }
    }

    static const std::vector<TSelectTestParam> SelectTests = {
        // Protocol 0: Split boundary is selected from histogram, if present.
        { .ProtocolVersion = 0, .HasSuggestedKey = false, .HasDataSizeHistogram = false, .Result = EMPTY },
        { .ProtocolVersion = 0, .HasSuggestedKey = false, .HasDataSizeHistogram = true,  .Result = SELECTED },
        { .ProtocolVersion = 0, .HasSuggestedKey = true,  .HasDataSizeHistogram = false, .Result = EMPTY },
        { .ProtocolVersion = 0, .HasSuggestedKey = true,  .HasDataSizeHistogram = true,  .Result = SELECTED },
        // Protocol 1: Split boundary is selected from histogram, if present.
        { .ProtocolVersion = 1, .HasSuggestedKey = false, .HasDataSizeHistogram = false, .Result = EMPTY },
        { .ProtocolVersion = 1, .HasSuggestedKey = false, .HasDataSizeHistogram = true,  .Result = SELECTED },
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasDataSizeHistogram = false, .Result = EMPTY },
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasDataSizeHistogram = true,  .Result = SELECTED },
        // Protocol 2: Split boundary is taken from suggested key, if present. Histogram is ignored.
        { .ProtocolVersion = 2, .HasSuggestedKey = false, .HasDataSizeHistogram = false, .Result = EMPTY },
        { .ProtocolVersion = 2, .HasSuggestedKey = false, .HasDataSizeHistogram = true,  .Result = EMPTY },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasDataSizeHistogram = false, .Result = SUGGESTED },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasDataSizeHistogram = true,  .Result = SUGGESTED },
        // Protocol 3: Same as version 2.
        { .ProtocolVersion = 3, .HasSuggestedKey = false, .HasDataSizeHistogram = false, .Result = EMPTY },
        { .ProtocolVersion = 3, .HasSuggestedKey = false, .HasDataSizeHistogram = true,  .Result = EMPTY },
        { .ProtocolVersion = 3, .HasSuggestedKey = true,  .HasDataSizeHistogram = false, .Result = SUGGESTED },
        { .ProtocolVersion = 3, .HasSuggestedKey = true,  .HasDataSizeHistogram = true,  .Result = SUGGESTED },
        // Protocol 4: Unknown version, can't use anything
        { .ProtocolVersion = 4, .HasSuggestedKey = false, .HasDataSizeHistogram = false, .Result = EMPTY },
        { .ProtocolVersion = 4, .HasSuggestedKey = false, .HasDataSizeHistogram = true,  .Result = EMPTY },
        { .ProtocolVersion = 4, .HasSuggestedKey = true,  .HasDataSizeHistogram = false, .Result = EMPTY },
        { .ProtocolVersion = 4, .HasSuggestedKey = true,  .HasDataSizeHistogram = true,  .Result = EMPTY },
    };
    struct TTestRegistrationSelectTest {
        TTestRegistrationSelectTest() {
            static std::vector<TString> TestNames;

            for (const auto& param : SelectTests) {
                TestNames.emplace_back(TStringBuilder()
                    << "Select-Protocol" << param.ProtocolVersion
                    << (param.HasSuggestedKey ? "-HasSuggestedKey": "")
                    << (param.HasDataSizeHistogram ? "-HasDataSizeHistogram": "")
                );

                TCurrentTest::AddTest(
                    TestNames.back().c_str(),
                    std::bind(std::bind(SelectTest, param), std::placeholders::_1),
                    /*forceFork*/ false
                );
            }
        }
    };
    static TTestRegistrationSelectTest testRegistrationSelectTest;
}


namespace {

struct TKeyHelper {
    const TVector<NKikimr::NScheme::TTypeInfo> KeyColumnTypes;

    TKeyHelper(const TVector<NScheme::TTypeInfo>& keyColumnTypes)
        : KeyColumnTypes(keyColumnTypes)
    {}

    TSerializedCellVec SelectSplitKey(const TTestKeySample& input) {
        auto hist = MakeKeyHist(input, KeyColumnTypes);
        return ChooseSplitKeyByKeySample(hist, KeyColumnTypes, /*sortHistogram*/ true);
    }

    TSerializedCellVec SelectSplitKey(const TTestKeySample& input, ui64 totalSize) {
        auto hist = MakeKeyHist(input, KeyColumnTypes);
        return ChooseSplitKeyByHistogram(hist, KeyColumnTypes, totalSize);
    }

    TString PrintKey(const TSerializedCellVec& key) {
        return ::PrintKey(key, KeyColumnTypes);
    }
};

}  // anonymous namespace


Y_UNIT_TEST_SUITE(TSchemeShardSplitBySample) {

    Y_UNIT_TEST(NoResultOnEmptySample) {
        auto result = ChooseSplitKeyByKeySample(NKikimrTableStats::THistogram(), {}, true);
        UNIT_ASSERT(IsEmpty(result));  // e.g. equal to TSerializedCellVec()
    }

    // Sample with size <= 2 considered empty
    Y_UNIT_TEST(NoResultOnSampleTooSmall) {
        TKeyHelper helper({
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
        });

        {
            auto result = helper.SelectSplitKey({
                {{"0", "a"}, 1},
            });
            UNIT_ASSERT(IsEmpty(result));  // is equal to TSerializedCellVec()
        }
        {
            auto result = helper.SelectSplitKey({
                {{"0", "a"}, 1},
                {{"11", "a"}, 1},
            });
            UNIT_ASSERT(IsEmpty(result));  // is equal to TSerializedCellVec()
        }
        {
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
                {{"0", "a"}, 5},
                {{"11", "a"}, 1},
                {{"100", "a"}, 1},
            });
            UNIT_ASSERT(IsEmpty(result));
        }
        {
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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

        auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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
            auto result = helper.SelectSplitKey({
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


Y_UNIT_TEST_SUITE(TSchemeShardSplitBySize) {

    Y_UNIT_TEST(SplitKey) {
        TKeyHelper helper({
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint64),
            NScheme::TTypeInfo(NScheme::NTypeIds::Utf8),
            NScheme::TTypeInfo(NScheme::NTypeIds::Uint32),
        });

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"1", "aaaaaaaaaaaaaaaaaaaaaa", "42"}, 1},
                    {{"3", "bbbbbbbb", "42"}, 2},
                    {{"5", "cccccccccccccccccccccccc", "42"}, 3},
                },
                4
            );
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 3, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"1", "aaaaaaaaaaaaaaaaaaaaaa", "42"}, 1},
                    {{"1", "bbbbbbbb", "42"}, 2},
                    {{"1", "cccccccccccccccccccccccc", "42"}, 3},
                },
                4
            );
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 1, Utf8 : bbbbbbbb, Uint32 : NULL)");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"1", "aaaaaaaaaaaaaaaaaaaaaa", "42"}, 1},
                    {{"1", "bb", "42"}, 2},
                    {{"1", "cc", "42"}, 3},
                    {{"2", "cd", "42"}, 4},
                    {{"2", "d", "42"}, 5},
                    {{"2", "e", "42"}, 6},
                    {{"2", "f", "42"}, 7},
                    {{"2", "g", "42"}, 8},
                },
                9
            );
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 2, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"1", "aaaaaaaaaaaaaaaaaaaaaa", "42"}, 1},
                    {{"1", "bb", "42"}, 2},
                    {{"1", "cc", "42"}, 3},
                    {{"1", "cd", "42"}, 4},
                    {{"1", "d", "42"}, 5},
                    {{"2", "e", "42"}, 6},
                    {{"2", "f", "42"}, 7},
                    {{"2", "g", "42"}, 8},
                },
                9
            );
            //TODO: FIX this case
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 2, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"1", "aaaaaaaaaaaaaaaaaaaaaa", "42"}, 1},
                    {{"1", "bb", "42"}, 2},
                    {{"1", "cc", "42"}, 3},
                    {{"1", "cd", "42"}, 4},
                    {{"1", "d", "42"}, 5},
                    {{"3", "e", "42"}, 6},
                    {{"3", "f", "42"}, 7},
                    {{"3", "g", "42"}, 8},
                },
                9
            );
            //TODO: FIX this case
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 2, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"1", "aaaaaaaaaaaaaaaaaaaaaa", "42"}, 1},
                    {{"1", "bb", "42"}, 2},
                    {{"1", "cc", "42"}, 3},
                    {{"1", "cd", "42"}, 4},
                    {{"2", "d", "42"}, 5},
                    {{"3", "e", "42"}, 6},
                    {{"3", "f", "42"}, 7},
                    {{"3", "g", "42"}, 8},
                },
                9
            );
            //TODO: FIX this case
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 2, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"1", "aaaaaaaaaaaaaaaaaaaaaa", "42"}, 1},
                    {{"2", "a", "42"}, 2},
                    {{"2", "b", "42"}, 3},
                    {{"2", "c", "42"}, 4},
                    {{"2", "d", "42"}, 5},
                    {{"2", "e", "42"}, 6},
                    {{"2", "f", "42"}, 7},
                    {{"3", "cccccccccccccccccccccccc", "42"}, 8},
                },
                9
            );
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 2, Utf8 : c, Uint32 : NULL)");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"2", "aaa", "1"}, 1},
                    {{"2", "aaa", "2"}, 2},
                    {{"2", "aaa", "3"}, 3},
                    {{"2", "aaa", "4"}, 4},
                    {{"2", "aaa", "5"}, 5},
                    {{"2", "bbb", "1"}, 6},
                    {{"2", "bbb", "2"}, 7},
                    {{"3", "ccc", "42"}, 8},
                },
                9
            );
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 2, Utf8 : bbb, Uint32 : NULL)");
        }

        {
            auto result = helper.SelectSplitKey({});
            UNIT_ASSERT(IsEmpty(result));
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"0", "a", "1"}, 53},
                },
                100
            );
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 0, Utf8 : a, Uint32 : 1)");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"0", "a", "1"}, 25},
                },
                100
            );
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 0, Utf8 : a, Uint32 : 1)");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"0", "a", "1"}, 75},
                },
                100
            );
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 0, Utf8 : a, Uint32 : 1)");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"0", "a", "1"}, 24},
                },
                100
            );
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "()");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"0", "a", "1"}, 76},
                },
                100
            );
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "()");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"0", "a", "1"}, 1},
                    {{"1", "a", "1"}, 2},
                    {{"2", "a", "2"}, 3},
                    {{"3", "a", "3"}, 4},
                    {{"4", "a", "4"}, 5},
                    {{"5", "a", "5"}, 6},
                    {{"6", "a", "1"}, 7},
                    {{"7", "a", "2"}, 8},
                    {{"8", "a", "42"}, 9},
                },
                10
            );
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 4, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"0", "a", "1"}, 1},
                    {{"1", "a", "1"}, 2},
                    {{"2", "a", "2"}, 3},
                    {{"3", "a", "3"}, 4},
                    {{"4", "a", "4"}, 5},
                    {{"5", "a", "5"}, 6},
                    {{"6", "a", "1"}, 30},
                    {{"7", "a", "2"}, 40},
                    {{"8", "a", "42"}, 70},
                },
                100
            );
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 7, Utf8 : NULL, Uint32 : NULL)");
        }

        {
            auto result = helper.SelectSplitKey(
                {
                    {{"0", "a", "1"}, 30},
                    {{"1", "a", "1"}, 40},
                    {{"2", "a", "2"}, 70},
                    {{"3", "a", "3"}, 90},
                    {{"4", "a", "4"}, 91},
                    {{"5", "a", "5"}, 92},
                    {{"6", "a", "1"}, 93},
                    {{"7", "a", "2"}, 94},
                    {{"8", "a", "42"}, 95},
                },
                100
            );
            UNIT_ASSERT_VALUES_EQUAL(helper.PrintKey(result), "(Uint64 : 1, Utf8 : NULL, Uint32 : NULL)");
        }
    }
}
