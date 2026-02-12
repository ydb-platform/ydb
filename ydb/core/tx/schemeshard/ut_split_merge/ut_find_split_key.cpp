#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers_flags_n.h>  // for Y_UNIT_TEST_FLAGS_N

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NSchemeShard {

// defined in ydb/core/tx/schemeshard/schemeshard__table_stats_histogram.cpp

std::pair<bool, bool> GetSplitBoundaryByLoadModeFlags(ui32 protocolVersion, bool canUseSuggestedKey, bool shouldSortHistogram);
TSerializedCellVec GetSplitBoundaryByLoad(
    const NKikimrTableStats::TTableStats& inputStats,
    const TConstArrayRef<NScheme::TTypeInfo> &keyColumnTypes,
    bool canUseSuggestedKey,
    bool shouldSortHistogram
);
TSerializedCellVec ChooseSplitKeyByKeySample(const NKikimrTableStats::THistogram& keySample, const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes, bool sortHistogram);

bool GetSplitBoundaryBySizeModeFlag(ui32 protocolVersion, bool expectSuggestedKey);
TSerializedCellVec GetSplitBoundaryBySize(
    const NKikimrTableStats::TTableStats& inputStats,
    const TConstArrayRef<NScheme::TTypeInfo> &keyColumnTypes,
    bool canUseSuggestedKey
);
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
    struct TFlagsTestParam {
        ui32 ProtocolVersion = 0;              // Protocol version in stats
        bool CanUseSuggestedKey = false;       // Feature flag: whether to expect and use suggested keys
        bool ShouldSortHistogram = false;      // Feature flag: whether to sort input histogram
        bool ResultUseSuggestedKey = false;    // Expected result: should schemeshard use the suggested key?
        bool ResultSortHistogram = true;       // Expected result: should schemeshard (re)sort input histogram?
    };

    static void FlagsTest(NUnitTest::TTestContext&, const TFlagsTestParam& param) {
        auto [useSuggestedKey, sortHistogram] = GetSplitBoundaryByLoadModeFlags(
            param.ProtocolVersion,
            param.CanUseSuggestedKey,
            param.ShouldSortHistogram
        );
        UNIT_ASSERT_VALUES_EQUAL(useSuggestedKey, param.ResultUseSuggestedKey);
        UNIT_ASSERT_VALUES_EQUAL(sortHistogram, param.ResultSortHistogram);
    }
    static const std::vector<TFlagsTestParam> FlagsTests = {
        // Protocol 0: No suggested key support. Feature flags have no effect.
        // Schemeshard must sort input sample into histogram and select key.
        { .ProtocolVersion = 0, .CanUseSuggestedKey = false, .ShouldSortHistogram = false, .ResultUseSuggestedKey = false, .ResultSortHistogram = true },
        { .ProtocolVersion = 0, .CanUseSuggestedKey = false, .ShouldSortHistogram = true,  .ResultUseSuggestedKey = false, .ResultSortHistogram = true },
        { .ProtocolVersion = 0, .CanUseSuggestedKey = true,  .ShouldSortHistogram = false, .ResultUseSuggestedKey = false, .ResultSortHistogram = true },
        { .ProtocolVersion = 0, .CanUseSuggestedKey = true,  .ShouldSortHistogram = true,  .ResultUseSuggestedKey = false, .ResultSortHistogram = true },
        // Protocol 1: Supports receiving sorted histogram and/or preselected split boundary/key prefix.
        // SchemesShard can use suggested key, select key from supplied histogram, or resort input histogram and select key.
        // Depending on feature flags.
        { .ProtocolVersion = 1, .CanUseSuggestedKey = false, .ShouldSortHistogram = false, .ResultUseSuggestedKey = false, .ResultSortHistogram = false },
        { .ProtocolVersion = 1, .CanUseSuggestedKey = false, .ShouldSortHistogram = true,  .ResultUseSuggestedKey = false, .ResultSortHistogram = true },
        { .ProtocolVersion = 1, .CanUseSuggestedKey = true,  .ShouldSortHistogram = false, .ResultUseSuggestedKey = true,  .ResultSortHistogram = false },
        { .ProtocolVersion = 1, .CanUseSuggestedKey = true,  .ShouldSortHistogram = true,  .ResultUseSuggestedKey = true,  .ResultSortHistogram = true },
        // Protocol 2+: Unknown version.
        // Schemeshard can expect and use suggested key.
        // Schemeshard can expect presense of input sample but can't assume it's properties and should sort it into histogram.
        // Depending on feature flags.
        { .ProtocolVersion = 2, .CanUseSuggestedKey = false, .ShouldSortHistogram = false, .ResultUseSuggestedKey = false, .ResultSortHistogram = true },
        { .ProtocolVersion = 2, .CanUseSuggestedKey = false, .ShouldSortHistogram = true,  .ResultUseSuggestedKey = false, .ResultSortHistogram = true },
        { .ProtocolVersion = 2, .CanUseSuggestedKey = true,  .ShouldSortHistogram = false, .ResultUseSuggestedKey = true,  .ResultSortHistogram = true },
        { .ProtocolVersion = 2, .CanUseSuggestedKey = true,  .ShouldSortHistogram = true,  .ResultUseSuggestedKey = true,  .ResultSortHistogram = true },
    };
    struct TTestRegistrationFlagsTest {
        TTestRegistrationFlagsTest() {
            static std::vector<TString> TestNames;

            for (const auto& param : FlagsTests) {
                TestNames.emplace_back(TStringBuilder()
                    << "Flags-Protocol" << param.ProtocolVersion
                    << (param.CanUseSuggestedKey ? "-CanUseSuggestedKey" : "")
                    << (param.ShouldSortHistogram ? "-ShouldSortHistogram" : "")
                );

                TCurrentTest::AddTest(
                    TestNames.back().c_str(),
                    std::bind(FlagsTest, std::placeholders::_1, param),
                    /*forceFork*/ false
                );
            }
        }
    };
    static TTestRegistrationFlagsTest testRegistrationFlagsTest;

    enum ESelectTestResult {
        EMPTY,
        SUGGESTED,
        SELECTED
    };
    struct TSelectTestParam {
        ui32 ProtocolVersion = 0;           // Protocol version in stats
        bool HasSuggestedKey = false;       // Whether stats contain a suggested split key
        bool HasKeyAccessSample = false;    // Whether stats contain a key access sample
        bool CanUseSuggestedKey = false;    // Feature flag: whether to expect and use suggested keys
        bool ShouldSortHistogram = false;   // Feature flag: whether to sort input histogram
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
            stats.SetSplitByAccessSuggestedKey(SerializeKey({"1000", "suggested-key"}, keyTypes));
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
        auto result = GetSplitBoundaryByLoad(stats, keyTypes, param.CanUseSuggestedKey, param.ShouldSortHistogram);

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

    // Protocol 0: No suggested key support.
    // Schemeshard must build proper histogram and select key itself. Feature flags has no effect.
    Y_UNIT_TEST_FLAGS_N(Select_Protocol0_FlagsHaveNoEffect, bool HasSuggestedKey, bool HasKeyAccessSample, bool CanUseSuggestedKey, bool ShouldSortHistogram) {
        SelectTest({
            .ProtocolVersion = 0,
            .HasSuggestedKey = HasSuggestedKey,
            .HasKeyAccessSample = HasKeyAccessSample,
            .CanUseSuggestedKey = CanUseSuggestedKey,
            .ShouldSortHistogram = ShouldSortHistogram,
            .Result = (HasKeyAccessSample ? SELECTED : EMPTY),
        });
    }

    // Protocol 1: No histogram and no suggested key gives empty split boundary. Feature flags has no effect.
    Y_UNIT_TEST_FLAGS_N(Select_Protocol1_EmptyStats_FlagsHaveNoEffect, bool CanUseSuggestedKey, bool ShouldSortHistogram) {
        SelectTest({
            .ProtocolVersion = 1,
            .HasSuggestedKey = false,
            .HasKeyAccessSample = false,
            .CanUseSuggestedKey = CanUseSuggestedKey,
            .ShouldSortHistogram = ShouldSortHistogram,
            .Result = EMPTY,
        });
    }

    // Protocol 2: Future version. No histogram and no suggested key gives empty split boundary. Feature flags has no effect.
    Y_UNIT_TEST_FLAGS_N(Select_Protocol2_EmptyStats_FlagsHaveNoEffect, bool CanUseSuggestedKey, bool ShouldSortHistogram) {
        SelectTest({
            .ProtocolVersion = 2,
            .HasSuggestedKey = false,
            .HasKeyAccessSample = false,
            .CanUseSuggestedKey = CanUseSuggestedKey,
            .ShouldSortHistogram = ShouldSortHistogram,
            .Result = EMPTY,
        });
    }

    static const std::vector<TSelectTestParam> SelectTests = {
        // Protocol 1: Proper histogram and no suggested key gives proper split boundary.
        { .ProtocolVersion = 1, .HasSuggestedKey = false, .HasKeyAccessSample = true,  .CanUseSuggestedKey = false, .ShouldSortHistogram = false, .Result = SELECTED },
        { .ProtocolVersion = 1, .HasSuggestedKey = false, .HasKeyAccessSample = true,  .CanUseSuggestedKey = false, .ShouldSortHistogram = true,  .Result = SELECTED },
        { .ProtocolVersion = 1, .HasSuggestedKey = false, .HasKeyAccessSample = true,  .CanUseSuggestedKey = true,  .ShouldSortHistogram = false, .Result = SELECTED },
        { .ProtocolVersion = 1, .HasSuggestedKey = false, .HasKeyAccessSample = true,  .CanUseSuggestedKey = true,  .ShouldSortHistogram = true,  .Result = SELECTED },
        // Protocol 1: Suggested key may be used if allowed by flags.
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasKeyAccessSample = true,  .CanUseSuggestedKey = false, .ShouldSortHistogram = false, .Result = SELECTED },
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasKeyAccessSample = true,  .CanUseSuggestedKey = false, .ShouldSortHistogram = true,  .Result = SELECTED },
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasKeyAccessSample = true,  .CanUseSuggestedKey = true,  .ShouldSortHistogram = false, .Result = SUGGESTED },
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasKeyAccessSample = true,  .CanUseSuggestedKey = true,  .ShouldSortHistogram = true,  .Result = SUGGESTED },
        // Protocol 1: Edge case -- Proper suggested key but no histogram.
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasKeyAccessSample = false, .CanUseSuggestedKey = false, .ShouldSortHistogram = false, .Result = EMPTY },
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasKeyAccessSample = false, .CanUseSuggestedKey = false, .ShouldSortHistogram = true,  .Result = EMPTY },
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasKeyAccessSample = false, .CanUseSuggestedKey = true,  .ShouldSortHistogram = false, .Result = SUGGESTED },
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasKeyAccessSample = false, .CanUseSuggestedKey = true,  .ShouldSortHistogram = true,  .Result = SUGGESTED },
        // Protocol 2: Future version -- Same as protocol 1, can use suggested key and histogram
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasKeyAccessSample = true,  .CanUseSuggestedKey = false, .ShouldSortHistogram = false, .Result = SELECTED },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasKeyAccessSample = true,  .CanUseSuggestedKey = false, .ShouldSortHistogram = true,  .Result = SELECTED },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasKeyAccessSample = true,  .CanUseSuggestedKey = true,  .ShouldSortHistogram = false, .Result = SUGGESTED },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasKeyAccessSample = true,  .CanUseSuggestedKey = true,  .ShouldSortHistogram = true,  .Result = SUGGESTED },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasKeyAccessSample = false, .CanUseSuggestedKey = false, .ShouldSortHistogram = false, .Result = EMPTY },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasKeyAccessSample = false, .CanUseSuggestedKey = false, .ShouldSortHistogram = true,  .Result = EMPTY },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasKeyAccessSample = false, .CanUseSuggestedKey = true,  .ShouldSortHistogram = false, .Result = SUGGESTED },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasKeyAccessSample = false, .CanUseSuggestedKey = true,  .ShouldSortHistogram = true,  .Result = SUGGESTED },
    };
    struct TTestRegistrationSelectTest {
        TTestRegistrationSelectTest() {
            static std::vector<TString> TestNames;

            for (const auto& param : SelectTests) {
                TestNames.emplace_back(TStringBuilder()
                    << "Select-Protocol" << param.ProtocolVersion
                    << (param.HasSuggestedKey ? "-HasSuggestedKey": "")
                    << (param.HasKeyAccessSample ? "-HasKeyAccessSample": "")
                    << (param.CanUseSuggestedKey ? "-CanUseSuggestedKey": "")
                    << (param.ShouldSortHistogram ? "-ShouldSortHistogram": "")
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
    struct TFlagsTestParam {
        ui32 ProtocolVersion = 0;              // Protocol version in stats (0 = old protocol, 1+ = new protocol)
        bool CanUseSuggestedKey = false;       // Feature flag: whether to expect and use suggested keys
        bool ResultUseSuggestedKey = false;    // Expected result: should schemeshard use the suggested key?
    };

    static void FlagsTest(NUnitTest::TTestContext&, const TFlagsTestParam& param) {
        bool useSuggestedKey = GetSplitBoundaryBySizeModeFlag(
            param.ProtocolVersion,
            param.CanUseSuggestedKey
        );

        UNIT_ASSERT_VALUES_EQUAL(useSuggestedKey, param.ResultUseSuggestedKey);
    }
    static const std::vector<TFlagsTestParam> FlagsTests = {
        // Protocol 0: No suggested key support. Schemeshard must select key itself. Feature flag has no effect.
        { .ProtocolVersion = 0, .CanUseSuggestedKey = false, .ResultUseSuggestedKey = false },
        { .ProtocolVersion = 0, .CanUseSuggestedKey = true,  .ResultUseSuggestedKey = false },
        { .ProtocolVersion = 0, .CanUseSuggestedKey = false, .ResultUseSuggestedKey = false },
        { .ProtocolVersion = 0, .CanUseSuggestedKey = true,  .ResultUseSuggestedKey = false },
        // Protocol 1: Schemeshard can use suggested key if allowed by flag.
        { .ProtocolVersion = 1, .CanUseSuggestedKey = false, .ResultUseSuggestedKey = false },
        { .ProtocolVersion = 1, .CanUseSuggestedKey = true,  .ResultUseSuggestedKey = true },
        { .ProtocolVersion = 1, .CanUseSuggestedKey = false, .ResultUseSuggestedKey = false },
        { .ProtocolVersion = 1, .CanUseSuggestedKey = true,  .ResultUseSuggestedKey = true },
        // Protocol 2: Schemeshard can use suggested key if allowed by flag.
        { .ProtocolVersion = 2, .CanUseSuggestedKey = false, .ResultUseSuggestedKey = false },
        { .ProtocolVersion = 2, .CanUseSuggestedKey = true,  .ResultUseSuggestedKey = true },
        { .ProtocolVersion = 2, .CanUseSuggestedKey = false, .ResultUseSuggestedKey = false },
        { .ProtocolVersion = 2, .CanUseSuggestedKey = true,  .ResultUseSuggestedKey = true },
    };
    struct TTestRegistrationFlagsTest {
        TTestRegistrationFlagsTest() {
            static std::vector<TString> TestNames;

            for (const auto& param: FlagsTests) {
                TestNames.emplace_back(TStringBuilder()
                    << "Flags-Protocol" << param.ProtocolVersion
                    << (param.CanUseSuggestedKey ? "-CanUseSuggestedKey": "")
                );

                TCurrentTest::AddTest(
                    TestNames.back().c_str(),
                    std::bind(FlagsTest, std::placeholders::_1, param),
                    /*forceFork*/ false
                );
            }
        }
    };
    static TTestRegistrationFlagsTest testRegistrationFlagsTest;

    enum ESelectTestResult {
        EMPTY,
        SUGGESTED,
        SELECTED
    };
    struct TSelectTestParam {
        ui32 ProtocolVersion = 0;           // Protocol version in stats
        bool HasSuggestedKey = false;       // Whether stats contain a suggested split key
        bool HasDataSizeHistogram = false;  // Whether stats contain a data size histogram
        bool CanUseSuggestedKey = false;    // Feature flag: whether to expect and use suggested keys
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
        auto result = GetSplitBoundaryBySize(stats, keyTypes, param.CanUseSuggestedKey);

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

    // Protocol 0: No suggested key support.
    // Schemeshard must select key. Feature flag has no effect.
    Y_UNIT_TEST_FLAGS_N(Select_Protocol0_FlagsHaveNoEffect, bool HasSuggestedKey, bool HasDataSizeHistogram, bool CanUseSuggestedKey) {
        SelectTest({
            .ProtocolVersion = 0,
            .HasSuggestedKey = HasSuggestedKey,
            .HasDataSizeHistogram = HasDataSizeHistogram,
            .CanUseSuggestedKey = CanUseSuggestedKey,
            .Result = (HasDataSizeHistogram ? SELECTED : EMPTY),
        });
    }

    // Protocol 1: No histogram and no suggested key gives empty split boundary. Feature flags has no effect.
    Y_UNIT_TEST_FLAGS_N(Select_Protocol1_EmptyStats_FlagsHaveNoEffect, bool CanUseSuggestedKey) {
        SelectTest({
            .ProtocolVersion = 1,
            .HasSuggestedKey = false,
            .HasDataSizeHistogram = false,
            .CanUseSuggestedKey = CanUseSuggestedKey,
            .Result = EMPTY,
        });
    }

    // Protocol 2: Future version. No histogram and no suggested key gives empty split boundary. Feature flags has no effect.
    Y_UNIT_TEST_FLAGS_N(Select_Protocol2_EmptyStats_FlagsHaveNoEffect, bool CanUseSuggestedKey) {
        SelectTest({
            .ProtocolVersion = 2,
            .HasSuggestedKey = false,
            .HasDataSizeHistogram = false,
            .CanUseSuggestedKey = CanUseSuggestedKey,
            .Result = EMPTY,
        });
    }

    static const std::vector<TSelectTestParam> SelectTests = {
        // Protocol 1: Histogram present and no suggested key gives proper split boundary.
        { .ProtocolVersion = 1, .HasSuggestedKey = false, .HasDataSizeHistogram = true,  .CanUseSuggestedKey = false, .Result = SELECTED },
        { .ProtocolVersion = 1, .HasSuggestedKey = false, .HasDataSizeHistogram = true,  .CanUseSuggestedKey = true,  .Result = SELECTED },
        // Protocol 1: Suggested key may be used if allowed by flag.
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasDataSizeHistogram = true,  .CanUseSuggestedKey = false, .Result = SELECTED },
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasDataSizeHistogram = true,  .CanUseSuggestedKey = true,  .Result = SUGGESTED },
        // Protocol 1: Edge case -- Proper suggested key but no histogram.
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasDataSizeHistogram = false, .CanUseSuggestedKey = false, .Result = EMPTY },
        { .ProtocolVersion = 1, .HasSuggestedKey = true,  .HasDataSizeHistogram = false, .CanUseSuggestedKey = true,  .Result = SUGGESTED },
        // Protocol 2: Future version -- Same as protocol 1, can use suggested key and histogram
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasDataSizeHistogram = true,  .CanUseSuggestedKey = false, .Result = SELECTED },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasDataSizeHistogram = true,  .CanUseSuggestedKey = true,  .Result = SUGGESTED },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasDataSizeHistogram = false, .CanUseSuggestedKey = false, .Result = EMPTY },
        { .ProtocolVersion = 2, .HasSuggestedKey = true,  .HasDataSizeHistogram = false, .CanUseSuggestedKey = true,  .Result = SUGGESTED },
    };
    struct TTestRegistrationSelectTest {
        TTestRegistrationSelectTest() {
            static std::vector<TString> TestNames;

            for (const auto& param : SelectTests) {
                TestNames.emplace_back(TStringBuilder()
                    << "Select-Protocol" << param.ProtocolVersion
                    << (param.HasSuggestedKey ? "-HasSuggestedKey": "")
                    << (param.CanUseSuggestedKey ? "-CanUseSuggestedKey": "")
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
