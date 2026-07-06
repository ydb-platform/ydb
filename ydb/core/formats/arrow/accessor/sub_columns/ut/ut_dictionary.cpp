#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/request.h>

#include <ydb/services/metadata/abstract/request_features.h>

#include <yql/essentials/types/binary_json/write.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(SubColumnsDictionary) {
    using namespace NKikimr;
    using namespace NKikimr::NArrow::NAccessor;

    Y_UNIT_TEST(KffValidation) {
        const auto deserializeKff = [](const TString& kff) {
            NSubColumns::TRequestedConstuctor constructor;
            NYql::TFeaturesExtractor features(THashMap<TString, TString>{ { "DICTIONARY_DETECTOR_KFF", kff } }, {});
            return constructor.DeserializeFromRequest(features);
        };

        {
            auto status = deserializeKff("0.5");
            UNIT_ASSERT_C(status.IsFail(), "fractional KFF < 1 must be rejected");
            UNIT_ASSERT_C(status.GetErrorMessage().Contains("DICTIONARY_DETECTOR_KFF"), status.GetErrorMessage());
        }
        UNIT_ASSERT_C(deserializeKff("-1").IsFail(), "negative KFF must be rejected");
        UNIT_ASSERT_C(deserializeKff("1").IsSuccess(), "KFF == 1 must be accepted");
        UNIT_ASSERT_C(deserializeKff("2").IsSuccess(), "KFF > 1 must be accepted");
    }

    Y_UNIT_TEST(IsDictionaryGate) {
        const auto gate = [](double kff, ui32 distinct, ui32 usage) {
            NSubColumns::TSettings settings(4, 1024, 0, 0, NSubColumns::TDataAdapterContainer::GetDefault(), kff);
            return settings.IsDictionary(distinct, usage);
        };
        UNIT_ASSERT(!gate(0, 5, 100));     // KFF < 1 -> feature off
        UNIT_ASSERT(gate(1, 300, 300));    // distinct == usage boundary passes
        UNIT_ASSERT(gate(1, 1, 300));
        UNIT_ASSERT(!gate(1, 0, 300));     // no distinct values -> never
        UNIT_ASSERT(!gate(2, 300, 300));   // distinct * 2 > usage -> rejected
        UNIT_ASSERT(gate(2, 150, 300));
        UNIT_ASSERT(!gate(2, 151, 300));
    }

    // Write-path application of the gate: an all-distinct separated column (300 distinct -> uint16
    // positions when encoded) is dictionary-encoded at KFF=1 (distinct == records) but stays a plain
    // Array at KFF=2.
    Y_UNIT_TEST(KffGate) {
        const auto encodedAsDictionary = [](double kff) {
            NSubColumns::TSettings settings(4, 1024, 0, /*othersFraction*/ 0, NSubColumns::TDataAdapterContainer::GetDefault(), kff);
            TTrivialArray::TPlainBuilder<arrow::BinaryType> builder;
            constexpr ui32 rows = 300;
            for (ui32 i = 0; i < rows; ++i) {
                auto v = NBinaryJson::SerializeToBinaryJson(TStringBuilder() << R"({"a":"v)" << i << R"("})");
                auto* bj = std::get_if<NBinaryJson::TBinaryJson>(&v);
                UNIT_ASSERT(bj);
                builder.AddRecord(i, std::string_view(bj->data(), bj->size()));
            }
            auto arr = builder.Finish(rows);
            auto data = TSubColumnsArray::Make(arr, settings, arr->GetDataType()).DetachResult();
            const auto& stats = data->GetColumnsData().GetStats();
            for (ui32 i = 0; i < stats.GetColumnsCount(); ++i) {
                if (stats.GetAccessorType(i) == IChunkedArray::EType::Dictionary) {
                    return true;
                }
            }
            return false;
        };
        UNIT_ASSERT_C(encodedAsDictionary(1), "KFF=1: all-distinct column must be dictionary-encoded");
        UNIT_ASSERT_C(!encodedAsDictionary(2), "KFF=2: all-distinct column must stay plain Array");
    }

    // The gate's present-count must account for explicit nulls ({"a":null} is a real stored value) but
    // NOT for records where the key is absent ({} leaves a gap).
    Y_UNIT_TEST(NullsInEncodingDecision) {
        const auto encodedAsDictionary = [](double kff, ui32 distinctValues, ui32 nullCount, ui32 absentCount) {
            NSubColumns::TSettings settings(4, 1024, 0, /*othersFraction*/ 0, NSubColumns::TDataAdapterContainer::GetDefault(), kff);
            TTrivialArray::TPlainBuilder<arrow::BinaryType> builder;
            ui32 row = 0;
            const auto addRecord = [&](const TString& json) {
                auto v = NBinaryJson::SerializeToBinaryJson(json);
                auto* bj = std::get_if<NBinaryJson::TBinaryJson>(&v);
                UNIT_ASSERT(bj);
                builder.AddRecord(row++, std::string_view(bj->data(), bj->size()));
            };
            for (ui32 i = 0; i < distinctValues; ++i) {
                addRecord(TStringBuilder() << R"({"a":"v)" << i << R"("})");
            }
            for (ui32 i = 0; i < nullCount; ++i) {
                addRecord(R"({"a":null})");
            }
            for (ui32 i = 0; i < absentCount; ++i) {
                addRecord(R"({})"); // key "a" absent -> a gap, must not count toward present
            }
            auto arr = builder.Finish(row);
            auto data = TSubColumnsArray::Make(arr, settings, arr->GetDataType()).DetachResult();
            const auto& stats = data->GetColumnsData().GetStats();
            for (ui32 i = 0; i < stats.GetColumnsCount(); ++i) {
                if (stats.GetAccessorType(i) == IChunkedArray::EType::Dictionary) {
                    return true;
                }
            }
            return false;
        };
        // Explicit nulls ARE accounted: 1 distinct value + 3 nulls -> distinct=2, present=4 -> 2*2 <= 4 ->
        // dictionary. Fails if nulls are excluded from present (distinct=1,present=1) or counted per-null
        // (distinct=4,present=4) -> so this pins both "nulls count as present" and "all nulls = 1 distinct".
        UNIT_ASSERT_C(encodedAsDictionary(2, /*distinctValues*/ 1, /*nullCount*/ 3, /*absentCount*/ 0),
            "KFF=2: explicit nulls must count toward present and collapse to one distinct value");
        // Absent records are NOT accounted: 2 distinct values + 10 absent -> present=2 (gaps excluded),
        // distinct=2 -> 2*2 <= 2 is false -> plain Array. Fails if absent records padded present (-> dict).
        UNIT_ASSERT_C(!encodedAsDictionary(2, /*distinctValues*/ 2, /*nullCount*/ 0, /*absentCount*/ 10),
            "KFF=2: records missing the key are gaps and must not count toward present");
    }
};
