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
};
