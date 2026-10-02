#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/request.h>

#include <ydb/services/metadata/abstract/request_features.h>

#include <yql/essentials/types/binary_json/write.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(SubColumnsDictionary) {
    using namespace NKikimr;
    using namespace NKikimr::NArrow::NAccessor;

    Y_UNIT_TEST(UniqueFractionValidation) {
        const auto deserializeFraction = [](const TString& fraction) {
            NSubColumns::TRequestedConstuctor constructor;
            NYql::TFeaturesExtractor features(THashMap<TString, TString>{ { "DICTIONARY_UNIQUE_FRACTION", fraction } }, {});
            return constructor.DeserializeFromRequest(features);
        };

        {
            auto status = deserializeFraction("2");
            UNIT_ASSERT_C(status.IsFail(), "fraction > 1 must be rejected");
            UNIT_ASSERT_C(status.GetErrorMessage().Contains("DICTIONARY_UNIQUE_FRACTION"), status.GetErrorMessage());
        }
        UNIT_ASSERT_C(deserializeFraction("-1").IsFail(), "negative fraction must be rejected");
        UNIT_ASSERT_C(deserializeFraction("0").IsSuccess(), "fraction == 0 (disabled) must be accepted");
        UNIT_ASSERT_C(deserializeFraction("0.5").IsSuccess(), "fraction in (0, 1) must be accepted");
        UNIT_ASSERT_C(deserializeFraction("1").IsSuccess(), "fraction == 1 must be accepted");
    }

    Y_UNIT_TEST(IsDictionaryGate) {
        // Feeds `distinct` unique values (so the decision counts exactly `distinct` uniques) through the
        // early-exit IsDictionary against `usage` records.
        const auto gate = [](double fraction, ui32 distinct, ui32 usage) {
            NSubColumns::TSettings settings(4, 1024, 0, 0, NSubColumns::TDataAdapterContainer::GetDefault(), fraction);
            std::vector<TString> values;
            values.reserve(distinct);
            for (ui32 i = 0; i < distinct; ++i) {
                values.push_back(ToString(i));
            }
            return settings.IsDictionary(usage, [&](const auto& consumer) {
                for (const auto& v : values) {
                    if (!consumer(std::string_view(v.data(), v.size()))) {
                        break;
                    }
                }
            });
        };
        UNIT_ASSERT_C(!gate(0, 5, 100), "fraction 0 -> feature off");
        UNIT_ASSERT_C(gate(1, 300, 300), "distinct == usage boundary must pass at fraction 1");
        UNIT_ASSERT_C(!gate(0.5, 300, 300), "300/300 = 1.0 > 0.5 -> must be rejected");
        UNIT_ASSERT_C(gate(0.5, 150, 300), "150/300 = 0.5 boundary must pass");
        UNIT_ASSERT_C(!gate(0.5, 151, 300), "151/300 > 0.5 -> must be rejected");
    }

    // Write-path application of the gate: an all-distinct separated column (300 distinct -> uint16
    // positions when encoded) is dictionary-encoded at fraction 1 (unique fraction == 1) but stays a
    // plain Array at fraction 0.5.
    Y_UNIT_TEST(KffGate) {
        const auto encodedAsDictionary = [](double fraction) {
            NSubColumns::TSettings settings(4, 1024, 0, /*othersFraction*/ 0, NSubColumns::TDataAdapterContainer::GetDefault(), fraction);
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
        UNIT_ASSERT_C(encodedAsDictionary(1), "fraction 1: all-distinct column must be dictionary-encoded");
        UNIT_ASSERT_C(!encodedAsDictionary(0.5), "fraction 0.5: all-distinct column must stay plain Array");
    }

    // The gate's present-count must account for explicit nulls ({"a":null} is a real stored value) but
    // NOT for records where the key is absent ({} leaves a gap).
    Y_UNIT_TEST(NullsInEncodingDecision) {
        const auto encodedAsDictionary = [](double fraction, ui32 distinctValues, ui32 nullCount, ui32 absentCount) {
            NSubColumns::TSettings settings(4, 1024, 0, /*othersFraction*/ 0, NSubColumns::TDataAdapterContainer::GetDefault(), fraction);
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
        // Explicit nulls ARE accounted: 1 distinct value + 3 nulls -> distinct=2, present=4 -> 2 <= 0.5*4 ->
        // dictionary. Fails if nulls are excluded from present (distinct=1,present=1) or counted per-null
        // (distinct=4,present=4) -> so this pins both "nulls count as present" and "all nulls = 1 distinct".
        UNIT_ASSERT_C(encodedAsDictionary(0.5, /*distinctValues*/ 1, /*nullCount*/ 3, /*absentCount*/ 0),
            "fraction 0.5: explicit nulls must count toward present and collapse to one distinct value");
        // Absent records are NOT accounted: 2 distinct values + 10 absent -> present=2 (gaps excluded),
        // distinct=2 -> 2 <= 0.5*2 is false -> plain Array. Fails if absent records padded present (-> dict).
        UNIT_ASSERT_C(!encodedAsDictionary(0.5, /*distinctValues*/ 2, /*nullCount*/ 0, /*absentCount*/ 10),
            "fraction 0.5: records missing the key are gaps and must not count toward present");
    }
};
