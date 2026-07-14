#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include "ut_helpers.h"

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

#include <algorithm>

using NKikimr::NArrow::NAccessor::NSubColumns::NTesting::PrintBinaryJsons;

Y_UNIT_TEST_SUITE(SubColumnsNativeScalars) {
    using namespace NKikimr;
    using namespace NKikimr::NArrow;
    using namespace NKikimr::NArrow::NAccessor;
    using namespace NKikimr::NArrow::NAccessor::NSubColumns;

    NSubColumns::TSettings NativeSettings(const double dictFraction) {
        NSubColumns::TSettings s(4, 1024, 0, 0, TDataAdapterContainer::GetDefault(), dictFraction);
        s.SetEnableNativeColumns(true);
        return s;
    }

    NSubColumns::TSettings OffSettings() {
        return NSubColumns::TSettings(4, 1024, 0, 0, TDataAdapterContainer::GetDefault());
    }

    std::shared_ptr<TSubColumnsArray> BuildSubColumns(const std::vector<TString>& jsons, const NSubColumns::TSettings& settings) {
        TTrivialArray::TPlainBuilder<arrow::BinaryType> b;
        ui32 idx = 0;
        for (auto&& j : jsons) {
            if (j != "null") {
                auto v = NBinaryJson::SerializeToBinaryJson(j);
                auto* bj = std::get_if<NBinaryJson::TBinaryJson>(&v);
                UNIT_ASSERT(bj);
                b.AddRecord(idx, std::string_view(bj->data(), bj->size()));
            }
            ++idx;
        }
        auto arr = b.Finish(jsons.size());
        return TSubColumnsArray::Make(arr, settings, arr->GetDataType()).DetachResult();
    }

    // Number of separated columns stored with the given value type.
    ui32 CountValueType(const std::shared_ptr<TSubColumnsArray>& arr, const EValueType vt) {
        const auto& stats = arr->GetColumnsData().GetStats();
        ui32 n = 0;
        for (ui32 i = 0; i < stats.GetColumnsCount(); ++i) {
            n += (stats.GetValueType(i) == vt);
        }
        return n;
    }

    std::shared_ptr<TSubColumnsArray> SerializeRoundTrip(const std::shared_ptr<TSubColumnsArray>& arr, const NSubColumns::TSettings& settings) {
        auto serializer = NSerialization::TSerializerContainer::GetDefaultSerializer();
        TChunkConstructionData cData(arr->GetRecordsCount(), nullptr, arrow::binary(), serializer);
        NSubColumns::TConstructor constructor(settings);
        return std::static_pointer_cast<TSubColumnsArray>(constructor.DeserializeFromString(arr->SerializeToString(cData), cData).DetachResult());
    }

    void AssertColumnType(const std::vector<TString>& docs, const EValueType vt, IChunkedArray::EType type, std::string description) {
        auto arr = BuildSubColumns(docs, NativeSettings(1));
        const auto& stats = arr->GetColumnsData().GetStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.GetColumnsCount(), 1);
        UNIT_ASSERT_C(stats.GetAccessorType(0) == type && stats.GetValueType(0) == vt,
                TStringBuilder() << "expected a " << description << " column with value_type " << (ui32)vt << ": " << arr->DebugJson().GetStringRobust());
        auto restored = SerializeRoundTrip(arr, NativeSettings(1));
        UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(restored->GetChunkedArray()), PrintBinaryJsons(arr->GetChunkedArray()));
    }

    void AssertDictColumn(const std::vector<TString>& docs, const EValueType vt) {
        AssertColumnType(docs, vt, IChunkedArray::EType::Dictionary, "Dictionary");
    }

    void AssertPlainNativeColumn(const std::vector<TString>& docs, const EValueType vt) {
        AssertColumnType(docs, vt, IChunkedArray::EType::Array, "plain native");
    }

    Y_UNIT_TEST(Detection) {
        const std::vector<TString> docs = {
            R"({"s":"x","n":1,"b":true})",
            R"({"s":"yy","n":2,"b":false})",
            R"({"s":"zzz","n":3,"b":true})",
        };
        auto arr = BuildSubColumns(docs, NativeSettings(0));
        UNIT_ASSERT_VALUES_EQUAL_C(CountValueType(arr, EValueType::String), 1, arr->DebugJson().GetStringRobust());
        UNIT_ASSERT_VALUES_EQUAL_C(CountValueType(arr, EValueType::Double), 1, arr->DebugJson().GetStringRobust());
        UNIT_ASSERT_VALUES_EQUAL_C(CountValueType(arr, EValueType::Bool), 1, arr->DebugJson().GetStringRobust());
        UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arr->GetChunkedArray()), PrintBinaryJsons(BuildSubColumns(docs, OffSettings())->GetChunkedArray()));
    }

    Y_UNIT_TEST(DoubleRoundTrip) {
        const std::vector<TString> docs = {
            R"({"n":3.5})", R"({"n":-2})", R"({"n":0})", R"({"n":1000000})", R"({"n":2.718281828})",
        };
        auto arr = BuildSubColumns(docs, NativeSettings(0));
        UNIT_ASSERT_VALUES_EQUAL_C(CountValueType(arr, EValueType::Double), 1, arr->DebugJson().GetStringRobust());
        UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arr->GetChunkedArray()), PrintBinaryJsons(BuildSubColumns(docs, OffSettings())->GetChunkedArray()));
    }

    // Double and Bool are never dictionary-encoded, even at low cardinality: the compact native arrays
    // make dictionary encoding excessive, so they stay plain native columns.
    Y_UNIT_TEST(DoubleStaysPlainAtLowCardinality) {
        std::vector<TString> docs;
        for (ui32 i = 0; i < 40; ++i) {
            docs.push_back(TStringBuilder() << R"({"n":)" << (i % 2 ? "1.5" : "2.5") << "}");
        }
        AssertPlainNativeColumn(docs, EValueType::Double);
    }

    Y_UNIT_TEST(BoolStaysPlainAtLowCardinality) {
        std::vector<TString> docs;
        for (ui32 i = 0; i < 40; ++i) {
            docs.push_back(TStringBuilder() << R"({"b":)" << (i % 2 ? "true" : "false") << "}");
        }
        AssertPlainNativeColumn(docs, EValueType::Bool);
    }

    Y_UNIT_TEST(BoolRoundTrip) {
        const std::vector<TString> docs = { R"({"b":true})", R"({"b":false})", R"({"b":true})" };
        auto arr = BuildSubColumns(docs, NativeSettings(0));
        UNIT_ASSERT_VALUES_EQUAL_C(CountValueType(arr, EValueType::Bool), 1, arr->DebugJson().GetStringRobust());
        auto restored = SerializeRoundTrip(arr, NativeSettings(0));
        UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(restored->GetChunkedArray()), PrintBinaryJsons(BuildSubColumns(docs, OffSettings())->GetChunkedArray()));
    }

    // A container value (array/object) is not a scalar, so the key stays BinaryJson.
    Y_UNIT_TEST(ContainerStaysBinaryJson) {
        const std::vector<TString> docs = { R"({"a":[1,2]})", R"({"a":[3]})" };
        auto arr = BuildSubColumns(docs, NativeSettings(0));
        UNIT_ASSERT_VALUES_EQUAL(CountValueType(arr, EValueType::Double), 0);
        UNIT_ASSERT_VALUES_EQUAL(CountValueType(arr, EValueType::String), 0);
        UNIT_ASSERT_VALUES_EQUAL(CountValueType(arr, EValueType::BinaryJson), 1);
        UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arr->GetChunkedArray()), PrintBinaryJsons(BuildSubColumns(docs, OffSettings())->GetChunkedArray()));
    }

    // With the setting off (the default), the same all-string key stays BinaryJson.
    Y_UNIT_TEST(OffByDefault) {
        const std::vector<TString> docs = { R"({"a":"x"})", R"({"a":"yy"})" };
        auto arr = BuildSubColumns(docs, OffSettings());
        UNIT_ASSERT_VALUES_EQUAL(CountValueType(arr, EValueType::String), 0);
        UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arr->GetChunkedArray()), R"([[{"a":"x"},{"a":"yy"}]])");
    }

    // A full serialize -> deserialize round-trip (exercising the 5-column stats blob) preserves the
    // native String column and reconstructs identical documents.
    Y_UNIT_TEST(SerializeRoundTripPreservesNative) {
        const std::vector<TString> docs = { R"({"a":"x","n":1})", R"({"a":"yy","n":2})" };
        auto arr = BuildSubColumns(docs, NativeSettings(0));
        auto restored = SerializeRoundTrip(arr, NativeSettings(0));
        UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(restored->GetChunkedArray()), PrintBinaryJsons(arr->GetChunkedArray()));
        UNIT_ASSERT_VALUES_EQUAL_C(CountValueType(restored, EValueType::String), 1, restored->DebugJson().GetStringRobust());
    }

    // A low-cardinality String key composes with dictionary encoding.
    Y_UNIT_TEST(StringComposesWithDictionary) {
        std::vector<TString> docs;
        for (ui32 i = 0; i < 40; ++i) {
            docs.push_back(TStringBuilder() << R"({"a":")" << (i % 2 ? "xxxx" : "yyyy") << R"("})");
        }
        AssertDictColumn(docs, EValueType::String);
    }

    Y_UNIT_TEST(MixedTypeBinaryJsonFallback) {
        auto arr = BuildSubColumns({ R"({"a":"x"})", R"({"a":7})" }, NativeSettings(0));
        UNIT_ASSERT_VALUES_EQUAL(CountValueType(arr, EValueType::BinaryJson), 1);
        UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arr->GetChunkedArray()), R"([[{"a":"x"},{"a":7}]])");
    }

    Y_UNIT_TEST(NullBinaryJsonFallback) {
        auto arr = BuildSubColumns({ R"({"a":"x"})", R"({"a":null})" }, NativeSettings(0));
        UNIT_ASSERT_VALUES_EQUAL(CountValueType(arr, EValueType::BinaryJson), 1);
        UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arr->GetChunkedArray()), R"([[{"a":"x"},{"a":null}]])");
    }

    Y_UNIT_TEST(SpecialCharsRoundTrip) {
        const std::vector<TString> docs = {
            R"({"a":"he said \"hi\""})",
            R"({"a":"tab\tend"})",
            R"({"a":"юникод"})",
        };
        auto arr = BuildSubColumns(docs, NativeSettings(0));
        UNIT_ASSERT_VALUES_EQUAL_C(CountValueType(arr, EValueType::String), 1, arr->DebugJson().GetStringRobust());
        UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arr->GetChunkedArray()), PrintBinaryJsons(BuildSubColumns(docs, OffSettings())->GetChunkedArray()));
    }
};
