#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/program/kernel_logic.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/binary_json/write.h>

#include <string_view>

namespace NKikimr::NArrow::NSSA {
namespace {

class TTestGetJsonPath: public TGetJsonPath {
public:
    using TGetJsonPath::ExtractArray;
};

NAccessor::NSubColumns::TSettings BuildSettings(const double dictionaryFraction, const ui32 columnsLimit) {
    NAccessor::NSubColumns::TSettings settings(
        4, columnsLimit, 0, 0, NAccessor::NSubColumns::TDataAdapterContainer::GetDefault(), dictionaryFraction);
    settings.SetEnableNativeColumns(true);
    return settings;
}

std::shared_ptr<NAccessor::TSubColumnsArray> BuildSubColumns(
    const std::vector<TString>& jsons, const NAccessor::NSubColumns::TSettings& settings) {
    NAccessor::TTrivialArray::TPlainBuilder<arrow::BinaryType> builder;
    ui32 index = 0;
    for (const TString& json : jsons) {
        if (json != "null") {
            const auto value = NBinaryJson::SerializeToBinaryJson(json);
            const auto* binaryJson = std::get_if<NBinaryJson::TBinaryJson>(&value);
            UNIT_ASSERT(binaryJson);
            builder.AddRecord(index, std::string_view(binaryJson->data(), binaryJson->size()));
        }
        ++index;
    }
    auto sourceJson = builder.Finish(index);
    return NAccessor::TSubColumnsArray::Make(sourceJson, settings, sourceJson->GetDataType()).DetachResult();
}

TString ExtractJsonValue(const std::shared_ptr<NAccessor::TSubColumnsArray>& input, const std::string_view path) {
    auto result = TTestGetJsonPath().ExtractArray(input, path);
    UNIT_ASSERT_VALUES_EQUAL(result->GetRecordsCount(), input->GetRecordsCount());
    TString values;
    result->VisitValues([&](const std::shared_ptr<arrow::Array>& chunk) {
        UNIT_ASSERT(chunk->type_id() == arrow::Type::STRING);
        const auto* strings = static_cast<const arrow::StringArray*>(chunk.get());
        for (i64 index = 0; index < strings->length(); ++index) {
            if (strings->IsNull(index)) {
                values.append("<null>");
            } else {
                const auto value = strings->GetView(index);
                values.append(value.data(), value.size());
            }
            values.append(";");
        }
    });
    return values;
}

}

Y_UNIT_TEST_SUITE(JsonValue) {
    Y_UNIT_TEST(UsesNativeStringBuffers) {
        auto input = BuildSubColumns({ R"({"s":"x"})", R"({"s":"yy"})", R"({"s":"zzz"})" }, BuildSettings(0, 1024));
        auto accessor = input->GetPathAccessor("$.s", input->GetRecordsCount()).DetachResult();
        const auto& source = accessor->GetChunkedArrayAccessor();
        UNIT_ASSERT(source->GetType() == NAccessor::IChunkedArray::EType::Array);

        auto result = TTestGetJsonPath().ExtractArray(input, "$.s");
        UNIT_ASSERT(source->GetDataType()->id() == arrow::Type::STRING);
        UNIT_ASSERT_VALUES_EQUAL(result.get(), source.get());
    }

    Y_UNIT_TEST(HandlesNestedAndAbsentPaths) {
        auto input = BuildSubColumns({ R"({"object":{"s":"x"}})", "null", R"({"object":{"s":"yy"}})" }, BuildSettings(0, 1024));
        UNIT_ASSERT_VALUES_EQUAL(ExtractJsonValue(input, "$.object.s"), "x;<null>;yy;");
        UNIT_ASSERT_VALUES_EQUAL(ExtractJsonValue(input, "$.absent"), "<null>;<null>;<null>;");
    }

    Y_UNIT_TEST(HandlesBinaryJsonAndDictionaryStrings) {
        std::vector<TString> dictionaryDocs;
        for (ui32 index = 0; index < 40; ++index) {
            dictionaryDocs.emplace_back(TStringBuilder() << R"({"s":")" << (index % 2 ? "x" : "yy") << R"("})");
        }
        auto dictionaryInput = BuildSubColumns(dictionaryDocs, BuildSettings(1, 1024));
        TString dictionaryExpected;
        for (ui32 index = 0; index < dictionaryDocs.size(); ++index) {
            dictionaryExpected.append(index % 2 ? "x;" : "yy;");
        }
        UNIT_ASSERT_VALUES_EQUAL(ExtractJsonValue(dictionaryInput, "$.s"), dictionaryExpected);

        auto binaryJsonInput = BuildSubColumns({ R"({"value":"x"})", R"({"value":1})" }, BuildSettings(0, 1024));
        UNIT_ASSERT_VALUES_EQUAL(ExtractJsonValue(binaryJsonInput, "$.value"), "x;1;");
    }

    Y_UNIT_TEST(ReadsFromOthers) {
        auto input = BuildSubColumns({ R"({"s":"x"})", R"({"s":"yy"})" }, BuildSettings(0, 0));
        UNIT_ASSERT_VALUES_EQUAL(input->GetColumnsData().GetStats().GetColumnsCount(), 0);
        UNIT_ASSERT_VALUES_EQUAL(input->GetOthersData().GetStats().GetColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ExtractJsonValue(input, "$.s"), "x;yy;");
    }
};

} // namespace NKikimr::NArrow::NSSA
