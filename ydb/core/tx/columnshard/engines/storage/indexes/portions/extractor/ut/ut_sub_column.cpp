#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/sub_column.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr::NOlap::NIndexes {

namespace {

using namespace NArrow::NAccessor;
using namespace NArrow::NAccessor::NSubColumns;

std::shared_ptr<TTrivialArray> MakeBinaryJsonArray(const TStringBuf data) {
    auto binaryJsonResult = NBinaryJson::SerializeToBinaryJson(data);
    UNIT_ASSERT(std::holds_alternative<NBinaryJson::TBinaryJson>(binaryJsonResult));
    const auto binaryJson = std::get<NBinaryJson::TBinaryJson>(binaryJsonResult);
    return std::make_shared<TTrivialArray>(TTrivialArray::BuildArrayFromScalar(std::make_shared<arrow::BinaryScalar>(
        std::make_shared<arrow::Buffer>((const ui8*)binaryJson.data(), binaryJson.size()), arrow::binary())));
}

TDictStats MakeStats(const std::initializer_list<TStringBuf>& names) {
    auto builder = TDictStats::MakeBuilder();
    for (const auto& name : names) {
        builder.Add(TString(name), 1, 1, IChunkedArray::EType::Array, EValueType::BinaryJson);
    }
    return builder.Finish();
}

std::shared_ptr<TSubColumnsArray> MakeArray() {
    auto columnsStats = MakeStats({ R"("a")", R"("a"."b"."c")" });
    auto columnsRecords = std::make_shared<NArrow::TGeneralContainer>(1);
    columnsRecords->AddField(columnsStats.GetField(0), MakeBinaryJsonArray(R"("columns")")).Validate();
    columnsRecords->AddField(columnsStats.GetField(1), MakeBinaryJsonArray(R"("descendant")")).Validate();

    auto othersStats = MakeStats({ R"("a"."b")" });
    auto binaryJsonResult = NBinaryJson::SerializeToBinaryJson(R"("others")");
    UNIT_ASSERT(std::holds_alternative<NBinaryJson::TBinaryJson>(binaryJsonResult));
    const auto& binaryJson = std::get<NBinaryJson::TBinaryJson>(binaryJsonResult);
    auto othersBuilder = TOthersData::MakeMergedBuilder();
    othersBuilder->Add(0, 0, std::string_view(binaryJson.data(), binaryJson.size()));
    auto others = othersBuilder->Finish(TOthersData::TFinishContext(othersStats));

    return std::make_shared<TSubColumnsArray>(
        TColumnsData(std::move(columnsStats), columnsRecords), std::move(others), arrow::binary(), 1, TSettings());
}

}   // namespace

Y_UNIT_TEST_SUITE(TSubColumnDataExtractorTests) {
    Y_UNIT_TEST(UsesExactStoredPath) {
        TSubColumnDataExtractor extractor;
        NJson::TJsonValue config(NJson::JSON_MAP);
        config.InsertValue("sub_column_name", R"("a"."b")");
        UNIT_ASSERT(extractor.DeserializeFromJson(config).IsSuccess());

        TString result;
        extractor.VisitAll(MakeArray(), {}, [&result](const NArrow::NAccessor::TJsonValueView& value, ui64) {
            result = value.ToJsonValue().GetString();
        });
        UNIT_ASSERT_VALUES_EQUAL(result, "others");
    }
}

}   // namespace NKikimr::NOlap::NIndexes
