#include "ut_helpers.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>

#include <ydb/library/actors/core/log.h>

#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns::NTesting {

TString PrintBinaryJsons(const std::shared_ptr<arrow::ChunkedArray>& array) {
    TStringBuilder sb;
    sb << "[";
    for (auto&& i : array->chunks()) {
        sb << "[";
        AFL_VERIFY(i->type()->id() == arrow::binary()->id());
        auto views = std::static_pointer_cast<arrow::BinaryArray>(i);
        for (ui32 r = 0; r < views->length(); ++r) {
            if (views->IsNull(r)) {
                sb << "null";
            } else {
                sb << NKikimr::NBinaryJson::SerializeToJson(TStringBuf(views->GetView(r).data(), views->GetView(r).size()));
            }
            if (r + 1 != views->length()) {
                sb << ",";
            }
        }
        sb << "]";
    }
    sb << "]";
    return sb;
}

std::shared_ptr<TTrivialArray> CreateTrivialArrayAccessor(const TStringBuf data) {
    auto binaryJsonResult = NBinaryJson::SerializeToBinaryJson(data);
    AFL_VERIFY(std::holds_alternative<NBinaryJson::TBinaryJson>(binaryJsonResult));
    const auto binaryJson = std::get<NBinaryJson::TBinaryJson>(binaryJsonResult);
    return std::make_shared<TTrivialArray>(TTrivialArray::BuildArrayFromScalar(std::make_shared<arrow::BinaryScalar>(
        std::make_shared<arrow::Buffer>((const ui8*)binaryJson.data(), binaryJson.size()), arrow::binary())));
}

TDictStats BuildStats(const std::initializer_list<std::pair<TStringBuf, EValueType>>& columns) {
    auto builder = TDictStats::MakeBuilder();
    for (const auto& [name, valueType] : columns) {
        builder.Add(TString(name), 1, 1, IChunkedArray::EType::Array, valueType);
    }
    return builder.Finish();
}

std::shared_ptr<TSubColumnsArray> BuildArrayWithStoredPaths(const std::initializer_list<std::pair<TStringBuf, TStringBuf>>& columns,
    const TStringBuf otherName, const TStringBuf otherValue) {
    auto columnsBuilder = TDictStats::MakeBuilder();
    for (const auto& [name, _] : columns) {
        columnsBuilder.Add(TString(name), 1, 1, IChunkedArray::EType::Array, EValueType::BinaryJson);
    }
    auto columnsStats = columnsBuilder.Finish();
    auto columnsRecords = std::make_shared<NArrow::TGeneralContainer>(1);
    ui32 index = 0;
    for (const auto& [_, value] : columns) {
        columnsRecords->AddField(columnsStats.GetField(index++), CreateTrivialArrayAccessor(value)).Validate();
    }

    auto othersStats = BuildStats({ { otherName, EValueType::BinaryJson } });
    auto binaryJsonResult = NBinaryJson::SerializeToBinaryJson(otherValue);
    AFL_VERIFY(std::holds_alternative<NBinaryJson::TBinaryJson>(binaryJsonResult));
    const auto& binaryJson = std::get<NBinaryJson::TBinaryJson>(binaryJsonResult);
    auto othersBuilder = TOthersData::MakeMergedBuilder();
    othersBuilder->Add(0, 0, std::string_view(binaryJson.data(), binaryJson.size()));
    auto others = othersBuilder->Finish(TOthersData::TFinishContext(othersStats));

    return std::make_shared<TSubColumnsArray>(
        TColumnsData(std::move(columnsStats), columnsRecords), std::move(others), arrow::binary(), 1, TSettings());
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns::NTesting
