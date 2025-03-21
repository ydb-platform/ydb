#include "accessor.h"
#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/composite_serial/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstructDefault(const TChunkConstructionData& externalInfo) const {
    AFL_VERIFY(externalInfo.GetDefaultValue() == nullptr);
    return std::make_shared<TSubColumnsArray>(externalInfo.GetColumnType(), externalInfo.GetRecordsCount(), Settings);
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoDeserializeFromString(
    const TString& originalData, const TChunkConstructionData& externalInfo) const {
    auto headerConclusion = TSubColumnsHeader::ReadHeader(originalData, externalInfo);
    if (headerConclusion.IsFail()) {
        return headerConclusion;
    }
    ui32 currentIndex = headerConclusion->GetHeaderSize();
    const auto& proto = headerConclusion->GetAddressesProto();

    std::shared_ptr<TGeneralContainer> columnKeysContainer;
    {
        std::vector<std::shared_ptr<IChunkedArray>> columns;
        auto schema = headerConclusion->GetColumnStats().BuildColumnsSchema();
        AFL_VERIFY(headerConclusion->GetColumnStats().GetColumnsCount() == (ui32)proto.GetKeyColumns().size())(
                                                                             "schema", headerConclusion->GetColumnStats().GetColumnsCount())(
                                                  "proto", proto.GetKeyColumns().size());
        for (ui32 i = 0; i < (ui32)proto.GetKeyColumns().size(); ++i) {
            std::shared_ptr<TColumnLoader> columnLoader = std::make_shared<TColumnLoader>(
                externalInfo.GetDefaultSerializer(), headerConclusion->GetColumnStats().GetAccessorConstructor(i), schema->field(i), nullptr, 0);
            std::vector<TDeserializeChunkedArray::TChunk> chunks = { TDeserializeChunkedArray::TChunk(
                externalInfo.GetRecordsCount(), TStringBuf(originalData.data() + currentIndex, proto.GetKeyColumns(i).GetSize())) };
            columns.emplace_back(std::make_shared<TDeserializeChunkedArray>(externalInfo.GetRecordsCount(), columnLoader, std::move(chunks), true));
            currentIndex += proto.GetKeyColumns(i).GetSize();
        }
        columnKeysContainer = std::make_shared<TGeneralContainer>(schema, std::move(columns));
    }
    TOthersData otherData = TOthersData::BuildEmpty();
    if (proto.GetOtherColumns().size() && proto.GetOtherRecordsCount()) {
        AFL_VERIFY(currentIndex < originalData.size());
        std::shared_ptr<TGeneralContainer> otherKeysContainer =
            BuildOthersContainer(TStringBuf(originalData.data() + currentIndex, headerConclusion->GetOthersSize()), proto, externalInfo, false)
                .DetachResult();
        currentIndex += headerConclusion->GetOthersSize();
        otherData = TOthersData(headerConclusion->GetOtherStats(), otherKeysContainer);
    }
    TColumnsData columnData(headerConclusion->GetColumnStats(), columnKeysContainer);
    auto result = std::make_shared<TSubColumnsArray>(
        std::move(columnData), std::move(otherData), externalInfo.GetColumnType(), externalInfo.GetRecordsCount(), Settings);
    result->StoreSourceString(originalData);
    AFL_VERIFY(currentIndex == originalData.size())("index", currentIndex)("size", originalData.size());
    return result;
}

NKikimrArrowAccessorProto::TConstructor TConstructor::DoSerializeToProto() const {
    NKikimrArrowAccessorProto::TConstructor result;
    *result.MutableSubColumns()->MutableSettings() = Settings.SerializeToProto();
    return result;
}

bool TConstructor::DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& proto) {
    return Settings.DeserializeFromProto(proto.GetSubColumns().GetSettings());
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstruct(
    const std::shared_ptr<IChunkedArray>& originalData, const TChunkConstructionData& /*externalInfo*/) const {
    return NAccessor::TSubColumnsArray::Make(originalData, DataExtractor, Settings).DetachResult();
}

TString TConstructor::DoSerializeToString(const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    const std::shared_ptr<TSubColumnsArray> arr = std::static_pointer_cast<TSubColumnsArray>(columnData);
    return arr->SerializeToString(externalInfo);
}

TConclusion<std::shared_ptr<TGeneralContainer>> TConstructor::BuildOthersContainer(const TStringBuf data,
    const NKikimrArrowAccessorProto::TSubColumnsAccessor& proto, const TChunkConstructionData& externalInfo, const bool deserialize) {
    std::vector<std::shared_ptr<IChunkedArray>> columns;
    AFL_VERIFY(TOthersData::GetSchema()->num_fields() == proto.GetOtherColumns().size())("proto", proto.GetOtherColumns().size())(
                                                           "schema", TOthersData::GetSchema()->num_fields());
    auto schema = TOthersData::GetSchema();
    ui32 currentIndex = 0;
    for (ui32 i = 0; i < (ui32)proto.GetOtherColumns().size(); ++i) {
        std::shared_ptr<TColumnLoader> columnLoader = std::make_shared<TColumnLoader>(
            externalInfo.GetDefaultSerializer(), std::make_shared<NPlain::TConstructor>(), schema->field(i), nullptr, 0);
        if (deserialize) {
            columns.emplace_back(columnLoader->ApplyVerified(
                TString(data.data() + currentIndex, proto.GetOtherColumns(i).GetSize()), proto.GetOtherRecordsCount()));
        } else {
            std::vector<TDeserializeChunkedArray::TChunk> chunks = { TDeserializeChunkedArray::TChunk(
                proto.GetOtherRecordsCount(), TStringBuf(data.data() + currentIndex, proto.GetOtherColumns(i).GetSize())) };
            columns.emplace_back(
                std::make_shared<TDeserializeChunkedArray>(proto.GetOtherRecordsCount(), columnLoader, std::move(chunks), true));
        }
        currentIndex += proto.GetOtherColumns(i).GetSize();
    }
    return std::make_shared<TGeneralContainer>(schema, std::move(columns));
}

TConclusion<std::shared_ptr<TSubColumnsPartialArray>> TConstructor::BuildPartialReader(
    const TString& originalData, const TChunkConstructionData& externalInfo) {
    auto headerConclusion = TSubColumnsHeader::ReadHeader(originalData, externalInfo);
    if (headerConclusion.IsFail()) {
        return headerConclusion;
    }
    return std::make_shared<TSubColumnsPartialArray>(
        headerConclusion.DetachResult(), externalInfo.GetRecordsCount(), externalInfo.GetColumnType());
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
