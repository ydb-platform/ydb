#include "accessor.h"
#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/composite_serial/accessor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstructDefault(const TChunkConstructionData& externalInfo) const {
    AFL_VERIFY(externalInfo.GetDefaultValue() == nullptr);
    return std::make_shared<TSubColumnsArray>(externalInfo.GetColumnType(), externalInfo.GetRecordsCount());
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoDeserializeFromString(
    const TString& originalData, const TChunkConstructionData& externalInfo) const {
    TStringInput si(originalData);
    ui32 protoSize;
    si.Read(&protoSize, sizeof(protoSize));
    ui64 currentIndex = sizeof(protoSize);
    NKikimrArrowAccessorProto::TSubColumnsAccessor proto;
    if (!proto.ParseFromArray(originalData.data() + currentIndex, protoSize)) {
        return TConclusionStatus::Fail("cannot parse proto");
    }
    currentIndex += protoSize;
    std::shared_ptr<arrow::RecordBatch> rbColumnStats = TStatusValidator::GetValid(externalInfo.GetDefaultSerializer()->Deserialize(
        TString(originalData.data() + currentIndex, proto.GetColumnStatsSize()), TDictStats::GetSchema()));
    TDictStats columnStats(rbColumnStats);
    currentIndex += proto.GetColumnStatsSize();
    std::shared_ptr<arrow::RecordBatch> rbOtherStats = TStatusValidator::GetValid(externalInfo.GetDefaultSerializer()->Deserialize(
        TString(originalData.data() + currentIndex, proto.GetOtherStatsSize()), TDictStats::GetSchema()));
    TDictStats otherStats(rbOtherStats);
    currentIndex += proto.GetOtherStatsSize();

    std::shared_ptr<TGeneralContainer> columnKeysContainer;
    {
        std::vector<std::shared_ptr<IChunkedArray>> columns;
        auto schema = columnStats.GetSchema();
        AFL_VERIFY(rbColumnStats->num_rows() == proto.GetKeyColumns().size());
        for (ui32 i = 0; i < (ui32)proto.GetKeyColumns().size(); ++i) {
            std::shared_ptr<TColumnLoader> columnLoader = std::make_shared<TColumnLoader>(externalInfo.GetDefaultSerializer(),
                columnStats.GetAccessorConstructor(i, externalInfo.GetRecordsCount()), schema->field(i), nullptr, 0);
            std::vector<TDeserializeChunkedArray::TChunk> chunks = { TDeserializeChunkedArray::TChunk(
                externalInfo.GetRecordsCount(), originalData.substr(currentIndex, proto.GetKeyColumns(i).GetSize())) };
            columns.emplace_back(std::make_shared<TDeserializeChunkedArray>(externalInfo.GetRecordsCount(), columnLoader, std::move(chunks)));
            currentIndex += proto.GetKeyColumns(i).GetSize();
        }
        columnKeysContainer = std::make_shared<TGeneralContainer>(schema, std::move(columns));
    }
    std::shared_ptr<TGeneralContainer> otherKeysContainer;
    {
        std::vector<std::shared_ptr<IChunkedArray>> columns;
        AFL_VERIFY(rbOtherStats->num_rows() == proto.GetOtherColumns().size());
        auto schema = otherStats.GetSchema();
        for (ui32 i = 0; i < (ui32)proto.GetOtherColumns().size(); ++i) {
            std::shared_ptr<TColumnLoader> columnLoader = std::make_shared<TColumnLoader>(externalInfo.GetDefaultSerializer(),
                otherStats.GetAccessorConstructor(i, proto.GetOtherRecordsCount()), schema->field(i), nullptr, 0);
            std::vector<TDeserializeChunkedArray::TChunk> chunks = { TDeserializeChunkedArray::TChunk(
                proto.GetOtherRecordsCount(), originalData.substr(currentIndex, proto.GetOtherColumns(i).GetSize())) };
            columns.emplace_back(std::make_shared<TDeserializeChunkedArray>(proto.GetOtherRecordsCount(), columnLoader, std::move(chunks)));
            currentIndex += proto.GetOtherColumns(i).GetSize();
        }
        otherKeysContainer = std::make_shared<TGeneralContainer>(schema, std::move(columns));
    }
    TColumnsData columnData(columnStats, columnKeysContainer);
    TOthersData otherData(otherStats, otherKeysContainer);
    return std::make_shared<TSubColumnsArray>(
        std::move(columnData), std::move(otherData), externalInfo.GetColumnType(), externalInfo.GetRecordsCount());
}

NKikimrArrowAccessorProto::TConstructor TConstructor::DoSerializeToProto() const {
    NKikimrArrowAccessorProto::TConstructor result;
    *result.MutableSubColumns() = {};
    return result;
}

bool TConstructor::DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& /*proto*/) {
    return true;
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstruct(
    const std::shared_ptr<IChunkedArray>& originalData, const TChunkConstructionData& /*externalInfo*/) const {
    return NAccessor::TSubColumnsArray::Make(originalData, DataExtractor).DetachResult();
}

TString TConstructor::DoSerializeToString(const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    const std::shared_ptr<TSubColumnsArray> arr = std::static_pointer_cast<TSubColumnsArray>(columnData);
    return arr->SerializeToString(externalInfo);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
