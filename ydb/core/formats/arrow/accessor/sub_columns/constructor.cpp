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
    TStringInput si(originalData);
    ui32 protoSize;
    si.Read(&protoSize, sizeof(protoSize));
    ui64 currentIndex = sizeof(protoSize);
    NKikimrArrowAccessorProto::TSubColumnsAccessor proto;
    if (!proto.ParseFromArray(originalData.data() + currentIndex, protoSize)) {
        return TConclusionStatus::Fail("cannot parse proto");
    }
    currentIndex += protoSize;
    TDictStats columnStats = [&]() {
        if (proto.GetColumnStatsSize()) {
            std::shared_ptr<arrow::RecordBatch> rbColumnStats = TStatusValidator::GetValid(externalInfo.GetDefaultSerializer()->Deserialize(
                TString(originalData.data() + currentIndex, proto.GetColumnStatsSize()), TDictStats::GetStatsSchema()));
            return TDictStats(rbColumnStats);
        } else {
            return TDictStats::BuildEmpty();
        }
    }();
    currentIndex += proto.GetColumnStatsSize();
    TDictStats otherStats = [&]() {
        if (proto.GetOtherStatsSize()) {
            std::shared_ptr<arrow::RecordBatch> rbOtherStats = TStatusValidator::GetValid(externalInfo.GetDefaultSerializer()->Deserialize(
                TString(originalData.data() + currentIndex, proto.GetOtherStatsSize()), TDictStats::GetStatsSchema()));
            return TDictStats(rbOtherStats);
        } else {
            return TDictStats::BuildEmpty();
        }
    }();
    currentIndex += proto.GetOtherStatsSize();

    std::shared_ptr<TGeneralContainer> columnKeysContainer;
    {
        std::vector<std::shared_ptr<IChunkedArray>> columns;
        auto schema = columnStats.BuildColumnsSchema();
        AFL_VERIFY(columnStats.GetColumnsCount() == (ui32)proto.GetKeyColumns().size())("schema", columnStats.GetColumnsCount())(
                                                  "proto", proto.GetKeyColumns().size());
        for (ui32 i = 0; i < (ui32)proto.GetKeyColumns().size(); ++i) {
            std::shared_ptr<TColumnLoader> columnLoader = std::make_shared<TColumnLoader>(
                externalInfo.GetDefaultSerializer(), columnStats.GetAccessorConstructor(i), schema->field(i), nullptr, 0);
            std::vector<TDeserializeChunkedArray::TChunk> chunks = { TDeserializeChunkedArray::TChunk(
                externalInfo.GetRecordsCount(), originalData.substr(currentIndex, proto.GetKeyColumns(i).GetSize())) };
            columns.emplace_back(std::make_shared<TDeserializeChunkedArray>(externalInfo.GetRecordsCount(), columnLoader, std::move(chunks), true));
            currentIndex += proto.GetKeyColumns(i).GetSize();
        }
        columnKeysContainer = std::make_shared<TGeneralContainer>(schema, std::move(columns));
    }
    TOthersData otherData = TOthersData::BuildEmpty();
    if (proto.GetOtherColumns().size() && proto.GetOtherRecordsCount()) {
        std::shared_ptr<TGeneralContainer> otherKeysContainer;
        std::vector<std::shared_ptr<IChunkedArray>> columns;
        AFL_VERIFY(TOthersData::GetSchema()->num_fields() == proto.GetOtherColumns().size())("proto", proto.GetOtherColumns().size())(
                                                               "schema", TOthersData::GetSchema()->num_fields());
        auto schema = TOthersData::GetSchema();
        for (ui32 i = 0; i < (ui32)proto.GetOtherColumns().size(); ++i) {
            std::shared_ptr<TColumnLoader> columnLoader = std::make_shared<TColumnLoader>(
                externalInfo.GetDefaultSerializer(), std::make_shared<NPlain::TConstructor>(), schema->field(i), nullptr, 0);
            std::vector<TDeserializeChunkedArray::TChunk> chunks = { TDeserializeChunkedArray::TChunk(
                proto.GetOtherRecordsCount(), originalData.substr(currentIndex, proto.GetOtherColumns(i).GetSize())) };
            columns.emplace_back(std::make_shared<TDeserializeChunkedArray>(proto.GetOtherRecordsCount(), columnLoader, std::move(chunks), true));
            currentIndex += proto.GetOtherColumns(i).GetSize();
        }
        otherKeysContainer = std::make_shared<TGeneralContainer>(schema, std::move(columns));
        otherData = TOthersData(otherStats, otherKeysContainer);
    }
    TColumnsData columnData(columnStats, columnKeysContainer);
    return std::make_shared<TSubColumnsArray>(
        std::move(columnData), std::move(otherData), externalInfo.GetColumnType(), externalInfo.GetRecordsCount(), Settings);
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

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
