#include "meta.h"
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/formats/arrow/hash/xx_hash.h>
#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/formats/arrow/serializer/full.h>

namespace NKikimr::NOlap::NIndexes {

void TPortionIndexChunk::DoAddIntoPortion(const TBlobRange& bRange, TPortionInfo& portionInfo) const {
    portionInfo.AddIndex(TIndexChunk(GetEntityId(), GetChunkIdx(), bRange));
}

std::shared_ptr<NKikimr::NOlap::IPortionDataChunk> TIndexByColumns::DoBuildIndex(const ui32 indexId, std::map<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const TIndexInfo& indexInfo) const {
    std::vector<TChunkedColumnReader> columnReaders;
    for (auto&& i : ColumnIds) {
        auto it = data.find(i);
        AFL_VERIFY(it != data.end());
        columnReaders.emplace_back(it->second, indexInfo.GetColumnLoaderVerified(i));
    }
    TChunkedBatchReader reader(std::move(columnReaders));
    std::shared_ptr<arrow::RecordBatch> indexBatch = DoBuildIndexImpl(reader);
    const TString indexData = TColumnSaver(nullptr, Serializer).Apply(indexBatch);
    return std::make_shared<TPortionIndexChunk>(indexId, indexData);
}

bool TIndexByColumns::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& /*proto*/) {
    Serializer = std::make_shared<NArrow::NSerialization::TFullDataSerializer>(arrow::ipc::IpcWriteOptions::Defaults());
    return true;
}

TIndexByColumns::TIndexByColumns(const ui32 indexId, const std::set<ui32>& columnIds)
    : TBase(indexId)
    , ColumnIds(columnIds)
{
    Serializer = std::make_shared<NArrow::NSerialization::TFullDataSerializer>(arrow::ipc::IpcWriteOptions::Defaults());
}

}   // namespace NKikimr::NOlap::NIndexes