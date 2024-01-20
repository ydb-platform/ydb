#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/formats/arrow/hash/xx_hash.h>
#include <ydb/core/formats/arrow/hash/calcer.h>

namespace NKikimr::NOlap::NIndexes {

void TPortionIndexChunk::DoAddIntoPortion(const TBlobRange& bRange, TPortionInfo& portionInfo) const {
    portionInfo.AddIndex(TIndexChunk(GetEntityId(), bRange));
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
    const TString indexData = Saver->Apply(indexBatch);
    return std::make_shared<TPortionIndexChunk>(indexId, indexData);
}

std::shared_ptr<arrow::RecordBatch> TBloomIndexConstructor::DoBuildIndexImpl(TChunkedBatchReader& reader) const {
    std::vector<bool> flags;
    flags.resize(BitsCount, false);
    for (ui32 i = 0; i < HashesCount; ++i) {
        NArrow::NHash::NXX64::TStreamStringHashCalcer hashCalcer(3 * i);
        for (; reader.IsCorrect(); reader.ReadNext()) {
            hashCalcer.Start();
            for (auto&& i : reader) {
                NArrow::NHash::TXX64::AppendField(i.GetCurrentChunk(), i.GetCurrentRecordIndex(), hashCalcer);
            }
            flags[hashCalcer.Finish() % BitsCount] = true;
        }
    }

    arrow::BooleanBuilder builder;
    auto res = builder.Reserve(flags.size());
    NArrow::TStatusValidator::Validate(builder.AppendValues(flags));
    std::shared_ptr<arrow::BooleanArray> out;
    NArrow::TStatusValidator::Validate(builder.Finish(&out));

    return arrow::RecordBatch::Make(ResultSchema, BitsCount, {out});
}

}   // namespace NKikimr::NOlap::NIndexes