#pragma once
#include <ydb/core/formats/arrow/accessor/common/const.h>
#include <ydb/core/formats/arrow/accessor/compact_kv/accessor.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>

namespace NKikimr::NOlap::NCompaction {

// Merger for COMPACT_KV columns. Internally a COMPACT_KV column is just a plain
// binary (JsonDocument) array; the compact representation is produced only at
// (de)serialization time by the accessor constructor. So merging is a straight
// remap of binary values into a single binary array which is then re-serialized
// through the COMPACT_KV constructor (preserving the on-disk format).
class TCompactKVMerger: public IColumnMerger {
private:
    static inline auto Registrator = TFactory::TRegistrator<TCompactKVMerger>(NArrow::NAccessor::TGlobalConst::CompactKVDataAccessorName);
    using TBase = IColumnMerger;
    std::vector<std::shared_ptr<arrow::BinaryArray>> Sources;

    virtual void DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext) override;
    virtual TColumnPortionResult DoExecute(const TChunkMergeContext& context, TMergingContext& mergeContext) override;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NCompaction
