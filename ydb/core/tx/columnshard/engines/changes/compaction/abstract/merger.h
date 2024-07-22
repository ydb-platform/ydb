#pragma once
#include <ydb/core/tx/columnshard/engines/changes/compaction/common/result.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/common/context.h>

namespace NKikimr::NOlap::NCompaction {
class IColumnMerger {
private:
    bool Started = false;

    virtual std::vector<TColumnPortionResult> DoExecute(
        const NCompaction::TColumnMergeContext& context, const std::shared_ptr<arrow::RecordBatch>& remap) = 0;
    virtual void DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input) = 0;

public:
    static inline const TString PortionIdFieldName = "$$__portion_id";
    static inline const TString PortionRecordIndexFieldName = "$$__portion_record_idx";
    static inline const std::shared_ptr<arrow::Field> PortionIdField =
        std::make_shared<arrow::Field>(PortionIdFieldName, std::make_shared<arrow::UInt16Type>());
    static inline const std::shared_ptr<arrow::Field> PortionRecordIndexField =
        std::make_shared<arrow::Field>(PortionRecordIndexFieldName, std::make_shared<arrow::UInt32Type>());

    virtual ~IColumnMerger() = default;

    void Start(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input) {
        AFL_VERIFY(!Started);
        Started = true;
        return DoStart(input);
    }

    std::vector<TColumnPortionResult> Execute(
        const NCompaction::TColumnMergeContext& context, const std::shared_ptr<arrow::RecordBatch>& remap) {
        return DoExecute(context, remap);
    }
};

}   // namespace NKikimr::NOlap::NCompaction
