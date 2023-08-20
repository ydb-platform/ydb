#pragma once
#include "abstract/mark.h"
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>

namespace NKikimr::NOlap {

class TMarksGranules {
public:
    using TPair = std::pair<TMark, ui64>;

    TMarksGranules() = default;
    TMarksGranules(std::vector<TPair>&& marks) noexcept;
    TMarksGranules(std::vector<TMark>&& points);
    TMarksGranules(const TSelectInfo& selectInfo);

    const std::vector<TPair>& GetOrderedMarks() const noexcept {
        return Marks;
    }

    bool Empty() const noexcept {
        return Marks.empty();
    }

    bool MakePrecedingMark(const TIndexInfo& indexInfo);

    THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch, const TIndexInfo& indexInfo);
    static THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch,
        const std::vector<std::pair<TMark, ui64>>& granules,
        const TIndexInfo& indexInfo);

    static std::shared_ptr<arrow::RecordBatch> GetEffectiveKey(const std::shared_ptr<arrow::RecordBatch>& batch,
        const TIndexInfo& indexInfo);

private:
    std::vector<TPair> Marks;
};

}
