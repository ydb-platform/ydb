#include "stats.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NArrow::NSplitter {

std::optional<TBatchSerializationStat> TSerializationStats::GetStatsForRecordBatch(const std::shared_ptr<arrow::Schema>& schema) const {
    std::optional<TBatchSerializationStat> result;
    for (auto&& i : schema->fields()) {
        auto columnInfo = GetColumnInfo(i->name());
        if (!columnInfo || columnInfo->GetRecordsCount() == 0) {
            return {};
        } else if (!result) {
            result = TBatchSerializationStat();
        }
        result->Merge(*columnInfo);
    }
    return result;
}

std::optional<TBatchSerializationStat> TSerializationStats::GetStatsForRecordBatch(const std::shared_ptr<arrow::RecordBatch>& rb) const {
    return GetStatsForRecordBatch(rb->schema());
}

}
