#include "column_engine.h"
#include <ydb/core/base/appdata.h>
#include <util/system/info.h>

namespace NKikimr::NOlap {

const std::shared_ptr<arrow::Schema>& IColumnEngine::GetReplaceKey() const {
    return GetVersionedIndex().GetLastSchema()->GetIndexInfo().GetReplaceKey();
}

ui64 IColumnEngine::GetMetadataLimit() {
    static const auto MemoryTotal = NSystemInfo::TotalMemorySize();
    if (!HasAppData()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("total", MemoryTotal);
        return MemoryTotal * 0.3;
    } else if (AppDataVerified().ColumnShardConfig.GetIndexMetadataMemoryLimit().HasAbsoluteValue()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("value", AppDataVerified().ColumnShardConfig.GetIndexMetadataMemoryLimit().GetAbsoluteValue());
        return AppDataVerified().ColumnShardConfig.GetIndexMetadataMemoryLimit().GetAbsoluteValue();
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("total", MemoryTotal)("kff", AppDataVerified().ColumnShardConfig.GetIndexMetadataMemoryLimit().GetTotalRatio());
        return MemoryTotal * AppDataVerified().ColumnShardConfig.GetIndexMetadataMemoryLimit().GetTotalRatio();
    }
}

}
