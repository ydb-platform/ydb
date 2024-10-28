#include "column_engine.h"
#include "portions/portion_info.h"
#include <ydb/core/base/appdata.h>
#include <util/system/info.h>

#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NOlap {

const std::shared_ptr<arrow::Schema>& IColumnEngine::GetReplaceKey() const {
    return GetVersionedIndex().GetLastSchema()->GetIndexInfo().GetReplaceKey();
}

ui64 IColumnEngine::GetMetadataLimit() {
    static const ui64 MemoryTotal = NSystemInfo::TotalMemorySize();
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

TSelectInfo::TStats TSelectInfo::Stats() const {
    TStats out;
    out.Portions = PortionsOrderedPK.size();

    THashSet<TUnifiedBlobId> uniqBlob;
    for (auto& portionInfo : PortionsOrderedPK) {
        out.Rows += portionInfo->GetRecordsCount();
        for (auto& blobId : portionInfo->GetBlobIds()) {
            out.Bytes += blobId.BlobSize();
        }
        out.Blobs += portionInfo->GetBlobIdsCount();
    }
    return out;
}

void TSelectInfo::DebugStream(IOutputStream& out) {
    if (PortionsOrderedPK.size()) {
        out << "portions:";
        for (auto& portionInfo : PortionsOrderedPK) {
            out << portionInfo->DebugString();
        }
    }
}

}
