#include "column_engine.h"

#include "portions/portion_info.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/data_accessor/request.h>

#include <util/system/info.h>

namespace NKikimr::NOlap {

const std::shared_ptr<arrow::Schema>& IColumnEngine::GetReplaceKey() const {
    return GetVersionedIndex().GetLastSchema()->GetIndexInfo().GetReplaceKey();
}

ui64 IColumnEngine::GetMetadataLimit() {
    static const ui64 MemoryTotal = NSystemInfo::TotalMemorySize();
    if (!HasAppData()) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_WRITE)("total", MemoryTotal);
        return MemoryTotal * 0.3;
    } else if (AppDataVerified().ColumnShardConfig.GetIndexMetadataMemoryLimit().HasAbsoluteValue()) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_WRITE)(
            "value", AppDataVerified().ColumnShardConfig.GetIndexMetadataMemoryLimit().GetAbsoluteValue());
        return AppDataVerified().ColumnShardConfig.GetIndexMetadataMemoryLimit().GetAbsoluteValue();
    } else {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_WRITE)("total", MemoryTotal)(
            "kff", AppDataVerified().ColumnShardConfig.GetIndexMetadataMemoryLimit().GetTotalRatio());
        return MemoryTotal * AppDataVerified().ColumnShardConfig.GetIndexMetadataMemoryLimit().GetTotalRatio();
    }
}

void IColumnEngine::FetchDataAccessors(const std::shared_ptr<TDataAccessorsRequest>& request) const {
    AFL_VERIFY(!!request);
    AFL_VERIFY(!request->IsEmpty());
    DoFetchDataAccessors(request);
}

TSelectInfo::TStats TSelectInfo::Stats() const {
    TStats out;
    out.Portions = Portions.size();

    THashSet<TUnifiedBlobId> uniqBlob;
    for (auto& portionInfo : Portions) {
        out.Rows += portionInfo->GetRecordsCount();
        out.Bytes += portionInfo->GetTotalBlobBytes();
    }
    return out;
}

TString TSelectInfo::DebugString() const {
    TStringBuilder result;
    result << "count:" << Portions.size() << ";";
    if (Portions.size()) {
        result << "portions:";
        for (auto& portionInfo : Portions) {
            result << portionInfo->DebugString();
        }
    }
    return result;
}

}   // namespace NKikimr::NOlap
