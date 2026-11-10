#pragma once

#include "meta.h"

#include <ydb/core/formats/arrow/reader/stats/stats.h>

#include <util/string/cast.h>

namespace NKikimr::NOlap::NIndexes {

template <class TResolve>
void AddIndexStatsDescriptor(NScanStats::TStep& record, const IIndexMeta& index, const TResolve& resolve) {
    for (const auto& descriptor : record.GetIndexDescriptors()) {
        if (descriptor.HasIndexId() && descriptor.GetIndexId() == index.GetIndexId()) {
            return;
        }
    }
    auto* descriptor = record.AddIndexDescriptors();
    descriptor->SetIndexId(index.GetIndexId());
    if (index.GetIndexName()) {
        descriptor->SetName(index.GetIndexName());
    }
    descriptor->SetType(index.GetClassName());
    for (ui32 columnId : index.GetColumnIdsForStats()) {
        const TString name = resolve(columnId);
        descriptor->AddColumns(name ? name : TString("column_id=") + ToString(columnId));
    }
}

}   // namespace NKikimr::NOlap::NIndexes
