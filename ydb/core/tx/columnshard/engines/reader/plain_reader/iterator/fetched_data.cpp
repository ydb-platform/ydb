#include "fetched_data.h"
#include <ydb/core/formats/arrow/simple_arrays_cache.h>
#include <ydb/core/formats/arrow/common/validation.h>
#include <ydb/core/formats/arrow/common/accessor.h>

namespace NKikimr::NOlap {

void TFetchedData::SyncTableColumns(const std::vector<std::shared_ptr<arrow::Field>>& fields) {
    for (auto&& i : fields) {
        if (Table->GetSchema()->GetFieldByName(i->name())) {
            continue;
        }
        Table->AddField(i, std::make_shared<NArrow::NAccessor::TTrivialArray>(
            NArrow::TThreadSimpleArraysCache::GetNull(i->type(), Table->num_rows()))).Validate();
    }
}

}
