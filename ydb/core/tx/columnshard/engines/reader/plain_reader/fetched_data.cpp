#include "fetched_data.h"
#include <ydb/core/formats/arrow/simple_arrays_cache.h>
#include <ydb/core/formats/arrow/common/validation.h>

namespace NKikimr::NOlap {

void TFetchedData::SyncTableColumns(const std::vector<std::shared_ptr<arrow::Field>>& fields) {
    for (auto&& i : fields) {
        if (Table->GetColumnByName(i->name())) {
            continue;
        }
        Table = NArrow::TStatusValidator::GetValid(Table->AddColumn(Table->num_columns(), i,
            std::make_shared<arrow::ChunkedArray>(NArrow::TThreadSimpleArraysCache::GetNull(i->type(), Table->num_rows()))));
    }
}

}
