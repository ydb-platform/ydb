#include "fetched_data.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <ydb/library/formats/arrow/validation/validation.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NCommon {

void TFetchedData::SyncTableColumns(const std::vector<std::shared_ptr<arrow::Field>>& fields, const ISnapshotSchema& schema, const ui32 recordsCount) {
    for (auto&& i : fields) {
        const ui32 id = schema.GetColumnId(i->name());
        if (Table->HasColumn(id)) {
            continue;
        }
        Table->AddVerified(id, std::make_shared<NArrow::NAccessor::TTrivialArray>(NArrow::TThreadSimpleArraysCache::Get(
                                   i->type(), schema.GetExternalDefaultValueVerified(i->name()), recordsCount)), true);
    }
}

}   // namespace NKikimr::NOlap::NReader::NCommon
