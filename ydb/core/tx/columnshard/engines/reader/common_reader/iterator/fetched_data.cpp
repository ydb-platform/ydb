#include "constructor.h"
#include "fetched_data.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>
#include <ydb/library/formats/arrow/validation/validation.h>

namespace NKikimr::NOlap::NReader::NCommon {

void TFetchedData::SyncTableColumns(
    const std::vector<std::shared_ptr<arrow::Field>>& fields, const ISnapshotSchema& schema, const ui32 recordsCount) {
    for (auto&& i : fields) {
        const ui32 id = schema.GetColumnId(i->name());
        if (Table->HasColumn(id)) {
            continue;
        }
        Table->AddVerified(id,
            std::make_shared<NArrow::NAccessor::TTrivialArray>(
                NArrow::TThreadSimpleArraysCache::Get(i->type(), schema.GetExternalDefaultValueVerified(i->name()), recordsCount)),
            true);
    }
}

void TFetchedData::AddFetchers(const std::vector<std::shared_ptr<IKernelFetchLogic>>& fetchers) {
    for (auto&& i : fetchers) {
        AFL_VERIFY(Fetchers.emplace(i->GetEntityId(), i).second);
    }
}

void TFetchedData::AddFetcher(const std::shared_ptr<IKernelFetchLogic>& fetcher) {
    AFL_VERIFY(Fetchers.emplace(fetcher->GetEntityId(), fetcher).second);
}

}   // namespace NKikimr::NOlap::NReader::NCommon
