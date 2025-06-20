#include "manager.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

TDuplicateManager::TDuplicateManager(const TSpecialReadContext& context)
    : TActor(&TDuplicateManager::StateMain)
    , Fetcher(context.GetCommonContext(), SelfId()) {
}

void TDuplicateManager::Handle(const NPrivate::TEvFilterConstructionResult::TPtr&) {
    Y_ABORT("unimplemented");
}

void TDuplicateManager::Handle(const NPrivate::TEvDuplicateFilterDataFetched::TPtr& ev) {
    Fetcher.OnFetchingResult(ev);
}

void TDuplicateManager::Handle(const NPrivate::TEvDuplicateSourceCacheResult::TPtr&) {
    Y_ABORT("unimplemented");
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
    filter.Add(true, ev->Get()->GetRecordsCount());
    ev->Get()->GetSubscriber()->OnFilterReady(std::move(filter));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
