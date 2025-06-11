#include "manager.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

TDuplicateManager::TDuplicateManager(const TSpecialReadContext& /*context*/)
    : TActor(&TDuplicateManager::StateMain) {
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
    filter.Add(true, ev->Get()->GetRecordsCount());
    ev->Get()->GetSubscriber()->OnFilterReady(std::move(filter));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
