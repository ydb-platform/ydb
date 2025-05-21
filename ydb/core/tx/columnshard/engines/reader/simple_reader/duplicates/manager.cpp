#include "manager.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>

namespace NKikimr::NOlap::NReader::NSimple {

TDuplicateManager::TDuplicateManager(const TSpecialReadContext& /*context*/)
    : TActor(&TDuplicateManager::StateMain) {
}

void TDuplicateManager::Handle(const TEvRequestFilter::TPtr& ev) {
    ev->Get()->GetSubscriber()->OnFilterReady(NArrow::TColumnFilter::BuildAllowFilter());
}

}   // namespace NKikimr::NOlap::NReader::NSimple
