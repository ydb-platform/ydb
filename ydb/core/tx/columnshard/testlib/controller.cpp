#include "controller.h"
#include <ydb/core/tx/columnshard/engines/reader/order_control/pk_with_limit.h>
#include <ydb/core/tx/columnshard/engines/reader/order_control/default.h>

namespace NKikimr::NYDBTest::NColumnShard {

bool TController::DoOnSortingPolicy(std::shared_ptr<NOlap::NIndexedReader::IOrderPolicy> policy) {
    if (dynamic_cast<const NOlap::NIndexedReader::TPKSortingWithLimit*>(policy.get())) {
        SortingWithLimit.Inc();
    } else if (dynamic_cast<const NOlap::NIndexedReader::TAnySorting*>(policy.get())) {
        AnySorting.Inc();
    } else {
        Y_VERIFY(false);
    }
    return true;
}

bool TController::HasPKSortingOnly() const {
    return SortingWithLimit.Val() && !AnySorting.Val();
}

}
