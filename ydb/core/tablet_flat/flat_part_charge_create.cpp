#include "flat_part_charge_create.h"
#include "flat_part_charge_btree_index.h"
#include "flat_part_charge_flat_index.h"

namespace NKikimr::NTable {

std::unique_ptr<ICharge> CreateCharge(IPages *env, const TPart &part, TTagsRef tags, bool includeHistory) {
    if (part.IndexPages.HasBTree()) {
        return std::make_unique<TChargeBTreeIndex>(env, part, tags, includeHistory);
    } else {
        return std::make_unique<TChargeFlatIndex>(env, part, tags, includeHistory);
    }
}

}
