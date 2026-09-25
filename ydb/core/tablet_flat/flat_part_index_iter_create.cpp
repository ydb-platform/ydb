
#include "flat_part_index_iter_bree_index.h"
#include "flat_part_index_iter_flat_index.h"

namespace NKikimr::NTable {

std::unique_ptr<IPartGroupIndexIter> CreateIndexIter(const TPart* part, IPages* env, NPage::TGroupId groupId)
{
    if (part->IndexPages.HasBTree()) {
        return std::make_unique<TPartGroupBtreeIndexIter>(part, env, groupId);
    } else {
        return std::make_unique<TPartGroupFlatIndexIter>(part, env, groupId);
    }
}

}
