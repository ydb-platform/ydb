
#include "flat_part_index_iter_bree_index.h"
#include "flat_part_index_iter_flat_index.h"

namespace NKikimr::NTable {

THolder<IPartGroupIndexIter> CreateIndexIter(const TPart* part, IPages* env, NPage::TGroupId groupId)
{
    if (part->IndexPages.HasBTree()) {
        return MakeHolder<TPartGroupBtreeIndexIter>(part, env, groupId);
    } else {
        return MakeHolder<TPartGroupFlatIndexIter>(part, env, groupId);
    }
}

}
