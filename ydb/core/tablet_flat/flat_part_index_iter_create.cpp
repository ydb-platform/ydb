#include "flat_part_index_iter.h"
#include "flat_part_btree_index_iter.h"

namespace NKikimr::NTable {

THolder<IPartGroupIndexIter> CreateIndexIter(const TPart* part, IPages* env, NPage::TGroupId groupId)
{
    if (part->IndexPages.HasBTree()) {
        return MakeHolder<TPartGroupBtreeIndexIterer>(part, env, groupId);
    } else {
        return MakeHolder<TPartGroupFlatIndexIter>(part, env, groupId);
    }
}

}
