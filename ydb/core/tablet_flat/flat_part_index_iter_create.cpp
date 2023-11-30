#include "flat_part_index_iter.h"
#include "flat_part_btree_index_iter.h"

namespace NKikimr::NTable {

THolder<IIndexIter> CreateIndexIter(const TPart* part, IPages* env, NPage::TGroupId groupId)
{
    if (groupId.Index < (groupId.IsHistoric() ? part->IndexPages.BTreeHistoric : part->IndexPages.BTreeGroups).size()) {
        return MakeHolder<TPartBtreeIndexIt>(part, env, groupId);
    } else {
        return MakeHolder<TPartIndexIt>(part, env, groupId);
    }
}

}
