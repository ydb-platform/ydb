#include "flat_stat_part_group_iter_iface.h"
#include "flat_stat_part_group_btree_index.h"
#include "flat_part_index_iter.h"

namespace NKikimr::NTable {

THolder<IStatsPartGroupIterator> CreateStatsPartGroupIterator(const TPart* part, IPages* env, NPage::TGroupId groupId)
{
    if (groupId.Index < (groupId.IsHistoric() ? part->IndexPages.BTreeHistoric : part->IndexPages.BTreeGroups).size()) {
        return MakeHolder<TStatsPartGroupBtreeIndexIterator>(part, env, groupId);
    } else {
        return MakeHolder<TPartIndexIt>(part, env, groupId);
    }
}

}
