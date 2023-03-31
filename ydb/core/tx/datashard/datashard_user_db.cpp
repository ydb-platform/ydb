#include "datashard_user_db.h"

namespace NKikimr::NDataShard {

NTable::EReady TDataShardUserDb::SelectRow(
        const TTableId& tableId,
        TArrayRef<const TRawTypeValue> key,
        TArrayRef<const NTable::TTag> tags,
        NTable::TRowState& row,
        NTable::TSelectStats& stats,
        const TMaybe<TRowVersion>& readVersion)
{
    auto tid = Self.GetLocalTableId(tableId);
    Y_VERIFY(tid != 0, "Unexpected SelectRow for an unknown table");

    return Db.Select(tid, key, tags, row, stats, /* readFlags */ 0, readVersion.GetOrElse(ReadVersion));
}

NTable::EReady TDataShardUserDb::SelectRow(
        const TTableId& tableId,
        TArrayRef<const TRawTypeValue> key,
        TArrayRef<const NTable::TTag> tags,
        NTable::TRowState& row,
        const TMaybe<TRowVersion>& readVersion)
{
    NTable::TSelectStats stats;
    return SelectRow(tableId, key, tags, row, stats, readVersion);
}

} // namespace NKikimr::NDataShard
