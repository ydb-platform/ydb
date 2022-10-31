#include "datashard_user_db.h"

namespace NKikimr::NDataShard {

NTable::EReady TDataShardUserDb::SelectRow(
        const TTableId& tableId,
        TArrayRef<const TRawTypeValue> key,
        TArrayRef<const NTable::TTag> tags,
        NTable::TRowState& row)
{
    auto tid = Self.GetLocalTableId(tableId);
    Y_VERIFY(tid != 0, "Unexpected SelectRow for an unknown table");

    return Db.Select(tid, key, tags, row, /* readFlags */ 0, ReadVersion);
}

} // namespace NKikimr::NDataShard
