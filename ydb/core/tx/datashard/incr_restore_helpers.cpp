#include "incr_restore_helpers.h"

namespace NKikimr::NDataShard::NIncrRestoreHelpers {

std::optional<TVector<TUpdateOp>> MakeRestoreUpdates(TArrayRef<const TCell> cells, TArrayRef<const TTag> tags, const TMap<ui32, TUserTable::TUserColumn>& columns) {
    Y_ENSURE(cells.size() >= 1);
    TVector<TUpdateOp> updates(::Reserve(cells.size() - 1));

    bool foundSpecialColumn = false;
    Y_ENSURE(cells.size() == tags.size());
    for (TPos pos = 0; pos < cells.size(); ++pos) {
        const auto tag = tags.at(pos);
        auto it = columns.find(tag);
        Y_ENSURE(it != columns.end());
        if (it->second.Name == "__ydb_incrBackupImpl_deleted") {
            if (const auto& cell = cells.at(pos); !cell.IsNull() && cell.AsValue<bool>()) {
                return std::nullopt;
            }
            foundSpecialColumn = true;
            continue;
        }
        updates.emplace_back(tag, ECellOp::Set, TRawTypeValue(cells.at(pos).AsRef(), it->second.Type.GetTypeId()));
    }
    Y_ENSURE(foundSpecialColumn);

    return updates;
}

} // namespace NKikimr::NBackup::NImpl
