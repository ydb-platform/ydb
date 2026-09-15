#include "flat_backup.h"

namespace NKikimr::NTable {

bool TBackupExclusion::HasTable(TTableId tableId) const {
    return ExcludedTableIds.contains(tableId);
}

bool TBackupExclusion::HasColumn(TTableId tableId, TColumnId columnId) const {
    TFullColumnId fullColumnId = {tableId, columnId};
    return ExcludedColumnIds.contains(fullColumnId);
}

void TBackupExclusion::AddTable(TTableId tableId) {
    ExcludedTableIds.insert(tableId);
}

void TBackupExclusion::AddColumn(TTableId tableId, TColumnId columnId) {
    ExcludedColumnIds.insert({tableId, columnId});
}

} // namespace NKikimr::NTable
