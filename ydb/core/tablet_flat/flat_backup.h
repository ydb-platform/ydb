#pragma once

#include <util/generic/hash_set.h>

namespace NKikimr::NTable {

class TBackupExclusion : public TThrRefBase {
public:
    using TTableId = ui32;
    using TColumnId = ui32;
    using TFullColumnId = std::pair<TTableId, TColumnId>;

    TBackupExclusion() = default;

    bool HasTable(TTableId tableId) const;
    bool HasColumn(TTableId tableId, TColumnId columnId) const;
    void AddTable(TTableId tableId);
    void AddColumn(TTableId tableId, TColumnId columnId);

private:
    THashSet<TTableId> ExcludedTableIds;
    THashSet<TFullColumnId> ExcludedColumnIds;
};

} // namespace NKikimr::NTable
