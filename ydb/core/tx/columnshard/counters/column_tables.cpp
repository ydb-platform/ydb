#include "column_tables.h"

namespace NKikimr::NColumnShard {

TSingleColumnTableCounters& TColumnTablesCounters::GetPathIdCounter(ui64 pathId) {
    return PathIdCounters.try_emplace(pathId, *this).first->second;
}

} // namespace NKikimr::NColumnShard
