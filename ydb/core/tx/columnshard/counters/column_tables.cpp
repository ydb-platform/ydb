#include "column_tables.h"
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NColumnShard {

std::shared_ptr<TSingleColumnTableCounters> TColumnTablesCounters::GetPathIdCounter(const TInternalPathId pathId) {
    auto findCounter = PathIdCounters.FindPtr(pathId);
    if (findCounter) {
        return *findCounter;
    }
    return PathIdCounters.emplace(pathId, std::make_shared<TSingleColumnTableCounters>(*this)).first->second;
}

} // namespace NKikimr::NColumnShard
