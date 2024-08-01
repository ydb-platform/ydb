#include "column_tables.h"

namespace NKikimr::NColumnShard {

std::shared_ptr<TSingleColumnTableCounters> TColumnTablesCounters::GetPathIdCounter(ui64 pathId) {
    auto findCounter = PathIdCounters.FindPtr(pathId);
    if (findCounter) {
        return *findCounter;
    }
    return PathIdCounters.emplace(pathId, std::make_shared<TSingleColumnTableCounters>(*this)).first->second;
}

} // namespace NKikimr::NColumnShard
