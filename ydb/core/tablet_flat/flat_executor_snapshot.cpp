#include "flat_executor_snapshot.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

NTable::TSnapEdge TTableSnapshotContext::Edge(ui32 table) const {
    Y_ABORT_UNLESS(Impl, "Snapshot context is not initialized");
    return Impl->Edge(table);
}

}
}
