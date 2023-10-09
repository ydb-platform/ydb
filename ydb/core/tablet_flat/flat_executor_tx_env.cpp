#include "flat_executor_tx_env.h"
#include "flat_database.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    void TPageCollectionTxEnv::MakeSnapshot(TIntrusivePtr<TTableSnapshotContext> snap)
    {
        auto tables = snap->TablesToSnapshot();
        Y_ABORT_UNLESS(tables);

        for (ui32 table : tables) {
            auto& entry = MakeSnap[table];
            entry.Context.push_back(snap);
            auto epoch = DB.TxSnapTable(table);
            if (entry.Epoch) {
                Y_ABORT_UNLESS(*entry.Epoch == epoch, "Table snapshot changed unexpectedly");
            } else {
                entry.Epoch.emplace(epoch);
            }
        }
    }

} // namespace NTabletFlatExecutor
} // namespace NKikimr
