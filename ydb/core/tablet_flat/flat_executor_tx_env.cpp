#include "flat_executor_tx_env.h"
#include "flat_database.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    std::unique_ptr<TSeat> TPrivatePageCacheBackend::MakeSeat(ui64 uniqueId, TAutoPtr<ITransaction> transaction) {
        return std::make_unique<TSeat>(uniqueId, std::move(transaction));
    }

    void TPrivatePageCacheBackend::WithTransactionPages(
        NTable::TDatabase& database, TSeat& seat, TFunctionRef<void(ITransactionPages&, TTransactionEnv&)> execute) {
        TPageCollectionTxEnv frame(database, *Cache_, seat);
        execute(frame, frame);
        // The callback may destroy the seat; do not access it afterward
    }

    void TPrivatePageCacheBackend::ReleasePins(TSeat& seat) {
        seat.Pinned.clear();
    }

    void TTransactionEnv::MakeSnapshot(TIntrusivePtr<TTableSnapshotContext> snap)
    {
        auto tables = snap->TablesToSnapshot();
        Y_ENSURE(tables);

        for (ui32 table : tables) {
            auto& entry = MakeSnap[table];
            entry.Context.push_back(snap);
            auto epoch = DB.TxSnapTable(table);
            if (entry.Epoch) {
                Y_ENSURE(*entry.Epoch == epoch, "Table snapshot changed unexpectedly");
            } else {
                entry.Epoch.emplace(epoch);
            }
        }
    }

} // namespace NTabletFlatExecutor
} // namespace NKikimr
